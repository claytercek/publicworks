use crate::admission::{Boundary, Position, unanswered};
use crate::{wire::*, *};
use futures_util::future::{Either, select};
use publicworks_runtime::*;
use serde_json::json;
use std::collections::BTreeMap;

mod checkpoint;
mod results;
use checkpoint::*;
use results::*;

pub const TURN_KIND: &str = "agent.turn";
pub const TOOL_KIND: &str = "agent.tool";
#[derive(Clone)]
pub struct Agent(Rc<Installation>);
struct Installation {
    model: Rc<dyn Model>,
    registry: AgentRegistrySnapshot,
    host_default: Option<Vec<String>>,
}
impl Agent {
    /// Compatibility constructor: install tools in a single `default` extension.
    pub fn new(model: impl Model + 'static, mut tools: Vec<Tool>) -> Result<Self, SessionError> {
        // Retain the legacy constructor's alphabetical declaration order.
        tools.sort_by(|a, b| a.declaration().name.cmp(&b.declaration().name));
        let mut registry = AgentRegistry::new();
        registry.install(Extension::new("default", tools))?;
        Ok(Self::with_registry(model, registry.snapshot(), None))
    }

    /// Capture immutable code and host defaults. Conversation policy is read
    /// lazily through the invocation fence, at most once in each resolving phase.
    pub fn with_registry(
        model: impl Model + 'static,
        registry: AgentRegistrySnapshot,
        host_default: Option<Vec<String>>,
    ) -> Self {
        Self(Rc::new(Installation {
            model: Rc::new(model),
            registry,
            host_default,
        }))
    }
    pub fn registry_snapshot(&self) -> AgentRegistrySnapshot {
        self.0.registry.clone()
    }
    pub fn with_snapshot(&self, registry: AgentRegistrySnapshot) -> Self {
        Self(Rc::new(Installation {
            model: self.0.model.clone(),
            registry,
            host_default: self.0.host_default.clone(),
        }))
    }

    /// Project one captured agent snapshot and all other host definitions into a
    /// single Harness publication/wakeup. This replaces the whole runtime map;
    /// callers must include their non-agent definitions. No callbacks run here.
    /// Old definitions retain the old Agent. Failure leaves Harness unchanged.
    pub fn publish_snapshot(
        &self,
        harness: &Harness,
        snapshot: AgentRegistrySnapshot,
        other_definitions: impl IntoIterator<Item = TaskDefinition>,
    ) -> Result<Self, HarnessError> {
        let agent = self.with_snapshot(snapshot);
        let registry = TaskRegistry::new(agent.definitions().into_iter().chain(other_definitions))
            .map_err(|e| HarnessError::Session(invalid(e.to_string())))?;
        harness.replace_registry(registry)?;
        Ok(agent)
    }
    async fn resolve(&self, runtime: &TaskRuntime) -> Result<ResolvedExtensions, SessionError> {
        let config = runtime
            .read(|tx, task| {
                Box::pin(async move {
                    let state = tx.conversation_state(task.conversation_id).await?;
                    ExtensionConfig::decode(state.as_ref().and_then(|s| s.agent_config.as_ref()))
                })
            })
            .await?
            .value;
        Ok(self
            .0
            .registry
            .resolve(&config.selection, self.0.host_default.as_deref()))
    }
    pub fn definitions(&self) -> [TaskDefinition; 2] {
        [self.turn_definition(), self.tool_definition()]
    }
    pub(crate) fn turn_input(&self, config: TurnConfig) -> Result<Value, SessionError> {
        validate_config(&config)?;
        Ok(
            json!({"config":{"model":config.model,"instructions":config.instructions,"maxModelRounds":config.max_model_rounds}}),
        )
    }
    pub(crate) fn turn_definition(&self) -> TaskDefinition {
        // Runtime selects the durable phase; one typed decode dispatches it here.
        let agent = self.clone();
        let handler: PhaseHandler = Rc::new(move |record, runtime| {
            let agent = agent.clone();
            Box::pin(async move { agent.turn(record, runtime).await.map_err(fault) })
        });
        let phases = ["prepare", "request", "tools"]
            .into_iter()
            .map(|name| (name.to_owned(), handler.clone()))
            .collect();
        let agent = self.clone();
        TaskDefinition::new(
            TURN_KIND,
            TURN_VERSION,
            |input| {
                decode_input(input)?;
                TurnCheckpoint::Prepare { round: 0 }.encode()
            },
            phases,
        )
        .with_abort_handler(Rc::new(move |record, runtime| {
            let agent = agent.clone();
            Box::pin(async move { agent.abort_turn(record, runtime).await.map_err(fault) })
        }))
    }
    fn tool_definition(&self) -> TaskDefinition {
        let agent = self.clone();
        let handler: PhaseHandler = Rc::new(move |record, runtime| {
            let agent = agent.clone();
            Box::pin(async move { agent.tool(record, runtime).await.map_err(fault) })
        });
        let phases = ["call", "execute"]
            .into_iter()
            .map(|name| (name.to_owned(), handler.clone()))
            .collect();
        TaskDefinition::new(
            TOOL_KIND,
            TOOL_VERSION,
            |input| {
                ToolInput::decode(input)?;
                ToolCheckpoint::Call.encode()
            },
            phases,
        )
        .with_abort_handler(Rc::new(|record, runtime| {
            Box::pin(async move {
                let intent =
                    ToolCheckpoint::decode(checkpoint(&record).map_err(fault)?).map_err(fault)?;
                let (input, call) = runtime
                    .read(|tx, task| Box::pin(async move { tool_abort_context(tx, &task).await }))
                    .await
                    .map_err(fault)?
                    .value;
                let content = match intent {
                    ToolCheckpoint::Execute { .. } => {
                        "Tool aborted; the operation may have partially or fully run."
                    }
                    ToolCheckpoint::Call => "Tool aborted before execution.",
                };
                finish_tool(
                    &runtime,
                    &input,
                    &call.id,
                    ToolResult {
                        content: content.into(),
                        is_error: true,
                        usage: None,
                    },
                    Some("aborted"),
                    End::Abort,
                )
                .await
                .map_err(fault)
            })
        }))
    }
    async fn turn(&self, record: TaskRecord, runtime: TaskRuntime) -> Result<(), SessionError> {
        let (config, state) = TurnCheckpoint::decode(&record)?;
        match state {
            TurnCheckpoint::Prepare { round } => self.prepare(runtime, config, round).await,
            TurnCheckpoint::Request { round, request, .. } => {
                self.request(runtime, request, round).await
            }
            TurnCheckpoint::Tools(batch) => self.tools(runtime, batch).await,
        }
    }
    async fn prepare(
        &self,
        runtime: TaskRuntime,
        config: TurnConfig,
        round: u64,
    ) -> Result<(), SessionError> {
        if round >= config.max_model_rounds {
            return fail(&runtime, "Maximum model rounds exhausted").await;
        }
        let resolved = self.resolve(&runtime).await?;
        let tools = resolved
            .tools()
            .iter()
            .map(|t| t.declaration().clone())
            .collect();
        let projection = runtime
            .read(|tx, task| {
                Box::pin(async move { project_context(tx, task.conversation_id, None).await })
            })
            .await?
            .value;
        let cutoff = projection
            .cutoff
            .ok_or_else(|| invalid("Agent request requires a visible cutoff"))?;
        let checkpoint = TurnCheckpoint::Request {
            round: round + 1,
            cutoff,
            request: ModelRequest {
                model: config.model,
                instructions: config.instructions,
                tools,
                messages: projection.messages,
            },
        }
        .encode()?;
        runtime
            .commit(move |_, _| {
                Box::pin(async move { Ok(Some(TaskUpdate::Checkpoint(checkpoint))) })
            })
            .await?;
        Ok(())
    }
    async fn request(
        &self,
        runtime: TaskRuntime,
        request: ModelRequest,
        round: u64,
    ) -> Result<(), SessionError> {
        if runtime.is_cancelled() {
            return Ok(());
        }
        let resolved = self.resolve(&runtime).await?;
        let request = resolved.before_request(&runtime, request).await?;
        // Hook replacements obey the provider-neutral contract before dispatch.
        validate_request(&request)?;
        if runtime.is_cancelled() {
            return Ok(());
        }
        let offers = request
            .tools
            .iter()
            .map(|t| (t.name.clone(), t.version))
            .collect();
        let future = self
            .0
            .model
            .complete(request, Cancellation(runtime.clone()));
        let response = match select(future, Box::pin(runtime.cancelled())).await {
            Either::Left((result, _)) => result,
            Either::Right(_) => return Ok(()),
        };
        if runtime.is_cancelled() {
            return Ok(());
        }
        let response = match response {
            Err(error) => return self.model_failure(&runtime, error).await,
            Ok(response) => response,
        };
        if let Err(error) = validate_response(&response) {
            return self
                .model_failure(
                    &runtime,
                    ModelError {
                        message: format!("Malformed model response: {error}"),
                        partial_response: Some(response),
                        usage: None,
                    },
                )
                .await;
        }
        resolved.after_response(&runtime, response.clone()).await?;
        let agent = self.clone();
        runtime
            .commit(move |tx, task| {
                Box::pin(async move {
                    let mut boundary = Boundary::read(tx, task.conversation_id).await?;
                    boundary.require_run(task.id)?;
                    let final_answer = response.tool_calls.is_empty();
                    let entry = tx
                        .append_entry(
                            task.conversation_id,
                            message_draft(
                                "agent.assistant",
                                ModelMessage::Assistant {
                                    text: response.text,
                                    tool_calls: response.tool_calls,
                                },
                                Some(response_data("completed", response.usage)),
                            ),
                        )
                        .await?;
                    if final_answer {
                        boundary
                            .settle(tx, SubmissionSettlement::Done { answer: entry.id })
                            .await?;
                        agent
                            .place_boundary(tx, &mut boundary, Position::Final)
                            .await?;
                        boundary.save(tx).await?;
                        Ok(Some(TaskUpdate::Complete(
                            json!({"answerEntryId":entry.id.get()}),
                        )))
                    } else {
                        let child = agent.create_child(tx, task.id, entry.id, 0).await?;
                        Ok(Some(TaskUpdate::Wait {
                            checkpoint: TurnCheckpoint::Tools(ToolBatch {
                                round,
                                assistant: entry.id,
                                index: 0,
                                child,
                                offers,
                            })
                            .encode()?,
                            on: vec![child],
                            policy: JoinPolicy::AllSettled,
                        }))
                    }
                })
            })
            .await?;
        Ok(())
    }
    async fn tools(&self, runtime: TaskRuntime, batch: ToolBatch) -> Result<(), SessionError> {
        let observed = batch.clone();
        let (calls, receipt) = runtime
            .read(move |tx, task| {
                Box::pin(async move {
                    let calls = assistant_calls(tx, &task, observed.assistant).await?;
                    let call = calls
                        .get(observed.index)
                        .ok_or_else(|| invalid("Invalid tool index"))?;
                    let receipt = child_receipt(tx, &task, &observed, call).await?;
                    Ok((calls, receipt))
                })
            })
            .await?
            .value;
        if batch.index + 1 == calls.len() {
            let resolved = self.resolve(&runtime).await?;
            let assistant = batch.assistant;
            let recorded = runtime
                .read(move |tx, task| {
                    Box::pin(
                        async move { collect_results(tx, task.conversation_id, assistant).await },
                    )
                })
                .await?
                .value;
            let outcomes = calls
                .iter()
                .map(|call| ToolOutcome {
                    call: call.clone(),
                    result: recorded
                        .get(&call.id)
                        .map(|r| r.result.clone())
                        .unwrap_or_else(unavailable),
                })
                .collect();
            resolved.after_tools(&runtime, outcomes).await?;
        }
        let agent = self.clone();
        runtime
            .commit(move |tx, task| {
                Box::pin(async move {
                    let call = &calls[batch.index];
                    let mut boundary = Boundary::read(tx, task.conversation_id).await?;
                    boundary.require_run(task.id)?;
                    // Ordinary child completion uses its validated receipt, with no scan.
                    // Fault/orphan recovery consults raw results before adding a fallback.
                    if receipt.is_none()
                        && !collect_results(tx, task.conversation_id, batch.assistant)
                            .await?
                            .contains_key(&call.id)
                    {
                        append_result(
                            tx,
                            task.conversation_id,
                            batch.assistant,
                            &call.id,
                            unavailable(),
                            Some("missing_result"),
                        )
                        .await?;
                    }
                    if batch.index + 1 < calls.len() {
                        let index = batch.index + 1;
                        let child = agent
                            .create_child(tx, task.id, batch.assistant, index)
                            .await?;
                        Ok(Some(TaskUpdate::Wait {
                            checkpoint: TurnCheckpoint::Tools(ToolBatch {
                                index,
                                child,
                                ..batch
                            })
                            .encode()?,
                            on: vec![child],
                            policy: JoinPolicy::AllSettled,
                        }))
                    } else {
                        let reset = agent
                            .place_boundary(tx, &mut boundary, Position::PostTools)
                            .await?;
                        boundary.save(tx).await?;
                        Ok(Some(if reset {
                            TaskUpdate::Complete(json!({"reset":true}))
                        } else {
                            TaskUpdate::Checkpoint(
                                TurnCheckpoint::Prepare { round: batch.round }.encode()?,
                            )
                        }))
                    }
                })
            })
            .await?;
        Ok(())
    }
    async fn create_child(
        &self,
        tx: &Tx,
        root: Id,
        assistant: Id,
        index: usize,
    ) -> Result<Id, SessionError> {
        Ok(tx
            .create_task(
                self.tool_definition(),
                ToolInput { assistant, index }.encode(),
                TaskOptions {
                    ownership: TaskOwnership::Task(root),
                    conversation_id: None,
                    background: false,
                },
            )
            .await?
            .id)
    }
    async fn model_failure(
        &self,
        runtime: &TaskRuntime,
        error: ModelError,
    ) -> Result<(), SessionError> {
        runtime.commit(move |tx, task| Box::pin(async move {
            let mut boundary = Boundary::read(tx, task.conversation_id).await?;
            boundary.require_run(task.id)?;
            let mut data = json!({"message": error.message});
            // Usage is independent evidence. Explicit null is present, not a
            // fallback request; malformed partial calls must not discard it.
            let usage = error.usage.or_else(|| error.partial_response.as_ref().and_then(|partial| partial.usage.clone()));
            if let Some(usage) = usage {
                data["usage"] = usage;
                if validate_entry(None, Some(&data)).is_err() {
                    data.as_object_mut().expect("diagnostic object").remove("usage");
                    data["usageOmitted"] = json!("invalid JSON payload");
                }
            }
            if let Some(partial) = error.partial_response {
                data["partialResponse"] = json!({"message": encode_message(&ModelMessage::Assistant {
                    text: partial.text, tool_calls: partial.tool_calls,
                })});
                if validate_entry(None, Some(&data)).is_err() {
                    data.as_object_mut().expect("diagnostic object").remove("partialResponse");
                    data["partialResponseOmitted"] = json!("invalid JSON payload");
                } else if let Some(usage) = partial.usage {
                    data["partialResponse"]["usage"] = usage;
                    if validate_entry(None, Some(&data)).is_err() {
                        data["partialResponse"].as_object_mut().expect("partial object").remove("usage");
                        data["partialResponse"]["usageOmitted"] = json!("invalid JSON payload");
                    }
                }
            }
            let mut draft = EntryDraft::new("agent.diagnostic");
            draft.data = Some(data);
            tx.append_entry(task.conversation_id, draft).await?;
            boundary.settle(tx, unanswered("model_error")).await?;
            boundary.save(tx).await?;
            Ok(Some(TaskUpdate::Fail(TaskOutcomeError { message: error.message, detail: None }, None)))
        })).await?;
        Ok(())
    }
    async fn tool(&self, record: TaskRecord, runtime: TaskRuntime) -> Result<(), SessionError> {
        let intent = ToolCheckpoint::decode(checkpoint(&record)?)?;
        let (input, original, offered) = runtime
            .read(|tx, task| Box::pin(async move { tool_context(tx, &task).await }))
            .await?
            .value;
        if runtime.is_cancelled() {
            return Ok(());
        }
        // Never resolve even a validator for a name absent from the pinned offer.
        let resolved = if offered.is_some() {
            Some(self.resolve(&runtime).await?)
        } else {
            None
        };
        if runtime.is_cancelled() {
            return Ok(());
        }
        let tool = resolved.as_ref().and_then(|r| {
            r.tools()
                .iter()
                .find(|t| t.declaration().name == original.name)
        });
        match intent {
            ToolCheckpoint::Execute { arguments, policy } => {
                if offered.is_none() {
                    return Err(invalid("Execute intent for unoffered tool"));
                }
                let call = ToolCall {
                    arguments,
                    ..original
                };
                // Durable effective arguments/policy are authoritative. Do not
                // rerun validation or beforeTool after the intent commit.
                if let Some(tool) = tool
                    && policy == ReplayPolicy::Safe
                    && tool.replay_policy == ReplayPolicy::Safe
                    && Some(tool.declaration.version) == offered
                {
                    return execute_tool(
                        &runtime,
                        &input,
                        resolved.as_ref().expect("resolved tool"),
                        tool,
                        call,
                    )
                    .await;
                }
                finish_tool(&runtime, &input, &call.id, ToolResult {
                    content: format!("interrupted_effect: tool '{}' may have partially or fully run; its result was not durably recorded. It was not replayed.", call.name),
                    is_error: true, usage: None,
                }, Some("interrupted_effect"), End::Fail("interrupted_effect".into())).await
            }
            ToolCheckpoint::Call => {
                self.call(&runtime, &input, original, offered, resolved.as_ref(), tool)
                    .await
            }
        }
    }
    async fn call(
        &self,
        runtime: &TaskRuntime,
        input: &ToolInput,
        call: ToolCall,
        offered: Option<u64>,
        resolved: Option<&ResolvedExtensions>,
        tool: Option<&Tool>,
    ) -> Result<(), SessionError> {
        let rejection = match (offered, tool) {
            (None, _) => Some("Tool was not offered".to_owned()),
            (_, None) => Some("Tool is not installed".to_owned()),
            (Some(version), Some(tool)) if version != tool.declaration.version => {
                Some("Tool version mismatch".to_owned())
            }
            (_, Some(tool)) => (tool.validate)(&call.arguments).err(),
        };
        if let Some(message) = rejection {
            return finish_tool(
                runtime,
                input,
                &call.id,
                ToolResult {
                    content: message,
                    is_error: true,
                    usage: None,
                },
                Some("invalid_tool_call"),
                End::Complete,
            )
            .await;
        }
        let tool = tool.expect("validated installed tool");
        let resolved = resolved.expect("offered tool resolution");
        let (call, block) = resolved.before_tool(runtime, call).await?;
        // Rewrites run before intent; the first block cannot be overridden.
        let rejection = block.map(|message| (message, "tool_blocked")).or_else(|| {
            if !resolved
                .extensions()
                .iter()
                .any(|e| e.hooks().before_tool.is_some())
            {
                return None;
            }
            validate_call(&call)
                .map_err(|e| e.to_string())
                .and_then(|()| (tool.validate)(&call.arguments))
                .err()
                .map(|message| (message, "invalid_tool_call"))
        });
        if let Some((message, code)) = rejection {
            return finish_tool(
                runtime,
                input,
                &call.id,
                ToolResult {
                    content: message,
                    is_error: true,
                    usage: None,
                },
                Some(code),
                End::Complete,
            )
            .await;
        }
        if runtime.is_cancelled() {
            return Ok(());
        }
        let intent = ToolCheckpoint::Execute {
            arguments: call.arguments.clone(),
            policy: tool.replay_policy,
        }
        .encode()?;
        runtime
            .commit(move |_, _| Box::pin(async move { Ok(Some(TaskUpdate::Checkpoint(intent))) }))
            .await?;
        execute_tool(runtime, input, resolved, tool, call).await
    }
    async fn abort_turn(
        &self,
        record: TaskRecord,
        runtime: TaskRuntime,
    ) -> Result<(), SessionError> {
        let (_, state) = TurnCheckpoint::decode(&record)?;
        runtime.commit(move |tx, task| Box::pin(async move {
            let mut boundary = Boundary::read(tx, task.conversation_id).await?;
            boundary.require_run(task.id)?;
            if let TurnCheckpoint::Tools(batch) = state {
                let calls = assistant_calls(tx, &task, batch.assistant).await?;
                let recorded = collect_results(tx, task.conversation_id, batch.assistant).await?;
                for call in calls {
                    if !recorded.contains_key(&call.id) {
                        append_result(tx, task.conversation_id, batch.assistant, &call.id, ToolResult {
                            content: "Turn aborted; tool result unavailable. Started operations may have run.".into(), is_error: true, usage: None,
                        }, Some("aborted")).await?;
                    }
                }
            }
            boundary.settle(tx, unanswered("aborted")).await?;
            boundary.save(tx).await?;
            Ok(Some(TaskUpdate::Abort { reason: Some("Agent turn aborted".into()), result: None }))
        })).await?;
        Ok(())
    }
}

// No durable partial progress channel exists yet. Only final results publish.
async fn execute_tool(
    runtime: &TaskRuntime,
    input: &ToolInput,
    resolved: &ResolvedExtensions,
    tool: &Tool,
    call: ToolCall,
) -> Result<(), SessionError> {
    // The checkpoint ACK is not a cancellation barrier.
    if runtime.is_cancelled() {
        return Ok(());
    }
    let future = (tool.execute)(call.clone(), Cancellation(runtime.clone()));
    let result = match select(future, Box::pin(runtime.cancelled())).await {
        Either::Left((result, _)) => result,
        Either::Right(_) => return Ok(()),
    };
    if runtime.is_cancelled() {
        return Ok(());
    }
    let (result, code, end) = match result {
        Ok(result) => (result, None, End::Complete),
        Err(error) => (
            ToolResult {
                content: error.message.clone(),
                is_error: true,
                usage: error.usage,
            },
            Some("tool_error"),
            End::Fail(error.message),
        ),
    };
    let call_id = call.id.clone();
    let result = resolved
        .after_tool(runtime, ToolOutcome { call, result })
        .await?;
    finish_tool(runtime, input, &call_id, result, code, end).await
}
fn validate_config(config: &TurnConfig) -> Result<(), SessionError> {
    if config.model.is_empty() || config.max_model_rounds == 0 {
        return Err(invalid(
            "Model identity and positive max_model_rounds required",
        ));
    }
    Ok(())
}
pub(crate) fn decode_input(input: &Value) -> Result<TurnConfig, SessionError> {
    let cfg = input
        .get("config")
        .ok_or_else(|| invalid("Missing config"))?;
    let config = TurnConfig {
        model: string(cfg, "model")?,
        instructions: string(cfg, "instructions")?,
        max_model_rounds: number(cfg, "maxModelRounds")?,
    };
    validate_config(&config)?;
    validate_entry(None, Some(input))?;
    Ok(config)
}
fn validate_request(request: &ModelRequest) -> Result<(), SessionError> {
    if request.model.is_empty() {
        return Err(invalid("Model identity required"));
    }
    validate_declarations(&request.tools, 2)?;
    validate_messages(&request.messages)
}
fn validate_response(response: &ModelResponse) -> Result<(), SessionError> {
    validate_calls(&response.tool_calls)?;
    if let Some(usage) = &response.usage {
        publicworks_runtime::validate_native_json_value(usage, 1)?;
    }
    Ok(())
}
fn response_data(status: &str, usage: Option<Value>) -> Value {
    let mut data = json!({"status":status});
    if let Some(usage) = usage {
        data["usage"] = usage;
    }
    data
}
pub(crate) fn message_draft(kind: &str, message: ModelMessage, data: Option<Value>) -> EntryDraft {
    let mut draft = EntryDraft::new(kind);
    draft.model = Some(vec![encode_message(&message)]);
    draft.data = data;
    draft
}
async fn fail(runtime: &TaskRuntime, message: &str) -> Result<(), SessionError> {
    let message = message.to_owned();
    runtime
        .commit(move |tx, task| {
            Box::pin(async move {
                let mut boundary = Boundary::read(tx, task.conversation_id).await?;
                boundary.require_run(task.id)?;
                boundary.settle(tx, unanswered("model_error")).await?;
                boundary.save(tx).await?;
                Ok(Some(TaskUpdate::Fail(
                    TaskOutcomeError {
                        message,
                        detail: None,
                    },
                    None,
                )))
            })
        })
        .await?;
    Ok(())
}
fn fault(error: SessionError) -> TaskOutcomeError {
    TaskOutcomeError {
        message: error.to_string(),
        detail: None,
    }
}

#[cfg(test)]
mod tests;
