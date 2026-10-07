use crate::admission::{Boundary, Position, unanswered};
use crate::{wire::*, *};
use futures_util::future::{Either, select};
use publicworks_runtime::*;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

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
        let input = json!({"config":{"model":config.model,"instructions":config.instructions,"maxModelRounds":config.max_model_rounds}});
        decode_input(&input)?;
        Ok(input)
    }
    pub(crate) fn turn_definition(&self) -> TaskDefinition {
        let mut phases = BTreeMap::new();
        for phase in ["prepare", "request", "tools"] {
            let agent = self.clone();
            phases.insert(
                phase.to_owned(),
                Rc::new(move |record, runtime| {
                    let agent = agent.clone();
                    Box::pin(async move { agent.turn(record, runtime, phase).await.map_err(fault) })
                        as PhaseFuture
                }) as PhaseHandler,
            );
        }
        let agent = self.clone();
        TaskDefinition::new(
            TURN_KIND,
            2,
            |input| {
                decode_input(input)?;
                Ok(json!({"phase":"prepare","round":0}))
            },
            phases,
        )
        .with_abort_handler(Rc::new(move |record, runtime| {
            let agent = agent.clone();
            Box::pin(async move { agent.abort_turn(record, runtime).await.map_err(fault) })
        }))
    }
    fn tool_definition(&self) -> TaskDefinition {
        let mut phases = BTreeMap::new();
        for phase in ["call", "execute"] {
            let agent = self.clone();
            phases.insert(
                phase.to_owned(),
                Rc::new(move |record, runtime| {
                    let agent = agent.clone();
                    Box::pin(async move { agent.tool(record, runtime, phase).await.map_err(fault) })
                        as PhaseFuture
                }) as PhaseHandler,
            );
        }
        TaskDefinition::new(
            TOOL_KIND,
            3,
            |input| {
                decode_tool_input(input)?;
                Ok(json!({"phase":"call"}))
            },
            phases,
        )
        .with_abort_handler(Rc::new(|record, runtime| {
            Box::pin(async move {
                let input = decode_tool_input(&record.input).map_err(fault)?;
                let intent = decode_tool_checkpoint(checkpoint(&record).map_err(fault)?, &input)
                    .map_err(fault)?;
                let content = if intent.is_some() {
                    "Tool aborted; the operation may have partially or fully run."
                } else {
                    "Tool aborted before execution."
                };
                finish_tool(
                    &runtime,
                    &input,
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
    async fn turn(
        &self,
        record: TaskRecord,
        runtime: TaskRuntime,
        phase: &str,
    ) -> Result<(), SessionError> {
        if record.owner.is_some() || record.background {
            return fail(&runtime, "Unsupported agent turn scope").await;
        }
        let config = decode_input(&record.input)?;
        let cp = checkpoint(&record)?.clone();
        let round = number(&cp, "round")?;
        match phase {
            "prepare" => {
                if round >= config.max_model_rounds {
                    return fail(&runtime, "Maximum model rounds exhausted").await;
                }
                let resolved = self.resolve(&runtime).await?;
                let offers: Vec<_> = resolved
                    .tools()
                    .iter()
                    .map(|t| t.declaration().clone())
                    .collect();
                let projection = runtime
                    .read(|tx, task| {
                        Box::pin(
                            async move { project_context(tx, task.conversation_id, None).await },
                        )
                    })
                    .await?
                    .value;
                let cutoff = projection
                    .cutoff
                    .ok_or_else(|| invalid("Agent request requires a visible cutoff"))?;
                let request = json!({"phase":"request","round":round+1,"cutoff":cutoff.get(),
                    "model":config.model,"instructions":config.instructions,"tools":offers.iter().map(declaration).collect::<Vec<_>>(),
                    "messages":projection.messages.iter().map(encode_message).collect::<Vec<_>>()});
                runtime
                    .commit(move |_, _| {
                        Box::pin(async move { Ok(Some(TaskUpdate::Checkpoint(request))) })
                    })
                    .await?;
                Ok(())
            }
            "request" => {
                if round == 0 || round > config.max_model_rounds {
                    return Err(invalid("Invalid model round"));
                }
                // Read through the invocation-bound fence even though the request is self-contained.
                let request = runtime
                    .read(|_, task| Box::pin(async move { decode_request(checkpoint(&task)?) }))
                    .await?
                    .value;
                if request.model != config.model || request.instructions != config.instructions {
                    return Err(invalid("Request differs from pinned turn configuration"));
                }
                if runtime.is_cancelled() {
                    return Ok(());
                }
                let resolved = self.resolve(&runtime).await?;
                let request = resolved.before_request(&runtime, request).await?;
                // Replacements remain provider-neutral but must obey the same
                // wire/JSON contract before being offered to a model.
                validate_request(&request)?;
                if runtime.is_cancelled() {
                    return Ok(());
                }
                let offers = request.tools.clone();
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
                match response {
                    Err(error) => self.model_failure(&runtime, error).await,
                    Ok(response) => {
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
                        let ModelMessage::Assistant { ref tool_calls, .. } = response.message
                        else {
                            unreachable!()
                        };
                        if tool_calls.is_empty() {
                            let agent = self.clone();
                            runtime
                                .commit(move |tx, task| {
                                    Box::pin(async move {
                                        let mut boundary =
                                            Boundary::read(tx, task.conversation_id).await?;
                                        boundary.require_run(task.id)?;
                                        let entry = tx
                                            .append_entry(
                                                task.conversation_id,
                                                message_draft(
                                                    "agent.assistant",
                                                    response.message,
                                                    Some(response_data(
                                                        "completed",
                                                        response.usage,
                                                    )),
                                                ),
                                            )
                                            .await?;
                                        boundary
                                            .settle(
                                                tx,
                                                SubmissionSettlement::Done { answer: entry.id },
                                            )
                                            .await?;
                                        agent
                                            .place_boundary(tx, &mut boundary, Position::Final)
                                            .await?;
                                        boundary.save(tx).await?;
                                        Ok(Some(TaskUpdate::Complete(
                                            json!({"answerEntryId":entry.id.get()}),
                                        )))
                                    })
                                })
                                .await?;
                        } else {
                            let calls = tool_calls.clone();
                            let agent = self.clone();
                            runtime
                                .commit(move |tx, task| {
                                    Box::pin(async move {
                                        let entry = tx
                                            .append_entry(
                                                task.conversation_id,
                                                message_draft(
                                                    "agent.assistant",
                                                    response.message,
                                                    Some(response_data(
                                                        "completed",
                                                        response.usage,
                                                    )),
                                                ),
                                            )
                                            .await?;
                                        let child = agent
                                            .create_child(tx, task.id, entry.id, &calls[0], &offers)
                                            .await?;
                                        Ok(Some(TaskUpdate::Wait {
                                            checkpoint: tools_checkpoint(
                                                round, entry.id, &calls, 0, child, &offers,
                                            ),
                                            on: vec![child],
                                            policy: JoinPolicy::AllSettled,
                                        }))
                                    })
                                })
                                .await?;
                        }
                        Ok(())
                    }
                }
            }
            "tools" => {
                let offers = checkpoint_offers(&cp)?;
                let assistant = id(&cp, "assistantEntryId")?;
                let child = id(&cp, "child")?;
                let index = usize::try_from(number(&cp, "index")?)
                    .map_err(|_| invalid("Invalid tool index"))?;
                let calls = decode_calls(&cp)?;
                let call = calls
                    .get(index)
                    .ok_or_else(|| invalid("Invalid tool index"))?
                    .clone();
                let mut known_current_result = false;
                if index + 1 == calls.len() {
                    let resolved = self.resolve(&runtime).await?;
                    let observed_calls = calls.clone();
                    let (outcomes, recorded) = runtime
                        .read(move |tx, task| {
                            Box::pin(async move {
                                let settled = tx
                                    .task(child)
                                    .await?
                                    .ok_or_else(|| invalid("Missing tool child"))?;
                                if settled.status() != TaskStatus::Terminal
                                    || settled.owner != Some(task.id)
                                {
                                    return Err(invalid("Tool child not settled or wrong owner"));
                                }
                                tool_outcomes(tx, task.conversation_id, assistant, observed_calls)
                                    .await
                            })
                        })
                        .await?
                        .value;
                    known_current_result = recorded.contains(&call.id);
                    resolved.after_tools(&runtime, outcomes).await?;
                }
                let agent = self.clone();
                runtime
                    .commit(move |tx, task| {
                        Box::pin(async move {
                            let settled = tx
                                .task(child)
                                .await?
                                .ok_or_else(|| invalid("Missing tool child"))?;
                            if settled.status() != TaskStatus::Terminal
                                || settled.owner != Some(task.id)
                            {
                                return Err(invalid("Tool child not settled or wrong owner"));
                            }
                            let recorded = if known_current_result {
                                None
                            } else {
                                Some(result_ids(tx, task.conversation_id, assistant).await?)
                            };
                            let mut boundary = Boundary::read(tx, task.conversation_id).await?;
                            boundary.require_run(task.id)?;
                            if recorded
                                .as_ref()
                                .is_some_and(|recorded| !recorded.contains(&call.id))
                            {
                                append_result(
                                    tx,
                                    task.conversation_id,
                                    assistant,
                                    &call.id,
                                    ToolResult {
                                        content:
                                            "Tool result unavailable; the operation may have run."
                                                .into(),
                                        is_error: true,
                                        usage: None,
                                    },
                                    Some("missing_result"),
                                )
                                .await?;
                            }
                            if index + 1 < calls.len() {
                                let child = agent
                                    .create_child(
                                        tx,
                                        task.id,
                                        assistant,
                                        &calls[index + 1],
                                        &offers,
                                    )
                                    .await?;
                                Ok(Some(TaskUpdate::Wait {
                                    checkpoint: tools_checkpoint(
                                        round,
                                        assistant,
                                        &calls,
                                        index + 1,
                                        child,
                                        &offers,
                                    ),
                                    on: vec![child],
                                    policy: JoinPolicy::AllSettled,
                                }))
                            } else {
                                let reset = agent
                                    .place_boundary(tx, &mut boundary, Position::PostTools)
                                    .await?;
                                boundary.save(tx).await?;
                                if reset {
                                    return Ok(Some(TaskUpdate::Complete(json!({"reset":true}))));
                                }
                                Ok(Some(TaskUpdate::Checkpoint(
                                    json!({"phase":"prepare","round":round}),
                                )))
                            }
                        })
                    })
                    .await?;
                Ok(())
            }
            _ => Err(invalid("Unknown turn phase")),
        }
    }
    async fn create_child(
        &self,
        tx: &Tx,
        root: Id,
        assistant: Id,
        call: &ToolCall,
        offers: &[ToolDeclaration],
    ) -> Result<Id, SessionError> {
        let version = offers
            .iter()
            .find(|offer| offer.name == call.name)
            .map(|offer| offer.version);
        Ok(tx.create_task(self.tool_definition(),json!({"rootTaskId":root.get(),"assistantEntryId":assistant.get(),"callId":call.id,"name":call.name,"version":version}),TaskOptions{ownership:TaskOwnership::Task(root),conversation_id:None,background:false}).await?.id)
    }
    async fn model_failure(
        &self,
        runtime: &TaskRuntime,
        error: ModelError,
    ) -> Result<(), SessionError> {
        runtime
            .commit(move |tx, task| {
                Box::pin(async move {
                    let mut boundary = Boundary::read(tx, task.conversation_id).await?;
                    boundary.require_run(task.id)?;
                    let mut data = json!({"message": error.message});
                    // Usage is independent evidence: an invalid partial message must
                    // not discard it. Explicit null is present, not a fallback request.
                    let usage = error.usage.or_else(|| {
                        error
                            .partial_response
                            .as_ref()
                            .and_then(|partial| partial.usage.clone())
                    });
                    if let Some(usage) = usage {
                        data["usage"] = usage;
                        if validate_entry(None, Some(&data)).is_err() {
                            data.as_object_mut()
                                .expect("diagnostic object")
                                .remove("usage");
                            data["usageOmitted"] = json!("invalid JSON payload");
                        }
                    }
                    if let Some(partial) = error.partial_response {
                        data["partialResponse"] = json!({
                            "message": encode_message(&partial.message),
                            "finishReason": match partial.finish_reason {
                                FinishReason::Stop => "stop",
                                FinishReason::ToolCalls => "toolCalls",
                            }
                        });
                        if validate_entry(None, Some(&data)).is_err() {
                            data.as_object_mut()
                                .expect("diagnostic object")
                                .remove("partialResponse");
                            data["partialResponseOmitted"] = json!("invalid JSON payload");
                        } else if let Some(usage) = partial.usage {
                            data["partialResponse"]["usage"] = usage;
                            // Validate in the actual, deeper partial-response envelope.
                            if validate_entry(None, Some(&data)).is_err() {
                                data["partialResponse"]
                                    .as_object_mut()
                                    .expect("partial object")
                                    .remove("usage");
                                data["partialResponse"]["usageOmitted"] =
                                    json!("invalid JSON payload");
                            }
                        }
                    }
                    let mut draft = EntryDraft::new("agent.diagnostic");
                    draft.data = Some(data);
                    tx.append_entry(task.conversation_id, draft).await?;
                    boundary.settle(tx, unanswered("model_error")).await?;
                    boundary.save(tx).await?;
                    Ok(Some(TaskUpdate::Fail(
                        TaskOutcomeError {
                            message: error.message,
                            detail: None,
                        },
                        None,
                    )))
                })
            })
            .await?;
        Ok(())
    }
    async fn tool(
        &self,
        record: TaskRecord,
        runtime: TaskRuntime,
        phase: &str,
    ) -> Result<(), SessionError> {
        let input = decode_tool_input(&record.input)?;
        let intent = decode_tool_checkpoint(checkpoint(&record)?, &input)?;
        if (phase == "execute") != intent.is_some() {
            return Err(invalid("Tool phase differs from checkpoint"));
        }
        let read_input = input.clone();
        let (call, offered) = runtime
            .read(move |tx, task| {
                Box::pin(async move {
                    if task.owner != Some(read_input.root) || task.background {
                        return Err(invalid("Invalid tool ownership"));
                    }
                    let root = tx
                        .task(read_input.root)
                        .await?
                        .ok_or_else(|| invalid("Missing turn"))?;
                    if root.kind != TURN_KIND
                        || root.version != 2
                        || root.conversation_id != task.conversation_id
                        || root.owner.is_some()
                        || root.background
                    {
                        return Err(invalid("Invalid tool root"));
                    }
                    decode_input(&root.input)?;
                    let root_cp = checkpoint(&root)?;
                    let offers = checkpoint_offers(root_cp)?;
                    let index = usize::try_from(number(root_cp, "index")?)
                        .map_err(|_| invalid("Invalid tool index"))?;
                    let calls = decode_calls(root_cp)?;
                    if root_cp.get("phase").and_then(Value::as_str) != Some("tools")
                        || id(root_cp, "child")? != task.id
                        || id(root_cp, "assistantEntryId")? != read_input.assistant
                        || calls.get(index).is_none_or(|call| {
                            call.id != read_input.call_id || call.name != read_input.name
                        })
                    {
                        return Err(invalid("Tool child does not match active turn intent"));
                    }
                    let assistant = tx
                        .visible_entry(task.conversation_id, read_input.assistant)
                        .await?
                        .ok_or_else(|| invalid("Missing assistant entry"))?
                        .entry;
                    if assistant.kind != "agent.assistant" || assistant.by_task_id != Some(root.id)
                    {
                        return Err(invalid("Invalid assistant provenance"));
                    }
                    let messages = assistant
                        .model
                        .ok_or_else(|| invalid("Missing assistant message"))?;
                    if messages.len() != 1 {
                        return Err(invalid("Invalid assistant messages"));
                    }
                    let ModelMessage::Assistant { tool_calls, .. } = decode_message(&messages[0])?
                    else {
                        return Err(invalid("Invalid assistant message"));
                    };
                    let call = tool_calls
                        .into_iter()
                        .find(|call| call.id == read_input.call_id)
                        .ok_or_else(|| invalid("Missing assistant call"))?;
                    if call.name != read_input.name || calls.get(index) != Some(&call) {
                        return Err(invalid("Tool does not match assistant call"));
                    }
                    let offered = offers.into_iter().find(|offer| offer.name == call.name);
                    if offered.as_ref().map(|offer| offer.version) != read_input.version {
                        return Err(invalid("Tool version does not match pinned offer"));
                    }
                    Ok((call, offered))
                })
            })
            .await?
            .value;
        if runtime.is_cancelled() {
            return Ok(());
        }
        // Do not resolve even a validator for a name absent from the pinned request.
        let resolved = if offered.is_some() {
            Some(self.resolve(&runtime).await?)
        } else {
            None
        };
        // Configuration resolution awaited the Session line; abort or close may
        // have overtaken that read. Do not invoke a validator after cancellation.
        if runtime.is_cancelled() {
            return Ok(());
        }
        let tool = resolved
            .as_ref()
            .and_then(|r| r.tools().iter().find(|t| t.declaration().name == call.name));
        if let Some((call, policy)) = intent {
            // The execute checkpoint is authoritative. Never prepare, validate
            // against current code, or rerun beforeTool after durable intent.
            if let (ReplayPolicy::Safe, Some(offer), Some(tool)) = (policy, &offered, tool)
                && tool.replay_policy == ReplayPolicy::Safe
                && tool.declaration.version == offer.version
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
            return finish_tool(
                &runtime,
                &input,
                ToolResult {
                    content: format!(
                        "interrupted_effect: tool '{}' may have partially or fully run; its result was not durably recorded. It was not replayed.",
                        input.name
                    ),
                    is_error: true,
                    usage: None,
                },
                Some("interrupted_effect"),
                End::Fail("interrupted_effect".into()),
            ).await;
        }
        let rejection = match (&offered, tool) {
            (None, _) => Some("Tool was not offered".to_owned()),
            (_, None) => Some("Tool is not installed".to_owned()),
            (Some(offer), Some(tool)) if offer.version != tool.declaration.version => {
                Some("Tool version mismatch".to_owned())
            }
            (_, Some(tool)) => {
                let validation = [encode_message(&ModelMessage::Assistant {
                    text: String::new(),
                    tool_calls: vec![call.clone()],
                })];
                validate_entry(Some(&validation), None)?;
                (tool.validate)(&call.arguments).err()
            }
        };
        if let Some(message) = rejection {
            return finish_tool(
                &runtime,
                &input,
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
        let resolved = resolved.as_ref().expect("offered tool resolution");
        let (call, block) = resolved.before_tool(&runtime, call).await?;
        // All rewrites run before intent. Validate native JSON as well as the
        // pinned tool's schema; no later extension can override a first block.
        let rejection = block.map(|message| (message, "tool_blocked")).or_else(|| {
            if !resolved
                .extensions()
                .iter()
                .any(|e| e.hooks().before_tool.is_some())
            {
                return None;
            }
            let validation = [encode_message(&ModelMessage::Assistant {
                text: String::new(),
                tool_calls: vec![call.clone()],
            })];
            validate_entry(Some(&validation), None)
                .map_err(|e| e.to_string())
                .and_then(|()| (tool.validate)(&call.arguments))
                .err()
                .map(|message| (message, "invalid_tool_call"))
        });
        if let Some((message, code)) = rejection {
            return finish_tool(
                &runtime,
                &input,
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
        let intent = execute_checkpoint(&input, &call, tool.replay_policy)?;
        runtime
            .commit(move |_, _| Box::pin(async move { Ok(Some(TaskUpdate::Checkpoint(intent))) }))
            .await?;
        execute_tool(&runtime, &input, resolved, tool, call).await
    }
    async fn abort_turn(
        &self,
        record: TaskRecord,
        runtime: TaskRuntime,
    ) -> Result<(), SessionError> {
        let cp = checkpoint(&record)?.clone();
        runtime.commit(move |tx,task|Box::pin(async move {
            let mut boundary = Boundary::read(tx, task.conversation_id).await?;
            boundary.require_run(task.id)?;
            if cp.get("phase").and_then(Value::as_str)==Some("tools") {
                let assistant=id(&cp,"assistantEntryId")?; let calls=decode_calls(&cp)?;
                let recorded=result_ids(tx,task.conversation_id,assistant).await?;
                for call in calls {
                    if !recorded.contains(&call.id) {append_result(tx,task.conversation_id,assistant,&call.id,ToolResult{content:"Turn aborted; tool result unavailable. Started operations may have run.".into(),is_error:true,usage:None},Some("aborted")).await?;}
                }
            }
            boundary.settle(tx, unanswered("aborted")).await?;
            boundary.save(tx).await?;
            Ok(Some(TaskUpdate::Abort{reason:Some("Agent turn aborted".into()),result:None}))
        })).await?;
        Ok(())
    }
}

// No durable partial progress channel exists yet, so reset before replay and
// retention on interruption are vacuous. Only final results are published.
async fn execute_tool(
    runtime: &TaskRuntime,
    input: &ToolInput,
    resolved: &ResolvedExtensions,
    tool: &Tool,
    call: ToolCall,
) -> Result<(), SessionError> {
    // The checkpoint ACK is not a cancellation barrier: abort/close may overtake it.
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
    let result = resolved
        .after_tool(runtime, ToolOutcome { call, result })
        .await?;
    finish_tool(runtime, input, result, code, end).await
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
fn checkpoint(task: &TaskRecord) -> Result<&Value, SessionError> {
    match &task.state {
        TaskState::Pending { checkpoint }
        | TaskState::Running { checkpoint }
        | TaskState::Waiting { checkpoint, .. } => Ok(checkpoint),
        _ => Err(invalid("Task has no checkpoint")),
    }
}
fn validate_request(request: &ModelRequest) -> Result<(), SessionError> {
    if request.model.is_empty() {
        return Err(invalid("Model identity required"));
    }
    let tools = Value::Array(request.tools.iter().map(declaration).collect());
    declarations(&tools)?;
    let messages = request
        .messages
        .iter()
        .map(encode_message)
        .collect::<Vec<_>>();
    for message in &messages {
        decode_message(message)?;
    }
    let data = json!({"model":request.model,"instructions":request.instructions,"tools":tools});
    validate_entry(Some(&messages), Some(&data))
}
fn decode_request(cp: &Value) -> Result<ModelRequest, SessionError> {
    id(cp, "cutoff")?;
    let messages = cp
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("Missing request messages"))?
        .iter()
        .map(decode_message)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ModelRequest {
        model: string(cp, "model")?,
        instructions: string(cp, "instructions")?,
        tools: declarations(
            cp.get("tools")
                .ok_or_else(|| invalid("Missing request tools"))?,
        )?,
        messages,
    })
}
fn validate_response(response: &ModelResponse) -> Result<(), SessionError> {
    let encoded = encode_message(&response.message);
    decode_message(&encoded)?;
    let ModelMessage::Assistant { tool_calls, .. } = &response.message else {
        return Err(invalid("Expected assistant response"));
    };
    if (tool_calls.is_empty() && response.finish_reason != FinishReason::Stop)
        || (!tool_calls.is_empty() && response.finish_reason != FinishReason::ToolCalls)
    {
        return Err(invalid("Finish reason inconsistent with tool calls"));
    }
    let messages = [encoded];
    let data = response_data("completed", response.usage.clone());
    validate_entry(Some(&messages), Some(&data))
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
fn tools_checkpoint(
    round: u64,
    assistant: Id,
    calls: &[ToolCall],
    index: usize,
    child: Id,
    offers: &[ToolDeclaration],
) -> Value {
    json!({"phase":"tools","round":round,"assistantEntryId":assistant.get(),"calls":calls.iter().map(encode_call).collect::<Vec<_>>(),"index":index,"child":child.get(),"tools":offers.iter().map(declaration).collect::<Vec<_>>()})
}
fn checkpoint_offers(cp: &Value) -> Result<Vec<ToolDeclaration>, SessionError> {
    declarations(
        cp.get("tools")
            .ok_or_else(|| invalid("Missing pinned request tools"))?,
    )
}
fn decode_calls(cp: &Value) -> Result<Vec<ToolCall>, SessionError> {
    cp.get("calls")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("Missing calls"))?
        .iter()
        .map(decode_call)
        .collect()
}
#[derive(Clone)]
struct ToolInput {
    root: Id,
    assistant: Id,
    call_id: String,
    name: String,
    version: Option<u64>,
}
fn decode_tool_input(value: &Value) -> Result<ToolInput, SessionError> {
    let version = match value.get("version") {
        Some(Value::Null) => None,
        Some(value) => Some(
            value
                .as_u64()
                .ok_or_else(|| invalid("Invalid tool version"))?,
        ),
        None => return Err(invalid("Missing tool version")),
    };
    let result = ToolInput {
        root: id(value, "rootTaskId")?,
        assistant: id(value, "assistantEntryId")?,
        call_id: string(value, "callId")?,
        name: string(value, "name")?,
        version,
    };
    if result.call_id.is_empty() || result.name.is_empty() {
        return Err(invalid("Empty tool input ID or name"));
    }
    Ok(result)
}
// Input pins ownership/provenance; execute intent redundantly pins call identity
// and offered version alongside the final arguments and policy. Reject old or
// malformed records rather than guessing what an interrupted effect received.
fn execute_checkpoint(
    input: &ToolInput,
    call: &ToolCall,
    policy: ReplayPolicy,
) -> Result<Value, SessionError> {
    let cp = json!({"phase":"execute", "callId":call.id, "name":call.name,
        "version":input.version, "arguments":call.arguments,
        "replay":match policy { ReplayPolicy::Unsafe => "unsafe", ReplayPolicy::Safe => "safe" }});
    decode_tool_checkpoint(&cp, input)?;
    Ok(cp)
}
fn decode_tool_checkpoint(
    cp: &Value,
    input: &ToolInput,
) -> Result<Option<(ToolCall, ReplayPolicy)>, SessionError> {
    // A checkpoint is persisted inside the TaskState object, which consumes one
    // structural level beyond the checkpoint value itself.
    publicworks_runtime::validate_native_json_value(cp, 1)?;
    let object = cp
        .as_object()
        .ok_or_else(|| invalid("Invalid tool checkpoint"))?;
    match cp.get("phase").and_then(Value::as_str) {
        Some("call") if object.len() == 1 => Ok(None),
        Some("execute") => {
            let fields = ["phase", "callId", "name", "version", "arguments", "replay"];
            if object.len() != fields.len() || fields.iter().any(|key| !object.contains_key(*key)) {
                return Err(invalid("Invalid execute intent fields"));
            }
            if string(cp, "callId")? != input.call_id
                || string(cp, "name")? != input.name
                || Some(number(cp, "version")?) != input.version
            {
                return Err(invalid("Execute intent differs from pinned tool input"));
            }
            let policy = match cp.get("replay").and_then(Value::as_str) {
                Some("unsafe") => ReplayPolicy::Unsafe,
                Some("safe") => ReplayPolicy::Safe,
                _ => return Err(invalid("Invalid execute replay policy")),
            };
            Ok(Some((
                ToolCall {
                    id: input.call_id.clone(),
                    name: input.name.clone(),
                    arguments: cp["arguments"].clone(),
                },
                policy,
            )))
        }
        _ => Err(invalid("Invalid tool checkpoint phase")),
    }
}
// Observe the batch in original call order. Missing child results use the same
// fallback that the tools phase will durably append; observers cannot write it.
async fn tool_outcomes(
    tx: &Tx,
    conversation: Id,
    assistant: Id,
    calls: Vec<ToolCall>,
) -> Result<(Vec<ToolOutcome>, BTreeSet<String>), SessionError> {
    let mut results = BTreeMap::new();
    let mut cursor = None;
    loop {
        let page = tx
            .scan_entries(EntryQuery::new(conversation), 128, cursor)
            .await?;
        for entry in page.items {
            if entry.kind != "agent.toolResult"
                || entry
                    .data
                    .as_ref()
                    .and_then(|d| d.get("assistantEntryId"))
                    .and_then(Value::as_u64)
                    != Some(assistant.get())
            {
                continue;
            }
            if let Some(messages) = entry.model {
                for message in messages {
                    if let ModelMessage::ToolResult {
                        call_id,
                        content,
                        is_error,
                    } = decode_message(&message)?
                    {
                        results.insert(
                            call_id,
                            ToolResult {
                                content,
                                is_error,
                                usage: entry.data.as_ref().and_then(|d| d.get("usage")).cloned(),
                            },
                        );
                    }
                }
            }
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    let recorded = results.keys().cloned().collect();
    let outcomes = calls
        .into_iter()
        .map(|call| {
            let result = results.remove(&call.id).unwrap_or_else(|| ToolResult {
                content: "Tool result unavailable; the operation may have run.".into(),
                is_error: true,
                usage: None,
            });
            ToolOutcome { call, result }
        })
        .collect();
    Ok((outcomes, recorded))
}
async fn result_ids(
    tx: &Tx,
    conversation: Id,
    assistant: Id,
) -> Result<BTreeSet<String>, SessionError> {
    let mut cursor = None;
    let mut result = BTreeSet::new();
    loop {
        let page = tx
            .scan_entries(EntryQuery::new(conversation), 128, cursor)
            .await?;
        for entry in page.items {
            if entry.kind == "agent.toolResult"
                && entry
                    .data
                    .as_ref()
                    .and_then(|d| d.get("assistantEntryId"))
                    .and_then(Value::as_u64)
                    == Some(assistant.get())
            {
                let data = entry.data.as_ref().expect("checked data");
                result.insert(string(data, "callId")?);
            }
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    Ok(result)
}
async fn append_result(
    tx: &Tx,
    conversation: Id,
    assistant: Id,
    call_id: &str,
    result: ToolResult,
    code: Option<&str>,
) -> Result<Id, SessionError> {
    let mut data = json!({"assistantEntryId":assistant.get(),"callId":call_id});
    if let Some(code) = code {
        data["code"] = Value::String(code.into());
    }
    if let Some(usage) = result.usage {
        data["usage"] = usage;
    }
    Ok(tx
        .append_entry(
            conversation,
            message_draft(
                "agent.toolResult",
                ModelMessage::ToolResult {
                    call_id: call_id.into(),
                    content: result.content,
                    is_error: result.is_error,
                },
                Some(data),
            ),
        )
        .await?
        .id)
}
enum End {
    Complete,
    Fail(String),
    Abort,
}
async fn finish_tool(
    runtime: &TaskRuntime,
    input: &ToolInput,
    result: ToolResult,
    code: Option<&str>,
    end: End,
) -> Result<(), SessionError> {
    let input = input.clone();
    let code = code.map(str::to_owned);
    runtime
        .commit(move |tx, task| {
            Box::pin(async move {
                let recorded = result_ids(tx, task.conversation_id, input.assistant).await?;
                let entry = if recorded.contains(&input.call_id) {
                    None
                } else {
                    Some(
                        append_result(
                            tx,
                            task.conversation_id,
                            input.assistant,
                            &input.call_id,
                            result,
                            code.as_deref(),
                        )
                        .await?,
                    )
                };
                let result = json!({"resultEntryId":entry.map(Id::get)});
                Ok(Some(match end {
                    End::Complete => TaskUpdate::Complete(result),
                    End::Fail(message) => TaskUpdate::Fail(
                        TaskOutcomeError {
                            message,
                            detail: None,
                        },
                        Some(result),
                    ),
                    End::Abort => TaskUpdate::Abort {
                        reason: Some("Tool aborted".into()),
                        result: Some(result),
                    },
                }))
            })
        })
        .await?;
    Ok(())
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
mod tests {
    use super::*;
    use futures_lite::future::{block_on, zip};

    fn agent() -> Agent {
        Agent::new(
            |_: ModelRequest, _: Cancellation| -> ModelFuture {
                Box::pin(async { panic!("validation must not invoke the model") })
            },
            vec![],
        )
        .unwrap()
    }

    fn tool_input() -> ToolInput {
        ToolInput {
            root: ROOT_CONVERSATION,
            assistant: ROOT_CONVERSATION,
            call_id: "call".into(),
            name: "effect".into(),
            version: Some(7),
        }
    }

    #[test]
    fn execute_intent_is_strict_and_pins_identity_policy_and_arguments() {
        let input = tool_input();
        let call = ToolCall {
            id: input.call_id.clone(),
            name: input.name.clone(),
            arguments: Value::Null,
        };
        let cp = execute_checkpoint(&input, &call, ReplayPolicy::Unsafe).unwrap();
        assert_eq!(
            decode_tool_checkpoint(&cp, &input).unwrap(),
            Some((call, ReplayPolicy::Unsafe))
        );
        assert_eq!(
            decode_tool_checkpoint(&json!({"phase":"call"}), &input).unwrap(),
            None
        );
        for key in ["phase", "callId", "name", "version", "arguments", "replay"] {
            let mut missing = cp.clone();
            missing.as_object_mut().unwrap().remove(key);
            assert!(
                decode_tool_checkpoint(&missing, &input).is_err(),
                "missing {key}"
            );
        }
        for (key, value) in [
            ("phase", json!("in_flight")),
            ("callId", json!("other")),
            ("name", json!("other")),
            ("version", json!(8)),
            ("version", Value::Null),
            ("version", json!(7.5)),
            ("replay", Value::Null),
            ("replay", json!(true)),
            ("replay", json!("Safe")),
            ("extra", json!(true)),
        ] {
            let mut bad = cp.clone();
            bad[key] = value;
            assert!(decode_tool_checkpoint(&bad, &input).is_err(), "{bad}");
        }
        for bad in [
            json!({"phase":"in_flight"}),
            json!({"phase":"call", "replay":"safe"}),
            json!([]),
        ] {
            assert!(decode_tool_checkpoint(&bad, &input).is_err());
        }
        let mut unoffered = input.clone();
        unoffered.version = None;
        assert!(decode_tool_checkpoint(&cp, &unoffered).is_err());
        assert_eq!(agent().tool_definition().version(), 3);
    }

    #[test]
    fn execute_intent_uses_native_json_and_counts_its_wrapper_depth() {
        let input = tool_input();
        let mut call = ToolCall {
            id: input.call_id.clone(),
            name: input.name.clone(),
            arguments: json!({"max":u64::MAX,"min":i64::MIN,"float":1.25,
                "opaque":{"$serde_json::private::Number":"opaque"}}),
        };
        let cp = execute_checkpoint(&input, &call, ReplayPolicy::Safe).unwrap();
        let decoded = decode_native_json(&serde_json::to_vec(&cp).unwrap()).unwrap();
        assert_eq!(
            decode_tool_checkpoint(&decoded, &input).unwrap(),
            Some((call.clone(), ReplayPolicy::Safe))
        );
        // TaskState and the checkpoint object consume two levels before the
        // opaque arguments value.
        call.arguments = (0..MAX_JSON_DEPTH - 2).fold(Value::Null, |v, _| json!([v]));
        assert!(execute_checkpoint(&input, &call, ReplayPolicy::Safe).is_ok());
        call.arguments = json!([call.arguments]);
        assert!(execute_checkpoint(&input, &call, ReplayPolicy::Safe).is_err());
        // Under arbitrary_precision these remain non-native tokens; baseline
        // serde_json may normalize them or reject them at parse time instead.
        for token in [
            "18446744073709551616",
            "1e999",
            "1.00000000000000000001",
            "1e0",
        ] {
            if let Ok(value) = serde_json::from_str::<Value>(token) {
                let retained_token = value.to_string();
                if retained_token == token {
                    call.arguments = value;
                    assert!(
                        execute_checkpoint(&input, &call, ReplayPolicy::Safe).is_err(),
                        "{token}"
                    );
                }
            }
        }
    }

    #[test]
    fn invalid_config_admission_has_no_user_or_task_writes() {
        block_on(async {
            let (session, driver) = Session::new(MemoryStorage::new());
            let command = async {
                let conversation = session
                    .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
                    .await
                    .unwrap()
                    .value
                    .id;
                let installation = agent();
                let result = session
                    .commit(move |tx| {
                        installation.admit_input(
                            tx,
                            conversation,
                            "hello",
                            TurnConfig {
                                model: "fake".into(),
                                instructions: "".into(),
                                max_model_rounds: 0,
                            },
                            crate::SubmitOptions::default(),
                        )
                    })
                    .await;
                assert!(matches!(result, Err(SessionError::Invalid(_))));
                session
                    .commit(move |tx| {
                        Box::pin(async move {
                            assert!(
                                tx.scan_entries(EntryQuery::new(conversation), 128, None)
                                    .await?
                                    .items
                                    .is_empty()
                            );
                            assert!(
                                tx.scan_tasks(TaskQuery::default(), 128, None)
                                    .await?
                                    .items
                                    .is_empty()
                            );
                            Ok(())
                        })
                    })
                    .await
                    .unwrap();
                session.close().await.unwrap();
            };
            zip(command, driver).await;
        });
    }

    #[test]
    fn initial_factories_reject_malformed_raw_task_input() {
        block_on(async {
            let (session, driver) = Session::new(MemoryStorage::new());
            let command = async {
                let conversation = session
                    .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
                    .await
                    .unwrap()
                    .value
                    .id;
                for definition in agent().definitions() {
                    let result = session
                        .commit(move |tx| {
                            Box::pin(async move {
                                tx.create_task(
                                    definition,
                                    json!({}),
                                    TaskOptions {
                                        ownership: TaskOwnership::Conversation,
                                        conversation_id: Some(conversation),
                                        background: false,
                                    },
                                )
                                .await
                            })
                        })
                        .await;
                    assert!(matches!(result, Err(SessionError::Invalid(_))));
                }
                session.close().await.unwrap();
            };
            zip(command, driver).await;
        });
    }

    #[test]
    fn response_validation_rejects_every_invalid_envelope_before_children() {
        let calls = vec![ToolCall {
            id: "a".into(),
            name: "t".into(),
            arguments: json!({}),
        }];
        let response = |message, finish_reason| ModelResponse {
            message,
            finish_reason,
            usage: None,
        };
        assert!(
            validate_response(&response(
                ModelMessage::User {
                    text: "wrong".into()
                },
                FinishReason::Stop
            ))
            .is_err()
        );
        assert!(
            validate_response(&response(
                ModelMessage::Assistant {
                    text: "".into(),
                    tool_calls: calls.clone()
                },
                FinishReason::Stop
            ))
            .is_err()
        );
        assert!(
            validate_response(&response(
                ModelMessage::Assistant {
                    text: "".into(),
                    tool_calls: vec![]
                },
                FinishReason::ToolCalls
            ))
            .is_err()
        );
        let mut duplicate = calls.clone();
        duplicate.extend(calls);
        assert!(
            validate_response(&response(
                ModelMessage::Assistant {
                    text: "".into(),
                    tool_calls: duplicate
                },
                FinishReason::ToolCalls
            ))
            .is_err()
        );
    }

    #[test]
    fn pinned_request_requires_an_actual_cutoff_not_latest_sentinel() {
        let cp = json!({"model":"fake","instructions":"","tools":[],"messages":[],"cutoff":null});
        assert!(decode_request(&cp).is_err());
    }
}
