use crate::{wire::*, *};
use futures_util::future::{Either, select};
use publicworks_runtime::*;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

pub const TURN_KIND: &str = "agent.turn";
pub const TOOL_KIND: &str = "agent.tool";
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnHandle {
    pub task_id: Id,
    pub user_entry_id: Id,
}
#[derive(Clone)]
pub struct Agent(Rc<Installation>);
struct Installation {
    model: Rc<dyn Model>,
    tools: BTreeMap<String, Tool>,
}
impl Agent {
    pub fn new(model: impl Model + 'static, tools: Vec<Tool>) -> Result<Self, SessionError> {
        let mut registry = BTreeMap::new();
        for tool in tools {
            decode_declaration(&declaration(&tool.declaration))?;
            validate_entry(None, Some(declaration(&tool.declaration)))?;
            if registry
                .insert(tool.declaration.name.clone(), tool)
                .is_some()
            {
                return Err(invalid("Duplicate tool name"));
            }
        }
        Ok(Self(Rc::new(Installation {
            model: Rc::new(model),
            tools: registry,
        })))
    }
    pub fn definitions(&self) -> [TaskDefinition; 2] {
        [self.turn_definition(), self.tool_definition()]
    }
    pub fn admit_turn<'a>(
        &self,
        tx: &'a Tx,
        conversation: Id,
        text: impl Into<String>,
        config: TurnConfig,
    ) -> TxFuture<'a, TurnHandle> {
        let agent = self.clone();
        let text = text.into();
        Box::pin(async move {
            validate_config(&config)?;
            let conversation_record = tx
                .conversation(conversation)
                .await?
                .ok_or_else(|| invalid("Unknown conversation"))?;
            if conversation_record.owner.is_some() {
                return Err(invalid("Owned conversations are unsupported"));
            }
            let mut cursor = None;
            let mut busy = false;
            loop {
                let page = tx
                    .scan_tasks(
                        TaskQuery {
                            conversation_id: Some(conversation),
                            ..TaskQuery::default()
                        },
                        128,
                        cursor,
                    )
                    .await?;
                busy |= page
                    .items
                    .iter()
                    .any(|task| task.kind == TURN_KIND && task.status() != TaskStatus::Terminal);
                cursor = page.next;
                if cursor.is_none() {
                    break;
                }
            }
            if busy {
                return Err(invalid("Conversation already has a nonterminal agent turn"));
            }
            let input = json!({"config":{"model":config.model,"instructions":config.instructions,"maxModelRounds":config.max_model_rounds},
                "tools":agent.0.tools.values().map(|tool|declaration(&tool.declaration)).collect::<Vec<_>>()});
            decode_input(&input)?;
            let user = tx
                .append_entry(
                    conversation,
                    message_draft("agent.user", ModelMessage::User { text }, None),
                )
                .await?;
            let task = tx
                .create_task(
                    agent.turn_definition(),
                    input,
                    TaskOptions {
                        ownership: TaskOwnership::Conversation,
                        conversation_id: Some(conversation),
                        background: false,
                    },
                )
                .await?;
            Ok(TurnHandle {
                task_id: task.id,
                user_entry_id: user.id,
            })
        })
    }
    fn turn_definition(&self) -> TaskDefinition {
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
            1,
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
        for phase in ["call", "in_flight"] {
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
            1,
            |input| {
                decode_tool_input(input)?;
                Ok(json!({"phase":"call"}))
            },
            phases,
        )
        .with_abort_handler(Rc::new(|record, runtime| {
            Box::pin(async move {
                let input = decode_tool_input(&record.input).map_err(fault)?;
                let in_flight = checkpoint(&record)
                    .ok()
                    .and_then(|cp| cp.get("phase"))
                    .and_then(Value::as_str)
                    == Some("in_flight");
                let content = if in_flight {
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
        let (config, offers) = decode_input(&record.input)?;
        let cp = checkpoint(&record)?.clone();
        let round = number(&cp, "round")?;
        match phase {
            "prepare" => {
                if round >= config.max_model_rounds {
                    return fail(&runtime, "Maximum model rounds exhausted").await;
                }
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
                if request.model != config.model
                    || request.instructions != config.instructions
                    || request.tools != offers
                {
                    return Err(invalid("Request differs from pinned turn configuration"));
                }
                if runtime.is_cancelled() {
                    return Ok(());
                }
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
                        let ModelMessage::Assistant { ref tool_calls, .. } = response.message
                        else {
                            unreachable!()
                        };
                        if tool_calls.is_empty() {
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
                                                round, entry.id, &calls, 0, child,
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
                let assistant = id(&cp, "assistantEntryId")?;
                let child = id(&cp, "child")?;
                let index = usize::try_from(number(&cp, "index")?)
                    .map_err(|_| invalid("Invalid tool index"))?;
                let calls = decode_calls(&cp)?;
                let call = calls
                    .get(index)
                    .ok_or_else(|| invalid("Invalid tool index"))?
                    .clone();
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
                            let recorded = result_ids(tx, task.conversation_id, assistant).await?;
                            if !recorded.contains(&call.id) {
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
                                    ),
                                    on: vec![child],
                                    policy: JoinPolicy::AllSettled,
                                }))
                            } else {
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
                        if validate_entry(None, Some(data.clone())).is_err() {
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
                        if validate_entry(None, Some(data.clone())).is_err() {
                            data.as_object_mut()
                                .expect("diagnostic object")
                                .remove("partialResponse");
                            data["partialResponseOmitted"] = json!("invalid JSON payload");
                        } else if let Some(usage) = partial.usage {
                            data["partialResponse"]["usage"] = usage;
                            // Validate in the actual, deeper partial-response envelope.
                            if validate_entry(None, Some(data.clone())).is_err() {
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
                        || root.version != 1
                        || root.conversation_id != task.conversation_id
                        || root.owner.is_some()
                        || root.background
                    {
                        return Err(invalid("Invalid tool root"));
                    }
                    let (_, offers) = decode_input(&root.input)?;
                    let root_cp = checkpoint(&root)?;
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
        if phase == "in_flight" {
            return finish_tool(&runtime,&input,ToolResult{content:"interrupted_effect: the operation may have partially or fully run; its result was not durably recorded. It was not replayed.".into(),is_error:true,usage:None},Some("interrupted_effect"),End::Fail("interrupted_effect".into())).await;
        }
        let tool = self.0.tools.get(&call.name);
        let rejection = match (&offered, tool) {
            (None, _) => Some("Tool was not offered".to_owned()),
            (_, None) => Some("Tool is not installed".to_owned()),
            (Some(offer), Some(tool)) if offer.version != tool.declaration.version => {
                Some("Tool version mismatch".to_owned())
            }
            (_, Some(tool)) => {
                validate_entry(
                    Some(vec![encode_message(&ModelMessage::Assistant {
                        text: String::new(),
                        tool_calls: vec![call.clone()],
                    })]),
                    None,
                )?;
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
        let execute = tool.expect("validated installed tool").execute.clone();
        runtime
            .commit(|_, _| {
                Box::pin(async { Ok(Some(TaskUpdate::Checkpoint(json!({"phase":"in_flight"})))) })
            })
            .await?;
        // The checkpoint ACK is not a cancellation barrier: abort/close may overtake it.
        if runtime.is_cancelled() {
            return Ok(());
        }
        let future = execute(call, Cancellation(runtime.clone()));
        let result = match select(future, Box::pin(runtime.cancelled())).await {
            Either::Left((result, _)) => result,
            Either::Right(_) => return Ok(()),
        };
        if runtime.is_cancelled() {
            return Ok(());
        }
        match result {
            Ok(result) => finish_tool(&runtime, &input, result, None, End::Complete).await,
            Err(error) => {
                finish_tool(
                    &runtime,
                    &input,
                    ToolResult {
                        content: error.message.clone(),
                        is_error: true,
                        usage: error.usage,
                    },
                    Some("tool_error"),
                    End::Fail(error.message),
                )
                .await
            }
        }
    }
    async fn abort_turn(
        &self,
        record: TaskRecord,
        runtime: TaskRuntime,
    ) -> Result<(), SessionError> {
        let cp = checkpoint(&record)?.clone();
        runtime.commit(move |tx,task|Box::pin(async move {
            if cp.get("phase").and_then(Value::as_str)==Some("tools") {
                let assistant=id(&cp,"assistantEntryId")?; let calls=decode_calls(&cp)?;
                let recorded=result_ids(tx,task.conversation_id,assistant).await?;
                for call in calls {
                    if !recorded.contains(&call.id) {append_result(tx,task.conversation_id,assistant,&call.id,ToolResult{content:"Turn aborted; tool result unavailable. Started operations may have run.".into(),is_error:true,usage:None},Some("aborted")).await?;}
                }
            }
            Ok(Some(TaskUpdate::Abort{reason:Some("Agent turn aborted".into()),result:None}))
        })).await?;
        Ok(())
    }
}

fn validate_config(config: &TurnConfig) -> Result<(), SessionError> {
    if config.model.is_empty() || config.max_model_rounds == 0 {
        return Err(invalid(
            "Model identity and positive max_model_rounds required",
        ));
    }
    Ok(())
}
fn decode_input(input: &Value) -> Result<(TurnConfig, Vec<ToolDeclaration>), SessionError> {
    let cfg = input
        .get("config")
        .ok_or_else(|| invalid("Missing config"))?;
    let config = TurnConfig {
        model: string(cfg, "model")?,
        instructions: string(cfg, "instructions")?,
        max_model_rounds: number(cfg, "maxModelRounds")?,
    };
    validate_config(&config)?;
    let tools = declarations(input.get("tools").ok_or_else(|| invalid("Missing tools"))?)?;
    validate_entry(None, Some(input.clone()))?;
    Ok((config, tools))
}
fn checkpoint(task: &TaskRecord) -> Result<&Value, SessionError> {
    match &task.state {
        TaskState::Pending { checkpoint }
        | TaskState::Running { checkpoint }
        | TaskState::Waiting { checkpoint, .. } => Ok(checkpoint),
        _ => Err(invalid("Task has no checkpoint")),
    }
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
    validate_entry(
        Some(vec![encoded]),
        Some(response_data("completed", response.usage.clone())),
    )
}
fn response_data(status: &str, usage: Option<Value>) -> Value {
    let mut data = json!({"status":status});
    if let Some(usage) = usage {
        data["usage"] = usage;
    }
    data
}
fn message_draft(kind: &str, message: ModelMessage, data: Option<Value>) -> EntryDraft {
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
) -> Value {
    json!({"phase":"tools","round":round,"assistantEntryId":assistant.get(),"calls":calls.iter().map(encode_call).collect::<Vec<_>>(),"index":index,"child":child.get()})
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
        .commit(move |_, _| {
            Box::pin(async move {
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
                        installation.admit_turn(
                            tx,
                            conversation,
                            "hello",
                            TurnConfig {
                                model: "fake".into(),
                                instructions: "".into(),
                                max_model_rounds: 0,
                            },
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
