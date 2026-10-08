//! Internal durable formats. Original calls live only in the assistant entry.
use super::*;

pub(super) const TURN_VERSION: u64 = 3;
pub(super) const TOOL_VERSION: u64 = 4;

pub(super) enum TurnCheckpoint {
    Prepare {
        round: u64,
    },
    Request {
        round: u64,
        cutoff: Id,
        request: ModelRequest,
    },
    Tools(ToolBatch),
}
#[derive(Clone)]
pub(super) struct ToolBatch {
    pub round: u64,
    pub assistant: Id,
    pub index: usize,
    pub child: Id,
    pub offers: BTreeMap<String, u64>,
}
#[derive(Clone, Debug, PartialEq)]
pub(super) struct ToolInput {
    pub assistant: Id,
    pub index: usize,
}
#[derive(Debug, PartialEq)]
pub(super) enum ToolCheckpoint {
    Call,
    Execute {
        arguments: Value,
        policy: ReplayPolicy,
    },
}

fn fields(value: &Value, expected: &[&str]) -> Result<(), SessionError> {
    let object = object(value)?;
    if object.len() != expected.len() || expected.iter().any(|key| !object.contains_key(*key)) {
        return Err(invalid("Unsupported checkpoint fields"));
    }
    Ok(())
}
pub(super) fn checkpoint(task: &TaskRecord) -> Result<&Value, SessionError> {
    match &task.state {
        TaskState::Pending { checkpoint }
        | TaskState::Running { checkpoint }
        | TaskState::Waiting { checkpoint, .. } => Ok(checkpoint),
        _ => Err(invalid("Task has no checkpoint")),
    }
}
impl TurnCheckpoint {
    pub fn decode(task: &TaskRecord) -> Result<(TurnConfig, Self), SessionError> {
        let config = turn_config(task)?;
        let cp = checkpoint(task)?;
        publicworks_runtime::validate_native_json_value(cp, 1)?;
        let round = number(cp, "round")?;
        let state = match cp.get("phase").and_then(Value::as_str) {
            Some("prepare") => {
                fields(cp, &["phase", "round"])?;
                Self::Prepare { round }
            }
            Some("request") => {
                fields(
                    cp,
                    &[
                        "phase",
                        "round",
                        "cutoff",
                        "model",
                        "instructions",
                        "tools",
                        "messages",
                    ],
                )?;
                let request = ModelRequest {
                    model: string(cp, "model")?,
                    instructions: string(cp, "instructions")?,
                    tools: declarations(&cp["tools"])?,
                    messages: cp["messages"]
                        .as_array()
                        .ok_or_else(|| invalid("Missing request messages"))?
                        .iter()
                        .map(decode_message)
                        .collect::<Result<_, _>>()?,
                };
                if round == 0
                    || round > config.max_model_rounds
                    || request.model != config.model
                    || request.instructions != config.instructions
                {
                    return Err(invalid("Request differs from pinned turn configuration"));
                }
                Self::Request {
                    round,
                    cutoff: id(cp, "cutoff")?,
                    request,
                }
            }
            Some("tools") => {
                fields(
                    cp,
                    &[
                        "phase",
                        "round",
                        "assistantEntryId",
                        "index",
                        "child",
                        "offers",
                    ],
                )?;
                if round == 0 || round > config.max_model_rounds {
                    return Err(invalid("Invalid tool round"));
                }
                let offers = object(&cp["offers"])?
                    .iter()
                    .map(|(name, version)| {
                        if name.is_empty() {
                            return Err(invalid("Empty offer name"));
                        }
                        Ok((
                            name.clone(),
                            version
                                .as_u64()
                                .ok_or_else(|| invalid("Invalid offer version"))?,
                        ))
                    })
                    .collect::<Result<_, SessionError>>()?;
                Self::Tools(ToolBatch {
                    round,
                    assistant: id(cp, "assistantEntryId")?,
                    index: index(cp)?,
                    child: id(cp, "child")?,
                    offers,
                })
            }
            _ => return Err(invalid("Unsupported turn checkpoint")),
        };
        Ok((config, state))
    }
    pub fn encode(self) -> Result<Value, SessionError> {
        let cp = match self {
            Self::Prepare { round } => json!({"phase":"prepare","round":round}),
            Self::Request {
                round,
                cutoff,
                request,
            } => json!({"phase":"request","round":round,"cutoff":cutoff.get(),
                "model":request.model,"instructions":request.instructions,"tools":request.tools.iter().map(declaration).collect::<Vec<_>>(),
                "messages":request.messages.iter().map(encode_message).collect::<Vec<_>>()}),
            Self::Tools(batch) => {
                json!({"phase":"tools","round":batch.round,"assistantEntryId":batch.assistant.get(),
                "index":batch.index,"child":batch.child.get(),"offers":batch.offers})
            }
        };
        publicworks_runtime::validate_native_json_value(&cp, 1)?;
        Ok(cp)
    }
}
fn index(value: &Value) -> Result<usize, SessionError> {
    usize::try_from(number(value, "index")?).map_err(|_| invalid("Invalid tool index"))
}
impl ToolInput {
    pub fn decode(value: &Value) -> Result<Self, SessionError> {
        fields(value, &["assistantEntryId", "index"])?;
        Ok(Self {
            assistant: id(value, "assistantEntryId")?,
            index: index(value)?,
        })
    }
    pub fn encode(&self) -> Value {
        json!({"assistantEntryId":self.assistant.get(),"index":self.index})
    }
}
impl ToolCheckpoint {
    pub fn decode(cp: &Value) -> Result<Self, SessionError> {
        // TaskState consumes a structural level outside this checkpoint.
        publicworks_runtime::validate_native_json_value(cp, 1)?;
        match cp.get("phase").and_then(Value::as_str) {
            Some("call") => {
                fields(cp, &["phase"])?;
                Ok(Self::Call)
            }
            Some("execute") => {
                fields(cp, &["phase", "arguments", "replay"])?;
                publicworks_runtime::validate_native_json_value(&cp["arguments"], 4)?;
                let policy = match cp["replay"].as_str() {
                    Some("safe") => ReplayPolicy::Safe,
                    Some("unsafe") => ReplayPolicy::Unsafe,
                    _ => return Err(invalid("Invalid execute replay policy")),
                };
                Ok(Self::Execute {
                    arguments: cp["arguments"].clone(),
                    policy,
                })
            }
            _ => Err(invalid("Unsupported tool checkpoint")),
        }
    }
    pub fn encode(self) -> Result<Value, SessionError> {
        let cp = match self {
            Self::Call => json!({"phase":"call"}),
            Self::Execute { arguments, policy } => json!({"phase":"execute","arguments":arguments,
                "replay":match policy { ReplayPolicy::Safe => "safe", ReplayPolicy::Unsafe => "unsafe" }}),
        };
        publicworks_runtime::validate_native_json_value(&cp, 1)?;
        Ok(cp)
    }
}

pub(super) async fn assistant_calls(
    tx: &Tx,
    root: &TaskRecord,
    assistant: Id,
) -> Result<Vec<ToolCall>, SessionError> {
    let entry = tx
        .visible_entry(root.conversation_id, assistant)
        .await?
        .ok_or_else(|| invalid("Missing assistant entry"))?
        .entry;
    if entry.kind != "agent.assistant"
        || entry.by_task_id != Some(root.id)
        || entry.conversation_id != root.conversation_id
    {
        return Err(invalid("Invalid assistant provenance"));
    }
    let messages = entry
        .model
        .ok_or_else(|| invalid("Missing assistant message"))?;
    if messages.len() != 1 {
        return Err(invalid("Invalid assistant messages"));
    }
    match decode_message(&messages[0])? {
        ModelMessage::Assistant { tool_calls, .. } if !tool_calls.is_empty() => Ok(tool_calls),
        _ => Err(invalid("Invalid assistant call batch")),
    }
}

fn turn_config(task: &TaskRecord) -> Result<TurnConfig, SessionError> {
    if task.kind != TURN_KIND
        || task.version != TURN_VERSION
        || task.owner.is_some()
        || task.background
    {
        return Err(invalid("Unsupported agent turn version or scope"));
    }
    decode_input(&task.input)
}

/// Immutable provenance survives an owner's transition to Completing.
async fn tool_origin(tx: &Tx, task: &TaskRecord) -> Result<(ToolInput, TaskRecord), SessionError> {
    if task.kind != TOOL_KIND || task.version != TOOL_VERSION || task.background {
        return Err(invalid("Unsupported tool version or scope"));
    }
    let input = ToolInput::decode(&task.input)?;
    let root = tx
        .task(task.owner.ok_or_else(|| invalid("Missing tool owner"))?)
        .await?
        .ok_or_else(|| invalid("Missing turn"))?;
    turn_config(&root)?;
    if root.conversation_id != task.conversation_id {
        return Err(invalid("Tool conversation differs from owner"));
    }
    Ok((input, root))
}

pub(super) async fn tool_abort_context(
    tx: &Tx,
    task: &TaskRecord,
) -> Result<(ToolInput, ToolCall), SessionError> {
    let (input, root) = tool_origin(tx, task).await?;
    if matches!(root.state, TaskState::Completing { .. }) {
        let calls = assistant_calls(tx, &root, input.assistant).await?;
        let call = calls
            .into_iter()
            .nth(input.index)
            .ok_or_else(|| invalid("Invalid tool index"))?;
        Ok((input, call))
    } else {
        let (input, call, _) = tool_context(tx, task).await?;
        Ok((input, call))
    }
}

/// No callback runs until active child/offer checks pass.
pub(super) async fn tool_context(
    tx: &Tx,
    task: &TaskRecord,
) -> Result<(ToolInput, ToolCall, Option<u64>), SessionError> {
    let (input, root) = tool_origin(tx, task).await?;
    let (_, TurnCheckpoint::Tools(batch)) = TurnCheckpoint::decode(&root)? else {
        return Err(invalid("Tool owner is not running tools"));
    };
    if root.conversation_id != task.conversation_id
        || batch.child != task.id
        || batch.assistant != input.assistant
        || batch.index != input.index
    {
        return Err(invalid("Tool child does not match active turn intent"));
    }
    let calls = assistant_calls(tx, &root, input.assistant).await?;
    let call = calls
        .into_iter()
        .nth(input.index)
        .ok_or_else(|| invalid("Invalid tool index"))?;
    let version = batch.offers.get(&call.name).copied();
    Ok((input, call, version))
}
