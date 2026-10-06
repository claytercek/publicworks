//! Struct/raw-map intermediates avoid serde enum Content buffering in either member order.
use super::*;
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;

type Fields = BTreeMap<String, Box<RawValue>>;
fn invalid(message: &str) -> StorageError {
    StorageError::Other(message.into())
}
fn parse<T: DeserializeOwned>(raw: &RawValue) -> Result<T, StorageError> {
    serde_json::from_str(raw.get()).map_err(|e| invalid(&e.to_string()))
}
fn take(fields: &mut Fields, name: &str) -> Result<Box<RawValue>, StorageError> {
    fields
        .remove(name)
        .ok_or_else(|| invalid(&format!("Missing {name}")))
}
fn required<T: DeserializeOwned>(fields: &mut Fields, name: &str) -> Result<T, StorageError> {
    parse(&take(fields, name)?)
}
fn payload(fields: &mut Fields, name: &str, depth: usize) -> Result<Value, StorageError> {
    json::decode(&take(fields, name)?, depth)
}
fn optional_payload(
    fields: &mut Fields,
    name: &str,
    depth: usize,
) -> Result<Option<Value>, StorageError> {
    fields
        .remove(name)
        .map(|v| json::decode(&v, depth))
        .transpose()
}
fn exhausted(fields: Fields) -> Result<(), StorageError> {
    if fields.is_empty() {
        Ok(())
    } else {
        Err(invalid("Unexpected task fields"))
    }
}
#[derive(Deserialize)]
pub(super) struct StateFields(Fields);
impl TryFrom<StateFields> for TaskState {
    type Error = StorageError;
    fn try_from(value: StateFields) -> Result<Self, Self::Error> {
        let mut f = value.0;
        let status: String = required(&mut f, "status")?;
        let state = match status.as_str() {
            "pending" => Self::Pending {
                checkpoint: payload(&mut f, "checkpoint", 1)?,
            },
            "running" => Self::Running {
                checkpoint: payload(&mut f, "checkpoint", 1)?,
            },
            "waiting" => Self::Waiting {
                checkpoint: payload(&mut f, "checkpoint", 1)?,
                on: required(&mut f, "on")?,
                policy: required(&mut f, "policy")?,
            },
            "completing" => Self::Completing {
                outcome: required(&mut f, "outcome")?,
            },
            "terminal" => Self::Terminal {
                outcome: required(&mut f, "outcome")?,
            },
            _ => return Err(invalid("Unknown task state")),
        };
        exhausted(f)?;
        Ok(state)
    }
}
#[derive(Deserialize)]
pub(super) struct OutcomeFields(Fields);
impl TryFrom<OutcomeFields> for TaskOutcome {
    type Error = StorageError;
    fn try_from(value: OutcomeFields) -> Result<Self, Self::Error> {
        let mut f = value.0;
        let status: String = required(&mut f, "status")?;
        let outcome = match status.as_str() {
            "completed" => Self::Completed {
                result: payload(&mut f, "result", 2)?,
            },
            "failed" => Self::Failed {
                error: required(&mut f, "error")?,
                result: optional_payload(&mut f, "result", 2)?,
            },
            "aborted" => Self::Aborted {
                reason: f.remove("reason").map(|v| parse(&v)).transpose()?,
                result: optional_payload(&mut f, "result", 2)?,
            },
            "orphaned" => Self::Orphaned {
                reason: required(&mut f, "reason")?,
            },
            "faulted" => Self::Faulted {
                error: required(&mut f, "error")?,
            },
            _ => return Err(invalid("Unknown task outcome")),
        };
        exhausted(f)?;
        Ok(outcome)
    }
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RecordFields {
    id: Id,
    conversation_id: Id,
    kind: String,
    version: u64,
    input: Box<RawValue>,
    owner: Option<Id>,
    background: bool,
    abort_requested: bool,
    state: TaskState,
    #[serde(default, deserialize_with = "memos")]
    memos: Option<BTreeMap<String, Box<RawValue>>>,
}
impl TryFrom<RecordFields> for TaskRecord {
    type Error = StorageError;
    fn try_from(f: RecordFields) -> Result<Self, Self::Error> {
        let record = Self {
            id: f.id,
            conversation_id: f.conversation_id,
            kind: f.kind,
            version: f.version,
            input: json::decode(&f.input, 0)?,
            owner: f.owner,
            background: f.background,
            abort_requested: f.abort_requested,
            state: f.state,
            memos: f
                .memos
                .map(|m| {
                    m.into_iter()
                        .map(|(k, v)| Ok((k, json::decode(&v, 1)?)))
                        .collect::<Result<_, StorageError>>()
                })
                .transpose()?,
        };
        record.validate_payloads()?;
        Ok(record)
    }
}

// A present memo dictionary must be an object, never null. In particular,
// `memos: null` cannot evade the completing/terminal absence constraint.
fn memos<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Fields>, D::Error> {
    Fields::deserialize(d).map(Some)
}
