//! Durable submission receipts and structural per-conversation admission state.
//!
//! These types intentionally contain no admission, inbox-selection, or agent
//! behavior. They are the storage-level shapes used by later lifecycle slices.
use crate::*;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned, ser::SerializeMap,
};
use serde_json::value::RawValue;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SubmissionType {
    Input,
    Write,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SubmissionStatus {
    Queued,
    Placed,
    Done,
    Unanswered,
}

/// Legal combinations of submission type, status, and status-specific fields.
#[derive(Clone, Debug, PartialEq)]
pub enum SubmissionState {
    InputQueued,
    InputPlaced {
        entry: Id,
    },
    InputDone {
        entry: Id,
        answer: Id,
    },
    InputUnanswered {
        entry: Option<Id>,
        reason: String,
        /// `None` is omitted; `Some(Value::Null)` is an explicit JSON null.
        detail: Option<Value>,
    },
    WriteQueued,
    WriteDone {
        entry: Id,
    },
    WriteUnanswered {
        reason: String,
        /// `None` is omitted; `Some(Value::Null)` is an explicit JSON null.
        detail: Option<Value>,
    },
}
impl SubmissionState {
    pub fn submission_type(&self) -> SubmissionType {
        match self {
            Self::InputQueued
            | Self::InputPlaced { .. }
            | Self::InputDone { .. }
            | Self::InputUnanswered { .. } => SubmissionType::Input,
            Self::WriteQueued | Self::WriteDone { .. } | Self::WriteUnanswered { .. } => {
                SubmissionType::Write
            }
        }
    }

    pub fn status(&self) -> SubmissionStatus {
        match self {
            Self::InputQueued | Self::WriteQueued => SubmissionStatus::Queued,
            Self::InputPlaced { .. } => SubmissionStatus::Placed,
            Self::InputDone { .. } | Self::WriteDone { .. } => SubmissionStatus::Done,
            Self::InputUnanswered { .. } | Self::WriteUnanswered { .. } => {
                SubmissionStatus::Unanswered
            }
        }
    }

    pub(crate) fn detail(&self) -> Option<&Value> {
        match self {
            Self::InputUnanswered { detail, .. } | Self::WriteUnanswered { detail, .. } => {
                detail.as_ref()
            }
            _ => None,
        }
    }
}

/// Complete durable receipt for an admitted input or passive write.
#[derive(Clone, Debug, PartialEq)]
pub struct SubmissionRecord {
    pub id: Id,
    pub conversation_id: Id,
    pub request_id: Option<String>,
    pub state: SubmissionState,
}
impl SubmissionRecord {
    pub fn submission_type(&self) -> SubmissionType {
        self.state.submission_type()
    }

    pub fn status(&self) -> SubmissionStatus {
        self.state.status()
    }

    pub fn validate_payloads(&self) -> Result<(), StorageError> {
        json::validate(
            self.state
                .detail()
                .into_iter()
                .map(|value| (value, 0))
                .collect(),
        )
    }
}

/// Terminal change requested by lifecycle code. Applicability is checked by Tx
/// in a later slice; the enum itself cannot encode a malformed settlement.
#[derive(Clone, Debug, PartialEq)]
pub enum SubmissionSettlement {
    Done {
        answer: Id,
    },
    Unanswered {
        reason: String,
        /// `None` is omitted; `Some(Value::Null)` is an explicit JSON null.
        detail: Option<Value>,
    },
}
impl SubmissionSettlement {
    pub fn validate_payloads(&self) -> Result<(), StorageError> {
        let values = match self {
            Self::Done { .. } => Vec::new(),
            Self::Unanswered { detail, .. } => detail.iter().map(|value| (value, 0)).collect(),
        };
        json::validate(values)
    }
}

#[derive(Clone, Debug, Default)]
pub struct SubmissionQuery {
    pub conversation_id: Option<Id>,
    pub status: Option<SubmissionStatus>,
}
impl SubmissionQuery {
    pub fn matches(&self, record: &SubmissionRecord) -> bool {
        self.conversation_id
            .is_none_or(|value| value == record.conversation_id)
            && self.status.is_none_or(|value| value == record.status())
    }
}

/// Active generation marker. `input_submission_ids` preserves placement order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConversationRun {
    pub task_id: Id,
    pub input_submission_ids: Vec<Id>,
}

/// Ordered inbox member. Policy and payload interpretation belong to the agent.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InboxItem {
    pub submission_id: Id,
    #[serde(deserialize_with = "json::value")]
    pub payload: Value,
}

/// Dedicated per-conversation state needed for atomic submission admission.
/// This is structural runtime data; the runtime does not interpret agent payloads.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    rename_all = "camelCase",
    deny_unknown_fields,
    try_from = "ConversationStateFields"
)]
pub struct ConversationStateRecord {
    pub id: Id,
    pub conversation_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<ConversationRun>,
    #[serde(default)]
    pub inbox: Vec<InboxItem>,
    /// Agent-owned configuration snapshot. Absence and explicit null are distinct.
    #[serde(
        default,
        deserialize_with = "json::data",
        skip_serializing_if = "Option::is_none"
    )]
    pub agent_config: Option<Value>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConversationStateFields {
    id: Id,
    conversation_id: Id,
    #[serde(default)]
    run: Option<ConversationRun>,
    #[serde(default)]
    inbox: Vec<InboxItem>,
    #[serde(default, deserialize_with = "json::data")]
    agent_config: Option<Value>,
}
impl TryFrom<ConversationStateFields> for ConversationStateRecord {
    type Error = StorageError;

    fn try_from(fields: ConversationStateFields) -> Result<Self, Self::Error> {
        let record = Self {
            id: fields.id,
            conversation_id: fields.conversation_id,
            run: fields.run,
            inbox: fields.inbox,
            agent_config: fields.agent_config,
        };
        record.validate_payloads()?;
        Ok(record)
    }
}
impl ConversationStateRecord {
    /// Inbox array and item object are structural payload wrappers and count
    /// toward the shared depth limit. The record envelope itself does not.
    pub fn validate_payloads(&self) -> Result<(), StorageError> {
        let mut values: Vec<(&Value, usize)> =
            self.inbox.iter().map(|item| (&item.payload, 2)).collect();
        if let Some(config) = &self.agent_config {
            values.push((config, 0));
        }
        json::validate(values)
    }
}

type Fields = BTreeMap<String, Box<RawValue>>;
fn invalid(message: impl Into<String>) -> StorageError {
    StorageError::Other(message.into())
}
fn parse<T: DeserializeOwned>(raw: &RawValue) -> Result<T, StorageError> {
    serde_json::from_str(raw.get()).map_err(|error| invalid(error.to_string()))
}
fn take(fields: &mut Fields, name: &str) -> Result<Box<RawValue>, StorageError> {
    fields
        .remove(name)
        .ok_or_else(|| invalid(format!("Missing {name}")))
}
fn required<T: DeserializeOwned>(fields: &mut Fields, name: &str) -> Result<T, StorageError> {
    parse(&take(fields, name)?)
}
fn optional<T: DeserializeOwned>(
    fields: &mut Fields,
    name: &str,
) -> Result<Option<T>, StorageError> {
    fields.remove(name).map(|raw| parse(&raw)).transpose()
}
fn optional_payload(fields: &mut Fields, name: &str) -> Result<Option<Value>, StorageError> {
    fields
        .remove(name)
        .map(|raw| json::decode(&raw, 0))
        .transpose()
}
fn exhausted(fields: Fields, what: &str) -> Result<(), StorageError> {
    if fields.is_empty() {
        Ok(())
    } else {
        Err(invalid(format!("Unexpected {what} fields")))
    }
}

fn decode_state(fields: &mut Fields) -> Result<SubmissionState, StorageError> {
    let kind: String = required(fields, "type")?;
    let status: String = required(fields, "status")?;
    let state = match (kind.as_str(), status.as_str()) {
        ("input", "queued") => SubmissionState::InputQueued,
        ("input", "placed") => SubmissionState::InputPlaced {
            entry: required(fields, "entry")?,
        },
        ("input", "done") => SubmissionState::InputDone {
            entry: required(fields, "entry")?,
            answer: required(fields, "answer")?,
        },
        ("input", "unanswered") => SubmissionState::InputUnanswered {
            entry: optional(fields, "entry")?,
            reason: required(fields, "reason")?,
            detail: optional_payload(fields, "detail")?,
        },
        ("write", "queued") => SubmissionState::WriteQueued,
        ("write", "done") => SubmissionState::WriteDone {
            entry: required(fields, "entry")?,
        },
        ("write", "unanswered") => SubmissionState::WriteUnanswered {
            reason: required(fields, "reason")?,
            detail: optional_payload(fields, "detail")?,
        },
        _ => return Err(invalid("Invalid submission type/status combination")),
    };
    Ok(state)
}

impl Serialize for SubmissionState {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("type", &self.submission_type())?;
        map.serialize_entry("status", &self.status())?;
        match self {
            Self::InputPlaced { entry } | Self::WriteDone { entry } => {
                map.serialize_entry("entry", entry)?;
            }
            Self::InputDone { entry, answer } => {
                map.serialize_entry("entry", entry)?;
                map.serialize_entry("answer", answer)?;
            }
            Self::InputUnanswered {
                entry,
                reason,
                detail,
            } => {
                if let Some(entry) = entry {
                    map.serialize_entry("entry", entry)?;
                }
                map.serialize_entry("reason", reason)?;
                if let Some(detail) = detail {
                    map.serialize_entry("detail", detail)?;
                }
            }
            Self::WriteUnanswered { reason, detail } => {
                map.serialize_entry("reason", reason)?;
                if let Some(detail) = detail {
                    map.serialize_entry("detail", detail)?;
                }
            }
            Self::InputQueued | Self::WriteQueued => {}
        }
        map.end()
    }
}
impl<'de> Deserialize<'de> for SubmissionState {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut fields = Fields::deserialize(deserializer)?;
        let state = decode_state(&mut fields).map_err(serde::de::Error::custom)?;
        exhausted(fields, "submission state").map_err(serde::de::Error::custom)?;
        Ok(state)
    }
}

impl Serialize for SubmissionRecord {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("id", &self.id)?;
        map.serialize_entry("conversationId", &self.conversation_id)?;
        if let Some(request_id) = &self.request_id {
            map.serialize_entry("requestId", request_id)?;
        }
        map.serialize_entry("type", &self.state.submission_type())?;
        map.serialize_entry("status", &self.state.status())?;
        match &self.state {
            SubmissionState::InputPlaced { entry } | SubmissionState::WriteDone { entry } => {
                map.serialize_entry("entry", entry)?;
            }
            SubmissionState::InputDone { entry, answer } => {
                map.serialize_entry("entry", entry)?;
                map.serialize_entry("answer", answer)?;
            }
            SubmissionState::InputUnanswered {
                entry,
                reason,
                detail,
            } => {
                if let Some(entry) = entry {
                    map.serialize_entry("entry", entry)?;
                }
                map.serialize_entry("reason", reason)?;
                if let Some(detail) = detail {
                    map.serialize_entry("detail", detail)?;
                }
            }
            SubmissionState::WriteUnanswered { reason, detail } => {
                map.serialize_entry("reason", reason)?;
                if let Some(detail) = detail {
                    map.serialize_entry("detail", detail)?;
                }
            }
            SubmissionState::InputQueued | SubmissionState::WriteQueued => {}
        }
        map.end()
    }
}
impl<'de> Deserialize<'de> for SubmissionRecord {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut fields = Fields::deserialize(deserializer)?;
        let id = required(&mut fields, "id").map_err(serde::de::Error::custom)?;
        let conversation_id =
            required(&mut fields, "conversationId").map_err(serde::de::Error::custom)?;
        let request_id = optional(&mut fields, "requestId").map_err(serde::de::Error::custom)?;
        let state = decode_state(&mut fields).map_err(serde::de::Error::custom)?;
        exhausted(fields, "submission record").map_err(serde::de::Error::custom)?;
        let record = Self {
            id,
            conversation_id,
            request_id,
            state,
        };
        record
            .validate_payloads()
            .map_err(serde::de::Error::custom)?;
        Ok(record)
    }
}

impl Serialize for SubmissionSettlement {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        match self {
            Self::Done { answer } => {
                map.serialize_entry("status", "done")?;
                map.serialize_entry("answer", answer)?;
            }
            Self::Unanswered { reason, detail } => {
                map.serialize_entry("status", "unanswered")?;
                map.serialize_entry("reason", reason)?;
                if let Some(detail) = detail {
                    map.serialize_entry("detail", detail)?;
                }
            }
        }
        map.end()
    }
}
impl<'de> Deserialize<'de> for SubmissionSettlement {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut fields = Fields::deserialize(deserializer)?;
        let status: String = required(&mut fields, "status").map_err(serde::de::Error::custom)?;
        let settlement = match status.as_str() {
            "done" => Self::Done {
                answer: required(&mut fields, "answer").map_err(serde::de::Error::custom)?,
            },
            "unanswered" => Self::Unanswered {
                reason: required(&mut fields, "reason").map_err(serde::de::Error::custom)?,
                detail: optional_payload(&mut fields, "detail")
                    .map_err(serde::de::Error::custom)?,
            },
            _ => return Err(serde::de::Error::custom("Invalid submission settlement")),
        };
        exhausted(fields, "submission settlement").map_err(serde::de::Error::custom)?;
        settlement
            .validate_payloads()
            .map_err(serde::de::Error::custom)?;
        Ok(settlement)
    }
}
