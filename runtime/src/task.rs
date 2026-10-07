//! Durable task records only; these types do not execute or schedule work.
use crate::*;
use std::collections::BTreeMap;

mod decode;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum JoinPolicy {
    FailFast,
    AllSettled,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TaskStatus {
    Pending,
    Running,
    Waiting,
    Completing,
    Terminal,
}
#[derive(Clone, Debug, Default)]
pub struct TaskQuery {
    pub conversation_id: Option<Id>,
    pub owner: Option<Id>,
    pub kind: Option<String>,
    pub status: Option<TaskStatus>,
    pub abort_requested: Option<bool>,
    pub background: Option<bool>,
}
impl TaskQuery {
    pub fn matches(&self, record: &TaskRecord) -> bool {
        self.conversation_id
            .is_none_or(|v| v == record.conversation_id)
            && self.owner.is_none_or(|v| record.owner == Some(v))
            && self.kind.as_ref().is_none_or(|v| v == &record.kind)
            && self.status.is_none_or(|v| v == record.status())
            && self
                .abort_requested
                .is_none_or(|v| v == record.abort_requested)
            && self.background.is_none_or(|v| v == record.background)
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskOutcomeError {
    pub message: String,
    #[serde(
        default,
        deserialize_with = "json::data",
        skip_serializing_if = "Option::is_none"
    )]
    pub detail: Option<Value>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "status",
    rename_all = "camelCase",
    try_from = "decode::OutcomeFields"
)]
pub enum TaskOutcome {
    Completed {
        result: Value,
    },
    Failed {
        error: TaskOutcomeError,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<Value>,
    },
    Aborted {
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<Value>,
    },
    Orphaned {
        reason: String,
    },
    Faulted {
        error: TaskOutcomeError,
    },
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "status",
    rename_all = "camelCase",
    try_from = "decode::StateFields"
)]
pub enum TaskState {
    Pending {
        checkpoint: Value,
    },
    Running {
        checkpoint: Value,
    },
    Waiting {
        checkpoint: Value,
        on: Vec<Id>,
        policy: JoinPolicy,
    },
    Completing {
        outcome: TaskOutcome,
    },
    Terminal {
        outcome: TaskOutcome,
    },
}
impl TaskState {
    pub fn status(&self) -> TaskStatus {
        match self {
            Self::Pending { .. } => TaskStatus::Pending,
            Self::Running { .. } => TaskStatus::Running,
            Self::Waiting { .. } => TaskStatus::Waiting,
            Self::Completing { .. } => TaskStatus::Completing,
            Self::Terminal { .. } => TaskStatus::Terminal,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", try_from = "decode::RecordFields")]
pub struct TaskRecord {
    pub id: Id,
    pub conversation_id: Id,
    pub kind: String,
    pub version: u64,
    pub input: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<Id>,
    pub background: bool,
    pub abort_requested: bool,
    pub state: TaskState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memos: Option<BTreeMap<String, Value>>,
}
impl TaskRecord {
    pub fn status(&self) -> TaskStatus {
        self.state.status()
    }
    /// Validate the native JSON domain, counting state/outcome/error/memo wrappers.
    /// Storage does not validate scheduler transitions or semantic references.
    pub fn validate_payloads(&self) -> Result<(), StorageError> {
        let mut values = vec![(&self.input, 0)];
        match &self.state {
            TaskState::Pending { checkpoint }
            | TaskState::Running { checkpoint }
            | TaskState::Waiting { checkpoint, .. } => values.push((checkpoint, 1)),
            TaskState::Completing { outcome } | TaskState::Terminal { outcome } => {
                if self.memos.is_some() {
                    return Err(StorageError::Other(
                        "Completing/terminal tasks cannot retain memos".into(),
                    ));
                }
                match outcome {
                    TaskOutcome::Completed { result } => values.push((result, 2)),
                    TaskOutcome::Failed { error, result } => {
                        if let Some(result) = result {
                            values.push((result, 2));
                        }
                        if let Some(detail) = &error.detail {
                            values.push((detail, 3));
                        }
                    }
                    TaskOutcome::Aborted { result, .. } => {
                        if let Some(result) = result {
                            values.push((result, 2));
                        }
                    }
                    TaskOutcome::Faulted { error } => {
                        if let Some(detail) = &error.detail {
                            values.push((detail, 3));
                        }
                    }
                    TaskOutcome::Orphaned { .. } => {}
                }
            }
        }
        if let Some(memos) = &self.memos {
            values.extend(memos.values().map(|v| (v, 1)));
        }
        json::validate(values)
    }
}
