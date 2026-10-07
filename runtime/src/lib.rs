//! Public Works' Session/storage kernel and host-polled foreground task scheduling.
//! Hosts poll a HarnessDriver, or lower-level SessionDriver and TaskDriver futures,
//! through shutdown. No executor or agent-specific policy is included.
//! Adapters are trusted. Boxed futures permit async hosts, but the built-in
//! adapters perform synchronous work when polled. See the workspace design doc.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fmt, future::Future, pin::Pin};

mod session;
pub use session::{
    AbortResult, AbortWaiter, BlockReason, CloseWaiter, CommitReceipt, CommitWaiter,
    ConversationAbortOptions, ConversationAbortWaiter, ConversationHandle, ConversationStateDraft,
    ConversationWaiter, EntryDraft, Harness, HarnessAbortWaiter, HarnessCloseWaiter, HarnessDriver,
    HarnessError, HarnessInspection, HarnessOpenWaiter, Head, IdleWaiter, InspectionWaiter, Owner,
    PhaseFuture, PhaseHandler, RunError, RunResult, RunWaiter, RunnerCloseWaiter, Session,
    SessionDriver, SessionError, Submission, SubmissionWaiter, TaskDefinition, TaskDriver,
    TaskInspection, TaskOptions, TaskOwnership, TaskRegistry, TaskRunner, TaskRuntime, TaskUpdate,
    TaskWaiter, Tx, TxFuture, WithdrawalResult,
};

mod task;
pub use task::{
    JoinPolicy, TaskOutcome, TaskOutcomeError, TaskQuery, TaskRecord, TaskState, TaskStatus,
};
mod submission;
pub use submission::{
    ConversationRun, ConversationStateRecord, InboxItem, SubmissionQuery, SubmissionRecord,
    SubmissionSettlement, SubmissionState, SubmissionStatus, SubmissionType,
};
mod json;
pub use json::decode_native_json;
mod memory;
pub use memory::MemoryStorage;
#[doc(hidden)]
pub mod snapshot;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

/// Largest exactly representable JavaScript integer used by the ID wire format.
pub const MAX_NUMBER: u64 = 9_007_199_254_740_991;
/// Maximum payload container nesting, excluding the record envelope.
/// Includes model/edit wrappers and task state/outcome/error/memo wrappers.
pub const MAX_JSON_DEPTH: usize = 64;

macro_rules! number {
    ($name:ident) => {
        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(try_from = "u64", into = "u64")]
        pub struct $name(u64);
        impl $name {
            pub fn new(value: u64) -> Result<Self, StorageError> {
                if (1..=MAX_NUMBER).contains(&value) {
                    Ok(Self(value))
                } else {
                    Err(StorageError::Other(format!(
                        "{} out of range: {value}",
                        stringify!($name)
                    )))
                }
            }
            pub fn get(self) -> u64 {
                self.0
            }
        }
        impl TryFrom<u64> for $name {
            type Error = StorageError;
            fn try_from(value: u64) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }
        impl From<$name> for u64 {
            fn from(value: $name) -> Self {
                value.0
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}
// IDs share one namespace; record roles are expressed by fields, not separate brands.
number!(Id);
number!(Seq);
pub const ROOT_CONVERSATION: Id = Id(1);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ParentLink {
    pub conversation_id: Id,
    pub at: Id,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OwnerLink {
    pub conversation_id: Id,
    pub task_id: Id,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationRecord {
    pub id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<ParentLink>,
    /// Task ownership, validated against final candidates by Session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<OwnerLink>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "camelCase",
    try_from = "json::EditFields"
)]
pub enum ContextEdit {
    Omit { target: Id },
    Replace { target: Id, messages: Vec<Value> },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EntryRecord {
    pub id: Id,
    pub conversation_id: Id,
    pub kind: String,
    /// Opaque JSON messages; provider-specific message types are not validated.
    #[serde(
        default,
        deserialize_with = "json::model",
        skip_serializing_if = "Option::is_none"
    )]
    pub model: Option<Vec<Value>>,
    /// Absence and explicit JSON null are distinct, including across serde roundtrips.
    #[serde(
        default,
        deserialize_with = "json::data",
        skip_serializing_if = "Option::is_none"
    )]
    pub data: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edits: Option<Vec<ContextEdit>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by_task_id: Option<Id>,
}
impl EntryRecord {
    /// Validate the storage payload domain without recursively walking caller data.
    /// Scalars have depth zero; each array or object adds one level. Invalid
    /// payloads are ordinary errors, not recoverable Session rejections.
    pub fn validate_payloads(&self) -> Result<(), StorageError> {
        let mut pending = Vec::new();
        if let Some(data) = &self.data {
            pending.push((data, 0));
        }
        if let Some(model) = &self.model {
            pending.extend(model.iter().map(|v| (v, 1)));
        }
        if let Some(edits) = &self.edits {
            for edit in edits {
                if let ContextEdit::Replace { messages, .. } = edit {
                    pending.extend(messages.iter().map(|v| (v, 3)));
                }
            }
        }
        json::validate(pending)
    }

    pub fn new(id: Id, conversation_id: Id, kind: impl Into<String>) -> Self {
        Self {
            id,
            conversation_id,
            kind: kind.into(),
            model: None,
            data: None,
            head: None,
            edits: None,
            by_task_id: None,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "camelCase",
    try_from = "json::WriteFields"
)]
pub enum StorageWrite {
    Conversation(ConversationRecord),
    Entry(EntryRecord),
    Task(TaskRecord),
    Submission(SubmissionRecord),
    ConversationState(ConversationStateRecord),
}
impl StorageWrite {
    pub fn id(&self) -> Id {
        match self {
            Self::Conversation(r) => r.id,
            Self::Entry(r) => r.id,
            Self::Task(r) => r.id,
            Self::Submission(r) => r.id,
            Self::ConversationState(r) => r.id,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredEntry {
    pub entry: EntryRecord,
    pub commit_seq: Seq,
}

/// Opaque continuation state. Only round-trip to the same scan and query.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    after: Id,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<Cursor>,
}
#[derive(Clone, Debug, Default)]
pub struct ConversationQuery {
    pub owner_conversation_id: Option<Id>,
    pub owner_task_id: Option<Id>,
}
#[derive(Clone, Debug)]
pub struct EntryQuery {
    pub conversation_id: Id,
    pub min_entry_id: Option<Id>,
    pub max_entry_id: Option<Id>,
}
impl EntryQuery {
    pub fn new(conversation_id: Id) -> Self {
        Self {
            conversation_id,
            min_entry_id: None,
            max_entry_id: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageError {
    /// Explicit guarantee that this operation had no effect. Safe rejection only.
    Rejected(String),
    /// No no-effect guarantee. A caller must treat a failed commit as potentially committed.
    Other(String),
}
impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(s) => write!(f, "storage rejected: {s}"),
            Self::Other(s) => write!(f, "storage error (outcome not guaranteed): {s}"),
        }
    }
}
impl std::error::Error for StorageError {}
pub type StorageFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, StorageError>> + 'a>>;

/// Atomic storage seam. One logical owner is expected; no CAS or process-owner lock.
/// Methods take an exclusive borrow, including reads. Futures need not be Send.
/// A dropped, unpolled future does nothing in the built-in adapters; once polled,
/// synchronous operations settle in that poll. This is NOT Session cancellation ownership.
/// Semantic references/ancestry and task ownership belong to Session.
pub trait Storage {
    /// Ordered atomic batch, including empty batches, consumes one global Seq on success.
    /// Duplicate/global ID collisions are ordinary Other errors, not safe Rejected errors.
    fn commit(&mut self, writes: Vec<StorageWrite>) -> StorageFuture<'_, Seq>;
    fn mint_id(&mut self) -> StorageFuture<'_, Id>;
    fn conversation(&mut self, id: Id) -> StorageFuture<'_, Option<ConversationRecord>>;
    fn scan_conversations(
        &mut self,
        query: ConversationQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<ConversationRecord>>;
    fn task(&mut self, id: Id) -> StorageFuture<'_, Option<TaskRecord>>;
    fn scan_tasks(
        &mut self,
        query: TaskQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<TaskRecord>>;
    fn submission(&mut self, id: Id) -> StorageFuture<'_, Option<SubmissionRecord>>;
    fn scan_submissions(
        &mut self,
        query: SubmissionQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<SubmissionRecord>>;
    fn submission_by_request(
        &mut self,
        conversation_id: Id,
        request_id: &str,
    ) -> StorageFuture<'_, Option<SubmissionRecord>>;
    /// Return the dedicated admission state for a conversation, when present.
    fn conversation_state(
        &mut self,
        conversation_id: Id,
    ) -> StorageFuture<'_, Option<ConversationStateRecord>>;
    fn entry(&mut self, id: Id) -> StorageFuture<'_, Option<StoredEntry>>;
    fn visible_entry(&mut self, conversation: Id, id: Id)
    -> StorageFuture<'_, Option<StoredEntry>>;
    fn scan_entries(
        &mut self,
        query: EntryQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<EntryRecord>>;
    fn find_latest_head_marker(
        &mut self,
        conversation: Id,
        at_or_before: Option<Id>,
    ) -> StorageFuture<'_, Option<EntryRecord>>;
    /// Closes admission permanently. Even repeated close rejects.
    fn close(&mut self) -> StorageFuture<'_, ()>;
}
