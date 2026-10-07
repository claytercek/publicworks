use super::*;
use rusqlite::{Row, types::FromSql};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Conversation,
    Entry,
    Task,
    Submission,
    ConversationState,
}
impl Kind {
    pub const ALL: [Self; 5] = [
        Self::Conversation,
        Self::Entry,
        Self::Task,
        Self::Submission,
        Self::ConversationState,
    ];
    pub fn name(self) -> &'static str {
        match self {
            Self::Conversation => "conversation",
            Self::Entry => "entry",
            Self::Task => "task",
            Self::Submission => "submission",
            Self::ConversationState => "conversation_state",
        }
    }
    pub fn table(self) -> &'static str {
        match self {
            Self::Conversation => "publicworks_conversations",
            Self::Entry => "publicworks_entries",
            Self::Task => "publicworks_tasks",
            Self::Submission => "publicworks_submissions",
            Self::ConversationState => "publicworks_conversation_states",
        }
    }
    pub fn of(write: &StorageWrite) -> Self {
        match write {
            StorageWrite::Conversation(_) => Self::Conversation,
            StorageWrite::Entry(_) => Self::Entry,
            StorageWrite::Task(_) => Self::Task,
            StorageWrite::Submission(_) => Self::Submission,
            StorageWrite::ConversationState(_) => Self::ConversationState,
        }
    }
    pub fn mutable(self) -> bool {
        matches!(
            self,
            Self::Task | Self::Submission | Self::ConversationState
        )
    }
    pub fn select(self, suffix: &str) -> String {
        format!(
            "SELECT t.*, r.kind AS registry_kind, r.commit_seq FROM {} t LEFT JOIN publicworks_ids r ON r.id=t.id {suffix}",
            self.table()
        )
    }
    pub fn decode(self, row: &Row<'_>) -> Result<(StorageWrite, Seq), StorageError> {
        let registry: String = get(row, "registry_kind")?;
        if registry != self.name() {
            return Err(other("Typed record/registry kind mismatch"));
        }
        let seq = Seq::new(get(row, "commit_seq")?)?;
        let id = id(row, "id")?;
        let record = match self {
            Self::Conversation => {
                let parent = match (
                    optional_id(row, "parent_conversation_id")?,
                    optional_id(row, "parent_at")?,
                ) {
                    (None, None) => None,
                    (Some(conversation_id), Some(at)) => Some(ParentLink {
                        conversation_id,
                        at,
                    }),
                    _ => return Err(other("Incomplete parent link")),
                };
                let owner = match (
                    optional_id(row, "owner_conversation_id")?,
                    optional_id(row, "owner_task_id")?,
                ) {
                    (None, None) => None,
                    (Some(conversation_id), Some(task_id)) => Some(OwnerLink {
                        conversation_id,
                        task_id,
                    }),
                    _ => return Err(other("Incomplete owner link")),
                };
                StorageWrite::Conversation(ConversationRecord { id, parent, owner })
            }
            Self::Entry => {
                let model = opaque_optional(row, "model")?
                    .map(|v| match v {
                        Value::Array(values) => Ok(values),
                        _ => Err(other("Entry model must be an array")),
                    })
                    .transpose()?;
                StorageWrite::Entry(EntryRecord {
                    id,
                    conversation_id: self::id(row, "conversation_id")?,
                    kind: get(row, "kind")?,
                    model,
                    data: opaque_optional(row, "data")?,
                    head: optional_id(row, "head")?,
                    edits: json_optional(row, "edits")?,
                    by_task_id: optional_id(row, "by_task_id")?,
                })
            }
            Self::Task => {
                // SQLite INTEGER cannot represent all u64 versions. Fixed-width
                // big-endian bytes round-trip the entire domain without JSON.
                let version: Vec<u8> = get(row, "version")?;
                let version = u64::from_be_bytes(
                    version
                        .try_into()
                        .map_err(|_| other("Invalid task version"))?,
                );
                let memos = opaque_optional(row, "memos")?
                    .map(|v| match v {
                        Value::Object(values) => Ok(values.into_iter().collect()),
                        _ => Err(other("Task memos must be an object")),
                    })
                    .transpose()?;
                let task = TaskRecord {
                    id,
                    conversation_id: self::id(row, "conversation_id")?,
                    kind: get(row, "kind")?,
                    version,
                    input: opaque(row, "input")?,
                    owner: optional_id(row, "owner")?,
                    background: boolean(row, "background")?,
                    abort_requested: boolean(row, "abort_requested")?,
                    state: json(row, "state")?,
                    memos,
                };
                if get::<String>(row, "status")? != task_status(task.status()) {
                    return Err(other("Task status/state mismatch"));
                }
                StorageWrite::Task(task)
            }
            Self::Submission => {
                let submission_type: String = get(row, "submission_type")?;
                let status: String = get(row, "status")?;
                let entry = optional_id(row, "entry")?;
                let answer = optional_id(row, "answer")?;
                let reason: Option<String> = get(row, "reason")?;
                let detail = opaque_optional(row, "detail")?;
                let state = match (
                    submission_type.as_str(),
                    status.as_str(),
                    entry,
                    answer,
                    reason,
                    detail,
                ) {
                    ("input", "queued", None, None, None, None) => SubmissionState::InputQueued,
                    ("write", "queued", None, None, None, None) => SubmissionState::WriteQueued,
                    ("input", "placed", Some(entry), None, None, None) => {
                        SubmissionState::InputPlaced { entry }
                    }
                    ("input", "done", Some(entry), Some(answer), None, None) => {
                        SubmissionState::InputDone { entry, answer }
                    }
                    ("write", "done", Some(entry), None, None, None) => {
                        SubmissionState::WriteDone { entry }
                    }
                    ("input", "unanswered", entry, None, Some(reason), detail) => {
                        SubmissionState::InputUnanswered {
                            entry,
                            reason,
                            detail,
                        }
                    }
                    ("write", "unanswered", None, None, Some(reason), detail) => {
                        SubmissionState::WriteUnanswered { reason, detail }
                    }
                    _ => return Err(other("Invalid submission state columns")),
                };
                StorageWrite::Submission(SubmissionRecord {
                    id,
                    conversation_id: self::id(row, "conversation_id")?,
                    request_id: get(row, "request_id")?,
                    state,
                })
            }
            Self::ConversationState => StorageWrite::ConversationState(ConversationStateRecord {
                id,
                conversation_id: self::id(row, "conversation_id")?,
                run: json_optional(row, "run")?,
                inbox: json(row, "inbox")?,
                agent_config: opaque_optional(row, "agent_config")?,
            }),
        };
        validate(&record)?;
        Ok((record, seq))
    }
}
fn get<T: FromSql>(row: &Row<'_>, column: &str) -> Result<T, StorageError> {
    row.get(column).map_err(other)
}
fn id(row: &Row<'_>, column: &str) -> Result<Id, StorageError> {
    Id::new(get(row, column)?)
}
fn optional_id(row: &Row<'_>, column: &str) -> Result<Option<Id>, StorageError> {
    get::<Option<u64>>(row, column)?.map(Id::new).transpose()
}
fn boolean(row: &Row<'_>, column: &str) -> Result<bool, StorageError> {
    match get::<i64>(row, column)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(other("Invalid stored boolean")),
    }
}
fn json<T: DeserializeOwned>(row: &Row<'_>, column: &str) -> Result<T, StorageError> {
    serde_json::from_str(&get::<String>(row, column)?).map_err(other)
}
fn json_optional<T: DeserializeOwned>(
    row: &Row<'_>,
    column: &str,
) -> Result<Option<T>, StorageError> {
    get::<Option<String>>(row, column)?
        .map(|s| serde_json::from_str(&s).map_err(other))
        .transpose()
}
fn opaque(row: &Row<'_>, column: &str) -> Result<Value, StorageError> {
    decode_native_json(get::<String>(row, column)?.as_bytes())
}
fn opaque_optional(row: &Row<'_>, column: &str) -> Result<Option<Value>, StorageError> {
    get::<Option<String>>(row, column)?
        .map(|s| decode_native_json(s.as_bytes()))
        .transpose()
}
fn encode<T: Serialize>(value: &T) -> Result<String, StorageError> {
    serde_json::to_string(value).map_err(other)
}
fn encode_optional<T: Serialize>(value: &Option<T>) -> Result<Option<String>, StorageError> {
    value.as_ref().map(encode).transpose()
}
fn number(value: Option<Id>) -> Option<u64> {
    value.map(Id::get)
}
pub(crate) fn task_status(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Pending => "pending",
        TaskStatus::Running => "running",
        TaskStatus::Waiting => "waiting",
        TaskStatus::Completing => "completing",
        TaskStatus::Terminal => "terminal",
    }
}
pub(crate) fn submission_status(status: SubmissionStatus) -> &'static str {
    match status {
        SubmissionStatus::Queued => "queued",
        SubmissionStatus::Placed => "placed",
        SubmissionStatus::Done => "done",
        SubmissionStatus::Unanswered => "unanswered",
    }
}
pub(crate) fn validate(write: &StorageWrite) -> Result<(), StorageError> {
    match write {
        StorageWrite::Conversation(_) => Ok(()),
        StorageWrite::Entry(r) => r.validate_payloads(),
        StorageWrite::Task(r) => r.validate_payloads(),
        StorageWrite::Submission(r) => r.validate_payloads(),
        StorageWrite::ConversationState(r) => r.validate_payloads(),
    }
}

pub(crate) fn write(tx: &Connection, write: &StorageWrite, seq: Seq) -> Result<(), StorageError> {
    let kind = Kind::of(write);
    let id = write.id().get();
    let existing: Option<String> = tx
        .prepare_cached("SELECT kind FROM publicworks_ids WHERE id=?1")
        .map_err(other)?
        .query_row([id], |r| r.get(0))
        .optional()
        .map_err(other)?;
    match existing {
        Some(existing) if existing == kind.name() && kind.mutable() => {
            execute(
                tx,
                "UPDATE publicworks_ids SET commit_seq=?1 WHERE id=?2",
                params![seq.get(), id],
            )
            .map_err(other)?;
        }
        Some(_) => return Err(other(format!("Duplicate/global ID collision: {id}"))),
        None => {
            execute(
                tx,
                "INSERT INTO publicworks_ids VALUES (?1,?2,?3)",
                params![id, kind.name(), seq.get()],
            )
            .map_err(other)?;
        }
    }
    match write {
        StorageWrite::Conversation(r) => {
            execute(
                tx,
                "INSERT INTO publicworks_conversations VALUES (?1,?2,?3,?4,?5)",
                params![
                    id,
                    r.parent.as_ref().map(|p| p.conversation_id.get()),
                    r.parent.as_ref().map(|p| p.at.get()),
                    r.owner.as_ref().map(|p| p.conversation_id.get()),
                    r.owner.as_ref().map(|p| p.task_id.get())
                ],
            )
            .map_err(other)?;
        }
        StorageWrite::Entry(r) => {
            execute(
                tx,
                "INSERT INTO publicworks_entries VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    id,
                    r.conversation_id.get(),
                    r.kind,
                    encode_optional(&r.model)?,
                    encode_optional(&r.data)?,
                    number(r.head),
                    encode_optional(&r.edits)?,
                    number(r.by_task_id)
                ],
            )
            .map_err(other)?;
        }
        StorageWrite::Task(r) => {
            execute(tx, "INSERT INTO publicworks_tasks VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
                ON CONFLICT(id) DO UPDATE SET conversation_id=excluded.conversation_id,kind=excluded.kind,version=excluded.version,
                input=excluded.input,owner=excluded.owner,background=excluded.background,abort_requested=excluded.abort_requested,
                status=excluded.status,state=excluded.state,memos=excluded.memos",params![id,r.conversation_id.get(),r.kind,
                r.version.to_be_bytes().as_slice(),encode(&r.input)?,number(r.owner),r.background,r.abort_requested,task_status(r.status()),
                encode(&r.state)?,encode_optional(&r.memos)?]).map_err(other)?;
        }
        StorageWrite::Submission(r) => {
            let (entry, answer, reason, detail) = match &r.state {
                SubmissionState::InputQueued | SubmissionState::WriteQueued => {
                    (None, None, None, None)
                }
                SubmissionState::InputPlaced { entry } | SubmissionState::WriteDone { entry } => {
                    (Some(*entry), None, None, None)
                }
                SubmissionState::InputDone { entry, answer } => {
                    (Some(*entry), Some(*answer), None, None)
                }
                SubmissionState::InputUnanswered {
                    entry,
                    reason,
                    detail,
                } => (*entry, None, Some(reason.as_str()), detail.as_ref()),
                SubmissionState::WriteUnanswered { reason, detail } => {
                    (None, None, Some(reason.as_str()), detail.as_ref())
                }
            };
            let submission_type = match r.submission_type() {
                SubmissionType::Input => "input",
                SubmissionType::Write => "write",
            };
            execute(tx, "INSERT INTO publicworks_submissions VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)
                ON CONFLICT(id) DO UPDATE SET conversation_id=excluded.conversation_id,request_id=excluded.request_id,
                submission_type=excluded.submission_type,status=excluded.status,entry=excluded.entry,answer=excluded.answer,
                reason=excluded.reason,detail=excluded.detail",params![id,r.conversation_id.get(),r.request_id,submission_type,
                submission_status(r.status()),number(entry),number(answer),reason,detail.map(encode).transpose()?]).map_err(other)?;
        }
        StorageWrite::ConversationState(r) => {
            execute(tx, "INSERT INTO publicworks_conversation_states VALUES (?1,?2,?3,?4,?5)
                ON CONFLICT(id) DO UPDATE SET conversation_id=excluded.conversation_id,run=excluded.run,inbox=excluded.inbox,agent_config=excluded.agent_config",
                params![id,r.conversation_id.get(),encode_optional(&r.run)?,encode(&r.inbox)?,encode_optional(&r.agent_config)?]).map_err(other)?;
        }
    }
    Ok(())
}

pub(crate) trait Record: Sized {
    const KIND: Kind;
    fn from_stored(write: StorageWrite, seq: Seq) -> Self;
    fn id(&self) -> Id;
}
macro_rules! record {
    ($ty:ty,$kind:ident) => {
        impl Record for $ty {
            const KIND: Kind = Kind::$kind;
            fn from_stored(write: StorageWrite, _: Seq) -> Self {
                let StorageWrite::$kind(record) = write else {
                    unreachable!("decoder kind is static")
                };
                record
            }
            fn id(&self) -> Id {
                self.id
            }
        }
    };
}
record!(ConversationRecord, Conversation);
record!(TaskRecord, Task);
record!(SubmissionRecord, Submission);
record!(ConversationStateRecord, ConversationState);
impl Record for StoredEntry {
    const KIND: Kind = Kind::Entry;
    fn from_stored(write: StorageWrite, commit_seq: Seq) -> Self {
        let StorageWrite::Entry(entry) = write else {
            unreachable!("decoder kind is static")
        };
        Self { entry, commit_seq }
    }
    fn id(&self) -> Id {
        self.entry.id
    }
}

// Batch commits reuse the same small set of statements instead of recompiling
// DDL-aware INSERT/UPSERT programs for every record.
pub(crate) fn execute(
    tx: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
) -> rusqlite::Result<usize> {
    tx.prepare_cached(sql)?.execute(params)
}
