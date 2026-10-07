use crate::{snapshot::RecordSnapshot, *};
use std::collections::BTreeMap;

/// Built-in reference adapter. Detached values and atomic batches, no persistence.
pub struct MemoryStorage {
    records: RecordSnapshot,
    next_id: u64,
    next_seq: u64,
    closed: bool,
}
impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}
impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            records: RecordSnapshot::default(),
            next_id: 2,
            next_seq: 1,
            closed: false,
        }
    }
    fn open(&self) -> Result<(), StorageError> {
        if self.closed {
            Err(StorageError::Rejected("Storage is closed".into()))
        } else {
            Ok(())
        }
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum RecordKind {
    Conversation,
    Entry,
    Task,
    Submission,
    ConversationState,
}
impl RecordKind {
    fn of(write: &StorageWrite) -> Self {
        match write {
            StorageWrite::Conversation(_) => Self::Conversation,
            StorageWrite::Entry(_) => Self::Entry,
            StorageWrite::Task(_) => Self::Task,
            StorageWrite::Submission(_) => Self::Submission,
            StorageWrite::ConversationState(_) => Self::ConversationState,
        }
    }
    fn replaceable(self) -> bool {
        matches!(
            self,
            Self::Task | Self::Submission | Self::ConversationState
        )
    }
}
impl MemoryStorage {
    fn kind(&self, id: Id) -> Option<RecordKind> {
        if self.records.conversations.contains_key(&id) {
            Some(RecordKind::Conversation)
        } else if self.records.entries.contains_key(&id) {
            Some(RecordKind::Entry)
        } else if self.records.tasks.contains_key(&id) {
            Some(RecordKind::Task)
        } else if self.records.submissions.contains_key(&id) {
            Some(RecordKind::Submission)
        } else if self.records.conversation_states.contains_key(&id) {
            Some(RecordKind::ConversationState)
        } else {
            None
        }
    }
}
impl Storage for MemoryStorage {
    fn commit(&mut self, writes: Vec<StorageWrite>) -> StorageFuture<'_, Seq> {
        Box::pin(async move {
            self.open()?;
            for write in &writes {
                match write {
                    StorageWrite::Entry(entry) => entry.validate_payloads()?,
                    StorageWrite::Task(task) => task.validate_payloads()?,
                    StorageWrite::Submission(submission) => submission.validate_payloads()?,
                    StorageWrite::ConversationState(state) => state.validate_payloads()?,
                    StorageWrite::Conversation(_) => {}
                }
            }
            let seq = Seq::new(self.next_seq)?;
            // Preflight only the IDs in this batch. Cloning the complete store made
            // every small commit proportional to retained history and payload size.
            // No operation below this pass is fallible, so direct adoption remains
            // atomic while replacements retain their ordered last-write-wins rule.
            let mut staged = BTreeMap::new();
            let mut next_id = self.next_id;
            for write in &writes {
                let id = write.id();
                let incoming = RecordKind::of(write);
                let current = staged.get(&id).copied().or_else(|| self.kind(id));
                if current.is_some_and(|kind| kind != incoming || !incoming.replaceable()) {
                    return Err(StorageError::Other(format!("ID {id} is already claimed")));
                }
                staged.insert(id, incoming);
                next_id = next_id.max(id.get() + 1);
            }
            for write in writes {
                let id = write.id();
                match write {
                    StorageWrite::Task(record) => {
                        self.records.tasks.insert(id, record);
                    }
                    StorageWrite::Submission(record) => {
                        self.records.submissions.insert(id, record);
                    }
                    StorageWrite::ConversationState(record) => {
                        self.records.conversation_states.insert(id, record);
                    }
                    StorageWrite::Conversation(record) => {
                        self.records.conversations.insert(id, record);
                    }
                    StorageWrite::Entry(entry) => {
                        self.records.entries.insert(
                            id,
                            StoredEntry {
                                entry,
                                commit_seq: seq,
                            },
                        );
                    }
                }
            }
            self.next_id = next_id;
            self.next_seq += 1;
            Ok(seq)
        })
    }
    fn mint_id(&mut self) -> StorageFuture<'_, Id> {
        Box::pin(async move {
            self.open()?;
            let id = Id::new(self.next_id)?;
            self.next_id += 1;
            Ok(id)
        })
    }
    fn conversation(&mut self, id: Id) -> StorageFuture<'_, Option<ConversationRecord>> {
        Box::pin(async move {
            self.open()?;
            Ok(self.records.conversations.get(&id).cloned())
        })
    }
    fn task(&mut self, id: Id) -> StorageFuture<'_, Option<TaskRecord>> {
        Box::pin(async move {
            self.open()?;
            Ok(self.records.task(id))
        })
    }
    fn scan_tasks(
        &mut self,
        query: TaskQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<TaskRecord>> {
        Box::pin(async move {
            self.open()?;
            self.records.scan_tasks(query, limit, cursor)
        })
    }
    fn submission(&mut self, id: Id) -> StorageFuture<'_, Option<SubmissionRecord>> {
        Box::pin(async move {
            self.open()?;
            Ok(self.records.submission(id))
        })
    }
    fn scan_submissions(
        &mut self,
        query: SubmissionQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<SubmissionRecord>> {
        Box::pin(async move {
            self.open()?;
            self.records.scan_submissions(query, limit, cursor)
        })
    }
    fn submission_by_request(
        &mut self,
        conversation_id: Id,
        request_id: &str,
    ) -> StorageFuture<'_, Option<SubmissionRecord>> {
        let request_id = request_id.to_owned();
        Box::pin(async move {
            self.open()?;
            Ok(self
                .records
                .submission_by_request(conversation_id, &request_id))
        })
    }
    fn conversation_state(
        &mut self,
        conversation_id: Id,
    ) -> StorageFuture<'_, Option<ConversationStateRecord>> {
        Box::pin(async move {
            self.open()?;
            Ok(self.records.conversation_state(conversation_id))
        })
    }
    fn entry(&mut self, id: Id) -> StorageFuture<'_, Option<StoredEntry>> {
        Box::pin(async move {
            self.open()?;
            Ok(self.records.entries.get(&id).cloned())
        })
    }
    fn scan_conversations(
        &mut self,
        query: ConversationQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<ConversationRecord>> {
        Box::pin(async move {
            self.open()?;
            self.records.scan_conversations(query, limit, cursor)
        })
    }
    fn visible_entry(
        &mut self,
        conversation: Id,
        id: Id,
    ) -> StorageFuture<'_, Option<StoredEntry>> {
        Box::pin(async move {
            self.open()?;
            self.records.visible_entry(conversation, id)
        })
    }
    fn scan_entries(
        &mut self,
        query: EntryQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<EntryRecord>> {
        Box::pin(async move {
            self.open()?;
            self.records.scan_entries(query, limit, cursor)
        })
    }
    fn find_latest_head_marker(
        &mut self,
        conversation: Id,
        at: Option<Id>,
    ) -> StorageFuture<'_, Option<EntryRecord>> {
        Box::pin(async move {
            self.open()?;
            self.records.find_latest_head_marker(conversation, at)
        })
    }
    fn close(&mut self) -> StorageFuture<'_, ()> {
        Box::pin(async move {
            self.open()?;
            self.closed = true;
            Ok(())
        })
    }
}
