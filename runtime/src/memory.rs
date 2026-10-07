use crate::{snapshot::RecordSnapshot, *};

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
            let mut candidate = self.records.clone();
            let mut next_id = self.next_id;
            for write in writes {
                let id = write.id();
                let claimed = candidate.conversations.contains_key(&id)
                    || candidate.entries.contains_key(&id)
                    || candidate.tasks.contains_key(&id)
                    || candidate.submissions.contains_key(&id)
                    || candidate.conversation_states.contains_key(&id);
                let replaceable = match &write {
                    StorageWrite::Task(_) => candidate.tasks.contains_key(&id),
                    StorageWrite::Submission(_) => candidate.submissions.contains_key(&id),
                    StorageWrite::ConversationState(_) => {
                        candidate.conversation_states.contains_key(&id)
                    }
                    StorageWrite::Conversation(_) | StorageWrite::Entry(_) => false,
                };
                if claimed && !replaceable {
                    return Err(StorageError::Other(format!("ID {id} is already claimed")));
                }
                next_id = next_id.max(id.get() + 1);
                match write {
                    StorageWrite::Task(record) => {
                        candidate.tasks.insert(id, record);
                    }
                    StorageWrite::Submission(record) => {
                        candidate.submissions.insert(id, record);
                    }
                    StorageWrite::ConversationState(record) => {
                        candidate.conversation_states.insert(id, record);
                    }
                    StorageWrite::Conversation(record) => {
                        candidate.conversations.insert(id, record);
                    }
                    StorageWrite::Entry(entry) => {
                        candidate.entries.insert(
                            id,
                            StoredEntry {
                                entry,
                                commit_seq: seq,
                            },
                        );
                    }
                }
            }
            self.records = candidate;
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
