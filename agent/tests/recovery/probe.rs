use super::*;

type StorageSlot = Rc<RefCell<Option<Box<dyn Storage>>>>;

// Keep MemoryStorage alive across driver loss; SQLite is genuinely dropped and
// reopened. Both run the real agent phases, not handcrafted execute records.
pub(super) struct Store {
    db: Option<Database>,
    memory: StorageSlot,
}
impl Store {
    pub(super) fn new(sqlite: bool) -> Self {
        Self {
            db: sqlite.then(Database::new),
            memory: Rc::new(RefCell::new(
                (!sqlite).then(|| Box::new(MemoryStorage::new()) as Box<dyn Storage>),
            )),
        }
    }
    pub(super) fn open(&self, cut: Cut, reached: Gate, intent: Rc<Cell<bool>>) -> ProbeStorage {
        ProbeStorage {
            inner: Some(match &self.db {
                Some(db) => Box::new(db.open()),
                None => self
                    .memory
                    .borrow_mut()
                    .take()
                    .expect("previous driver dropped"),
            }),
            restore: self.db.is_none().then(|| self.memory.clone()),
            cut,
            reached,
            intent,
            release: Gate::default(),
        }
    }
}
pub(super) struct ProbeStorage {
    inner: Option<Box<dyn Storage>>,
    restore: Option<StorageSlot>,
    cut: Cut,
    pub(super) release: Gate,
    reached: Gate,
    intent: Rc<Cell<bool>>,
}
impl ProbeStorage {
    fn inner(&mut self) -> &mut dyn Storage {
        self.inner.as_mut().unwrap().as_mut()
    }
}
impl Drop for ProbeStorage {
    fn drop(&mut self) {
        if let Some(slot) = &self.restore {
            *slot.borrow_mut() = self.inner.take();
        }
    }
}
impl Storage for ProbeStorage {
    fn commit(&mut self, writes: Vec<StorageWrite>) -> StorageFuture<'_, Seq> {
        Box::pin(async move {
            let intent = writes.iter().any(|w| matches!(w, StorageWrite::Task(t) if t.kind == TOOL_KIND && matches!(&t.state, TaskState::Running {checkpoint} if checkpoint["phase"] == "execute")));
            let result = writes
                .iter()
                .any(|w| matches!(w, StorageWrite::Entry(e) if e.kind == "agent.toolResult"));
            if intent && self.cut == Cut::BeforeIntentCommit {
                self.reached.release();
                self.release.wait().await;
            }
            let seq = self.inner().commit(writes).await?;
            if intent {
                self.intent.set(true);
            }
            if (intent && self.cut == Cut::IntentAck) || (result && self.cut == Cut::ResultAck) {
                self.reached.release();
                self.release.wait().await;
            }
            Ok(seq)
        })
    }
    fn mint_id(&mut self) -> StorageFuture<'_, Id> {
        self.inner().mint_id()
    }
    fn conversation(&mut self, id: Id) -> StorageFuture<'_, Option<ConversationRecord>> {
        self.inner().conversation(id)
    }
    fn scan_conversations(
        &mut self,
        query: ConversationQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<ConversationRecord>> {
        self.inner().scan_conversations(query, limit, cursor)
    }
    fn task(&mut self, id: Id) -> StorageFuture<'_, Option<TaskRecord>> {
        self.inner().task(id)
    }
    fn scan_tasks(
        &mut self,
        query: TaskQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<TaskRecord>> {
        self.inner().scan_tasks(query, limit, cursor)
    }
    fn submission(&mut self, id: Id) -> StorageFuture<'_, Option<SubmissionRecord>> {
        self.inner().submission(id)
    }
    fn scan_submissions(
        &mut self,
        query: SubmissionQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<SubmissionRecord>> {
        self.inner().scan_submissions(query, limit, cursor)
    }
    fn submission_by_request(
        &mut self,
        conversation_id: Id,
        request_id: &str,
    ) -> StorageFuture<'_, Option<SubmissionRecord>> {
        self.inner()
            .submission_by_request(conversation_id, request_id)
    }
    fn conversation_state(
        &mut self,
        conversation_id: Id,
    ) -> StorageFuture<'_, Option<ConversationStateRecord>> {
        self.inner().conversation_state(conversation_id)
    }
    fn entry(&mut self, id: Id) -> StorageFuture<'_, Option<StoredEntry>> {
        self.inner().entry(id)
    }
    fn visible_entry(
        &mut self,
        conversation: Id,
        id: Id,
    ) -> StorageFuture<'_, Option<StoredEntry>> {
        self.inner().visible_entry(conversation, id)
    }
    fn scan_entries(
        &mut self,
        query: EntryQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<EntryRecord>> {
        self.inner().scan_entries(query, limit, cursor)
    }
    fn find_latest_head_marker(
        &mut self,
        conversation: Id,
        at: Option<Id>,
    ) -> StorageFuture<'_, Option<EntryRecord>> {
        self.inner().find_latest_head_marker(conversation, at)
    }
    fn close(&mut self) -> StorageFuture<'_, ()> {
        self.inner().close()
    }
}
