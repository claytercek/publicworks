#[derive(Clone, Default)]
struct Gate(Rc<RefCell<(bool, Option<Waker>)>>);
impl Gate {
    async fn wait(&self) {
        std::future::poll_fn(|cx| {
            let mut state = self.0.borrow_mut();
            if state.0 {
                std::task::Poll::Ready(())
            } else {
                state.1 = Some(cx.waker().clone());
                std::task::Poll::Pending
            }
        })
        .await
    }
    fn release(&self) {
        let waker = {
            let mut state = self.0.borrow_mut();
            state.0 = true;
            state.1.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}
#[derive(Clone, Copy, Default)]
enum Fault {
    #[default]
    None,
    Rejected,
    Before,
    After,
    ConstructPanic,
    PollPanic,
}
#[derive(Default)]
struct Probe {
    events: RefCell<Vec<&'static str>>,
    task_reads: Cell<usize>,
    scan_reads: Cell<usize>,
    scan_gate: RefCell<Option<(usize, Gate)>>,
    task_gate: RefCell<Option<(usize, Gate)>>,
    commit_gate: RefCell<Option<Gate>>,
    fault: RefCell<Fault>,
    close_error: bool,
    close_panic: bool,
    close_gate: Option<Gate>,
    closed_tasks: RefCell<Vec<TaskRecord>>,
}
struct Store {
    memory: MemoryStorage,
    probe: Rc<Probe>,
}
fn store(probe: Rc<Probe>) -> Store {
    Store {
        memory: MemoryStorage::new(),
        probe,
    }
}
impl Storage for Store {
    fn commit(&mut self, writes: Vec<StorageWrite>) -> StorageFuture<'_, Seq> {
        let fault = self.probe.fault.replace(Fault::None);
        if matches!(fault, Fault::ConstructPanic) {
            panic!("commit construction");
        }
        Box::pin(async move {
            self.probe.events.borrow_mut().push("commit entered");
            let gate = self.probe.commit_gate.borrow().clone();
            if let Some(gate) = gate {
                gate.wait().await;
            }
            match fault {
                Fault::Rejected => return Err(StorageError::Rejected("no effect".into())),
                Fault::Before => return Err(StorageError::Other("before persistence".into())),
                Fault::PollPanic => panic!("commit poll"),
                _ => {}
            }
            let seq = self.memory.commit(writes).await?;
            self.probe.events.borrow_mut().push("persisted");
            if matches!(fault, Fault::After) {
                return Err(StorageError::Other("after persistence".into()));
            }
            Ok(seq)
        })
    }
    fn mint_id(&mut self) -> StorageFuture<'_, Id> {
        Box::pin(async move {
            self.probe.events.borrow_mut().push("mint entered");
            let id = self.memory.mint_id().await?;
            self.probe.events.borrow_mut().push("mint settled");
            Ok(id)
        })
    }
    fn conversation(&mut self, id: Id) -> StorageFuture<'_, Option<ConversationRecord>> {
        Box::pin(async move {
            self.probe.events.borrow_mut().push("read entered");
            let result = self.memory.conversation(id).await;
            self.probe.events.borrow_mut().push("read settled");
            result
        })
    }
    fn scan_conversations(
        &mut self,
        query: ConversationQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<ConversationRecord>> {
        self.memory.scan_conversations(query, limit, cursor)
    }
    fn task(&mut self, id: Id) -> StorageFuture<'_, Option<TaskRecord>> {
        Box::pin(async move {
            let count = self.probe.task_reads.get() + 1;
            self.probe.task_reads.set(count);
            let gate = self.probe.task_gate.borrow().clone();
            if let Some((at, gate)) = gate && at == count { gate.wait().await; }
            self.memory.task(id).await
        })
    }
    fn scan_tasks(
        &mut self,
        query: TaskQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<TaskRecord>> {
        Box::pin(async move {
            let count = self.probe.scan_reads.get() + 1;
            self.probe.scan_reads.set(count);
            let gate = self.probe.scan_gate.borrow().clone();
            if let Some((at, gate)) = gate && at == count { gate.wait().await; }
            self.memory.scan_tasks(query, limit, cursor).await
        })
    }
    fn entry(&mut self, id: Id) -> StorageFuture<'_, Option<StoredEntry>> {
        self.memory.entry(id)
    }
    fn visible_entry(
        &mut self,
        conversation: Id,
        id: Id,
    ) -> StorageFuture<'_, Option<StoredEntry>> {
        self.memory.visible_entry(conversation, id)
    }
    fn scan_entries(
        &mut self,
        query: EntryQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<EntryRecord>> {
        self.memory.scan_entries(query, limit, cursor)
    }
    fn find_latest_head_marker(
        &mut self,
        conversation: Id,
        at: Option<Id>,
    ) -> StorageFuture<'_, Option<EntryRecord>> {
        self.memory.find_latest_head_marker(conversation, at)
    }
    fn close(&mut self) -> StorageFuture<'_, ()> {
        Box::pin(async move {
            self.probe.events.borrow_mut().push("close");
            if let Some(gate) = &self.probe.close_gate {
                gate.wait().await;
            }
            if self.probe.close_panic {
                panic!("close poll");
            }
            *self.probe.closed_tasks.borrow_mut() = self
                .memory
                .scan_tasks(TaskQuery::default(), 1000, None)
                .await?
                .items;
            self.memory.close().await?;
            if self.probe.close_error {
                Err(StorageError::Other("close failed".into()))
            } else {
                Ok(())
            }
        })
    }
}
