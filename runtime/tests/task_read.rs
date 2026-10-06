use futures_channel::oneshot;
use futures_lite::future::{block_on, poll_once, zip};
use publicworks_runtime::*;
use serde_json::{Value, json};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    future::Future,
    rc::Rc,
    task::Waker,
};

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

    fn released(&self) -> bool {
        self.0.borrow().0
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

fn phase<F, Fut>(f: F) -> PhaseHandler
where
    F: Fn(TaskRecord, TaskRuntime) -> Fut + 'static,
    Fut: Future<Output = Result<(), TaskOutcomeError>> + 'static,
{
    Rc::new(move |task, runtime| Box::pin(f(task, runtime)))
}

fn definition(kind: &'static str, handler: PhaseHandler) -> TaskDefinition {
    TaskDefinition::new(
        kind,
        1,
        |_| Ok(json!({"phase":"work"})),
        BTreeMap::from([("work".into(), handler)]),
    )
}

fn options() -> TaskOptions {
    TaskOptions {
        conversation_id: Some(ROOT_CONVERSATION),
        ownership: TaskOwnership::Conversation,
        background: false,
    }
}

async fn memory() -> MemoryStorage {
    let mut storage = MemoryStorage::new();
    storage
        .commit(vec![StorageWrite::Conversation(ConversationRecord {
            id: ROOT_CONVERSATION,
            parent: None,
            owner: None,
        })])
        .await
        .unwrap();
    storage
}

async fn create(session: &Session, definition: TaskDefinition) -> TaskRecord {
    session
        .commit(move |tx| {
            Box::pin(async move {
                tx.create_task(definition, json!({"input":true}), options())
                    .await
            })
        })
        .await
        .unwrap()
        .value
}

fn complete() -> Option<TaskUpdate> {
    Some(TaskUpdate::Complete(json!("done")))
}

#[derive(Debug, PartialEq)]
struct Projected {
    task_id: Id,
    entry_kind: String,
    child_outcome: TaskOutcome,
    memo: Value,
    native: Value,
}

#[test]
fn read_returns_typed_projected_history_without_a_storage_commit() {
    block_on(async {
        let opaque = json!({"unsigned":u64::MAX, "signed":i64::MIN, "nested":[null, true]});
        let observed = Rc::new(RefCell::new(None));
        let stale = Rc::new(RefCell::new(None));
        let definition = definition(
            "project",
            phase({
                let observed = observed.clone();
                let stale = stale.clone();
                move |task, runtime| {
                    *stale.borrow_mut() = Some(runtime.clone());
                    let observed = observed.clone();
                    async move {
                        let receipt = runtime
                            .read(move |tx, current| {
                                Box::pin(async move {
                                    let stored = tx.task(current.id).await?.unwrap();
                                    let entry = tx.entry(Id::new(2).unwrap()).await?.unwrap();
                                    let child = tx.task(Id::new(4).unwrap()).await?.unwrap();
                                    let TaskState::Terminal {
                                        outcome: child_outcome,
                                    } = child.state
                                    else {
                                        panic!("expected terminal child")
                                    };
                                    let scanned = tx
                                        .scan_entries(
                                            EntryQuery::new(current.conversation_id),
                                            10,
                                            None,
                                        )
                                        .await?;
                                    assert_eq!(stored, current);
                                    assert_eq!(scanned.items, vec![entry.entry.clone()]);
                                    Ok(Projected {
                                        task_id: current.id,
                                        entry_kind: entry.entry.kind,
                                        child_outcome,
                                        memo: current.memos.unwrap()["projection"].clone(),
                                        native: entry.entry.data.unwrap(),
                                    })
                                })
                            })
                            .await
                            .unwrap();
                        assert_eq!(receipt.seq, None);
                        assert_eq!(receipt.value.task_id, task.id);
                        *observed.borrow_mut() = Some(receipt.value);
                        runtime
                            .commit(|_, _| Box::pin(async { Ok(complete()) }))
                            .await
                            .unwrap();
                        Ok(())
                    }
                }
            }),
        );

        let mut storage = memory().await;
        let entry = EntryRecord {
            id: Id::new(2).unwrap(),
            conversation_id: ROOT_CONVERSATION,
            kind: "history".into(),
            model: Some(vec![opaque.clone()]),
            data: Some(opaque.clone()),
            head: None,
            edits: None,
            by_task_id: None,
        };
        let task = TaskRecord {
            id: Id::new(3).unwrap(),
            conversation_id: ROOT_CONVERSATION,
            kind: "project".into(),
            version: 1,
            input: opaque.clone(),
            owner: None,
            background: false,
            abort_requested: false,
            state: TaskState::Pending {
                checkpoint: json!({"phase":"work", "native":opaque}),
            },
            memos: Some(BTreeMap::from([(
                "projection".into(),
                json!({"held":true}),
            )])),
        };
        let child = TaskRecord {
            id: Id::new(4).unwrap(),
            conversation_id: ROOT_CONVERSATION,
            kind: "finished-child".into(),
            version: 1,
            input: Value::Null,
            owner: Some(task.id),
            background: false,
            abort_requested: false,
            state: TaskState::Terminal {
                outcome: TaskOutcome::Completed {
                    result: opaque.clone(),
                },
            },
            memos: None,
        };
        storage
            .commit(vec![
                StorageWrite::Entry(entry),
                StorageWrite::Task(task.clone()),
                StorageWrite::Task(child),
            ])
            .await
            .unwrap();

        let (session, session_driver) = Session::new(storage);
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition]).unwrap()).unwrap();
        zip(
            async {
                assert!(matches!(
                    runner.run(task.id).await.unwrap(),
                    RunResult::Terminal(_)
                ));
                assert_eq!(
                    observed.borrow().as_ref().unwrap(),
                    &Projected {
                        task_id: task.id,
                        entry_kind: "history".into(),
                        child_outcome: TaskOutcome::Completed {
                            result: json!({
                                "unsigned":u64::MAX,
                                "signed":i64::MIN,
                                "nested":[null, true]
                            }),
                        },
                        memo: json!({"held":true}),
                        native: json!({
                            "unsigned":u64::MAX,
                            "signed":i64::MIN,
                            "nested":[null, true]
                        }),
                    }
                );
                let stale = stale.borrow().as_ref().unwrap().clone();
                assert!(
                    stale
                        .read(|_, _| {
                            Box::pin(async {
                                panic!("stale read callback ran");
                                #[allow(unreachable_code)]
                                Ok(())
                            })
                        })
                        .await
                        .is_err()
                );
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    });
}

#[test]
fn read_only_tx_rejects_every_public_mutation_before_side_effects() {
    block_on(async {
        let initial_calls = Rc::new(Cell::new(0));
        let denied_initial = TaskDefinition::new(
            "denied",
            1,
            {
                let initial_calls = initial_calls.clone();
                move |_| {
                    initial_calls.set(initial_calls.get() + 1);
                    Ok(json!({"phase":"none"}))
                }
            },
            BTreeMap::new(),
        );
        let result = Rc::new(RefCell::new(None));
        let definition = definition(
            "readonly",
            phase({
                let result = result.clone();
                move |_, runtime| {
                    let denied_initial = denied_initial.clone();
                    let result = result.clone();
                    async move {
                        let receipt = runtime
                            .read(move |tx, current| {
                                Box::pin(async move {
                                    assert!(tx.task(current.id).await?.is_some());
                                    let invalid = |error| matches!(error, SessionError::Invalid(_));
                                    assert!(invalid(tx.create_conversation().await.unwrap_err()));
                                    assert_eq!(
                                        tx.task(current.id).await.unwrap_err(),
                                        SessionError::ReadAfterWrite
                                    );
                                    assert!(invalid(
                                        tx.create_conversation_owned(Owner::Task(current.id))
                                            .await
                                            .unwrap_err()
                                    ));
                                    assert!(invalid(
                                        tx.fork_conversation(
                                            current.conversation_id,
                                            Id::new(99).unwrap(),
                                        )
                                        .await
                                        .unwrap_err()
                                    ));
                                    assert!(invalid(
                                        tx.fork_conversation_owned(
                                            current.conversation_id,
                                            Id::new(99).unwrap(),
                                            Owner::Task(current.id),
                                        )
                                        .await
                                        .unwrap_err()
                                    ));
                                    assert!(invalid(
                                        tx.append_entry(
                                            current.conversation_id,
                                            EntryDraft::new("denied"),
                                        )
                                        .await
                                        .unwrap_err()
                                    ));
                                    assert!(invalid(
                                        tx.create_task(
                                            denied_initial,
                                            Value::Null,
                                            TaskOptions {
                                                conversation_id: None,
                                                ownership: TaskOwnership::Task(current.id),
                                                background: false,
                                            },
                                        )
                                        .await
                                        .unwrap_err()
                                    ));
                                    // Handled method errors do not make callback settlement sticky.
                                    Ok("handled")
                                })
                            })
                            .await
                            .unwrap();
                        assert_eq!(receipt.seq, None);
                        *result.borrow_mut() = Some(receipt.value);
                        runtime
                            .commit(|_, _| Box::pin(async { Ok(complete()) }))
                            .await
                            .unwrap();
                        Ok(())
                    }
                }
            }),
        );
        let (session, session_driver) = Session::new(memory().await);
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = create(&session, definition).await;
                assert!(matches!(
                    runner.run(task.id).await.unwrap(),
                    RunResult::Terminal(_)
                ));
                assert_eq!(*result.borrow(), Some("handled"));
                assert_eq!(initial_calls.get(), 0);
                // The task consumed ID 2; denied mutations consumed none.
                let next = session
                    .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
                    .await
                    .unwrap()
                    .value;
                assert_eq!(next.id, Id::new(3).unwrap());
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    });
}

#[test]
fn dropped_unpolled_read_still_runs_before_later_runtime_work() {
    block_on(async {
        let calls = Rc::new(Cell::new(0));
        let definition = definition(
            "dropped",
            phase({
                let calls = calls.clone();
                move |_, runtime| {
                    let calls = calls.clone();
                    async move {
                        drop(runtime.read(move |tx, current| {
                            calls.set(calls.get() + 1);
                            Box::pin(async move {
                                assert!(tx.task(current.id).await?.is_some());
                                Ok(())
                            })
                        }));
                        runtime
                            .commit(|_, _| Box::pin(async { Ok(complete()) }))
                            .await
                            .unwrap();
                        Ok(())
                    }
                }
            }),
        );
        let (session, session_driver) = Session::new(memory().await);
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = create(&session, definition).await;
                assert!(matches!(
                    runner.run(task.id).await.unwrap(),
                    RunResult::Terminal(_)
                ));
                assert_eq!(calls.get(), 1);
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    });
}

fn captured_definition(kind: &'static str) -> (TaskDefinition, oneshot::Receiver<TaskRuntime>) {
    let (sender, receiver) = oneshot::channel();
    let sender = RefCell::new(Some(sender));
    let definition = definition(
        kind,
        phase(move |_, runtime| {
            sender
                .borrow_mut()
                .take()
                .unwrap()
                .send(runtime.clone())
                .ok();
            async move {
                runtime.cancelled().await;
                Ok(())
            }
        }),
    );
    (definition, receiver)
}

#[test]
fn close_rejects_a_queued_read_but_started_read_settles() {
    for started in [false, true] {
        block_on(async move {
            let (definition, runtime) =
                captured_definition(if started { "started" } else { "queued" });
            let (session, session_driver) = Session::new(memory().await);
            let (runner, task_driver) =
                TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap())
                    .unwrap();
            zip(
                async {
                    let task = create(&session, definition).await;
                    let run = runner.run(task.id);
                    let runtime = runtime.await.unwrap();
                    let gate = Gate::default();
                    let entered = Gate::default();
                    let called = Rc::new(Cell::new(false));

                    let blocker = if started {
                        None
                    } else {
                        let gate = gate.clone();
                        Some(session.commit(move |_| {
                            Box::pin(async move {
                                gate.wait().await;
                                Ok(())
                            })
                        }))
                    };
                    let read = runtime.read({
                        let gate = gate.clone();
                        let entered = entered.clone();
                        let called = called.clone();
                        move |_, _| {
                            called.set(true);
                            entered.release();
                            Box::pin(async move {
                                if started {
                                    gate.wait().await;
                                }
                                Ok(17_u32)
                            })
                        }
                    });
                    if started {
                        entered.wait().await;
                    }
                    let close = runner.close();
                    gate.release();
                    if let Some(blocker) = blocker {
                        blocker.await.unwrap();
                    }
                    if started {
                        let receipt = read.await.unwrap();
                        assert_eq!(receipt.value, 17);
                        assert_eq!(receipt.seq, None);
                        assert!(called.get());
                    } else {
                        assert!(matches!(read.await, Err(SessionError::Invalid(_))));
                        assert!(!called.get());
                    }
                    close.await.unwrap();
                    assert_eq!(run.await.unwrap(), RunResult::Interrupted);
                    session.close().await.unwrap();
                },
                zip(session_driver, task_driver),
            )
            .await;
        });
    }
}

#[test]
fn normal_mode_rejects_marked_tasks_while_abort_mode_can_read_them() {
    block_on(async {
        let normal_entered = Gate::default();
        let normal_rejected = Rc::new(Cell::new(false));
        let abort_observed = Rc::new(RefCell::new(None));
        let definition = definition(
            "abort-read",
            phase({
                let normal_entered = normal_entered.clone();
                let normal_rejected = normal_rejected.clone();
                move |_, runtime| {
                    let normal_entered = normal_entered.clone();
                    let normal_rejected = normal_rejected.clone();
                    async move {
                        normal_entered.release();
                        runtime.cancelled().await;
                        let result = runtime
                            .read(|_, _| {
                                Box::pin(async {
                                    panic!("normal marked callback ran");
                                    #[allow(unreachable_code)]
                                    Ok(())
                                })
                            })
                            .await;
                        normal_rejected.set(matches!(result, Err(SessionError::Invalid(_))));
                        Ok(())
                    }
                }
            }),
        )
        .with_abort_handler(phase({
            let abort_observed = abort_observed.clone();
            move |_, runtime| {
                let abort_observed = abort_observed.clone();
                async move {
                    let receipt = runtime
                        .read(|tx, current| {
                            Box::pin(async move {
                                assert!(current.abort_requested);
                                assert_eq!(tx.task(current.id).await?, Some(current.clone()));
                                Ok(current)
                            })
                        })
                        .await
                        .unwrap();
                    assert_eq!(receipt.seq, None);
                    *abort_observed.borrow_mut() = Some(receipt.value.id);
                    runtime
                        .commit(|_, _| {
                            Box::pin(async {
                                Ok(Some(TaskUpdate::Abort {
                                    reason: None,
                                    result: None,
                                }))
                            })
                        })
                        .await
                        .unwrap();
                    Ok(())
                }
            }
        }));
        let (session, session_driver) = Session::new(memory().await);
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = create(&session, definition).await;
                let run = runner.run(task.id);
                normal_entered.wait().await;
                let abort = runner.abort(task.id);
                let (result, marked) = zip(run, abort).await;
                assert!(matches!(result.unwrap(), RunResult::Terminal(_)));
                assert_eq!(marked.unwrap(), AbortResult::Marked);
                assert!(normal_rejected.get());
                assert_eq!(*abort_observed.borrow(), Some(task.id));
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    });
}

#[derive(Default)]
struct ReadProbe {
    fail_task_read: Cell<bool>,
}

struct ProbeStorage {
    memory: MemoryStorage,
    probe: Rc<ReadProbe>,
}
impl Storage for ProbeStorage {
    fn commit(&mut self, writes: Vec<StorageWrite>) -> StorageFuture<'_, Seq> {
        self.memory.commit(writes)
    }
    fn mint_id(&mut self) -> StorageFuture<'_, Id> {
        self.memory.mint_id()
    }
    fn conversation(&mut self, id: Id) -> StorageFuture<'_, Option<ConversationRecord>> {
        self.memory.conversation(id)
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
        if self.probe.fail_task_read.replace(false) {
            Box::pin(async { Err(StorageError::Other("uncertain read".into())) })
        } else {
            self.memory.task(id)
        }
    }
    fn scan_tasks(
        &mut self,
        query: TaskQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<TaskRecord>> {
        self.memory.scan_tasks(query, limit, cursor)
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
        self.memory.close()
    }
}

#[test]
fn failed_storage_read_does_not_poison_the_session_as_an_uncertain_commit() {
    block_on(async {
        let probe = Rc::new(ReadProbe::default());
        let (definition, runtime) = captured_definition("read-failure");
        let (session, session_driver) = Session::new(ProbeStorage {
            memory: memory().await,
            probe: probe.clone(),
        });
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = create(&session, definition).await;
                let run = runner.run(task.id);
                let runtime = runtime.await.unwrap();
                probe.fail_task_read.set(true);
                assert!(matches!(
                    runtime.read(|_, _| Box::pin(async { Ok(()) })).await,
                    Err(SessionError::Storage(StorageError::Other(message))) if message == "uncertain read"
                ));
                let marker = session
                    .commit(|tx| {
                        Box::pin(async move {
                            tx.append_entry(ROOT_CONVERSATION, EntryDraft::new("still open"))
                                .await
                        })
                    })
                    .await
                    .unwrap();
                assert!(marker.seq.is_some());
                runner.close().await.unwrap();
                assert_eq!(run.await.unwrap(), RunResult::Interrupted);
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    });
}

async fn tick<F: Future<Output = ()> + Unpin>(future: &mut F) {
    assert!(poll_once(future).await.is_none());
}

async fn start_captured(
    session_driver: &mut SessionDriver,
    task_driver: &mut TaskDriver,
    runner: &TaskRunner,
    task: Id,
    mut runtime: oneshot::Receiver<TaskRuntime>,
) -> (RunWaiter, TaskRuntime) {
    let run = runner.run(task);
    for _ in 0..12 {
        if let Some(runtime) = poll_once(&mut runtime).await {
            return (run, runtime.unwrap());
        }
        tick(task_driver).await;
        tick(session_driver).await;
    }
    panic!("handler was not dispatched")
}

async fn drive_commit<T>(
    mut waiter: CommitWaiter<T>,
    driver: &mut SessionDriver,
) -> Result<CommitReceipt<T>, SessionError> {
    loop {
        if let Some(result) = poll_once(&mut waiter).await {
            return result;
        }
        tick(driver).await;
    }
}

#[test]
fn driver_drops_forfeit_read_callbacks_and_receipts() {
    block_on(async {
        // TaskDriver drop after callback entry fences even a write-free read receipt.
        let (definition, runtime) = captured_definition("task-driver-drop");
        let mut storage = memory().await;
        let task = TaskRecord {
            id: Id::new(2).unwrap(),
            conversation_id: ROOT_CONVERSATION,
            kind: "task-driver-drop".into(),
            version: 1,
            input: Value::Null,
            owner: None,
            background: false,
            abort_requested: false,
            state: TaskState::Pending {
                checkpoint: json!({"phase":"work"}),
            },
            memos: None,
        };
        storage
            .commit(vec![StorageWrite::Task(task.clone())])
            .await
            .unwrap();
        let (session, mut session_driver) = Session::new(storage);
        let (runner, mut task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition]).unwrap()).unwrap();
        let (run, runtime) = start_captured(
            &mut session_driver,
            &mut task_driver,
            &runner,
            task.id,
            runtime,
        )
        .await;
        let entered = Gate::default();
        let release = Gate::default();
        let read = runtime.read({
            let entered = entered.clone();
            let release = release.clone();
            move |_, _| {
                entered.release();
                Box::pin(async move {
                    release.wait().await;
                    Ok(9_u8)
                })
            }
        });
        for _ in 0..12 {
            if entered.released() {
                break;
            }
            tick(&mut session_driver).await;
        }
        assert!(entered.released());
        drop(task_driver);
        release.release();
        assert!(matches!(
            drive_commit(read, &mut session_driver).await,
            Err(SessionError::Invalid(_))
        ));
        assert!(matches!(run.await, Err(RunError::DriverStopped)));
        drop(session_driver);

        // SessionDriver drop owns queued Session jobs and reports DriverStopped.
        let (definition, runtime) = captured_definition("session-driver-drop");
        let mut storage = memory().await;
        let mut task = task;
        task.kind = "session-driver-drop".into();
        storage
            .commit(vec![StorageWrite::Task(task.clone())])
            .await
            .unwrap();
        let (session, mut session_driver) = Session::new(storage);
        let (runner, mut task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition]).unwrap()).unwrap();
        let (_run, runtime) = start_captured(
            &mut session_driver,
            &mut task_driver,
            &runner,
            task.id,
            runtime,
        )
        .await;
        let called = Rc::new(Cell::new(false));
        let read = runtime.read({
            let called = called.clone();
            move |_, _| {
                called.set(true);
                Box::pin(async { Ok(()) })
            }
        });
        drop(session_driver);
        assert_eq!(read.await.unwrap_err(), SessionError::DriverStopped);
        assert!(!called.get());
        drop(task_driver);
    });
}
