use futures_channel::oneshot;
use futures_lite::future::{block_on, zip};
use futures_util::future::join_all;
use publicworks_runtime::*;
use serde_json::json;
use std::{cell::Cell, collections::BTreeMap, rc::Rc};

struct FailCommitStorage {
    inner: MemoryStorage,
    fail: Rc<Cell<bool>>,
}
impl Storage for FailCommitStorage {
    fn commit(&mut self, writes: Vec<StorageWrite>) -> StorageFuture<'_, Seq> {
        if self.fail.replace(false) {
            Box::pin(async { Err(StorageError::Other("uncertain test commit".into())) })
        } else {
            self.inner.commit(writes)
        }
    }
    fn mint_id(&mut self) -> StorageFuture<'_, Id> {
        self.inner.mint_id()
    }
    fn conversation(&mut self, id: Id) -> StorageFuture<'_, Option<ConversationRecord>> {
        self.inner.conversation(id)
    }
    fn scan_conversations(
        &mut self,
        query: ConversationQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<ConversationRecord>> {
        self.inner.scan_conversations(query, limit, cursor)
    }
    fn task(&mut self, id: Id) -> StorageFuture<'_, Option<TaskRecord>> {
        self.inner.task(id)
    }
    fn scan_tasks(
        &mut self,
        query: TaskQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<TaskRecord>> {
        self.inner.scan_tasks(query, limit, cursor)
    }
    fn entry(&mut self, id: Id) -> StorageFuture<'_, Option<StoredEntry>> {
        self.inner.entry(id)
    }
    fn visible_entry(
        &mut self,
        conversation: Id,
        id: Id,
    ) -> StorageFuture<'_, Option<StoredEntry>> {
        self.inner.visible_entry(conversation, id)
    }
    fn scan_entries(
        &mut self,
        query: EntryQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<EntryRecord>> {
        self.inner.scan_entries(query, limit, cursor)
    }
    fn find_latest_head_marker(
        &mut self,
        conversation: Id,
        at_or_before: Option<Id>,
    ) -> StorageFuture<'_, Option<EntryRecord>> {
        self.inner
            .find_latest_head_marker(conversation, at_or_before)
    }
    fn close(&mut self) -> StorageFuture<'_, ()> {
        self.inner.close()
    }
}

fn record(id: u64, conversation_id: u64, state: TaskState) -> TaskRecord {
    TaskRecord {
        id: Id::new(id).unwrap(),
        conversation_id: Id::new(conversation_id).unwrap(),
        kind: "recovered".into(),
        version: 1,
        input: json!({"task": id}),
        owner: None,
        background: false,
        abort_requested: false,
        state,
        memos: Some(BTreeMap::from([("kept".into(), json!(id))])),
    }
}

fn completing_definition(
    kind: &str,
    expected_running: usize,
    calls: Rc<Cell<usize>>,
) -> TaskDefinition {
    let handler: PhaseHandler = Rc::new(move |_, runtime| {
        let calls = calls.clone();
        Box::pin(async move {
            let running = runtime
                .read(move |tx, _| {
                    Box::pin(async move {
                        Ok(tx
                            .scan_tasks(
                                TaskQuery {
                                    status: Some(TaskStatus::Running),
                                    ..TaskQuery::default()
                                },
                                128,
                                None,
                            )
                            .await?
                            .items
                            .len())
                    })
                })
                .await
                .map_err(|error| TaskOutcomeError {
                    message: error.to_string(),
                    detail: None,
                })?
                .value;
            assert_eq!(running, expected_running);
            calls.set(calls.get() + 1);
            runtime
                .commit(|_, _| Box::pin(async { Ok(Some(TaskUpdate::Complete(json!("done")))) }))
                .await
                .map_err(|error| TaskOutcomeError {
                    message: error.to_string(),
                    detail: None,
                })?;
            Ok(())
        })
    });
    TaskDefinition::new(
        kind,
        1,
        |_| Ok(json!({"phase":"run"})),
        BTreeMap::from([("run".into(), handler)]),
    )
}

#[test]
fn resume_reserves_all_roots_and_runs_them_concurrently() {
    block_on(async {
        let calls = Rc::new(Cell::new(0));
        let definition = completing_definition("work", 3, calls.clone());
        let (opening, driver) = Harness::open(
            MemoryStorage::new(),
            TaskRegistry::new([definition.clone()]).unwrap(),
        );
        let command = async move {
            let harness = opening.await.unwrap();
            let tasks = harness
                .commit(move |tx| {
                    Box::pin(async move {
                        let mut tasks = Vec::new();
                        for value in 0..3 {
                            let conversation = tx.create_conversation().await?;
                            tasks.push(
                                tx.create_task(
                                    definition.clone(),
                                    json!(value),
                                    TaskOptions {
                                        ownership: TaskOwnership::Conversation,
                                        conversation_id: Some(conversation.id),
                                        background: false,
                                    },
                                )
                                .await?,
                            );
                        }
                        Ok(tasks)
                    })
                })
                .await
                .unwrap()
                .value;

            let paused = harness.inspect().await.unwrap();
            assert!(!paused.progress_enabled);
            assert_eq!(paused.tasks.len(), 3);
            assert_eq!(calls.get(), 0);

            let waiters = tasks
                .iter()
                .map(|task| harness.wait_task(task.id))
                .collect::<Vec<_>>();
            harness.resume().unwrap();
            let settled = join_all(waiters).await;
            assert!(settled.iter().all(|result| matches!(
                result,
                Ok(TaskRecord {
                    state: TaskState::Terminal {
                        outcome: TaskOutcome::Completed { .. }
                    },
                    ..
                })
            )));
            assert_eq!(calls.get(), 3);
            assert!(harness.inspect().await.unwrap().tasks.is_empty());
            harness.close().await.unwrap();
        };
        let ((), ()) = zip(command, driver).await;
    });
}

#[test]
fn opening_normalizes_and_reconciles_without_dispatch() {
    block_on(async {
        let mut storage = MemoryStorage::new();
        let conversation = ConversationRecord {
            id: Id::new(2).unwrap(),
            parent: None,
            owner: None,
        };
        let mut terminal = record(
            3,
            2,
            TaskState::Terminal {
                outcome: TaskOutcome::Completed { result: json!(3) },
            },
        );
        terminal.memos = None;
        let mut running = record(
            4,
            2,
            TaskState::Running {
                checkpoint: json!({"phase":"run", "value":4}),
            },
        );
        let mut waiting = record(
            5,
            2,
            TaskState::Waiting {
                checkpoint: json!({"phase":"run", "value":5}),
                on: vec![terminal.id],
                policy: JoinPolicy::AllSettled,
            },
        );
        let mut completing = record(
            6,
            2,
            TaskState::Completing {
                outcome: TaskOutcome::Completed { result: json!(6) },
            },
        );
        // Final records cannot retain invocation memos.
        completing.memos = None;
        running.memos = Some(BTreeMap::from([("kept".into(), json!(4))]));
        waiting.memos = Some(BTreeMap::from([("kept".into(), json!(5))]));
        storage
            .commit(vec![
                StorageWrite::Conversation(conversation),
                StorageWrite::Task(terminal),
                StorageWrite::Task(running.clone()),
                StorageWrite::Task(waiting.clone()),
                StorageWrite::Task(completing.clone()),
            ])
            .await
            .unwrap();

        let calls = Rc::new(Cell::new(0));
        let definition = completing_definition("recovered", 2, calls.clone());
        let (opening, driver) = Harness::open(storage, TaskRegistry::new([definition]).unwrap());
        let command = async move {
            let harness = opening.await.unwrap();
            let completed = harness.wait_task(completing.id).await.unwrap();
            assert!(matches!(completed.state, TaskState::Terminal { .. }));
            assert_eq!(calls.get(), 0);

            let inspection = harness.inspect().await.unwrap();
            assert_eq!(inspection.tasks.len(), 2);
            let recovered_running = inspection
                .tasks
                .iter()
                .find(|task| task.task.id == running.id)
                .unwrap();
            assert!(matches!(
                recovered_running.task.state,
                TaskState::Pending { .. }
            ));
            assert_eq!(recovered_running.task.memos, running.memos);
            let recovered_waiting = inspection
                .tasks
                .iter()
                .find(|task| task.task.id == waiting.id)
                .unwrap();
            assert!(matches!(
                recovered_waiting.task.state,
                TaskState::Pending { .. }
            ));
            assert_eq!(recovered_waiting.task.memos, waiting.memos);
            harness.close().await.unwrap();
        };
        let ((), ()) = zip(command, driver).await;
    });
}

#[test]
fn dropped_task_waiter_does_not_cancel_handler() {
    block_on(async {
        let (entered_sender, entered_receiver) = oneshot::channel();
        let entered_sender = Rc::new(std::cell::RefCell::new(Some(entered_sender)));
        let (release_sender, release_receiver) = oneshot::channel();
        let release_receiver = Rc::new(std::cell::RefCell::new(Some(release_receiver)));
        let handler: PhaseHandler = Rc::new(move |_, runtime| {
            let entered_sender = entered_sender.clone();
            let release_receiver = release_receiver.clone();
            Box::pin(async move {
                entered_sender
                    .borrow_mut()
                    .take()
                    .unwrap()
                    .send(())
                    .unwrap();
                let release = release_receiver.borrow_mut().take().unwrap();
                release.await.unwrap();
                runtime
                    .commit(|_, _| {
                        Box::pin(async { Ok(Some(TaskUpdate::Complete(json!("done")))) })
                    })
                    .await
                    .map_err(|error| TaskOutcomeError {
                        message: error.to_string(),
                        detail: None,
                    })?;
                Ok(())
            })
        });
        let definition = TaskDefinition::new(
            "gated",
            1,
            |_| Ok(json!({"phase":"run"})),
            BTreeMap::from([("run".into(), handler)]),
        );
        let (opening, driver) = Harness::open(
            MemoryStorage::new(),
            TaskRegistry::new([definition.clone()]).unwrap(),
        );
        let command = async move {
            let harness = opening.await.unwrap();
            let task = harness
                .commit(move |tx| {
                    Box::pin(async move {
                        let conversation = tx.create_conversation().await?;
                        tx.create_task(
                            definition,
                            json!(null),
                            TaskOptions {
                                ownership: TaskOwnership::Conversation,
                                conversation_id: Some(conversation.id),
                                background: false,
                            },
                        )
                        .await
                    })
                })
                .await
                .unwrap()
                .value;
            let abandoned = harness.wait_task(task.id);
            harness.resume().unwrap();
            entered_receiver.await.unwrap();
            drop(abandoned);
            release_sender.send(()).unwrap();
            assert_eq!(
                harness.wait_task(task.id).await.unwrap().status(),
                TaskStatus::Terminal
            );
            harness.close().await.unwrap();
        };
        let ((), ()) = zip(command, driver).await;
    });
}

#[test]
fn close_signals_and_joins_handlers_before_storage_close() {
    block_on(async {
        let (entered_sender, entered_receiver) = oneshot::channel();
        let entered_sender = Rc::new(std::cell::RefCell::new(Some(entered_sender)));
        let cancelled = Rc::new(Cell::new(false));
        let late_write_rejected = Rc::new(Cell::new(false));
        let handler: PhaseHandler = Rc::new({
            let cancelled = cancelled.clone();
            let late_write_rejected = late_write_rejected.clone();
            move |_, runtime| {
                let entered_sender = entered_sender.clone();
                let cancelled = cancelled.clone();
                let late_write_rejected = late_write_rejected.clone();
                Box::pin(async move {
                    entered_sender
                        .borrow_mut()
                        .take()
                        .unwrap()
                        .send(())
                        .unwrap();
                    runtime.cancelled().await;
                    cancelled.set(true);
                    late_write_rejected.set(
                        runtime
                            .read(|_, _| Box::pin(async { Ok(()) }))
                            .await
                            .is_err(),
                    );
                    Ok(())
                })
            }
        });
        let definition = TaskDefinition::new(
            "close-gate",
            1,
            |_| Ok(json!({"phase":"run"})),
            BTreeMap::from([("run".into(), handler)]),
        );
        let (opening, driver) = Harness::open(
            MemoryStorage::new(),
            TaskRegistry::new([definition.clone()]).unwrap(),
        );
        let command = async move {
            let harness = opening.await.unwrap();
            harness
                .commit(move |tx| {
                    Box::pin(async move {
                        let conversation = tx.create_conversation().await?;
                        tx.create_task(
                            definition,
                            json!(null),
                            TaskOptions {
                                ownership: TaskOwnership::Conversation,
                                conversation_id: Some(conversation.id),
                                background: false,
                            },
                        )
                        .await
                    })
                })
                .await
                .unwrap();
            harness.resume().unwrap();
            entered_receiver.await.unwrap();
            let close = harness.close();
            assert!(matches!(
                harness.commit(|_| Box::pin(async { Ok(()) })).await,
                Err(SessionError::Closed)
            ));
            assert!(matches!(
                harness.abort(Id::new(999).unwrap()).await,
                Err(HarnessError::Closed)
            ));
            close.await.unwrap();
            assert!(cancelled.get());
            assert!(late_write_rejected.get());
        };
        let ((), ()) = zip(command, driver).await;
    });
}

#[test]
fn paused_abort_orphans_a_missing_definition() {
    block_on(async {
        let definition = TaskDefinition::new(
            "missing",
            1,
            |_| Ok(json!({"phase":"run"})),
            BTreeMap::new(),
        );
        let (opening, driver) = Harness::open(MemoryStorage::new(), TaskRegistry::default());
        let command = async move {
            let harness = opening.await.unwrap();
            let task = harness
                .commit(move |tx| {
                    Box::pin(async move {
                        let conversation = tx.create_conversation().await?;
                        tx.create_task(
                            definition,
                            json!(null),
                            TaskOptions {
                                ownership: TaskOwnership::Conversation,
                                conversation_id: Some(conversation.id),
                                background: false,
                            },
                        )
                        .await
                    })
                })
                .await
                .unwrap()
                .value;
            assert_eq!(harness.abort(task.id).await.unwrap(), AbortResult::Marked);
            let terminal = harness.wait_task(task.id).await.unwrap();
            assert!(matches!(
                terminal.state,
                TaskState::Terminal {
                    outcome: TaskOutcome::Orphaned { .. }
                }
            ));
            harness.close().await.unwrap();
        };
        let ((), ()) = zip(command, driver).await;
    });
}

#[test]
fn failed_phase_settlement_stops_without_replaying_handler() {
    block_on(async {
        let fail = Rc::new(Cell::new(false));
        let calls = Rc::new(Cell::new(0));
        let handler: PhaseHandler = Rc::new({
            let fail = fail.clone();
            let calls = calls.clone();
            move |_, _| {
                fail.set(true);
                calls.set(calls.get() + 1);
                Box::pin(async { Ok(()) })
            }
        });
        let definition = TaskDefinition::new(
            "settlement-failure",
            1,
            |_| Ok(json!({"phase":"run"})),
            BTreeMap::from([("run".into(), handler)]),
        );
        let storage = FailCommitStorage {
            inner: MemoryStorage::new(),
            fail,
        };
        let (opening, driver) =
            Harness::open(storage, TaskRegistry::new([definition.clone()]).unwrap());
        let command = async move {
            let harness = opening.await.unwrap();
            let task = harness
                .commit(move |tx| {
                    Box::pin(async move {
                        let conversation = tx.create_conversation().await?;
                        tx.create_task(
                            definition,
                            json!(null),
                            TaskOptions {
                                ownership: TaskOwnership::Conversation,
                                conversation_id: Some(conversation.id),
                                background: false,
                            },
                        )
                        .await
                    })
                })
                .await
                .unwrap()
                .value;
            let waiter = harness.wait_task(task.id);
            harness.resume().unwrap();
            assert!(matches!(waiter.await, Err(HarnessError::Closed)));
            assert_eq!(calls.get(), 1);
            assert!(matches!(
                harness.close().await,
                Err(HarnessError::Session(SessionError::Storage(
                    StorageError::Other(_)
                )))
            ));
        };
        let ((), ()) = zip(command, driver).await;
    });
}

#[test]
fn uncertain_public_commit_wakes_scheduler_and_fails_close() {
    block_on(async {
        let fail = Rc::new(Cell::new(false));
        let storage = FailCommitStorage {
            inner: MemoryStorage::new(),
            fail: fail.clone(),
        };
        let definition = TaskDefinition::new(
            "blocked",
            1,
            |_| Ok(json!({"phase":"run"})),
            BTreeMap::new(),
        );
        let (opening, driver) = Harness::open(storage, TaskRegistry::default());
        let command = async move {
            let harness = opening.await.unwrap();
            let task = harness
                .commit(move |tx| {
                    Box::pin(async move {
                        let conversation = tx.create_conversation().await?;
                        tx.create_task(
                            definition,
                            json!(null),
                            TaskOptions {
                                ownership: TaskOwnership::Conversation,
                                conversation_id: Some(conversation.id),
                                background: false,
                            },
                        )
                        .await
                    })
                })
                .await
                .unwrap()
                .value;
            let task_waiter = harness.wait_task(task.id);
            fail.set(true);
            let failed = harness
                .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
                .await;
            assert!(matches!(
                failed,
                Err(SessionError::Storage(StorageError::Other(_)))
            ));
            assert!(matches!(task_waiter.await, Err(HarnessError::Closed)));
            assert!(matches!(
                harness.close().await,
                Err(HarnessError::Session(SessionError::Poisoned))
            ));
        };
        let ((), ()) = zip(command, driver).await;
    });
}

#[test]
fn registry_replacement_wakes_blocked_work_after_resume() {
    block_on(async {
        let calls = Rc::new(Cell::new(0));
        let definition = completing_definition("late", 1, calls.clone());
        let (opening, driver) = Harness::open(MemoryStorage::new(), TaskRegistry::default());
        let command = async move {
            let harness = opening.await.unwrap();
            let task = harness
                .commit(move |tx| {
                    Box::pin(async move {
                        let conversation = tx.create_conversation().await?;
                        tx.create_task(
                            definition.clone(),
                            json!(null),
                            TaskOptions {
                                ownership: TaskOwnership::Conversation,
                                conversation_id: Some(conversation.id),
                                background: false,
                            },
                        )
                        .await
                    })
                })
                .await
                .unwrap()
                .value;
            harness.resume().unwrap();
            // The missing definition remains committed pending work.
            let inspection = harness.inspect().await.unwrap();
            assert_eq!(
                inspection.tasks[0].blocked,
                Some(BlockReason::MissingDefinition)
            );
            assert_eq!(calls.get(), 0);

            harness
                .replace_registry(
                    TaskRegistry::new([completing_definition("late", 1, calls.clone())]).unwrap(),
                )
                .unwrap();
            let terminal = harness.wait_task(task.id).await.unwrap();
            assert_eq!(terminal.status(), TaskStatus::Terminal);
            assert_eq!(calls.get(), 1);
            harness.close().await.unwrap();
        };
        let ((), ()) = zip(command, driver).await;
    });
}
