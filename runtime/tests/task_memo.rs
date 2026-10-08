use futures_channel::oneshot;
use futures_lite::future::{block_on, poll_once, zip};
use publicworks_runtime::*;
#[path = "../src/test_support/scaffold.rs"]
mod scaffold;
#[path = "../src/test_support/storage.rs"]
mod storage_scaffold;
use scaffold::Gate;
use serde_json::{Value, json};
use std::{
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
};

fn phase<F, Fut>(f: F) -> PhaseHandler
where
    F: Fn(TaskRecord, TaskRuntime) -> Fut + 'static,
    Fut: Future<Output = Result<(), TaskOutcomeError>> + 'static,
{
    Rc::new(move |task, runtime| Box::pin(f(task, runtime)))
}

fn definition(
    kind: &'static str,
    phases: impl IntoIterator<Item = (&'static str, PhaseHandler)>,
) -> TaskDefinition {
    TaskDefinition::new(
        kind,
        1,
        |_| Ok(json!({"phase":"work"})),
        phases
            .into_iter()
            .map(|(name, phase)| (name.into(), phase))
            .collect(),
    )
}

fn options(owner: Option<Id>) -> TaskOptions {
    TaskOptions {
        conversation_id: Some(ROOT_CONVERSATION),
        ownership: owner.map_or(TaskOwnership::Conversation, TaskOwnership::Task),
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

async fn create(session: &Session, definition: TaskDefinition, owner: Option<Id>) -> TaskRecord {
    session
        .commit(move |tx| {
            Box::pin(async move {
                tx.create_task(definition, json!({"input":true}), options(owner))
                    .await
            })
        })
        .await
        .unwrap()
        .value
}

async fn read(session: &Session, id: Id) -> TaskRecord {
    session
        .commit(move |tx| Box::pin(async move { Ok(tx.task(id).await?.unwrap()) }))
        .await
        .unwrap()
        .value
}

fn complete(value: Value) -> Option<TaskUpdate> {
    Some(TaskUpdate::Complete(value))
}

fn invalid_memo_value() -> Value {
    let mut value = Value::Null;
    for _ in 0..MAX_JSON_DEPTH {
        value = Value::Array(vec![value]);
    }
    value
}

#[test]
fn fresh_get_insert_and_existing_winner_preserve_the_task_and_sequences() {
    block_on(async {
        let definition = definition(
            "memo-basic",
            [(
                "work",
                phase(|task, runtime| async move {
                    let missing = runtime.memo("missing").await.unwrap();
                    assert_eq!(missing.value, None);
                    assert_eq!(missing.seq, None);

                    let first = runtime
                        .memo_or_insert("answer", json!({"stable":42}))
                        .await
                        .unwrap();
                    let first_seq = first.seq.expect("a fresh memo must be persisted");
                    assert_eq!(first.value, json!({"stable":42}));

                    let read_back = runtime.memo("answer").await.unwrap();
                    assert_eq!(read_back.value, Some(first.value.clone()));
                    assert_eq!(read_back.seq, None);
                    let duplicate = runtime
                        .memo_or_insert("answer", json!({"stable":99}))
                        .await
                        .unwrap();
                    assert_eq!(duplicate.value, first.value);
                    assert_eq!(duplicate.seq, None);

                    for (name, value) in [
                        ("", json!("empty")),
                        ("__proto__", json!("opaque")),
                        ("$serde_json::private::Number", json!("ordinary-name")),
                        ("$serde_json::private::RawValue", json!("ordinary-name")),
                    ] {
                        let inserted = runtime.memo_or_insert(name, value.clone()).await.unwrap();
                        assert_eq!(inserted.value, value);
                        assert!(inserted.seq.is_some());
                    }

                    let current = runtime
                        .read(move |_, current| {
                            Box::pin(async move {
                                assert_eq!(current.id, task.id);
                                assert_eq!(current.conversation_id, task.conversation_id);
                                assert_eq!(current.kind, task.kind);
                                assert_eq!(current.version, task.version);
                                assert_eq!(current.input, task.input);
                                assert_eq!(current.owner, task.owner);
                                assert_eq!(current.background, task.background);
                                assert_eq!(current.abort_requested, task.abort_requested);
                                Ok(current)
                            })
                        })
                        .await
                        .unwrap();
                    assert_eq!(current.seq, None);
                    assert_eq!(
                        current.value.memos.as_ref().unwrap()["answer"],
                        json!({"stable":42})
                    );

                    let last = runtime.memo_or_insert("last", json!(true)).await.unwrap();
                    assert_eq!(last.seq.unwrap().get(), first_seq.get() + 5);
                    runtime
                        .commit(|_, _| Box::pin(async { Ok(complete(json!("done"))) }))
                        .await
                        .unwrap();
                    Ok(())
                }),
            )],
        );
        let (session, session_driver) = Session::new(memory().await);
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = create(&session, definition, None).await;
                let RunResult::Terminal(task) = runner.run(task.id).await.unwrap() else {
                    panic!("expected terminal task")
                };
                assert!(task.memos.is_none());
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    });
}

#[test]
fn eager_fifo_competitors_and_dropped_waiters_cannot_replace_the_first_writer() {
    block_on(async {
        let definition = definition(
            "memo-fifo",
            [(
                "work",
                phase(|_, runtime| async move {
                    let first = runtime.memo_or_insert("same", json!("first"));
                    let second = runtime.memo_or_insert("same", json!("second"));
                    let third = runtime.memo_or_insert("same", json!("third"));
                    drop(first);
                    assert_eq!(second.await.unwrap().value, json!("first"));
                    assert_eq!(third.await.unwrap().value, json!("first"));

                    drop(runtime.memo_or_insert("dropped", json!({"kept":true})));
                    assert_eq!(
                        runtime.memo("dropped").await.unwrap().value,
                        Some(json!({"kept":true}))
                    );
                    runtime
                        .commit(|_, _| Box::pin(async { Ok(complete(Value::Null)) }))
                        .await
                        .unwrap();
                    Ok(())
                }),
            )],
        );
        let (session, session_driver) = Session::new(memory().await);
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = create(&session, definition, None).await;
                assert!(matches!(
                    runner.run(task.id).await.unwrap(),
                    RunResult::Terminal(_)
                ));
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    });
}

#[test]
fn native_json_null_and_reserved_objects_survive_a_checkpoint() {
    block_on(async {
        let payload = json!({
            "signed": i64::MIN,
            "unsigned": u64::MAX,
            "$serde_json::private::Number": "ordinary value",
            "nested": {"$serde_json::private::RawValue": "also ordinary"}
        });
        let definition = definition(
            "memo-json",
            [
                (
                    "work",
                    phase({
                        let payload = payload.clone();
                        move |_, runtime| {
                            let payload = payload.clone();
                            async move {
                                assert!(
                                    runtime
                                        .memo_or_insert("native", payload)
                                        .await
                                        .unwrap()
                                        .seq
                                        .is_some()
                                );
                                assert!(
                                    runtime
                                        .memo_or_insert("null", Value::Null)
                                        .await
                                        .unwrap()
                                        .seq
                                        .is_some()
                                );
                                runtime
                                    .commit(|_, _| {
                                        Box::pin(async {
                                            Ok(Some(TaskUpdate::Checkpoint(
                                                json!({"phase":"verify"}),
                                            )))
                                        })
                                    })
                                    .await
                                    .unwrap();
                                Ok(())
                            }
                        }
                    }),
                ),
                (
                    "verify",
                    phase({
                        let payload = payload.clone();
                        move |task, runtime| {
                            let payload = payload.clone();
                            async move {
                                let memos = task.memos.as_ref().unwrap();
                                assert_eq!(memos["native"], payload);
                                assert_eq!(memos.get("null"), Some(&Value::Null));
                                assert_eq!(runtime.memo("missing").await.unwrap().value, None);
                                assert_eq!(
                                    runtime.memo("null").await.unwrap().value,
                                    Some(Value::Null)
                                );
                                runtime
                                    .commit(|_, _| Box::pin(async { Ok(complete(json!(true))) }))
                                    .await
                                    .unwrap();
                                Ok(())
                            }
                        }
                    }),
                ),
            ],
        );
        let (session, session_driver) = Session::new(memory().await);
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = create(&session, definition, None).await;
                assert!(matches!(
                    runner.run(task.id).await.unwrap(),
                    RunResult::Terminal(_)
                ));
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    });
}

#[test]
fn invalid_missing_candidates_are_rejected_without_poisoning_but_losers_are_ignored() {
    block_on(async {
        let definition = definition(
            "memo-validation",
            [(
                "work",
                phase(|_, runtime| async move {
                    let invalid = invalid_memo_value();
                    assert!(matches!(
                        runtime.memo_or_insert("bad", invalid.clone()).await,
                        Err(SessionError::Storage(StorageError::Other(_)))
                    ));
                    assert_eq!(runtime.memo("bad").await.unwrap().value, None);

                    assert_eq!(
                        runtime
                            .memo_or_insert("good", json!("winner"))
                            .await
                            .unwrap()
                            .value,
                        json!("winner")
                    );
                    let ignored = runtime.memo_or_insert("good", invalid).await.unwrap();
                    assert_eq!(ignored.value, json!("winner"));
                    assert_eq!(ignored.seq, None);

                    assert_eq!(
                        runtime
                            .memo_or_insert("after", json!("session-open"))
                            .await
                            .unwrap()
                            .value,
                        json!("session-open")
                    );
                    runtime
                        .commit(|_, _| Box::pin(async { Ok(complete(Value::Null)) }))
                        .await
                        .unwrap();
                    Ok(())
                }),
            )],
        );
        let (session, session_driver) = Session::new(memory().await);
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = create(&session, definition, None).await;
                assert!(matches!(
                    runner.run(task.id).await.unwrap(),
                    RunResult::Terminal(_)
                ));
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    });
}

#[test]
fn memo_only_work_faults_and_outcomes_clear_terminal_and_held_memos() {
    block_on(async {
        let no_progress = definition(
            "memo-no-progress",
            [(
                "work",
                phase(|_, runtime| async move {
                    runtime
                        .memo_or_insert("temporary", json!(true))
                        .await
                        .unwrap();
                    Ok(())
                }),
            )],
        );
        let (session, session_driver) = Session::new(memory().await);
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([no_progress.clone()]).unwrap())
                .unwrap();
        zip(
            async {
                let task = create(&session, no_progress, None).await;
                let RunResult::Terminal(task) = runner.run(task.id).await.unwrap() else {
                    panic!("expected terminal task")
                };
                assert!(matches!(
                    task.state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Faulted { .. }
                    }
                ));
                assert!(task.memos.is_none());
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    });

    block_on(async {
        let child_active = Gate::default();
        let child_release = Gate::default();
        let child = definition(
            "memo-held-child",
            [(
                "work",
                phase({
                    let child_active = child_active.clone();
                    let child_release = child_release.clone();
                    move |_, runtime| {
                        let child_active = child_active.clone();
                        let child_release = child_release.clone();
                        async move {
                            child_active.release();
                            child_release.wait().await;
                            runtime
                                .commit(|_, _| Box::pin(async { Ok(complete(json!("child"))) }))
                                .await
                                .unwrap();
                            Ok(())
                        }
                    }
                }),
            )],
        );
        let parent = definition(
            "memo-held-parent",
            [
                (
                    "work",
                    phase({
                        let child = child.clone();
                        move |_, runtime| {
                            let child = child.clone();
                            async move {
                                runtime
                                    .memo_or_insert("temporary", json!(true))
                                    .await
                                    .unwrap();
                                runtime
                                    .commit(move |tx, task| {
                                        Box::pin(async move {
                                            tx.create_task(
                                                child,
                                                Value::Null,
                                                options(Some(task.id)),
                                            )
                                            .await?;
                                            Ok(Some(TaskUpdate::Checkpoint(
                                                json!({"phase":"finish"}),
                                            )))
                                        })
                                    })
                                    .await
                                    .unwrap();
                                Ok(())
                            }
                        }
                    }),
                ),
                (
                    "finish",
                    phase(|_, runtime| async move {
                        runtime
                            .commit(|_, _| Box::pin(async { Ok(complete(json!("parent"))) }))
                            .await
                            .unwrap();
                        Ok(())
                    }),
                ),
            ],
        );
        let (session, session_driver) = Session::new(memory().await);
        let (runner, task_driver) = TaskRunner::attach(
            &session,
            TaskRegistry::new([parent.clone(), child]).unwrap(),
        )
        .unwrap();
        zip(
            async {
                let root = create(&session, parent, None).await;
                let run = runner.run(root.id);
                child_active.wait().await;
                let held = read(&session, root.id).await;
                assert!(matches!(held.state, TaskState::Completing { .. }));
                assert!(held.memos.is_none());
                child_release.release();
                assert!(matches!(run.await.unwrap(), RunResult::Terminal(_)));
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    });
}

#[test]
fn abort_mode_can_use_memos_while_marked_normal_and_stale_runtimes_cannot() {
    block_on(async {
        let entered = Gate::default();
        let normal_rejected = Rc::new(Cell::new(false));
        let stale = Rc::new(RefCell::new(None));
        let normal = definition(
            "memo-abort",
            [(
                "work",
                phase({
                    let entered = entered.clone();
                    let normal_rejected = normal_rejected.clone();
                    let stale = stale.clone();
                    move |_, runtime| {
                        *stale.borrow_mut() = Some(runtime.clone());
                        let entered = entered.clone();
                        let normal_rejected = normal_rejected.clone();
                        async move {
                            runtime
                                .memo_or_insert("normal", json!("kept"))
                                .await
                                .unwrap();
                            entered.release();
                            runtime.cancelled().await;
                            normal_rejected.set(runtime.memo("normal").await.is_err());
                            Ok(())
                        }
                    }
                }),
            )],
        )
        .with_abort_handler(phase(|task, runtime| async move {
            assert!(task.abort_requested);
            assert_eq!(
                runtime.memo("normal").await.unwrap().value,
                Some(json!("kept"))
            );
            assert_eq!(
                runtime
                    .memo_or_insert("cleanup", json!("allowed"))
                    .await
                    .unwrap()
                    .value,
                json!("allowed")
            );
            runtime
                .commit(|_, _| {
                    Box::pin(async {
                        Ok(Some(TaskUpdate::Abort {
                            reason: Some("test".into()),
                            result: None,
                        }))
                    })
                })
                .await
                .unwrap();
            Ok(())
        }));
        let (session, session_driver) = Session::new(memory().await);
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([normal.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = create(&session, normal, None).await;
                let run = runner.run(task.id);
                entered.wait().await;
                assert_eq!(runner.abort(task.id).await.unwrap(), AbortResult::Marked);
                let RunResult::Terminal(task) = run.await.unwrap() else {
                    panic!("expected terminal task")
                };
                assert!(normal_rejected.get());
                assert!(task.memos.is_none());
                let stale = stale.borrow().as_ref().unwrap().clone();
                assert!(stale.memo("normal").await.is_err());
                assert!(stale.memo_or_insert("late", json!(true)).await.is_err());
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
        [(
            "work",
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
        )],
    );
    (definition, receiver)
}

async fn tick(driver: &mut (impl Future<Output = ()> + Unpin)) {
    assert!(poll_once(driver).await.is_none());
}

async fn start_captured(
    session_driver: &mut SessionDriver,
    task_driver: &mut TaskDriver,
    runner: &TaskRunner,
    id: Id,
    mut receiver: oneshot::Receiver<TaskRuntime>,
) -> (RunWaiter, TaskRuntime) {
    let run = runner.run(id);
    for _ in 0..16 {
        if let Some(runtime) = poll_once(&mut receiver).await {
            return (run, runtime.unwrap());
        }
        tick(task_driver).await;
        tick(session_driver).await;
    }
    panic!("handler was not dispatched")
}

#[derive(Default)]
struct CommitProbe {
    gate: RefCell<Option<Gate>>,
    commits: Cell<usize>,
    closed_tasks: RefCell<Vec<TaskRecord>>,
}

struct ProbeStorage {
    memory: MemoryStorage,
    probe: Rc<CommitProbe>,
}
impl Storage for ProbeStorage {
    forward_storage_methods!(memory;
        mint_id,
        conversation,
        scan_conversations,
        task,
        scan_tasks,
        submission,
        scan_submissions,
        submission_by_request,
        conversation_state,
        entry,
        visible_entry,
        scan_entries,
        find_latest_head_marker,
    );
    fn commit(&mut self, writes: Vec<StorageWrite>) -> StorageFuture<'_, Seq> {
        Box::pin(async move {
            self.probe.commits.set(self.probe.commits.get() + 1);
            let gate = self.probe.gate.borrow().clone();
            if let Some(gate) = gate {
                gate.wait().await;
            }
            self.memory.commit(writes).await
        })
    }
    fn close(&mut self) -> StorageFuture<'_, ()> {
        Box::pin(async move {
            *self.probe.closed_tasks.borrow_mut() = self
                .memory
                .scan_tasks(TaskQuery::default(), 100, None)
                .await?
                .items;
            self.memory.close().await
        })
    }
}

async fn seeded_probe(kind: &str, probe: Rc<CommitProbe>) -> ProbeStorage {
    let mut memory = memory().await;
    memory
        .commit(vec![StorageWrite::Task(TaskRecord {
            id: Id::new(2).unwrap(),
            conversation_id: ROOT_CONVERSATION,
            kind: kind.into(),
            version: 1,
            input: Value::Null,
            owner: None,
            background: false,
            abort_requested: false,
            state: TaskState::Pending {
                checkpoint: json!({"phase":"work"}),
            },
            memos: None,
        })])
        .await
        .unwrap();
    ProbeStorage { memory, probe }
}

#[test]
fn runner_close_rejects_queued_memos_but_allows_started_persistence_to_settle() {
    block_on(async {
        let (definition, runtime) = captured_definition("memo-close-queued");
        let (session, session_driver) = Session::new(memory().await);
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = create(&session, definition, None).await;
                let run = runner.run(task.id);
                let runtime = runtime.await.unwrap();
                let gate = Gate::default();
                let blocker = session.commit({
                    let gate = gate.clone();
                    move |_| {
                        Box::pin(async move {
                            gate.wait().await;
                            Ok(())
                        })
                    }
                });
                let memo = runtime.memo_or_insert("queued", json!(true));
                let close = runner.close();
                gate.release();
                blocker.await.unwrap();
                assert!(matches!(memo.await, Err(SessionError::Invalid(_))));
                close.await.unwrap();
                assert_eq!(run.await.unwrap(), RunResult::Interrupted);
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    });

    block_on(async {
        let probe = Rc::new(CommitProbe::default());
        let gate = Gate::default();
        let (definition, runtime) = captured_definition("memo-close-started");
        let storage = seeded_probe("memo-close-started", probe.clone()).await;
        let (session, mut session_driver) = Session::new(storage);
        let (runner, mut task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition]).unwrap()).unwrap();
        let (run, runtime) = start_captured(
            &mut session_driver,
            &mut task_driver,
            &runner,
            Id::new(2).unwrap(),
            runtime,
        )
        .await;
        let baseline_commits = probe.commits.get();
        *probe.gate.borrow_mut() = Some(gate.clone());
        let memo = runtime.memo_or_insert("started", json!(true));
        for _ in 0..16 {
            if probe.commits.get() > baseline_commits {
                break;
            }
            tick(&mut session_driver).await;
        }
        assert_eq!(probe.commits.get(), baseline_commits + 1);
        let close = runner.close();
        gate.release();
        zip(
            async {
                assert!(memo.await.unwrap().seq.is_some());
                close.await.unwrap();
                assert_eq!(run.await.unwrap(), RunResult::Interrupted);
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
        assert_eq!(
            probe.closed_tasks.borrow()[0].memos.as_ref().unwrap()["started"],
            json!(true)
        );
    });
}

#[test]
fn task_driver_drop_fences_unstarted_memo_writes_but_not_started_storage_commits() {
    for started in [false, true] {
        block_on(async move {
            let probe = Rc::new(CommitProbe::default());
            let gate = Gate::default();
            let kind = if started {
                "memo-started"
            } else {
                "memo-unstarted"
            };
            let (definition, runtime) = captured_definition(kind);
            let storage = seeded_probe(kind, probe.clone()).await;
            let (session, mut session_driver) = Session::new(storage);
            let (runner, mut task_driver) =
                TaskRunner::attach(&session, TaskRegistry::new([definition]).unwrap()).unwrap();
            let (run, runtime) = start_captured(
                &mut session_driver,
                &mut task_driver,
                &runner,
                Id::new(2).unwrap(),
                runtime,
            )
            .await;
            let baseline_commits = probe.commits.get();
            if started {
                *probe.gate.borrow_mut() = Some(gate.clone());
            }
            let memo = runtime.memo_or_insert("boundary", json!("winner"));
            if started {
                for _ in 0..16 {
                    if probe.commits.get() > baseline_commits {
                        break;
                    }
                    tick(&mut session_driver).await;
                }
                assert_eq!(probe.commits.get(), baseline_commits + 1);
            }
            drop(task_driver);
            gate.release();

            let result = zip(
                async {
                    let result = memo.await;
                    session.close().await.unwrap();
                    result
                },
                &mut session_driver,
            )
            .await
            .0;
            assert!(matches!(run.await, Err(RunError::DriverStopped)));
            if started {
                let receipt = result.unwrap();
                assert!(receipt.seq.is_some());
                assert_eq!(probe.commits.get(), baseline_commits + 1);
                assert_eq!(
                    probe.closed_tasks.borrow()[0].memos.as_ref().unwrap()["boundary"],
                    json!("winner")
                );
            } else {
                assert!(matches!(result, Err(SessionError::Invalid(_))));
                assert_eq!(probe.commits.get(), baseline_commits);
                assert!(probe.closed_tasks.borrow()[0].memos.is_none());
            }
        });
    }
}

#[path = "support/phase_lifetime.rs"]
mod phase_lifetime;

#[test]
fn phase_handoff_joins_dropped_mutations_and_fences_retained_runtime() {
    block_on(phase_lifetime::phase_handoff(MemoryStorage::new()));
}
