use futures_lite::future::{block_on, poll_once, zip};
use publicworks_runtime::*;
use serde_json::{Value, json};
use std::{
    cell::{Cell, RefCell},
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

fn definition(
    kind: &'static str,
    version: u64,
    initial: impl Fn(&Value) -> Result<Value, SessionError> + 'static,
    normal: PhaseHandler,
) -> TaskDefinition {
    TaskDefinition::new(
        kind,
        version,
        initial,
        [(String::from("work"), normal)].into_iter().collect(),
    )
}

fn options() -> TaskOptions {
    TaskOptions {
        conversation_id: Some(ROOT_CONVERSATION),
        ownership: TaskOwnership::Conversation,
        background: false,
    }
}

async fn create_with(
    session: &Session,
    definition: TaskDefinition,
    options: TaskOptions,
) -> TaskRecord {
    session
        .commit(move |tx| {
            Box::pin(async move {
                tx.create_task(definition, json!({"input": true}), options)
                    .await
            })
        })
        .await
        .unwrap()
        .value
}

async fn create(session: &Session, definition: TaskDefinition) -> TaskRecord {
    create_with(session, definition, options()).await
}

async fn read(session: &Session, id: Id) -> TaskRecord {
    session
        .commit(move |tx| Box::pin(async move { Ok(tx.task(id).await?.unwrap()) }))
        .await
        .unwrap()
        .value
}

async fn marker(session: &Session) -> Seq {
    session
        .commit(|tx| {
            Box::pin(async move {
                tx.append_entry(ROOT_CONVERSATION, EntryDraft::new("marker"))
                    .await
            })
        })
        .await
        .unwrap()
        .seq
        .unwrap()
}

fn terminal(result: RunResult) -> TaskRecord {
    match result {
        RunResult::Terminal(task) => task,
        other => panic!("expected terminal task, got {other:?}"),
    }
}

fn handler_error(message: &str) -> TaskOutcomeError {
    TaskOutcomeError {
        message: message.into(),
        detail: Some(json!({"source":"test"})),
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

fn opaque_payload() -> Value {
    json!({
        "unsigned": u64::MAX,
        "signed": i64::MIN,
        "$serde_json::private::Number": "ordinary object member",
        "reference": {"taskId": 777},
    })
}

fn seeded_abort_task(id: u64) -> TaskRecord {
    TaskRecord {
        id: Id::new(id).unwrap(),
        conversation_id: ROOT_CONVERSATION,
        kind: "seeded-abort".into(),
        version: 7,
        input: opaque_payload(),
        owner: None,
        background: false,
        abort_requested: true,
        state: TaskState::Pending {
            checkpoint: json!({"phase":"work", "payload":opaque_payload()}),
        },
        memos: Some([("opaque".into(), opaque_payload())].into_iter().collect()),
    }
}

#[test]
fn pending_default_abort_is_durable_and_skips_normal_execution() {
    block_on(async {
        let initialized = Rc::new(Cell::new(0));
        let definition = definition(
            "default-abort",
            1,
            {
                let initialized = initialized.clone();
                move |_| {
                    initialized.set(initialized.get() + 1);
                    // Abort cleanup is independent of normal phase dispatch.
                    Ok(json!(null))
                }
            },
            phase(|_, _| async { panic!("normal phase must not run") }),
        );
        let (session, session_driver) = Session::new(memory().await);
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();

        zip(
            async {
                let task = create(&session, definition).await;
                assert_eq!(initialized.get(), 1);
                assert_eq!(runner.abort(task.id).await.unwrap(), AbortResult::Marked);
                let marked = read(&session, task.id).await;
                assert!(marked.abort_requested);
                assert_eq!(marked.state, task.state);

                let result = terminal(runner.run(task.id).await.unwrap());
                assert_eq!(
                    result.state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Aborted {
                            reason: None,
                            result: None,
                        }
                    }
                );
                assert!(result.memos.is_none());
                assert_eq!(initialized.get(), 1);
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    });
}

#[test]
fn custom_abort_handler_may_choose_any_terminal_outcome() {
    for case in 0..3 {
        block_on(async move {
            let abort = phase(move |_, runtime| async move {
                let update = match case {
                    0 => TaskUpdate::Abort {
                        reason: Some("requested by test".into()),
                        result: Some(Value::Null),
                    },
                    1 => TaskUpdate::Complete(json!({"recovered":true})),
                    _ => TaskUpdate::Fail(handler_error("cleanup failed"), None),
                };
                runtime
                    .commit(move |_, _| Box::pin(async move { Ok(Some(update)) }))
                    .await
                    .unwrap();
                Ok(())
            });
            let definition = definition(
                "custom-outcome",
                1,
                |_| Ok(json!({"phase":"work"})),
                phase(|_, _| async { panic!("normal phase must not run") }),
            )
            .with_abort_handler(abort);
            let (session, session_driver) = Session::new(memory().await);
            let (runner, task_driver) =
                TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap())
                    .unwrap();
            zip(
                async {
                    let task = create(&session, definition).await;
                    assert_eq!(runner.abort(task.id).await.unwrap(), AbortResult::Marked);
                    let task = terminal(runner.run(task.id).await.unwrap());
                    match case {
                        0 => assert_eq!(
                            task.state,
                            TaskState::Terminal {
                                outcome: TaskOutcome::Aborted {
                                    reason: Some("requested by test".into()),
                                    result: Some(Value::Null),
                                }
                            }
                        ),
                        1 => assert_eq!(
                            task.state,
                            TaskState::Terminal {
                                outcome: TaskOutcome::Completed {
                                    result: json!({"recovered":true})
                                }
                            }
                        ),
                        _ => assert_eq!(
                            task.state,
                            TaskState::Terminal {
                                outcome: TaskOutcome::Failed {
                                    error: handler_error("cleanup failed"),
                                    result: None,
                                }
                            }
                        ),
                    }
                    runner.close().await.unwrap();
                    session.close().await.unwrap();
                },
                zip(session_driver, task_driver),
            )
            .await;
        });
    }
}

#[test]
fn active_abort_joins_normal_but_not_fresh_cleanup_and_fences_old_runtime() {
    block_on(async {
        let normal_entered = Gate::default();
        let cleanup_entered = Gate::default();
        let release_cleanup = Gate::default();
        let old_runtime = Rc::new(RefCell::new(None));
        let cleanup_runtime = Rc::new(RefCell::new(None));
        let definition = definition(
            "active-abort",
            1,
            |_| Ok(json!({"phase":"work"})),
            phase({
                let normal_entered = normal_entered.clone();
                let old_runtime = old_runtime.clone();
                move |_, runtime| {
                    *old_runtime.borrow_mut() = Some(runtime.clone());
                    let normal_entered = normal_entered.clone();
                    async move {
                        normal_entered.release();
                        runtime.cancelled().await;
                        assert!(runtime.is_cancelled());
                        assert!(
                            runtime
                                .commit(|_, _| {
                                    Box::pin(async {
                                        Ok(Some(TaskUpdate::Complete(json!("too late"))))
                                    })
                                })
                                .await
                                .is_err()
                        );
                        Ok(())
                    }
                }
            }),
        )
        .with_abort_handler(phase({
            let cleanup_entered = cleanup_entered.clone();
            let release_cleanup = release_cleanup.clone();
            let cleanup_runtime = cleanup_runtime.clone();
            move |_, runtime| {
                assert!(!runtime.is_cancelled());
                *cleanup_runtime.borrow_mut() = Some(runtime.clone());
                let cleanup_entered = cleanup_entered.clone();
                let release_cleanup = release_cleanup.clone();
                async move {
                    cleanup_entered.release();
                    release_cleanup.wait().await;
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
                let mut run = runner.run(task.id);
                normal_entered.wait().await;
                assert_eq!(runner.abort(task.id).await.unwrap(), AbortResult::Marked);
                cleanup_entered.wait().await;

                // The acknowledgement joined the normal invocation only. Cleanup is
                // still gated, has a fresh uncancelled context, and the run is pending.
                assert!(poll_once(&mut run).await.is_none());
                assert!(!cleanup_runtime.borrow().as_ref().unwrap().is_cancelled());
                assert!(old_runtime.borrow().as_ref().unwrap().is_cancelled());
                release_cleanup.release();
                let task = terminal(run.await.unwrap());
                assert!(matches!(
                    task.state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Aborted { .. }
                    }
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
fn repeated_abort_during_cleanup_neither_writes_nor_signals_cleanup() {
    block_on(async {
        let normal_entered = Gate::default();
        let cleanup_entered = Gate::default();
        let cleanup_cancelled = Rc::new(Cell::new(false));
        let definition = definition(
            "repeat-abort",
            1,
            |_| Ok(json!({"phase":"work"})),
            phase({
                let normal_entered = normal_entered.clone();
                move |_, runtime| {
                    let normal_entered = normal_entered.clone();
                    async move {
                        normal_entered.release();
                        runtime.cancelled().await;
                        Ok(())
                    }
                }
            }),
        )
        .with_abort_handler(phase({
            let cleanup_entered = cleanup_entered.clone();
            let cleanup_cancelled = cleanup_cancelled.clone();
            move |_, runtime| {
                let cleanup_entered = cleanup_entered.clone();
                let cleanup_cancelled = cleanup_cancelled.clone();
                async move {
                    cleanup_entered.release();
                    runtime.cancelled().await;
                    cleanup_cancelled.set(true);
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
                assert_eq!(runner.abort(task.id).await.unwrap(), AbortResult::Marked);
                cleanup_entered.wait().await;
                let before = marker(&session).await;
                for _ in 0..3 {
                    assert_eq!(runner.abort(task.id).await.unwrap(), AbortResult::Marked);
                }
                assert!(!cleanup_cancelled.get());
                assert_eq!(marker(&session).await.get(), before.get() + 1);

                runner.close().await.unwrap();
                assert!(cleanup_cancelled.get());
                assert_eq!(run.await.unwrap(), RunResult::Interrupted);
                let retained = read(&session, task.id).await;
                assert!(retained.abort_requested);
                assert!(matches!(retained.state, TaskState::Running { .. }));
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    });
}

#[test]
fn unpolled_and_dropped_abort_waiters_still_persist_the_mark() {
    for poll_first in [false, true] {
        block_on(async move {
            let definition = definition(
                "dropped-abort",
                1,
                |_| Ok(json!({"phase":"work"})),
                phase(|_, _| async { panic!("normal phase must not run") }),
            );
            let (session, session_driver) = Session::new(memory().await);
            let (runner, task_driver) =
                TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap())
                    .unwrap();
            zip(
                async {
                    let task = create(&session, definition).await;
                    let mut abort = runner.abort(task.id);
                    if poll_first {
                        assert!(poll_once(&mut abort).await.is_none());
                    }
                    drop(abort);
                    let marked = read(&session, task.id).await;
                    assert!(marked.abort_requested);
                    assert!(matches!(
                        terminal(runner.run(task.id).await.unwrap()).state,
                        TaskState::Terminal {
                            outcome: TaskOutcome::Aborted { .. }
                        }
                    ));
                    runner.close().await.unwrap();
                    session.close().await.unwrap();
                },
                zip(session_driver, task_driver),
            )
            .await;
        });
    }
}

#[test]
fn normal_completion_and_abort_respect_session_line_order() {
    // A committed terminal outcome wins a later abort request.
    block_on(async {
        let definition = definition(
            "completion-first",
            1,
            |_| Ok(json!({"phase":"work"})),
            phase(|_, runtime| async move {
                runtime
                    .commit(|_, _| {
                        Box::pin(async { Ok(Some(TaskUpdate::Complete(json!("normal")))) })
                    })
                    .await
                    .unwrap();
                Ok(())
            }),
        );
        let (session, session_driver) = Session::new(memory().await);
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = create(&session, definition).await;
                let completed = terminal(runner.run(task.id).await.unwrap());
                assert_eq!(runner.abort(task.id).await.unwrap(), AbortResult::Terminal);
                assert_eq!(read(&session, task.id).await, completed);
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    });

    // A durable abort mark rejects a normal completion queued behind it.
    block_on(async {
        let normal_entered = Gate::default();
        let release_normal = Gate::default();
        let completion_rejected = Rc::new(Cell::new(false));
        let definition = definition(
            "abort-first",
            1,
            |_| Ok(json!({"phase":"work"})),
            phase({
                let normal_entered = normal_entered.clone();
                let release_normal = release_normal.clone();
                let completion_rejected = completion_rejected.clone();
                move |_, runtime| {
                    let normal_entered = normal_entered.clone();
                    let release_normal = release_normal.clone();
                    let completion_rejected = completion_rejected.clone();
                    async move {
                        normal_entered.release();
                        release_normal.wait().await;
                        let result = runtime
                            .commit(|_, _| {
                                Box::pin(async {
                                    Ok(Some(TaskUpdate::Complete(json!("too late"))))
                                })
                            })
                            .await;
                        completion_rejected.set(result.is_err());
                        Err(handler_error("late normal error"))
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
                let run = runner.run(task.id);
                normal_entered.wait().await;
                let abort = runner.abort(task.id);
                release_normal.release();
                assert_eq!(abort.await.unwrap(), AbortResult::Marked);
                assert!(completion_rejected.get());
                assert!(matches!(
                    terminal(run.await.unwrap()).state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Aborted { .. }
                    }
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
fn abort_handler_must_commit_a_terminal_outcome() {
    // unchanged, checkpoint-only, entry-only, error, panic, outcome-then-error,
    // and outcome-then-panic.
    for case in 0..7 {
        block_on(async move {
            let definition = definition(
                "abort-progress",
                1,
                |_| Ok(json!({"phase":"work"})),
                phase(|_, _| async { panic!("normal phase must not run") }),
            )
            .with_abort_handler(phase(move |_, runtime| async move {
                match case {
                    0 => {}
                    1 => {
                        runtime
                            .commit(|_, _| {
                                Box::pin(async {
                                    Ok(Some(TaskUpdate::Checkpoint(json!({"phase":"cleanup"}))))
                                })
                            })
                            .await
                            .unwrap();
                    }
                    2 => {
                        runtime
                            .commit(|tx, task| {
                                Box::pin(async move {
                                    tx.append_entry(
                                        task.conversation_id,
                                        EntryDraft::new("cleanup effect"),
                                    )
                                    .await?;
                                    Ok(None)
                                })
                            })
                            .await
                            .unwrap();
                    }
                    3 => return Err(handler_error("abort handler error")),
                    4 => panic!("abort handler panic"),
                    5 | 6 => {
                        runtime
                            .commit(|_, _| {
                                Box::pin(async {
                                    Ok(Some(TaskUpdate::Abort {
                                        reason: Some("done".into()),
                                        result: None,
                                    }))
                                })
                            })
                            .await
                            .unwrap();
                        if case == 5 {
                            return Err(handler_error("late error"));
                        }
                        panic!("late panic");
                    }
                    _ => unreachable!(),
                }
                Ok(())
            }));
            let (session, session_driver) = Session::new(memory().await);
            let (runner, task_driver) =
                TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap())
                    .unwrap();
            zip(
                async {
                    let task = create(&session, definition).await;
                    assert_eq!(runner.abort(task.id).await.unwrap(), AbortResult::Marked);
                    let result = terminal(runner.run(task.id).await.unwrap());
                    if case < 5 {
                        assert!(
                            matches!(
                                result.state,
                                TaskState::Terminal {
                                    outcome: TaskOutcome::Faulted { .. }
                                }
                            ),
                            "case {case}: {:?}",
                            result.state
                        );
                    } else {
                        assert_eq!(
                            result.state,
                            TaskState::Terminal {
                                outcome: TaskOutcome::Aborted {
                                    reason: Some("done".into()),
                                    result: None,
                                }
                            }
                        );
                    }
                    runner.close().await.unwrap();
                    session.close().await.unwrap();
                },
                zip(session_driver, task_driver),
            )
            .await;
        });
    }
}

#[test]
fn abort_handles_orphans_unsupported_scope_missing_tasks_and_terminal_noops() {
    block_on(async {
        let completes = definition(
            "supported",
            1,
            |_| Ok(json!({"phase":"work"})),
            phase(|_, runtime| async move {
                runtime
                    .commit(|_, _| Box::pin(async { Ok(Some(TaskUpdate::Complete(json!(null)))) }))
                    .await
                    .unwrap();
                Ok(())
            }),
        );
        let missing = definition(
            "missing-definition",
            7,
            |_| Ok(json!({"phase":"work"})),
            phase(|_, _| async { unreachable!() }),
        );
        let old_version = definition(
            "versioned",
            1,
            |_| Ok(json!({"phase":"work"})),
            phase(|_, _| async { unreachable!() }),
        );
        let current_version = definition(
            "versioned",
            2,
            |_| Ok(json!({"phase":"work"})),
            phase(|_, _| async { unreachable!() }),
        );
        let (session, session_driver) = Session::new(memory().await);
        let (runner, task_driver) = TaskRunner::attach(
            &session,
            TaskRegistry::new([completes.clone(), current_version]).unwrap(),
        )
        .unwrap();

        zip(
            async {
                let missing_task = create(&session, missing).await;
                let stale_task = create(&session, old_version).await;
                let unsupported = create_with(
                    &session,
                    completes.clone(),
                    TaskOptions {
                        background: true,
                        ..options()
                    },
                )
                .await;
                let terminal_task = create(&session, completes).await;

                assert_eq!(
                    runner.abort(missing_task.id).await.unwrap(),
                    AbortResult::Marked
                );
                assert!(matches!(
                    read(&session, missing_task.id).await.state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Orphaned { .. }
                    }
                ));
                assert_eq!(
                    runner.abort(stale_task.id).await.unwrap(),
                    AbortResult::Marked
                );
                assert!(matches!(
                    read(&session, stale_task.id).await.state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Orphaned { .. }
                    }
                ));

                let completed = terminal(runner.run(terminal_task.id).await.unwrap());
                let before = marker(&session).await;
                assert!(matches!(
                    runner.abort(Id::new(9_999).unwrap()).await,
                    Err(RunError::Session(_))
                ));
                assert_eq!(
                    runner.abort(unsupported.id).await.unwrap(),
                    AbortResult::Marked
                );
                let marked_background = read(&session, unsupported.id).await;
                assert!(marked_background.abort_requested);
                assert_eq!(marked_background.status(), TaskStatus::Pending);
                assert_eq!(
                    runner.abort(terminal_task.id).await.unwrap(),
                    AbortResult::Terminal
                );
                assert_eq!(read(&session, terminal_task.id).await, completed);
                assert_eq!(marker(&session).await.get(), before.get() + 2);

                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    });
}

#[test]
fn owned_terminal_child_after_first_scan_page_is_supported() {
    block_on(async {
        let mut storage = memory().await;
        let mut target = seeded_abort_task(10);
        target.abort_requested = false;
        let mut writes = vec![StorageWrite::Task(target.clone())];
        for id in 20..170 {
            let mut child = seeded_abort_task(id);
            child.kind = "terminal-child".into();
            child.memos = None;
            child.state = TaskState::Terminal {
                outcome: TaskOutcome::Completed {
                    result: Value::Null,
                },
            };
            // The target and earlier children fill the first 128-task page.
            if id == 169 {
                child.owner = Some(target.id);
            }
            writes.push(StorageWrite::Task(child));
        }

        // Terminal is a no-op before scope and definition checks, even when the
        // record itself has several otherwise unsupported properties.
        let mut terminal_target = target.clone();
        terminal_target.id = Id::new(9_999).unwrap();
        terminal_target.kind = "missing-terminal-definition".into();
        terminal_target.owner = Some(Id::new(8_888).unwrap());
        terminal_target.background = true;
        terminal_target.memos = None;
        terminal_target.state = TaskState::Terminal {
            outcome: TaskOutcome::Aborted {
                reason: Some("retained".into()),
                result: Some(opaque_payload()),
            },
        };
        writes.push(StorageWrite::Task(terminal_target.clone()));
        storage.commit(writes).await.unwrap();

        let definition = definition(
            "seeded-abort",
            7,
            |_| panic!("seeded task must not initialize"),
            phase(|_, _| async { panic!("blocked task must not run") }),
        );
        let (session, session_driver) = Session::new(storage);
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition]).unwrap()).unwrap();
        zip(
            async {
                let before = marker(&session).await;
                assert_eq!(runner.abort(target.id).await.unwrap(), AbortResult::Marked);
                let mut marked = target.clone();
                marked.abort_requested = true;
                assert_eq!(read(&session, target.id).await, marked);
                assert_eq!(
                    runner.abort(terminal_target.id).await.unwrap(),
                    AbortResult::Terminal
                );
                assert_eq!(read(&session, terminal_target.id).await, terminal_target);
                assert_eq!(marker(&session).await.get(), before.get() + 2);
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    });
}

#[test]
fn cleanup_atomically_attributes_effect_and_preserves_opaque_metadata() {
    for result in [None, Some(Value::Null), Some(opaque_payload())] {
        block_on(async move {
            let mut storage = memory().await;
            let task = seeded_abort_task(10);
            storage
                .commit(vec![StorageWrite::Task(task.clone())])
                .await
                .unwrap();

            let initial = task.clone();
            let expected_result = result.clone();
            let effect_payload = opaque_payload();
            let definition = definition(
                "seeded-abort",
                7,
                |_| panic!("seeded task must not initialize"),
                phase(|_, _| async { panic!("normal phase must not run") }),
            )
            .with_abort_handler(phase(move |observed, runtime| {
                let initial = initial.clone();
                let result = result.clone();
                let effect_payload = effect_payload.clone();
                async move {
                    let TaskState::Pending { checkpoint } = initial.state.clone() else {
                        unreachable!()
                    };
                    let mut expected_running = initial;
                    expected_running.state = TaskState::Running { checkpoint };
                    assert_eq!(observed, expected_running);

                    runtime
                        .commit(move |tx, current| {
                            Box::pin(async move {
                                let mut draft = EntryDraft::new("atomic abort cleanup");
                                draft.data = Some(effect_payload);
                                tx.append_entry(current.conversation_id, draft).await?;
                                Ok(Some(TaskUpdate::Abort {
                                    reason: Some("chosen".into()),
                                    result,
                                }))
                            })
                        })
                        .await
                        .unwrap();
                    Ok(())
                }
            }));
            let (session, session_driver) = Session::new(storage);
            let (runner, task_driver) =
                TaskRunner::attach(&session, TaskRegistry::new([definition]).unwrap()).unwrap();
            zip(
                async {
                    let before = marker(&session).await;
                    let outcome = terminal(runner.run(task.id).await.unwrap());
                    let mut expected = task.clone();
                    expected.memos = None;
                    expected.state = TaskState::Terminal {
                        outcome: TaskOutcome::Aborted {
                            reason: Some("chosen".into()),
                            result: expected_result.clone(),
                        },
                    };
                    assert_eq!(outcome, expected);

                    let entries = session
                        .commit(move |tx| {
                            Box::pin(async move {
                                tx.scan_entries(EntryQuery::new(ROOT_CONVERSATION), 10, None)
                                    .await
                            })
                        })
                        .await
                        .unwrap()
                        .value;
                    assert_eq!(entries.items[0].kind, "atomic abort cleanup");
                    assert_eq!(entries.items[0].by_task_id, Some(task.id));
                    assert_eq!(entries.items[0].data, Some(opaque_payload()));
                    // Reservation and the attributed effect/outcome are the only
                    // writes between the two markers.
                    assert_eq!(marker(&session).await.get(), before.get() + 3);

                    let encoded = serde_json::to_value(&outcome).unwrap();
                    let encoded_result = encoded["state"]["outcome"].get("result");
                    match &expected_result {
                        Some(expected) => assert_eq!(encoded_result, Some(expected)),
                        None => assert_eq!(encoded_result, None),
                    }
                    assert_eq!(
                        serde_json::from_value::<TaskRecord>(encoded).unwrap(),
                        outcome
                    );
                    runner.close().await.unwrap();
                    session.close().await.unwrap();
                },
                zip(session_driver, task_driver),
            )
            .await;
        });
    }
}

#[test]
fn marked_pending_abort_requires_an_explicit_run_and_survives_close() {
    block_on(async {
        let definition = definition(
            "explicit-cleanup",
            1,
            |_| Ok(json!({"phase":"work"})),
            phase(|_, _| async { panic!("normal phase must not run") }),
        );
        let (session, session_driver) = Session::new(memory().await);
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = create(&session, definition).await;
                assert_eq!(runner.abort(task.id).await.unwrap(), AbortResult::Marked);
                let marked = read(&session, task.id).await;
                assert!(marked.abort_requested);
                assert!(matches!(marked.state, TaskState::Pending { .. }));
                runner.close().await.unwrap();
                assert_eq!(read(&session, task.id).await, marked);
                assert!(matches!(runner.run(task.id).await, Err(RunError::Closed)));
                assert!(matches!(runner.abort(task.id).await, Err(RunError::Closed)));
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    });
}
