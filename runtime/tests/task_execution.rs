use futures_lite::future::{block_on, or, zip};
use publicworks_runtime::*;
use serde_json::{Value, json};
use std::{cell::RefCell, future::Future, rc::Rc, task::Waker};

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
    initial: Value,
    phases: impl IntoIterator<Item = (&'static str, PhaseHandler)>,
) -> TaskDefinition {
    TaskDefinition::new(
        "test",
        1,
        move |_| Ok(initial.clone()),
        phases.into_iter().map(|(k, v)| (k.into(), v)).collect(),
    )
}
fn options() -> TaskOptions {
    TaskOptions {
        conversation_id: Some(ROOT_CONVERSATION),
        ownership: TaskOwnership::Conversation,
        background: false,
    }
}
async fn create(session: &Session, definition: TaskDefinition) -> TaskRecord {
    session
        .commit(move |tx| {
            Box::pin(async move {
                tx.create_task(definition, json!({"input": true}), options())
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
        other => panic!("expected terminal, got {other:?}"),
    }
}
fn error() -> TaskOutcomeError {
    TaskOutcomeError {
        message: "handler failed".into(),
        detail: Some(json!({"reason": 7})),
    }
}

#[test]
fn phases_commit_entries_and_checkpoint_together_then_complete() {
    block_on(async {
        let escaped = Rc::new(RefCell::new(None));
        let capture = escaped.clone();
        let def = definition(
            json!({"phase":"first"}),
            [
                (
                    "first",
                    phase(move |task, runtime| {
                        *capture.borrow_mut() = Some(runtime.clone());
                        async move {
                            assert_eq!(runtime.task_id(), task.id);
                            assert_eq!(runtime.conversation_id(), task.conversation_id);
                            runtime
                                .commit(move |tx, current| {
                                    Box::pin(async move {
                                        assert!(matches!(current.state, TaskState::Running { .. }));
                                        let entry = tx
                                            .append_entry(
                                                task.conversation_id,
                                                EntryDraft::new("first effect"),
                                            )
                                            .await?;
                                        assert_eq!(entry.by_task_id, Some(task.id));
                                        assert_eq!(
                                            tx.task(task.id).await.unwrap_err(),
                                            SessionError::ReadAfterWrite
                                        );
                                        Ok(Some(TaskUpdate::Checkpoint(
                                            json!({"phase":"second", "answer":42}),
                                        )))
                                    })
                                })
                                .await
                                .unwrap();
                            Ok(())
                        }
                    }),
                ),
                (
                    "second",
                    phase(|task, runtime| async move {
                        assert_eq!(
                            task.state,
                            TaskState::Running {
                                checkpoint: json!({"phase":"second", "answer":42})
                            }
                        );
                        runtime
                            .commit(|tx, current| {
                                Box::pin(async move {
                                    let entries = tx
                                        .scan_entries(
                                            EntryQuery::new(current.conversation_id),
                                            10,
                                            None,
                                        )
                                        .await?;
                                    assert_eq!(entries.items.len(), 1);
                                    assert_eq!(entries.items[0].by_task_id, Some(current.id));
                                    // Root seed, creation, durable reservation, then checkpoint+entry.
                                    assert_eq!(
                                        tx.entry(entries.items[0].id)
                                            .await?
                                            .unwrap()
                                            .commit_seq
                                            .get(),
                                        4
                                    );
                                    Ok(Some(TaskUpdate::Complete(json!(42))))
                                })
                            })
                            .await
                            .unwrap();
                        Ok(())
                    }),
                ),
            ],
        );
        assert_eq!(def.kind(), "test");
        assert_eq!(def.version(), 1);
        assert!(
            matches!(TaskRegistry::new([def.clone(), def.clone()]), Err(RunError::DuplicateKind(kind)) if kind == "test")
        );
        let (session, sd) = Session::new(memory().await);
        let (runner, td) =
            TaskRunner::attach(&session, TaskRegistry::new([def.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = create(&session, def).await;
                let result = terminal(runner.run(task.id).await.unwrap());
                assert_eq!(
                    result.state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Completed { result: json!(42) }
                    }
                );
                assert_eq!(read(&session, task.id).await, result);
                let stale = escaped.borrow().as_ref().unwrap().clone();
                assert!(
                    stale
                        .commit(|_, _| Box::pin(async {
                            Ok(Some(TaskUpdate::Complete(json!(false))))
                        }))
                        .await
                        .is_err()
                );
                assert_eq!(read(&session, task.id).await, result);
                runner.close().await.unwrap();
                // Runner shutdown must leave Session/storage usable.
                assert_eq!(marker(&session).await.get(), 6);
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    });
}

#[test]
fn explicit_failure_preserves_error_and_optional_result() {
    block_on(async {
        let def = definition(
            json!({"phase":"fail"}),
            [(
                "fail",
                phase(|_, runtime| async move {
                    runtime
                        .commit(|_, _| {
                            Box::pin(async {
                                Ok(Some(TaskUpdate::Fail(error(), Some(json!(null)))))
                            })
                        })
                        .await
                        .unwrap();
                    Ok(())
                }),
            )],
        );
        let (session, sd) = Session::new(memory().await);
        let (runner, td) =
            TaskRunner::attach(&session, TaskRegistry::new([def.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = create(&session, def).await;
                assert_eq!(
                    terminal(runner.run(task.id).await.unwrap()).state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Failed {
                            error: error(),
                            result: Some(json!(null))
                        }
                    }
                );
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    });
}

#[test]
fn malformed_or_unknown_phase_faults_without_calling_a_handler() {
    for checkpoint in [
        json!(null),
        json!({}),
        json!({"phase":4}),
        json!({"phase":"unknown"}),
    ] {
        block_on(async {
            let def = definition(
                checkpoint,
                [("known", phase(|_, _| async { panic!("must not dispatch") }))],
            );
            let (session, sd) = Session::new(memory().await);
            let (runner, td) =
                TaskRunner::attach(&session, TaskRegistry::new([def.clone()]).unwrap()).unwrap();
            zip(
                async {
                    let task = create(&session, def).await;
                    assert!(matches!(
                        terminal(runner.run(task.id).await.unwrap()).state,
                        TaskState::Terminal {
                            outcome: TaskOutcome::Faulted { .. }
                        }
                    ));
                    runner.close().await.unwrap();
                    session.close().await.unwrap();
                },
                zip(sd, td),
            )
            .await;
        });
    }
}

#[test]
fn structural_progress_and_error_precedence() {
    // Returning, entry-only effects, reverting a checkpoint, and errors after a
    // checkpoint fault. A terminal outcome wins over a later error or panic.
    for case in 0..9 {
        block_on(async {
            let def = definition(
                json!({"phase":"work"}),
                [(
                    "work",
                    phase(move |_, runtime| async move {
                        match case {
                            0 => {}
                            1 => {
                                runtime
                                    .commit(|tx, task| {
                                        Box::pin(async move {
                                            tx.append_entry(
                                                task.conversation_id,
                                                EntryDraft::new("entry only"),
                                            )
                                            .await?;
                                            Ok(None)
                                        })
                                    })
                                    .await
                                    .unwrap();
                            }
                            2 => {
                                runtime
                                    .commit(|_, _| {
                                        Box::pin(async {
                                            Ok(Some(TaskUpdate::Checkpoint(
                                                json!({"phase":"other"}),
                                            )))
                                        })
                                    })
                                    .await
                                    .unwrap();
                                runtime
                                    .commit(|_, _| {
                                        Box::pin(async {
                                            Ok(Some(TaskUpdate::Checkpoint(
                                                json!({"phase":"work"}),
                                            )))
                                        })
                                    })
                                    .await
                                    .unwrap();
                            }
                            3 | 4 => {
                                runtime
                                    .commit(|_, _| {
                                        Box::pin(async {
                                            Ok(Some(TaskUpdate::Checkpoint(
                                                json!({"phase":"other"}),
                                            )))
                                        })
                                    })
                                    .await
                                    .unwrap();
                                if case == 3 {
                                    return Err(error());
                                }
                                panic!("after checkpoint");
                            }
                            5 | 6 => {
                                runtime
                                    .commit(|_, _| {
                                        Box::pin(async {
                                            Ok(Some(TaskUpdate::Complete(json!("done"))))
                                        })
                                    })
                                    .await
                                    .unwrap();
                                if case == 5 {
                                    return Err(error());
                                }
                                panic!("after terminal");
                            }
                            7 => {
                                runtime
                                    .commit(|_, _| {
                                        Box::pin(async { panic!("commit callback poll") })
                                    })
                                    .await
                                    .unwrap();
                            }
                            _ => {
                                runtime
                                    .commit(|_, _| panic!("commit callback construction"))
                                    .await
                                    .unwrap();
                            }
                        }
                        Ok(())
                    }),
                )],
            );
            let (session, sd) = Session::new(memory().await);
            let (runner, td) =
                TaskRunner::attach(&session, TaskRegistry::new([def.clone()]).unwrap()).unwrap();
            zip(
                async {
                    let task = create(&session, def).await;
                    let task = terminal(runner.run(task.id).await.unwrap());
                    if matches!(case, 5 | 6) {
                        assert_eq!(
                            task.state,
                            TaskState::Terminal {
                                outcome: TaskOutcome::Completed {
                                    result: json!("done")
                                }
                            }
                        );
                    } else {
                        assert!(
                            matches!(
                                task.state,
                                TaskState::Terminal {
                                    outcome: TaskOutcome::Faulted { .. }
                                }
                            ),
                            "case {case}: {:?}",
                            task.state
                        );
                    }
                    marker(&session).await; // Normalized task panics do not poison Session.
                    runner.close().await.unwrap();
                    session.close().await.unwrap();
                },
                zip(sd, td),
            )
            .await;
        });
    }
}

fn seeded_task(id: u64) -> TaskRecord {
    TaskRecord {
        id: Id::new(id).unwrap(),
        conversation_id: ROOT_CONVERSATION,
        kind: "test".into(),
        version: 1,
        input: json!(null),
        owner: None,
        background: false,
        abort_requested: false,
        state: TaskState::Pending {
            checkpoint: json!({"phase":"work"}),
        },
        memos: None,
    }
}

#[test]
fn blocked_requests_do_not_write_or_advance_sequence() {
    block_on(async {
        let mut store = memory().await;
        let mut records = Vec::new();
        let mut cases = vec![(Id::new(99).unwrap(), BlockReason::MissingTask)];
        for n in 2..=11 {
            let mut task = seeded_task(n);
            let reason = match n {
                2 => {
                    task.kind = "missing".into();
                    BlockReason::MissingDefinition
                }
                3 => {
                    task.version = 2;
                    BlockReason::VersionMismatch
                }
                4 => {
                    task.background = true;
                    BlockReason::UnsupportedScope
                }
                5 => {
                    task.owner = Some(Id::new(10).unwrap());
                    BlockReason::UnsupportedScope
                }
                6 => {
                    task.conversation_id = Id::new(20).unwrap();
                    BlockReason::UnsupportedScope
                }
                7 => {
                    task.state = TaskState::Running {
                        checkpoint: json!({"phase":"work"}),
                    };
                    BlockReason::NotPending
                }
                8 => {
                    task.abort_requested = true;
                    BlockReason::AbortRequested
                }
                9 => {
                    task.state = TaskState::Terminal {
                        outcome: TaskOutcome::Completed {
                            result: json!(null),
                        },
                    };
                    BlockReason::NotPending
                }
                _ => BlockReason::UnsupportedScope, // 10 owns task 5; 11 owns conversation 20.
            };
            cases.push((task.id, reason));
            records.push(task);
        }
        let mut writes: Vec<_> = records.iter().cloned().map(StorageWrite::Task).collect();
        writes.push(StorageWrite::Conversation(ConversationRecord {
            id: Id::new(20).unwrap(),
            parent: None,
            owner: Some(OwnerLink {
                conversation_id: ROOT_CONVERSATION,
                task_id: Id::new(11).unwrap(),
            }),
        }));
        store.commit(writes).await.unwrap();
        let def = definition(
            json!({"phase":"work"}),
            [("work", phase(|_, _| async { panic!("blocked handler") }))],
        );
        let (session, sd) = Session::new(store);
        let (runner, td) = TaskRunner::attach(&session, TaskRegistry::new([def]).unwrap()).unwrap();
        zip(async {
            let before = marker(&session).await;
            for (id, expected) in cases {
                assert!(matches!(runner.run(id).await.unwrap(), RunResult::Blocked(actual) if actual == expected), "task {id}");
            }
            for record in records { assert_eq!(read(&session, record.id).await, record); }
            assert_eq!(marker(&session).await.get(), before.get() + 1);
            runner.close().await.unwrap();
            session.close().await.unwrap();
        }, zip(sd, td)).await;
    });
}

#[test]
fn dropped_waiters_still_run_and_drain_commits_before_phase_decisions() {
    block_on(async {
        let escaped = Rc::new(RefCell::new(None));
        let def = definition(
            json!({"phase":"first"}),
            [
                (
                    "first",
                    phase(|_, runtime| async move {
                        drop(runtime.commit(|_, _| {
                            Box::pin(async {
                                Ok(Some(TaskUpdate::Checkpoint(json!({"phase":"last"}))))
                            })
                        }));
                        Ok(())
                    }),
                ),
                (
                    "last",
                    phase({
                        let escaped = escaped.clone();
                        move |_, runtime| {
                            *escaped.borrow_mut() = Some(runtime.clone());
                            async move {
                                drop(runtime.commit(|_, _| {
                                    Box::pin(async { Ok(Some(TaskUpdate::Complete(json!(true)))) })
                                }));
                                Ok(())
                            }
                        }
                    }),
                ),
            ],
        );
        let (session, sd) = Session::new(memory().await);
        let (runner, td) =
            TaskRunner::attach(&session, TaskRegistry::new([def.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = create(&session, def).await;
                drop(runner.run(task.id));
                // A second admitted run observes the first invocation's settled
                // decision, including commits whose waiters were discarded.
                assert_eq!(
                    runner.run(task.id).await.unwrap(),
                    RunResult::Blocked(BlockReason::NotPending)
                );
                let result = read(&session, task.id).await;
                assert_eq!(
                    result.state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Completed {
                            result: json!(true)
                        }
                    }
                );
                let stale = escaped.borrow().as_ref().unwrap().clone();
                assert!(
                    stale
                        .commit(|_, _| Box::pin(async {
                            Ok(Some(TaskUpdate::Complete(json!(false))))
                        }))
                        .await
                        .is_err()
                );
                assert_eq!(read(&session, task.id).await, result);
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    });
}

#[test]
fn pending_handler_does_not_block_session_and_leaf_ownership_is_guarded() {
    block_on(async {
        let entered = Gate::default();
        let release = Gate::default();
        let def = definition(
            json!({"phase":"work"}),
            [(
                "work",
                phase({
                    let entered = entered.clone();
                    let release = release.clone();
                    move |_, runtime| {
                        let entered = entered.clone();
                        let release = release.clone();
                        async move {
                            entered.release();
                            release.wait().await;
                            runtime
                                .commit(|_, _| {
                                    Box::pin(async { Ok(Some(TaskUpdate::Complete(json!(null)))) })
                                })
                                .await
                                .unwrap();
                            Ok(())
                        }
                    }
                }),
            )],
        );
        let (session, sd) = Session::new(memory().await);
        let (runner, td) =
            TaskRunner::attach(&session, TaskRegistry::new([def.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = create(&session, def.clone()).await;
                let waiter = runner.run(task.id);
                entered.wait().await;
                assert!(matches!(
                    read(&session, task.id).await.state,
                    TaskState::Running { .. }
                ));
                marker(&session).await;
                // A separate public transaction must not introduce owned work while
                // an external handler is suspended, regardless of staging order.
                for reverse in [false, true] {
                    let def = def.clone();
                    let result = session
                        .commit(move |tx| {
                            Box::pin(async move {
                                if reverse {
                                    tx.append_entry(
                                        ROOT_CONVERSATION,
                                        EntryDraft::new("rolled back"),
                                    )
                                    .await?;
                                }
                                tx.create_task(
                                    def,
                                    json!(null),
                                    TaskOptions {
                                        conversation_id: None,
                                        ownership: TaskOwnership::Task(task.id),
                                        background: false,
                                    },
                                )
                                .await?;
                                if !reverse {
                                    tx.append_entry(
                                        ROOT_CONVERSATION,
                                        EntryDraft::new("rolled back"),
                                    )
                                    .await?;
                                }
                                Ok(())
                            })
                        })
                        .await;
                    assert!(result.is_err());
                }
                assert!(
                    session
                        .commit(move |tx| Box::pin(async move {
                            tx.create_conversation_owned(Owner::Task(task.id)).await
                        }))
                        .await
                        .is_err()
                );
                release.release();
                terminal(waiter.await.unwrap());
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    });
}

#[test]
fn close_cancels_active_and_interrupts_queued_without_reserving() {
    block_on(async {
        let entered = Gate::default();
        let cancelled = Gate::default();
        let release = Gate::default();
        let def = definition(
            json!({"phase":"work"}),
            [(
                "work",
                phase({
                    let entered = entered.clone();
                    let cancelled = cancelled.clone();
                    let release = release.clone();
                    move |_, runtime| {
                        let entered = entered.clone();
                        let cancelled = cancelled.clone();
                        let release = release.clone();
                        async move {
                            entered.release();
                            runtime.cancelled().await;
                            assert!(runtime.is_cancelled());
                            cancelled.release();
                            release.wait().await;
                            Ok(())
                        }
                    }
                }),
            )],
        );
        let (session, sd) = Session::new(memory().await);
        let (runner, td) =
            TaskRunner::attach(&session, TaskRegistry::new([def.clone()]).unwrap()).unwrap();
        zip(
            async {
                let first = create(&session, def.clone()).await;
                let second = create(&session, def).await;
                let active = runner.run(first.id);
                entered.wait().await;
                let queued = runner.run(second.id);
                let mut close = runner.close();
                cancelled.wait().await;
                assert!(futures_lite::future::poll_once(&mut close).await.is_none());

                assert!(matches!(runner.run(second.id).await, Err(RunError::Closed)));
                assert_eq!(read(&session, second.id).await, second);
                release.release();
                close.await.unwrap();
                assert!(matches!(queued.await.unwrap(), RunResult::Interrupted));
                assert!(matches!(active.await.unwrap(), RunResult::Interrupted));
                assert!(matches!(
                    read(&session, first.id).await.state,
                    TaskState::Running { .. }
                ));
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    });
}

#[test]
fn session_close_signals_but_does_not_join_a_handler() {
    block_on(async {
        let entered = Gate::default();
        let cancelled = Gate::default();
        let release = Gate::default();
        let def = definition(
            json!({"phase":"work"}),
            [(
                "work",
                phase({
                    let entered = entered.clone();
                    let cancelled = cancelled.clone();
                    let release = release.clone();
                    move |_, runtime| {
                        let entered = entered.clone();
                        let cancelled = cancelled.clone();
                        let release = release.clone();
                        async move {
                            entered.release();
                            runtime.cancelled().await;
                            cancelled.release();
                            release.wait().await;
                            Ok(())
                        }
                    }
                }),
            )],
        );
        let (session, sd) = Session::new(memory().await);
        let (runner, td) =
            TaskRunner::attach(&session, TaskRegistry::new([def.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = create(&session, def).await;
                let active = runner.run(task.id);
                entered.wait().await;
                session.close().await.unwrap(); // Must finish before handler release.
                cancelled.wait().await;
                release.release();
                assert!(matches!(active.await.unwrap(), RunResult::Interrupted));
                runner.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    });
}

#[test]
fn driver_drop_wakes_waiters_and_fences_escaped_runtime() {
    block_on(async {
        let entered = Gate::default();
        let escaped = Rc::new(RefCell::new(None));
        let def = definition(
            json!({"phase":"work"}),
            [(
                "work",
                phase({
                    let entered = entered.clone();
                    let escaped = escaped.clone();
                    move |_, runtime| {
                        *escaped.borrow_mut() = Some(runtime);
                        let entered = entered.clone();
                        async move {
                            entered.release();
                            std::future::pending().await
                        }
                    }
                }),
            )],
        );
        let (session, mut sd) = Session::new(memory().await);
        let (runner, mut td) =
            TaskRunner::attach(&session, TaskRegistry::new([def.clone()]).unwrap()).unwrap();
        assert!(matches!(
            TaskRunner::attach(&session, TaskRegistry::new([]).unwrap()),
            Err(RunError::AlreadyAttached)
        ));
        let (id, waiter) = or(
            async {
                let task = create(&session, def).await;
                let waiter = runner.run(task.id);
                entered.wait().await;
                (task.id, waiter)
            },
            async {
                zip(&mut sd, &mut td).await;
                std::future::pending().await
            },
        )
        .await;
        drop(td);
        assert!(matches!(waiter.await, Err(RunError::DriverStopped)));
        assert_eq!(runner.close().await, Err(RunError::DriverStopped));
        let stale = escaped.borrow().as_ref().unwrap().clone();
        assert!(stale.is_cancelled());
        zip(
            async {
                assert!(
                    stale
                        .commit(|_, _| Box::pin(async {
                            Ok(Some(TaskUpdate::Complete(json!(null))))
                        }))
                        .await
                        .is_err()
                );
                assert!(matches!(
                    read(&session, id).await.state,
                    TaskState::Running { .. }
                ));
                // Attachment lifetime ends with the driver, not with runner handles.
                let (next, driver) =
                    TaskRunner::attach(&session, TaskRegistry::new([]).unwrap()).unwrap();
                zip(next.close(), driver).await.0.unwrap();
                session.close().await.unwrap();
            },
            sd,
        )
        .await;
    });
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

#[test]
fn owned_creation_racing_reservation_is_serialized_in_both_admission_orders() {
    for reservation_first in [false, true] {
        block_on(async {
            let def = definition(
                json!({"phase":"work"}),
                [(
                    "work",
                    phase(|_, runtime| async move {
                        runtime
                            .commit(|_, _| {
                                Box::pin(async { Ok(Some(TaskUpdate::Complete(json!(null)))) })
                            })
                            .await
                            .unwrap();
                        Ok(())
                    }),
                )],
            );
            let (session, sd) = Session::new(memory().await);
            let (runner, mut td) =
                TaskRunner::attach(&session, TaskRegistry::new([def.clone()]).unwrap()).unwrap();
            zip(
                async {
                    let task = create(&session, def.clone()).await;
                    let waiter = runner.run(task.id);
                    // Poll only TaskDriver once to admit its reservation to Session.
                    // In the opposite order, the public owned creation is admitted first.
                    if reservation_first {
                        assert!(futures_lite::future::poll_once(&mut td).await.is_none());
                    }
                    let owned = session.commit(move |tx| {
                        Box::pin(async move {
                            tx.create_task(
                                def,
                                json!(null),
                                TaskOptions {
                                    conversation_id: None,
                                    ownership: TaskOwnership::Task(task.id),
                                    background: false,
                                },
                            )
                            .await
                        })
                    });
                    zip(
                        async {
                            if reservation_first {
                                assert!(owned.await.is_err());
                                terminal(waiter.await.unwrap());
                            } else {
                                owned.await.unwrap();
                                assert_eq!(
                                    waiter.await.unwrap(),
                                    RunResult::Blocked(BlockReason::UnsupportedScope)
                                );
                                assert_eq!(read(&session, task.id).await, task);
                            }
                            runner.close().await.unwrap();
                            session.close().await.unwrap();
                        },
                        td,
                    )
                    .await;
                },
                sd,
            )
            .await;
        });
    }
}
