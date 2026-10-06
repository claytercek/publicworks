use super::phase::json_equal;
use super::*;
use futures_lite::future::{block_on, poll_once, zip};
use serde_json::json;

fn definition(handler: PhaseHandler) -> TaskDefinition {
    TaskDefinition::new(
        "leaf",
        1,
        |_| Ok(json!({"phase":"run"})),
        BTreeMap::from([("run".into(), handler)]),
    )
}
async fn seed(session: &Session, definition: TaskDefinition) -> TaskRecord {
    session
        .commit(move |tx| {
            Box::pin(async move {
                let conversation = tx.create_conversation().await?;
                tx.create_task(
                    definition,
                    Value::Null,
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
        .value
}
fn child_options(owner: Id) -> TaskOptions {
    TaskOptions {
        ownership: TaskOwnership::Task(owner),
        conversation_id: None,
        background: false,
    }
}
fn passive() -> TaskDefinition {
    definition(Rc::new(|_, runtime| {
        Box::pin(async move {
            runtime.cancelled().await;
            Ok(())
        })
    }))
}
fn captured() -> (TaskDefinition, oneshot::Receiver<TaskRuntime>) {
    let (sender, receiver) = oneshot::channel();
    let sender = RefCell::new(Some(sender));
    let definition = definition(Rc::new(move |_, runtime| {
        sender
            .borrow_mut()
            .take()
            .unwrap()
            .send(runtime.clone())
            .ok();
        Box::pin(async move {
            runtime.cancelled().await;
            Ok(())
        })
    }));
    (definition, receiver)
}

#[test]
fn leaf_guard_checks_all_transactions_and_both_staging_orders() {
    block_on(async {
        let (definition, context) = captured();
        let (session, driver) = Session::new(MemoryStorage::new());
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = seed(&session, definition).await;
                let run = runner.run(task.id);
                let runtime = context.await.unwrap();
                for conversation in [false, true] {
                    for owner_first in [false, true] {
                        let id = task.id;
                        let result = session
                            .commit(move |tx| {
                                Box::pin(async move {
                                    let record = tx.task(id).await?.unwrap();
                                    if owner_first {
                                        tx.set_task(record.clone()).await?;
                                    }
                                    if conversation {
                                        tx.create_conversation_owned(Owner::Task(id)).await?;
                                    } else {
                                        tx.create_task(passive(), Value::Null, child_options(id))
                                            .await?;
                                    }
                                    if !owner_first {
                                        tx.set_task(record).await?;
                                    }
                                    Ok(())
                                })
                            })
                            .await;
                        assert!(matches!(result, Err(SessionError::Invalid(_))));
                    }
                }
                let result = runtime
                    .commit(|tx, record| {
                        Box::pin(async move {
                            tx.create_task(passive(), Value::Null, child_options(record.id))
                                .await?;
                            Ok(Some(TaskUpdate::Complete(Value::Null)))
                        })
                    })
                    .await;
                assert!(matches!(result, Err(SessionError::Invalid(_))));
                runner.close().await.unwrap();
                assert_eq!(run.await.unwrap(), RunResult::Interrupted);
                // Settled invocations release the reservation even while old runtime
                // handles and the stopped driver still exist.
                let id = task.id;
                session
                    .commit(move |tx| {
                        Box::pin(async move { tx.create_conversation_owned(Owner::Task(id)).await })
                    })
                    .await
                    .unwrap();
                assert!(runtime.is_cancelled());
                assert!(
                    runtime
                        .commit(|_, _| Box::pin(async { panic!("stale callback ran") }))
                        .await
                        .is_err()
                );
                session.close().await.unwrap();
            },
            zip(driver, task_driver),
        )
        .await;
    });
}

#[test]
fn private_update_uses_candidates_and_preserves_read_latch_and_attribution() {
    block_on(async {
        let definition = definition(Rc::new(|_, runtime| {
            Box::pin(async move {
                runtime
                    .commit(|tx, mut current| {
                        Box::pin(async move {
                            tx.append_entry(current.conversation_id, EntryDraft::new("attributed"))
                                .await?;
                            assert_eq!(
                                tx.task(current.id).await.unwrap_err(),
                                SessionError::ReadAfterWrite
                            );
                            current.memos =
                                Some(BTreeMap::from([("candidate".into(), json!(true))]));
                            tx.set_task(current).await?;
                            Ok(Some(TaskUpdate::Complete(json!("done"))))
                        })
                    })
                    .await
                    .map_err(|e| fault(e.to_string()))?;
                Ok(())
            })
        }));
        let (session, driver) = Session::new(MemoryStorage::new());
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = seed(&session, definition).await;
                let RunResult::Terminal(terminal) = runner.run(task.id).await.unwrap() else {
                    panic!()
                };
                assert!(terminal.memos.is_none());
                assert_eq!(terminal.kind, task.kind);
                assert_eq!(terminal.input, task.input);
                assert_eq!(
                    terminal.state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Completed {
                            result: json!("done")
                        }
                    }
                );
                session
                    .commit(move |tx| {
                        Box::pin(async move {
                            let entry = tx
                                .scan_entries(EntryQuery::new(task.conversation_id), 10, None)
                                .await?
                                .items
                                .remove(0);
                            assert_eq!(entry.by_task_id, Some(task.id));
                            let written = tx
                                .append_entry(task.conversation_id, EntryDraft::new("host"))
                                .await?;
                            assert_eq!(written.by_task_id, None);
                            Ok(())
                        })
                    })
                    .await
                    .unwrap();
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(driver, task_driver),
        )
        .await;
    });
}

#[test]
fn propagated_callback_panics_fault_and_discard_associated_writes() {
    for construction in [false, true] {
        block_on(async {
            let definition = definition(Rc::new(move |_, runtime| {
                Box::pin(async move {
                    runtime
                        .commit(move |tx, record| {
                            if construction {
                                panic!("callback construction");
                            }
                            Box::pin(async move {
                                tx.append_entry(
                                    record.conversation_id,
                                    EntryDraft::new("discarded"),
                                )
                                .await?;
                                std::panic::panic_any(17_u32);
                            })
                        })
                        .await
                        .unwrap();
                    Ok(())
                })
            }));
            let (session, driver) = Session::new(MemoryStorage::new());
            let (runner, task_driver) =
                TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap())
                    .unwrap();
            zip(
                async {
                    let task = seed(&session, definition).await;
                    let RunResult::Terminal(result) = runner.run(task.id).await.unwrap() else {
                        panic!()
                    };
                    let TaskState::Terminal {
                        outcome: TaskOutcome::Faulted { error },
                    } = result.state
                    else {
                        panic!()
                    };
                    assert_eq!(
                        error.message,
                        if construction {
                            "callback construction"
                        } else {
                            "Task handler panicked with a non-string payload"
                        }
                    );
                    session
                        .commit(move |tx| {
                            Box::pin(async move {
                                assert!(
                                    tx.scan_entries(
                                        EntryQuery::new(task.conversation_id),
                                        10,
                                        None
                                    )
                                    .await?
                                    .items
                                    .is_empty()
                                );
                                Ok(())
                            })
                        })
                        .await
                        .unwrap();
                    runner.close().await.unwrap();
                    session.close().await.unwrap();
                },
                zip(driver, task_driver),
            )
            .await;
        });
    }
}

// Poll to quiescence without sleeps. A gate, not the executor, controls progress.
async fn tick(driver: &mut (impl Future<Output = ()> + Unpin)) -> Option<()> {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct Signal(AtomicBool);
    impl std::task::Wake for Signal {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let signal = Arc::new(Signal(AtomicBool::new(false)));
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    loop {
        signal.0.store(false, Ordering::SeqCst);
        if Pin::new(&mut *driver).poll(&mut cx).is_ready() {
            return Some(());
        }
        if !signal.0.load(Ordering::SeqCst) {
            return None;
        }
    }
}

#[test]
fn driver_drop_fences_pending_callbacks_releases_lease_and_wakes_waiters() {
    block_on(async {
        let (definition, context) = captured();
        let (session, mut driver) = Session::new(MemoryStorage::new());
        let (runner, mut task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        let mut seeding = Box::pin(seed(&session, definition));
        assert!(poll_once(&mut seeding).await.is_none());
        tick(&mut driver).await;
        let task = seeding.await;
        let run = runner.run(task.id);
        tick(&mut task_driver).await;
        tick(&mut driver).await;
        tick(&mut task_driver).await;
        tick(&mut driver).await;
        tick(&mut task_driver).await;
        let runtime = context.await.unwrap();
        let (gate, receiver) = oneshot::channel();
        let mutation = runtime.commit(move |tx, record| {
            Box::pin(async move {
                receiver.await.unwrap();
                tx.append_entry(record.conversation_id, EntryDraft::new("fenced"))
                    .await?;
                Ok(Some(TaskUpdate::Complete(Value::Null)))
            })
        });
        tick(&mut driver).await;
        let queued = runner.run(task.id);
        let close = runner.close();
        drop(task_driver);
        assert_eq!(run.await.unwrap_err(), RunError::DriverStopped);
        assert_eq!(queued.await.unwrap(), RunResult::Interrupted);
        assert_eq!(close.await.unwrap_err(), RunError::DriverStopped);
        assert!(runtime.is_cancelled());
        // An old context cannot steal the new driver's lease.
        let (replacement, mut replacement_driver) =
            TaskRunner::attach(&session, TaskRegistry::default()).unwrap();
        gate.send(()).unwrap();
        tick(&mut driver).await;
        assert!(mutation.await.is_err());
        let id = task.id;
        let inspect = session.commit(move |tx| {
            Box::pin(async move {
                assert!(
                    tx.scan_entries(EntryQuery::new(task.conversation_id), 10, None)
                        .await?
                        .items
                        .is_empty()
                );
                assert_eq!(tx.task(id).await?.unwrap().status(), TaskStatus::Running);
                tx.create_conversation_owned(Owner::Task(id)).await
            })
        });
        tick(&mut driver).await;
        inspect.await.unwrap();
        let close = replacement.close();
        assert!(tick(&mut replacement_driver).await.is_some());
        close.await.unwrap();
        let close = session.close();
        tick(&mut driver).await;
        close.await.unwrap();
    });
}

#[test]
fn structural_equality_keeps_native_integer_precision() {
    assert!(json_equal(
        &json!({"phase":"run","n":1}),
        &json!({"n":1.0,"phase":"run"})
    ));
    assert!(!json_equal(&json!(u64::MAX), &json!(u64::MAX - 1)));
    assert!(!json_equal(&json!(u64::MAX), &json!(u64::MAX as f64)));
    assert!(json_equal(
        &json!([null, {"n": -0.0}]),
        &json!([null, {"n": 0}])
    ));
}

include!("tests/probe.rs");

#[test]
fn reservation_rejection_releases_guard_but_uncertainty_poison_stops_runner() {
    for fault_mode in [
        Fault::Rejected,
        Fault::Before,
        Fault::After,
        Fault::ConstructPanic,
        Fault::PollPanic,
    ] {
        block_on(async {
            let probe = Rc::new(Probe::default());
            let definition = definition(Rc::new(|_, runtime| {
                Box::pin(async move {
                    runtime
                        .commit(|_, _| {
                            Box::pin(async { Ok(Some(TaskUpdate::Complete(Value::Null))) })
                        })
                        .await
                        .map_err(|e| fault(e.to_string()))?;
                    Ok(())
                })
            }));
            let (session, driver) = Session::new(store(probe.clone()));
            let (runner, task_driver) =
                TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap())
                    .unwrap();
            zip(
                async {
                    let task = seed(&session, definition).await;
                    *probe.fault.borrow_mut() = fault_mode;
                    let error = runner.run(task.id).await.unwrap_err();
                    if matches!(fault_mode, Fault::Rejected) {
                        assert!(matches!(
                            error,
                            RunError::Session(SessionError::Storage(StorageError::Rejected(_)))
                        ));
                        // Safe rejection freed the lease and left Pending durable.
                        let id = task.id;
                        session
                            .commit(move |tx| {
                                Box::pin(async move {
                                    assert_eq!(
                                        tx.task(id).await?.unwrap().status(),
                                        TaskStatus::Pending
                                    );
                                    tx.create_conversation_owned(Owner::Task(id)).await
                                })
                            })
                            .await
                            .unwrap();
                        assert_eq!(
                            runner.run(id).await.unwrap(),
                            RunResult::Blocked(BlockReason::UnsupportedScope)
                        );
                        runner.close().await.unwrap();
                    } else {
                        assert!(matches!(
                            error,
                            RunError::Session(SessionError::Storage(StorageError::Other(_)))
                                | RunError::Panicked(_)
                        ));
                        assert_eq!(
                            session
                                .commit(|_| Box::pin(async { Ok(()) }))
                                .await
                                .unwrap_err(),
                            SessionError::Poisoned
                        );
                        assert_eq!(
                            runner.close().await.unwrap_err(),
                            RunError::Session(SessionError::Poisoned)
                        );
                    }
                    session.close().await.unwrap();
                    let state = probe.closed_tasks.borrow()[0].status();
                    assert_eq!(
                        state,
                        if matches!(fault_mode, Fault::After) {
                            TaskStatus::Running
                        } else {
                            TaskStatus::Pending
                        }
                    );
                },
                zip(driver, task_driver),
            )
            .await;
        });
    }
}

#[test]
fn runtime_checkpoint_rejection_vs_uncertainty_never_reports_fake_terminal_success() {
    for fault_mode in [
        Fault::Rejected,
        Fault::Before,
        Fault::After,
        Fault::ConstructPanic,
        Fault::PollPanic,
    ] {
        block_on(async {
            let probe = Rc::new(Probe::default());
            let handler_probe = probe.clone();
            let definition = definition(Rc::new(move |_, runtime| {
                *handler_probe.fault.borrow_mut() = fault_mode;
                Box::pin(async move {
                    // Safe rejection is caught, then normal no-progress handling
                    // faults. Uncertainty poisons regardless of this catch.
                    let _ = runtime
                        .commit(|tx, current| {
                            Box::pin(async move {
                                tx.append_entry(current.conversation_id, EntryDraft::new("atomic"))
                                    .await?;
                                Ok(Some(TaskUpdate::Checkpoint(
                                    json!({"phase":"run", "next":true}),
                                )))
                            })
                        })
                        .await;
                    Ok(())
                })
            }));
            let (session, driver) = Session::new(store(probe.clone()));
            let (runner, task_driver) =
                TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap())
                    .unwrap();
            zip(
                async {
                    let task = seed(&session, definition).await;
                    let result = runner.run(task.id).await;
                    if matches!(fault_mode, Fault::Rejected) {
                        let RunResult::Terminal(task) = result.unwrap() else {
                            panic!()
                        };
                        assert!(matches!(
                            task.state,
                            TaskState::Terminal {
                                outcome: TaskOutcome::Faulted { .. }
                            }
                        ));
                        runner.close().await.unwrap();
                    } else {
                        assert_eq!(
                            result.unwrap_err(),
                            RunError::Session(SessionError::Poisoned)
                        );
                        assert_eq!(
                            runner.close().await.unwrap_err(),
                            RunError::Session(SessionError::Poisoned)
                        );
                    }
                    session.close().await.unwrap();
                    if !matches!(fault_mode, Fault::Rejected) {
                        let tasks = probe.closed_tasks.borrow();
                        let TaskState::Running { checkpoint } = &tasks[0].state else {
                            panic!()
                        };
                        assert_eq!(
                            checkpoint.get("next").is_some(),
                            matches!(fault_mode, Fault::After)
                        );
                    }
                },
                zip(driver, task_driver),
            )
            .await;
        });
    }
}

#[test]
fn runner_close_joins_dropped_runtime_mutation_during_session_close() {
    block_on(async {
        let probe = Rc::new(Probe::default());
        let (definition, context) = captured();
        let (session, mut driver) = Session::new(store(probe.clone()));
        let (runner, mut task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        let mut seeding = Box::pin(seed(&session, definition));
        poll_once(&mut seeding).await;
        tick(&mut driver).await;
        let task = seeding.await;
        let run = runner.run(task.id);
        tick(&mut task_driver).await;
        tick(&mut driver).await;
        tick(&mut task_driver).await;
        tick(&mut driver).await;
        tick(&mut task_driver).await;
        let runtime = context.await.unwrap();
        let gate = Gate::default();
        *probe.commit_gate.borrow_mut() = Some(gate.clone());
        drop(runtime.commit(|tx, current| {
            Box::pin(async move {
                tx.append_entry(current.conversation_id, EntryDraft::new("drained"))
                    .await?;
                Ok(Some(TaskUpdate::Checkpoint(
                    json!({"phase":"run", "saved":true}),
                )))
            })
        }));
        tick(&mut driver).await; // Enter Storage.commit and hold its settlement.
        let mut runner_close = runner.close();
        let mut session_close = session.close();
        tick(&mut task_driver).await; // Handler cooperatively returns; barrier queued.
        tick(&mut driver).await;
        assert!(poll_once(&mut runner_close).await.is_none());
        assert!(poll_once(&mut session_close).await.is_none());
        gate.release();
        tick(&mut driver).await;
        tick(&mut task_driver).await;
        runner_close.await.unwrap();
        session_close.await.unwrap();
        assert_eq!(run.await.unwrap(), RunResult::Interrupted);
        let tasks = probe.closed_tasks.borrow();
        assert_eq!(
            tasks[0].state,
            TaskState::Running {
                checkpoint: json!({"phase":"run", "saved":true})
            }
        );
    });
}

#[test]
fn final_fence_covers_reservation_candidate_and_assembly_awaits() {
    // Reads: reserve (#1), reservation assembly (#2), dispatch (#3), runtime gate (#4),
    // candidate lookup (#5), runtime assembly (#6). A fault decision instead
    // reads current (#4) and then assembles (#5).
    for (at, decision) in [(2, false), (5, false), (6, false), (5, true)] {
        block_on(async {
            let probe = Rc::new(Probe::default());
            let gate = Gate::default();
            *probe.task_gate.borrow_mut() = Some((at, gate.clone()));
            let definition = definition(Rc::new(move |_, runtime| {
                Box::pin(async move {
                    if !decision {
                        let _ = runtime
                            .commit(|_, _| {
                                Box::pin(async { Ok(Some(TaskUpdate::Complete(Value::Null))) })
                            })
                            .await;
                    }
                    Ok(())
                })
            }));
            let (session, mut driver) = Session::new(store(probe.clone()));
            let (runner, mut task_driver) =
                TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap())
                    .unwrap();
            let mut seeding = Box::pin(seed(&session, definition));
            poll_once(&mut seeding).await;
            tick(&mut driver).await;
            let task = seeding.await;
            let run = runner.run(task.id);
            for _ in 0..4 {
                tick(&mut task_driver).await;
                tick(&mut driver).await;
            }
            assert_eq!(probe.task_reads.get(), at);
            drop(task_driver);
            gate.release();
            tick(&mut driver).await;
            assert_eq!(run.await.unwrap_err(), RunError::DriverStopped);
            let close = session.close();
            tick(&mut driver).await;
            close.await.unwrap();
            assert_eq!(
                probe.closed_tasks.borrow()[0].status(),
                if at == 2 {
                    TaskStatus::Pending
                } else {
                    TaskStatus::Running
                }
            );
            assert_eq!(
                probe
                    .events
                    .borrow()
                    .iter()
                    .filter(|&&e| e == "persisted")
                    .count(),
                if at == 2 { 1 } else { 2 }
            );
        });
    }
}

#[test]
fn graceful_close_during_candidate_or_assembly_await_allows_started_callback_to_settle() {
    for at in [5, 6] {
        block_on(async {
            let probe = Rc::new(Probe::default());
            let gate = Gate::default();
            *probe.task_gate.borrow_mut() = Some((at, gate.clone()));
            let definition = definition(Rc::new(|_, runtime| {
                Box::pin(async move {
                    runtime
                        .commit(|_, _| {
                            Box::pin(async { Ok(Some(TaskUpdate::Complete(Value::Null))) })
                        })
                        .await
                        .unwrap();
                    Ok(())
                })
            }));
            let (session, mut driver) = Session::new(store(probe.clone()));
            let (runner, mut task_driver) =
                TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap())
                    .unwrap();
            let mut seeding = Box::pin(seed(&session, definition));
            poll_once(&mut seeding).await;
            tick(&mut driver).await;
            let task = seeding.await;
            let run = runner.run(task.id);
            for _ in 0..4 {
                tick(&mut task_driver).await;
                tick(&mut driver).await;
            }
            assert_eq!(probe.task_reads.get(), at);
            let close = runner.close();
            gate.release();
            tick(&mut driver).await;
            tick(&mut task_driver).await;
            tick(&mut driver).await;
            tick(&mut task_driver).await;
            assert!(matches!(run.await.unwrap(), RunResult::Terminal(_)));
            close.await.unwrap();
            let close = session.close();
            tick(&mut driver).await;
            close.await.unwrap();
        });
    }
}

#[test]
fn cancellation_wakers_can_reenter_session_admission_without_borrow_panics() {
    thread_local! {
        static REENTER: RefCell<Option<Session>> = const { RefCell::new(None) };
        static WAKES: Cell<usize> = const { Cell::new(0) };
    }
    struct Reenter;
    impl std::task::Wake for Reenter {
        fn wake(self: std::sync::Arc<Self>) {
            REENTER.with(|slot| {
                if let Some(session) = slot.borrow().as_ref() {
                    drop(session.commit(|_| Box::pin(async { Ok(()) })));
                }
            });
            WAKES.with(|count| count.set(count.get() + 1));
        }
    }
    for mode in 0..5 {
        block_on(async {
            let probe = Rc::new(Probe::default());
            let (definition, context) = captured();
            let (session, mut driver) = Session::new(store(probe.clone()));
            let (runner, mut task_driver) =
                TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap())
                    .unwrap();
            let mut seeding = Box::pin(seed(&session, definition));
            poll_once(&mut seeding).await;
            tick(&mut driver).await;
            let task = seeding.await;
            drop(runner.run(task.id));
            tick(&mut task_driver).await;
            tick(&mut driver).await;
            tick(&mut task_driver).await;
            tick(&mut driver).await;
            tick(&mut task_driver).await;
            let runtime = context.await.unwrap();
            REENTER.with(|slot| *slot.borrow_mut() = Some(session.clone()));
            WAKES.with(|count| count.set(0));
            let waker = Waker::from(std::sync::Arc::new(Reenter));
            let mut cancellation = Box::pin(runtime.cancelled());
            assert!(
                cancellation
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            match mode {
                0 => {
                    drop(session.close());
                }
                1 => {
                    drop(runner.close());
                }
                2 => {
                    *probe.fault.borrow_mut() = Fault::Before;
                    drop(session.commit(|tx| Box::pin(async { tx.create_conversation().await })));
                    tick(&mut driver).await;
                }
                3 => {
                    drop(runner.abort(task.id));
                    tick(&mut driver).await;
                }
                _ => {
                    drop(driver);
                }
            }
            assert!(WAKES.with(|count| count.get()) > 0);
            REENTER.with(|slot| {
                slot.borrow_mut().take();
            });
            // Driver drops deliberately forfeit settlement in this waker test.
            drop(task_driver);
        });
    }
}

#[test]
fn task_driver_drop_does_not_cancel_a_storage_commit_already_started() {
    block_on(async {
        let probe = Rc::new(Probe::default());
        let (definition, context) = captured();
        let (session, mut driver) = Session::new(store(probe.clone()));
        let (runner, mut task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        let mut seeding = Box::pin(seed(&session, definition));
        poll_once(&mut seeding).await;
        tick(&mut driver).await;
        let task = seeding.await;
        let run = runner.run(task.id);
        tick(&mut task_driver).await;
        tick(&mut driver).await;
        tick(&mut task_driver).await;
        tick(&mut driver).await;
        tick(&mut task_driver).await;
        let runtime = context.await.unwrap();
        let gate = Gate::default();
        *probe.commit_gate.borrow_mut() = Some(gate.clone());
        let mutation =
            runtime.commit(|_, _| Box::pin(async { Ok(Some(TaskUpdate::Complete(Value::Null))) }));
        tick(&mut driver).await;
        assert_eq!(probe.events.borrow().last(), Some(&"commit entered"));
        drop(task_driver);
        assert_eq!(run.await.unwrap_err(), RunError::DriverStopped);
        gate.release();
        tick(&mut driver).await;
        mutation.await.unwrap();
        let close = session.close();
        tick(&mut driver).await;
        close.await.unwrap();
        assert_eq!(
            probe.closed_tasks.borrow()[0].status(),
            TaskStatus::Terminal
        );
    });
}

#[test]
fn memo_only_commits_are_not_checkpoint_progress() {
    block_on(async {
        let definition = definition(Rc::new(|_, runtime| {
            Box::pin(async move {
                runtime
                    .commit(|tx, mut task| {
                        Box::pin(async move {
                            task.memos = Some(BTreeMap::from([("memo".into(), json!("changed"))]));
                            tx.set_task(task).await?;
                            Ok(None)
                        })
                    })
                    .await
                    .unwrap();
                Ok(())
            })
        }));
        let (session, driver) = Session::new(MemoryStorage::new());
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = seed(&session, definition).await;
                let RunResult::Terminal(task) = runner.run(task.id).await.unwrap() else {
                    panic!()
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
            zip(driver, task_driver),
        )
        .await;
    });
}

mod abort;
