// Private scheduling/fault regressions; public behavior lives in integration tests.
use super::*;

async fn pump(session: &mut SessionDriver, tasks: &mut TaskDriver) {
    for _ in 0..8 {
        if !tasks.control.0.borrow().finished {
            tick(tasks).await;
        }
        if !session.control.borrow().finished {
            tick(session).await;
        }
    }
}
async fn seeded(
    session: &Session,
    driver: &mut SessionDriver,
    definition: TaskDefinition,
) -> TaskRecord {
    let mut seed = Box::pin(seed(session, definition));
    poll_once(&mut seed).await;
    tick(driver).await;
    seed.await
}

#[test]
fn acknowledgment_is_driver_owned_and_joins_normal_not_cleanup() {
    for drop_waiter in [false, true] {
        block_on(async {
            let probe = Rc::new(Probe::default());
            let (definition, context) = captured();
            let cleanup_gate = Gate::default();
            let gate = cleanup_gate.clone();
            let fresh = Rc::new(RefCell::new(None));
            let seen = fresh.clone();
            let definition = definition.with_abort_handler(Rc::new(move |_, runtime| {
                *seen.borrow_mut() = Some(runtime.clone());
                let gate = gate.clone();
                Box::pin(async move {
                    gate.wait().await;
                    runtime
                        .commit(|_, _| {
                            Box::pin(async {
                                Ok(Some(TaskUpdate::Abort {
                                    reason: None,
                                    result: Some(Value::Null),
                                }))
                            })
                        })
                        .await
                        .unwrap();
                    Ok(())
                })
            }));
            let (session, mut driver) = Session::new(store(probe.clone()));
            let (runner, mut tasks) =
                TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap())
                    .unwrap();
            let task = seeded(&session, &mut driver, definition).await;
            let mut run = runner.run(task.id);
            pump(&mut driver, &mut tasks).await;
            let old = context.await.unwrap();
            let storage_gate = Gate::default();
            *probe.commit_gate.borrow_mut() = Some(storage_gate.clone());
            let mut abort = Some(runner.abort(task.id));
            if drop_waiter {
                drop(abort.take());
            }
            tick(&mut driver).await;
            assert!(!old.is_cancelled());
            storage_gate.release();
            tick(&mut driver).await;
            assert!(old.is_cancelled());
            if let Some(waiter) = abort.as_mut() {
                assert!(poll_once(waiter).await.is_none());
            }
            pump(&mut driver, &mut tasks).await;
            if let Some(waiter) = abort {
                assert_eq!(waiter.await.unwrap(), AbortResult::Marked);
            }
            assert!(poll_once(&mut run).await.is_none());
            let fresh = fresh.borrow().clone().unwrap();
            assert!(!fresh.is_cancelled());
            let count = probe
                .events
                .borrow()
                .iter()
                .filter(|&&e| e == "persisted")
                .count();
            let repeated = runner.abort(task.id);
            tick(&mut driver).await;
            assert_eq!(repeated.await.unwrap(), AbortResult::Marked);
            assert!(!fresh.is_cancelled());
            assert_eq!(
                probe
                    .events
                    .borrow()
                    .iter()
                    .filter(|&&e| e == "persisted")
                    .count(),
                count
            );
            let stale = old.commit(|_, _| Box::pin(async { panic!("old runtime callback") }));
            tick(&mut driver).await;
            assert!(stale.await.is_err());
            cleanup_gate.release();
            pump(&mut driver, &mut tasks).await;
            assert!(matches!(run.await.unwrap(), RunResult::Terminal(_)));
            let terminal = runner.abort(task.id);
            tick(&mut driver).await;
            assert_eq!(terminal.await.unwrap(), AbortResult::Terminal);
            let close = runner.close();
            pump(&mut driver, &mut tasks).await;
            close.await.unwrap();
            drop(tasks); // Completed-driver drop must not turn close into forfeiture.
            runner.close().await.unwrap();
            let close = session.close();
            tick(&mut driver).await;
            close.await.unwrap();
        });
    }
}

#[test]
fn abort_overtakes_reservation_or_dispatch_receipt_without_constructing_normal_handler() {
    for dispatch_queued in [false, true] {
        block_on(async {
            let definition = definition(Rc::new(|_, _| {
                panic!("normal constructor ran after abort ack")
            }));
            let (session, mut driver) = Session::new(MemoryStorage::new());
            let (runner, mut tasks) =
                TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap())
                    .unwrap();
            let task = seeded(&session, &mut driver, definition).await;
            let run = runner.run(task.id);
            tick(&mut tasks).await; // Reservation is queued.
            if dispatch_queued {
                tick(&mut driver).await;
                tick(&mut tasks).await; // Dispatch is now ahead of abort.
            }
            let abort = runner.abort(task.id);
            tick(&mut driver).await; // Both receipts exist before TaskDriver resumes.
            pump(&mut driver, &mut tasks).await;
            assert_eq!(abort.await.unwrap(), AbortResult::Marked);
            assert!(matches!(
                run.await.unwrap(),
                RunResult::Terminal(TaskRecord {
                    state: TaskState::Terminal {
                        outcome: TaskOutcome::Aborted { .. }
                    },
                    ..
                })
            ));
            let close = runner.close();
            pump(&mut driver, &mut tasks).await;
            close.await.unwrap();
            let close = session.close();
            tick(&mut driver).await;
            close.await.unwrap();
        });
    }
}

#[test]
fn admitted_abort_survives_task_driver_drop_at_every_mutation_stage() {
    for stage in 0..3 {
        block_on(async {
            let probe = Rc::new(Probe::default());
            let definition = passive();
            let (session, mut driver) = Session::new(store(probe.clone()));
            let (runner, mut tasks) =
                TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap())
                    .unwrap();
            let task = seeded(&session, &mut driver, definition).await;
            let gate = Gate::default();
            if stage == 1 {
                *probe.task_gate.borrow_mut() = Some((probe.task_reads.get() + 1, gate.clone()));
            }
            if stage == 2 {
                *probe.commit_gate.borrow_mut() = Some(gate.clone());
            }
            let abort = runner.abort(task.id);
            if stage != 0 {
                tick(&mut driver).await;
            }
            let close = runner.close();
            tick(&mut tasks).await;
            drop(tasks);
            assert_eq!(close.await.unwrap_err(), RunError::DriverStopped);
            assert_eq!(
                runner.abort(task.id).await.unwrap_err(),
                RunError::DriverStopped
            );
            gate.release();
            tick(&mut driver).await;
            assert_eq!(abort.await.unwrap(), AbortResult::Marked);
            let close = session.close();
            tick(&mut driver).await;
            close.await.unwrap();
            assert!(probe.closed_tasks.borrow()[0].abort_requested);
        });
    }
}

#[test]
fn mark_failures_never_acknowledge_abort_and_uncertainty_closes() {
    for mode in [
        Fault::Rejected,
        Fault::Before,
        Fault::After,
        Fault::ConstructPanic,
        Fault::PollPanic,
    ] {
        block_on(async {
            let probe = Rc::new(Probe::default());
            let (definition, context) = captured();
            let (session, mut driver) = Session::new(store(probe.clone()));
            let (runner, mut tasks) =
                TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap())
                    .unwrap();
            let task = seeded(&session, &mut driver, definition).await;
            let run = runner.run(task.id);
            pump(&mut driver, &mut tasks).await;
            let runtime = context.await.unwrap();
            *probe.fault.borrow_mut() = mode;
            let abort = runner.abort(task.id);
            tick(&mut driver).await;
            assert!(abort.await.is_err());
            if matches!(mode, Fault::Rejected) {
                assert!(!runtime.is_cancelled());
            } else {
                assert!(runtime.is_cancelled());
            }
            let close = runner.close();
            pump(&mut driver, &mut tasks).await;
            if matches!(mode, Fault::Rejected) {
                close.await.unwrap();
                assert_eq!(run.await.unwrap(), RunResult::Interrupted);
            } else {
                assert!(close.await.is_err());
                assert!(run.await.is_err());
            }
            let close = session.close();
            tick(&mut driver).await;
            close.await.unwrap();
        });
    }
}

#[test]
fn close_waits_for_dropped_abort_mutations_without_starting_cleanup() {
    block_on(async {
        let probe = Rc::new(Probe::default());
        let definition =
            passive().with_abort_handler(Rc::new(|_, _| panic!("close started cleanup")));
        let (session, mut driver) = Session::new(store(probe.clone()));
        let (runner, mut tasks) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        let task = seeded(&session, &mut driver, definition).await;
        let gate = Gate::default();
        *probe.commit_gate.borrow_mut() = Some(gate.clone());
        drop(runner.abort(task.id));
        tick(&mut driver).await;
        let mut close = runner.close();
        let storage_close = session.close();
        tick(&mut tasks).await;
        assert!(poll_once(&mut close).await.is_none());
        gate.release();
        pump(&mut driver, &mut tasks).await;
        close.await.unwrap();
        storage_close.await.unwrap();
        assert!(probe.closed_tasks.borrow()[0].abort_requested);
    });
}

#[test]
fn abort_outcome_rejection_faults_but_uncertainty_never_claims_a_terminal_receipt() {
    for mode in [
        Fault::Rejected,
        Fault::Before,
        Fault::After,
        Fault::ConstructPanic,
        Fault::PollPanic,
    ] {
        block_on(async {
            let probe = Rc::new(Probe::default());
            let failures = probe.clone();
            let definition = passive().with_abort_handler(Rc::new(move |_, runtime| {
                *failures.fault.borrow_mut() = mode;
                Box::pin(async move {
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
                        .map_err(|e| fault(e.to_string()))?;
                    Ok(())
                })
            }));
            let (session, mut driver) = Session::new(store(probe.clone()));
            let (runner, mut tasks) =
                TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap())
                    .unwrap();
            let task = seeded(&session, &mut driver, definition).await;
            let abort = runner.abort(task.id);
            tick(&mut driver).await;
            abort.await.unwrap();
            let run = runner.run(task.id);
            pump(&mut driver, &mut tasks).await;
            if matches!(mode, Fault::Rejected) {
                assert!(matches!(
                    run.await.unwrap(),
                    RunResult::Terminal(TaskRecord {
                        state: TaskState::Terminal {
                            outcome: TaskOutcome::Faulted { .. }
                        },
                        ..
                    })
                ));
            } else {
                assert!(run.await.is_err());
            }
            let close = runner.close();
            pump(&mut driver, &mut tasks).await;
            if matches!(mode, Fault::Rejected) {
                close.await.unwrap();
            } else {
                assert!(close.await.is_err());
            }
            let close = session.close();
            tick(&mut driver).await;
            close.await.unwrap();
            let record = &probe.closed_tasks.borrow()[0];
            assert!(record.abort_requested);
            if matches!(mode, Fault::After) {
                assert!(matches!(
                    record.state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Aborted { .. }
                    }
                ));
            }
        });
    }
}

#[test]
fn driver_drop_fences_abort_reservation_and_outcome_preparation_not_dispatched_storage() {
    // 0: reservation assembly; 1/2: outcome candidate/assembly; 3: dispatched commit.
    for stage in 0..4 {
        block_on(async {
            let probe = Rc::new(Probe::default());
            let gate = Gate::default();
            let preparation = probe.clone();
            let block = gate.clone();
            let definition = passive().with_abort_handler(Rc::new(move |_, runtime| {
                let probe = preparation.clone();
                let gate = block.clone();
                Box::pin(async move {
                    runtime
                        .commit(move |_, _| {
                            Box::pin(async move {
                                if stage == 3 {
                                    *probe.commit_gate.borrow_mut() = Some(gate);
                                } else {
                                    *probe.task_gate.borrow_mut() =
                                        Some((probe.task_reads.get() + stage, gate));
                                }
                                Ok(Some(TaskUpdate::Abort {
                                    reason: None,
                                    result: None,
                                }))
                            })
                        })
                        .await
                        .map_err(|e| fault(e.to_string()))?;
                    Ok(())
                })
            }));
            let (session, mut driver) = Session::new(store(probe.clone()));
            let (runner, mut tasks) =
                TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap())
                    .unwrap();
            let task = seeded(&session, &mut driver, definition).await;
            let mark = runner.abort(task.id);
            tick(&mut driver).await;
            mark.await.unwrap();
            if stage == 0 {
                *probe.task_gate.borrow_mut() = Some((probe.task_reads.get() + 2, gate.clone()));
            }
            let run = runner.run(task.id);
            pump(&mut driver, &mut tasks).await;
            assert!(
                gate.0.borrow().1.is_some(),
                "stage {stage} did not reach gate"
            );
            drop(tasks);
            gate.release();
            tick(&mut driver).await;
            assert_eq!(run.await.unwrap_err(), RunError::DriverStopped);
            let close = session.close();
            tick(&mut driver).await;
            close.await.unwrap();
            let record = &probe.closed_tasks.borrow()[0];
            assert!(record.abort_requested);
            assert_eq!(
                record.status(),
                match stage {
                    0 => TaskStatus::Pending,
                    3 => TaskStatus::Terminal,
                    _ => TaskStatus::Running,
                }
            );
        });
    }
}

#[test]
fn abort_handoff_keeps_tree_guard_for_both_candidate_staging_orders() {
    block_on(async {
        let (definition, context) = captured();
        let definition = definition.with_abort_handler(Rc::new(|_, runtime| {
            Box::pin(async move {
                for owner_first in [false, true] {
                    for conversation in [false, true] {
                        let rejected = runtime
                            .commit(move |tx, owner| {
                                Box::pin(async move {
                                    if owner_first {
                                        tx.set_task(owner.clone()).await?;
                                    }
                                    if conversation {
                                        tx.create_conversation_owned(Owner::Task(owner.id)).await?;
                                    } else {
                                        tx.create_task(
                                            passive(),
                                            Value::Null,
                                            child_options(owner.id),
                                        )
                                        .await?;
                                    }
                                    if !owner_first {
                                        tx.set_task(owner).await?;
                                    }
                                    Ok(Some(TaskUpdate::Complete(Value::Null)))
                                })
                            })
                            .await;
                        assert!(matches!(rejected, Err(SessionError::Invalid(_))));
                    }
                }
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
            })
        }));
        let (session, driver) = Session::new(MemoryStorage::new());
        let (runner, tasks) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = seed(&session, definition).await;
                let run = runner.run(task.id);
                let old = context.await.unwrap();
                let abort = runner.abort(task.id);
                // This queued mutation runs between acknowledgment and handoff.
                let id = task.id;
                let child = session.commit(move |tx| {
                    Box::pin(async move { tx.create_conversation_owned(Owner::Task(id)).await })
                });
                assert!(matches!(child.await, Err(SessionError::Invalid(_))));
                assert_eq!(abort.await.unwrap(), AbortResult::Marked);
                assert!(old.is_cancelled());
                assert!(matches!(run.await.unwrap(), RunResult::Terminal(_)));
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(driver, tasks),
        )
        .await;
    });
}

#[test]
fn session_driver_drop_releases_abort_drain_ownership() {
    for dispatched in [false, true] {
        block_on(async {
            let probe = Rc::new(Probe::default());
            let definition = passive();
            let (session, mut driver) = Session::new(store(probe.clone()));
            let (runner, mut tasks) =
                TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap())
                    .unwrap();
            let task = seeded(&session, &mut driver, definition).await;
            *probe.commit_gate.borrow_mut() = Some(Gate::default());
            let abort = runner.abort(task.id);
            if dispatched {
                tick(&mut driver).await;
            }
            let mut close = runner.close();
            tick(&mut tasks).await;
            assert!(poll_once(&mut close).await.is_none());
            drop(driver);
            assert_eq!(
                abort.await.unwrap_err(),
                RunError::Session(SessionError::DriverStopped)
            );
            assert!(tick(&mut tasks).await.is_some());
            assert_eq!(
                close.await.unwrap_err(),
                RunError::Session(SessionError::DriverStopped)
            );
        });
    }
}

#[test]
fn reentrant_abort_drain_wake_cannot_report_success_during_session_driver_drop() {
    thread_local! {
        static TASKS: RefCell<Option<TaskDriver>> = const { RefCell::new(None) };
    }
    struct Reenter;
    impl std::task::Wake for Reenter {
        fn wake(self: std::sync::Arc<Self>) {
            TASKS.with(|slot| {
                if let Some(tasks) = slot.borrow_mut().as_mut()
                    && !tasks.control.0.borrow().finished
                {
                    let waker = Waker::from(self);
                    let _ = Pin::new(tasks).poll(&mut Context::from_waker(&waker));
                }
            });
        }
    }
    block_on(async {
        let definition = passive();
        let (session, mut driver) = Session::new(MemoryStorage::new());
        let (runner, mut tasks) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        let task = seeded(&session, &mut driver, definition).await;
        let abort = runner.abort(task.id);
        let mut close = runner.close();
        let waker = Waker::from(std::sync::Arc::new(Reenter));
        assert!(
            Pin::new(&mut tasks)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        TASKS.with(|slot| *slot.borrow_mut() = Some(tasks));
        drop(driver);
        assert_eq!(
            poll_once(&mut close).await,
            Some(Err(RunError::Session(SessionError::DriverStopped)))
        );
        assert_eq!(
            abort.await.unwrap_err(),
            RunError::Session(SessionError::DriverStopped)
        );
        TASKS.with(|slot| {
            slot.borrow_mut().take();
        });
    });
}
