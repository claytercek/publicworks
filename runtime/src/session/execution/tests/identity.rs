use super::*;

fn invocation(runner: &Rc<RunnerControl>) -> Rc<Invocation> {
    Rc::new(Invocation {
        id: Id::new(10).unwrap(),
        abort_mode: false,
        joined: RefCell::new(Vec::new()),
        ended: Cell::new(false),
        cancelled: Cell::new(false),
        waiters: RefCell::new(Vec::new()),
        runner: Rc::downgrade(runner),
    })
}

#[test]
fn one_membership_map_preserves_identity_and_distinct_persistence_gates() {
    let runner = Rc::new(RunnerControl(RefCell::new(RunnerState::default())));
    let previous = invocation(&runner);
    let replacement = invocation(&runner);
    let stale = invocation(&runner);
    runner
        .0
        .borrow_mut()
        .actives
        .insert(previous.id, Rc::downgrade(&previous));
    assert!(previous.check().is_ok());
    assert!(replacement.check().is_err());
    runner
        .0
        .borrow_mut()
        .replace_current(&previous, &replacement);
    // A delayed handoff from an old phase must not replace the newer phase.
    runner.0.borrow_mut().replace_current(&previous, &stale);
    assert!(previous.check().is_err());
    assert!(replacement.check().is_ok());
    assert!(stale.check().is_err());
    let fence = PersistenceFence::Invocation {
        invocation: replacement.clone(),
        ending: false,
    };
    let ending = PersistenceFence::Invocation {
        invocation: replacement.clone(),
        ending: true,
    };
    runner.0.borrow_mut().closing = true;
    assert!(replacement.check().is_err());
    assert!(
        fence.check().is_ok(),
        "graceful close permits started persistence"
    );
    replacement.end();
    assert!(fence.check().is_err());
    assert!(
        ending.check().is_ok(),
        "phase settlement can persist its ending identity"
    );
    runner.0.borrow_mut().dropped = true;
    assert!(
        ending.check().is_err(),
        "driver forfeiture invalidates even ending fences"
    );
}

#[test]
fn driver_catches_session_handoff_panic_and_drains_the_escaped_replacement() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    thread_local! {
        static ON_HANDOFF: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
    }
    struct PanicAtHandoff;
    impl std::task::Wake for PanicAtHandoff {
        fn wake(self: Arc<Self>) {
            let callback = ON_HANDOFF.with(|slot| slot.borrow_mut().take());
            if let Some(callback) = callback {
                callback();
            }
        }
    }
    #[derive(Default)]
    struct ObserverWakes(AtomicUsize);
    impl std::task::Wake for ObserverWakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Escaped {
        runtime: TaskRuntime,
        cancelled: LocalFuture<'static, ()>,
        joined: LocalFuture<'static, ()>,
    }

    block_on(crate::test_support::bounded(async {
        let escaped = Rc::new(RefCell::new(None::<Escaped>));
        let wakes = Arc::new(ObserverWakes::default());
        let dispatched_second = Rc::new(Cell::new(false));
        let first: PhaseHandler = Rc::new({
            let escaped = escaped.clone();
            let wakes = wakes.clone();
            move |_, runtime| {
                let escaped = escaped.clone();
                let wakes = wakes.clone();
                Box::pin(async move {
                    runtime
                        .commit(|_, _| {
                            Box::pin(async {
                                Ok(Some(TaskUpdate::Checkpoint(json!({"phase":"second"}))))
                            })
                        })
                        .await
                        .unwrap();
                    let mut cancelled = Box::pin(runtime.cancelled());
                    let waker = Waker::from(Arc::new(PanicAtHandoff));
                    assert!(
                        cancelled
                            .as_mut()
                            .poll(&mut Context::from_waker(&waker))
                            .is_pending()
                    );
                    ON_HANDOFF.with(|slot| {
                        assert!(slot.borrow().is_none());
                        *slot.borrow_mut() = Some(Box::new(move || {
                            let runner = runtime.invocation.runner.upgrade().unwrap();
                            let replacement = runner.0.borrow().actives[&runtime.invocation.id]
                                .upgrade()
                                .unwrap();
                            assert!(!Rc::ptr_eq(&runtime.invocation, &replacement));
                            assert!(!replacement.ended.get());
                            assert!(replacement.check().is_ok());
                            let runtime = TaskRuntime {
                                invocation: replacement,
                                ..runtime
                            };
                            let mut cancelled = Box::pin(runtime.cancelled());
                            let invocation = runtime.invocation.clone();
                            let mut joined = Box::pin(async move { invocation.join().await });
                            let waker = Waker::from(wakes);
                            let mut cx = Context::from_waker(&waker);
                            assert!(cancelled.as_mut().poll(&mut cx).is_pending());
                            assert!(joined.as_mut().poll(&mut cx).is_pending());
                            *escaped.borrow_mut() = Some(Escaped {
                                runtime,
                                cancelled,
                                joined,
                            });
                            // This runs inside Session's phase-boundary callback,
                            // after publication of the replacement. The failed
                            // commit_join fences the OLD identity, then its waiter
                            // rethrows outside the handler's catch_unwind.
                            panic!("infrastructure wake during phase handoff");
                        }));
                    });
                    Ok(())
                })
            }
        });
        let second: PhaseHandler = Rc::new({
            let dispatched = dispatched_second.clone();
            move |_, _| {
                dispatched.set(true);
                Box::pin(async { Ok(()) })
            }
        });
        let definition = TaskDefinition::new(
            "panic-at-handoff",
            1,
            |_| Ok(json!({"phase":"first"})),
            BTreeMap::from([("first".into(), first), ("second".into(), second)]),
        );
        let (session, driver) = Session::new(MemoryStorage::new());
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = seed(&session, definition).await;
                assert_eq!(
                    runner.run(task.id).await,
                    Err(RunError::Panicked(
                        "infrastructure wake during phase handoff".into()
                    ))
                );
                assert!(!dispatched_second.get());
                let Escaped {
                    runtime,
                    cancelled,
                    joined,
                } = escaped
                    .borrow_mut()
                    .take()
                    .expect("handoff published a replacement before panicking");
                assert!(
                    runtime.invocation.ended.get(),
                    "driver must end the replacement, not just the old identity"
                );
                assert!(runtime.invocation.check().is_err());
                assert!(runner.control.0.borrow().actives.is_empty());
                assert_eq!(
                    wakes.0.load(Ordering::SeqCst),
                    2,
                    "both pending replacement observers were woken"
                );
                zip(cancelled, joined).await;
                // The panic is a failed Session callback, not an uncertain commit;
                // both drivers and close observers must still settle normally.
                session
                    .commit(|_| Box::pin(async { Ok(()) }))
                    .await
                    .unwrap();
                runner.close().await.unwrap();
                session.close().await.unwrap();
                ON_HANDOFF.with(|slot| assert!(slot.borrow().is_none()));
            },
            zip(driver, task_driver),
        )
        .await;
    }));
}

#[test]
fn foreground_completion_removes_and_ends_the_current_phase_replacement() {
    block_on(async {
        let seen = Rc::new(RefCell::new(Vec::<TaskRuntime>::new()));
        let first: PhaseHandler = Rc::new({
            let seen = seen.clone();
            move |_, runtime| {
                seen.borrow_mut().push(runtime.clone());
                Box::pin(async move {
                    runtime
                        .commit(|_, _| {
                            Box::pin(async {
                                Ok(Some(TaskUpdate::Checkpoint(json!({"phase":"second"}))))
                            })
                        })
                        .await
                        .unwrap();
                    Ok(())
                })
            }
        });
        let second: PhaseHandler = Rc::new({
            let seen = seen.clone();
            move |_, runtime| {
                assert!(seen.borrow()[0].invocation.check().is_err());
                assert!(runtime.invocation.check().is_ok());
                seen.borrow_mut().push(runtime.clone());
                Box::pin(async move {
                    runtime
                        .commit(|_, _| {
                            Box::pin(async { Ok(Some(TaskUpdate::Complete(Value::Null))) })
                        })
                        .await
                        .unwrap();
                    Ok(())
                })
            }
        });
        let definition = TaskDefinition::new(
            "two-phases",
            1,
            |_| Ok(json!({"phase":"first"})),
            BTreeMap::from([("first".into(), first), ("second".into(), second)]),
        );
        let (session, driver) = Session::new(MemoryStorage::new());
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        zip(
            async {
                let task = seed(&session, definition).await;
                assert!(matches!(
                    runner.run(task.id).await.unwrap(),
                    RunResult::Terminal(_)
                ));
                assert!(runner.control.0.borrow().actives.is_empty());
                {
                    let seen = seen.borrow();
                    assert_eq!(seen.len(), 2);
                    assert!(!Rc::ptr_eq(&seen[0].invocation, &seen[1].invocation));
                    for runtime in seen.iter() {
                        assert!(runtime.invocation.ended.get());
                        assert!(runtime.invocation.check().is_err());
                    }
                }
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(driver, task_driver),
        )
        .await;
    });
}
