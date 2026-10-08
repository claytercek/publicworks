use super::*;

fn id(n: u64) -> Id {
    Id::new(n).unwrap()
}
fn record(n: u64, owner: Option<u64>) -> TaskRecord {
    TaskRecord {
        id: id(n),
        conversation_id: id(1),
        kind: "leaf".into(),
        version: 1,
        input: Value::Null,
        owner: owner.map(id),
        background: false,
        abort_requested: false,
        state: TaskState::Pending {
            checkpoint: json!({"phase":"run"}),
        },
        memos: None,
    }
}
fn waiting(record: &mut TaskRecord, on: &[u64], policy: JoinPolicy) {
    record.state = TaskState::Waiting {
        checkpoint: json!({"phase":"run"}),
        on: on.iter().copied().map(id).collect(),
        policy,
    };
}
async fn seeded(probe: Rc<Probe>, records: Vec<TaskRecord>) -> Store {
    let mut storage = store(probe);
    let mut writes = vec![StorageWrite::Conversation(ConversationRecord {
        id: id(1),
        parent: None,
        owner: None,
    })];
    writes.extend(records.into_iter().map(StorageWrite::Task));
    storage.commit(writes).await.unwrap();
    storage
}
async fn pump(session: &mut SessionDriver, tasks: &mut TaskDriver) {
    let mut session_done = false;
    for _ in 0..16 {
        if tick(tasks).await.is_some() {
            break;
        }
        if !session_done {
            session_done = tick(session).await.is_some();
        }
    }
}
fn complete(kind: &str) -> TaskDefinition {
    TaskDefinition::new(
        kind,
        1,
        |_| Ok(json!({"phase":"run"})),
        BTreeMap::from([(
            "run".into(),
            Rc::new(|_, runtime: TaskRuntime| -> PhaseFuture {
                Box::pin(async move {
                    runtime
                        .commit(|_, _| {
                            Box::pin(async { Ok(Some(TaskUpdate::Complete(Value::Null))) })
                        })
                        .await
                        .map_err(|e| fault(e.to_string()))?;
                    Ok(())
                })
            }) as PhaseHandler,
        )]),
    )
}

#[test]
fn subtree_ack_signals_pending_child_and_joins_only_requested_target() {
    for orphan_fail_fast in [false, true] {
        for dropped_observer in [false, true] {
            block_on(async {
                let probe = Rc::new(Probe::default());
                let (child_definition, context) = captured();
                let mut root = record(10, None);
                root.kind = "root".into();
                waiting(
                    &mut root,
                    &[20, 30],
                    if orphan_fail_fast {
                        JoinPolicy::FailFast
                    } else {
                        JoinPolicy::AllSettled
                    },
                );
                let mut missing = record(20, Some(10));
                missing.kind = "missing".into();
                let child = record(30, Some(10));
                let storage = seeded(probe.clone(), vec![root, missing, child]).await;
                let (session, mut driver) = Session::new(storage);
                let (runner, mut tasks) = TaskRunner::attach(
                    &session,
                    TaskRegistry::new([complete("root"), child_definition]).unwrap(),
                )
                .unwrap();
                let run = runner.run(id(10));
                pump(&mut driver, &mut tasks).await;
                let runtime = context.await.unwrap();
                assert!(!runtime.is_cancelled());
                let gate = Gate::default();
                *probe.commit_gate.borrow_mut() = Some(gate.clone());
                let mut abort = Some(runner.abort(id(if orphan_fail_fast { 20 } else { 10 })));
                if dropped_observer {
                    drop(abort.take());
                }
                tick(&mut driver).await;
                assert!(!runtime.is_cancelled(), "no signal before acknowledgement");
                let before = probe
                    .events
                    .borrow()
                    .iter()
                    .filter(|&&e| e == "persisted")
                    .count();
                gate.release();
                tick(&mut driver).await;
                assert!(
                    runtime.is_cancelled(),
                    "Session ACK signals while child handler remains pending"
                );
                assert_eq!(
                    probe
                        .events
                        .borrow()
                        .iter()
                        .filter(|&&e| e == "persisted")
                        .count(),
                    before + 1
                );
                if let Some(abort) = abort {
                    assert_eq!(abort.await.unwrap(), AbortResult::Marked);
                }
                let read = session.commit(|tx| {
                    Box::pin(async { tx.scan_tasks(TaskQuery::default(), 10, None).await })
                });
                tick(&mut driver).await;
                let records = read.await.unwrap().value.items;
                assert_eq!(
                    records
                        .iter()
                        .find(|r| r.id == id(10))
                        .unwrap()
                        .abort_requested,
                    !orphan_fail_fast
                );
                assert!(
                    records
                        .iter()
                        .find(|r| r.id == id(30))
                        .unwrap()
                        .abort_requested
                );
                pump(&mut driver, &mut tasks).await;
                let RunResult::Terminal(root) = run.await.unwrap() else {
                    panic!()
                };
                assert!(
                    matches!(
                        root.state,
                        TaskState::Terminal {
                            outcome: TaskOutcome::Completed { .. }
                        }
                    ) == orphan_fail_fast
                );
                let close = runner.close();
                tick(&mut tasks).await;
                close.await.unwrap();
                let close = session.close();
                tick(&mut driver).await;
                close.await.unwrap();
            });
        }
    }
}

#[test]
fn drive_drop_fences_scans_in_reconciliation_reservation_and_final_assembly() {
    // No task invocation exists during the first scan; only the drive fence can
    // protect a reconciliation write. The later scans exercise invocation fences.
    for at in 1..=4 {
        block_on(async {
            let probe = Rc::new(Probe::default());
            let mut root = record(10, None);
            if at == 1 {
                waiting(&mut root, &[], JoinPolicy::AllSettled);
            }
            let storage = seeded(probe.clone(), vec![root.clone()]).await;
            let gate = Gate::default();
            *probe.scan_gate.borrow_mut() = Some((at, gate.clone()));
            let (session, mut driver) = Session::new(storage);
            let (runner, mut tasks) =
                TaskRunner::attach(&session, TaskRegistry::new([complete("leaf")]).unwrap())
                    .unwrap();
            let run = runner.run(root.id);
            pump(&mut driver, &mut tasks).await;
            assert_eq!(probe.scan_reads.get(), at);
            drop(tasks);
            gate.release();
            tick(&mut driver).await;
            assert_eq!(run.await.unwrap_err(), RunError::DriverStopped);
            let close = session.close();
            tick(&mut driver).await;
            close.await.unwrap();
            let persisted = &probe.closed_tasks.borrow()[0];
            if at == 1 {
                assert_eq!(persisted, &root);
            } else if at <= 3 {
                assert_eq!(persisted.status(), TaskStatus::Pending);
            } else {
                assert_eq!(persisted.status(), TaskStatus::Running);
            }
            assert_eq!(
                probe
                    .events
                    .borrow()
                    .iter()
                    .filter(|&&e| e == "persisted")
                    .count(),
                if at <= 3 { 1 } else { 2 }
            );
        });
    }
}

#[test]
fn late_page_background_and_owned_conversations_are_supported_boundaries() {
    for owned_conversation in [false, true] {
        block_on(async {
            let probe = Rc::new(Probe::default());
            let mut records = vec![record(10, None)];
            for n in 20..170 {
                let mut child = record(n, Some(10));
                if n == 169 && !owned_conversation {
                    child.owner = None;
                    child.background = true;
                }
                records.push(child);
            }
            let mut storage = seeded(probe.clone(), records.clone()).await;
            if owned_conversation {
                storage
                    .commit(vec![StorageWrite::Conversation(ConversationRecord {
                        id: id(200),
                        parent: None,
                        owner: Some(OwnerLink {
                            conversation_id: id(1),
                            task_id: id(169),
                        }),
                    })])
                    .await
                    .unwrap();
            }
            let (session, driver) = Session::new(storage);
            let (runner, tasks) =
                TaskRunner::attach(&session, TaskRegistry::new([complete("leaf")]).unwrap())
                    .unwrap();
            zip(
                async {
                    assert!(matches!(
                        runner.run(id(10)).await.unwrap(),
                        RunResult::Terminal(_)
                    ));
                    if !owned_conversation {
                        let background = session
                            .commit(|tx| Box::pin(async { tx.task(id(169)).await }))
                            .await
                            .unwrap()
                            .value
                            .unwrap();
                        assert_eq!(background.status(), TaskStatus::Pending);
                        assert!(matches!(
                            runner.run(id(169)).await.unwrap(),
                            RunResult::Terminal(_)
                        ));
                    }
                    runner.close().await.unwrap();
                    session.close().await.unwrap();
                },
                zip(driver, tasks),
            )
            .await;
        });
    }
}

#[test]
fn persisted_missing_wait_edges_and_terminal_owners_follow_reference() {
    block_on(async {
        let probe = Rc::new(Probe::default());
        let mut root = record(10, None);
        root.state = TaskState::Completing {
            outcome: TaskOutcome::Completed {
                result: json!("held"),
            },
        };
        let mut middle = record(20, Some(10));
        middle.abort_requested = true;
        middle.state = TaskState::Terminal {
            outcome: TaskOutcome::Failed {
                error: fault("old"),
                result: None,
            },
        };
        let mut leaf = record(30, Some(20));
        waiting(&mut leaf, &[999, 999], JoinPolicy::AllSettled);
        let storage = seeded(probe.clone(), vec![root, middle.clone(), leaf]).await;
        let (session, driver) = Session::new(storage);
        let (runner, tasks) =
            TaskRunner::attach(&session, TaskRegistry::new([complete("leaf")]).unwrap()).unwrap();
        zip(
            async {
                let RunResult::Terminal(root) = runner.run(id(10)).await.unwrap() else {
                    panic!()
                };
                assert_eq!(
                    root.state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Completed {
                            result: json!("held")
                        }
                    }
                );
                let leaf = session
                    .commit(|tx| Box::pin(async { tx.task(id(30)).await }))
                    .await
                    .unwrap()
                    .value
                    .unwrap();
                assert!(!leaf.abort_requested, "terminal ancestors do not cancel");
                assert!(matches!(
                    leaf.state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Completed { .. }
                    }
                ));
                runner.close().await.unwrap();
                session.close().await.unwrap();
                assert_eq!(probe.closed_tasks.borrow()[1], middle);
            },
            zip(driver, tasks),
        )
        .await;
    });
}

#[test]
fn conversation_owner_cycles_and_missing_chains_block_without_partial_writes() {
    block_on(async {
        let probe = Rc::new(Probe::default());
        let mut storage = store(probe.clone());
        let first = record(10, None);
        let mut second = record(20, None);
        second.conversation_id = id(2);
        let mut missing = record(30, None);
        missing.conversation_id = id(3);
        storage
            .commit(vec![
                StorageWrite::Conversation(ConversationRecord {
                    id: id(1),
                    parent: None,
                    owner: Some(OwnerLink {
                        conversation_id: id(2),
                        task_id: second.id,
                    }),
                }),
                StorageWrite::Conversation(ConversationRecord {
                    id: id(2),
                    parent: None,
                    owner: Some(OwnerLink {
                        conversation_id: id(1),
                        task_id: first.id,
                    }),
                }),
                StorageWrite::Conversation(ConversationRecord {
                    id: id(3),
                    parent: None,
                    owner: Some(OwnerLink {
                        conversation_id: id(99),
                        task_id: id(999),
                    }),
                }),
                StorageWrite::Task(first),
                StorageWrite::Task(second),
                StorageWrite::Task(missing),
            ])
            .await
            .unwrap();
        let (session, driver) = Session::new(storage);
        let (runner, tasks) =
            TaskRunner::attach(&session, TaskRegistry::new([complete("leaf")]).unwrap()).unwrap();
        zip(
            async {
                let before = probe
                    .events
                    .borrow()
                    .iter()
                    .filter(|&&event| event == "persisted")
                    .count();
                for target in [id(10), id(20), id(30)] {
                    assert_eq!(
                        runner.run(target).await.unwrap(),
                        RunResult::Blocked(BlockReason::UnsupportedScope)
                    );
                    assert_eq!(
                        runner.abort(target).await.unwrap(),
                        AbortResult::Blocked(BlockReason::UnsupportedScope)
                    );
                }
                assert_eq!(
                    probe
                        .events
                        .borrow()
                        .iter()
                        .filter(|&&event| event == "persisted")
                        .count(),
                    before
                );
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(driver, tasks),
        )
        .await;
    });
}

#[test]
fn malformed_ownership_and_wait_cycles_quiesce_without_partial_writes() {
    block_on(async {
        let probe = Rc::new(Probe::default());
        let root = record(10, None);
        let mut a = record(20, Some(10));
        let mut b = record(30, Some(10));
        waiting(&mut a, &[30], JoinPolicy::AllSettled);
        waiting(&mut b, &[20], JoinPolicy::AllSettled);
        let missing = record(40, Some(999));
        let mut cycle_a = record(50, Some(60));
        cycle_a.kind = "missing".into();
        let cycle_b = record(60, Some(50));
        let mut root = root;
        root.kind = "missing".into();
        let storage = seeded(
            probe.clone(),
            vec![root.clone(), a, b, missing, cycle_a, cycle_b],
        )
        .await;
        let (session, driver) = Session::new(storage);
        let (runner, tasks) = TaskRunner::attach(&session, TaskRegistry::default()).unwrap();
        zip(
            async {
                assert_eq!(
                    runner.run(id(10)).await.unwrap(),
                    RunResult::Suspended(root)
                );
                for target in [40, 50, 60] {
                    assert_eq!(
                        runner.abort(id(target)).await.unwrap(),
                        AbortResult::Blocked(BlockReason::UnsupportedScope)
                    );
                    assert_eq!(
                        runner.run(id(target)).await.unwrap(),
                        RunResult::Blocked(BlockReason::UnsupportedScope)
                    );
                }
                assert_eq!(
                    probe
                        .events
                        .borrow()
                        .iter()
                        .filter(|&&e| e == "persisted")
                        .count(),
                    1
                );
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(driver, tasks),
        )
        .await;
    });
}

#[test]
fn candidate_wait_is_atomic_and_final_owners_reject_new_children_in_both_orders() {
    for finish in [false, true] {
        for update_first in [false, true] {
            block_on(async {
                let probe = Rc::new(Probe::default());
                let root = record(10, None);
                let storage = seeded(probe.clone(), vec![root.clone()]).await;
                let (session, driver) = Session::new(storage);
                zip(
                    async {
                        let receipt = session
                            .commit(move |tx| {
                                Box::pin(async move {
                                    if update_first {
                                        tx.update_task(
                                            root.id,
                                            if finish {
                                                TaskUpdate::Complete(Value::Null)
                                            } else {
                                                TaskUpdate::Wait {
                                                    checkpoint: json!({"phase":"run"}),
                                                    on: vec![],
                                                    policy: JoinPolicy::AllSettled,
                                                }
                                            },
                                        )
                                        .await?;
                                    }
                                    let child = tx
                                        .create_task(
                                            complete("child"),
                                            Value::Null,
                                            child_options(root.id),
                                        )
                                        .await?;
                                    if !update_first {
                                        tx.update_task(
                                            root.id,
                                            if finish {
                                                TaskUpdate::Complete(Value::Null)
                                            } else {
                                                TaskUpdate::Wait {
                                                    checkpoint: json!({"phase":"run"}),
                                                    on: vec![child.id, child.id],
                                                    policy: JoinPolicy::FailFast,
                                                }
                                            },
                                        )
                                        .await?;
                                    }
                                    Ok(child)
                                })
                            })
                            .await;
                        if finish {
                            assert!(matches!(receipt, Err(SessionError::Invalid(_))));
                        } else {
                            assert!(receipt.unwrap().seq.is_some());
                        }
                        session.close().await.unwrap();
                        let tasks = probe.closed_tasks.borrow();
                        if finish {
                            assert_eq!(*tasks, vec![record(10, None)]);
                        } else {
                            assert_eq!(tasks.len(), 2);
                            let TaskState::Waiting { on, .. } = &tasks[0].state else {
                                panic!()
                            };
                            if update_first {
                                assert!(on.is_empty(), "children need not be named by a wait");
                            } else {
                                assert_eq!(on, &vec![tasks[1].id, tasks[1].id]);
                            }
                        }
                        assert_eq!(
                            probe
                                .events
                                .borrow()
                                .iter()
                                .filter(|&&e| e == "persisted")
                                .count(),
                            if finish { 1 } else { 2 }
                        );
                    },
                    driver,
                )
                .await;
            });
        }
    }
}

#[test]
fn candidate_scope_guard_accepts_owned_conversations_but_rejects_background_children() {
    block_on(async {
        let (definition, context) = captured();
        let (session, driver) = Session::new(MemoryStorage::new());
        let (runner, tasks) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        zip(
            async {
                let root = seed(&session, definition).await;
                let run = runner.run(root.id);
                let runtime = context.await.unwrap();
                for owner_first in [false, true] {
                    for background in [false, true] {
                        let result = runtime
                            .commit(move |tx, root| {
                                Box::pin(async move {
                                    let mut child = tx
                                        .create_task(
                                            complete("child"),
                                            Value::Null,
                                            child_options(root.id),
                                        )
                                        .await?;
                                    if background {
                                        child.background = true;
                                        tx.set_task(child).await?;
                                    } else {
                                        if owner_first {
                                            tx.set_task(child.clone()).await?;
                                        }
                                        tx.create_conversation_owned(Owner::Task(child.id)).await?;
                                        if !owner_first {
                                            tx.set_task(child).await?;
                                        }
                                    }
                                    Ok(None)
                                })
                            })
                            .await;
                        if background {
                            assert!(matches!(result, Err(SessionError::Invalid(_))));
                        } else {
                            result.unwrap();
                        }
                    }
                }
                let conversation_id = root.conversation_id;
                session
                    .commit(move |tx| {
                        Box::pin(async move {
                            assert_eq!(
                                tx.scan_tasks(TaskQuery::default(), 10, None)
                                    .await?
                                    .items
                                    .len(),
                                3
                            );
                            let unrelated = tx
                                .create_task(
                                    complete("unrelated"),
                                    Value::Null,
                                    TaskOptions {
                                        ownership: TaskOwnership::Conversation,
                                        conversation_id: Some(conversation_id),
                                        background: true,
                                    },
                                )
                                .await?;
                            tx.create_conversation_owned(Owner::Task(unrelated.id))
                                .await?;
                            Ok(())
                        })
                    })
                    .await
                    .unwrap();
                runner.close().await.unwrap();
                assert_eq!(run.await.unwrap(), RunResult::Interrupted);
                session.close().await.unwrap();
            },
            zip(driver, tasks),
        )
        .await;
    });
}

#[test]
fn old_drive_identity_cannot_fence_a_new_drive_of_the_same_root() {
    block_on(async {
        let probe = Rc::new(Probe::default());
        let mut root = record(10, None);
        root.kind = "missing".into();
        let storage = seeded(probe.clone(), vec![root.clone()]).await;
        let (session, mut driver) = Session::new(storage);
        let (runner, mut tasks) = TaskRunner::attach(&session, TaskRegistry::default()).unwrap();
        let first_gate = Gate::default();
        *probe.scan_gate.borrow_mut() = Some((1, first_gate.clone()));
        let first = runner.run(root.id);
        tick(&mut tasks).await;
        tick(&mut driver).await;
        let old = runner.control.0.borrow().drive.upgrade().unwrap();
        assert!(old.live());
        first_gate.release();
        pump(&mut driver, &mut tasks).await;
        assert_eq!(
            first.await.unwrap(),
            RunResult::Blocked(BlockReason::MissingDefinition)
        );
        // Keep the real old identity alive, like a queued context/fence does.
        // It must never regain validity when the same root is explicitly driven again.
        let gate = Gate::default();
        *probe.scan_gate.borrow_mut() = Some((probe.scan_reads.get() + 1, gate.clone()));
        let second = runner.run(root.id);
        tick(&mut tasks).await;
        tick(&mut driver).await;
        let current = runner.control.0.borrow().drive.upgrade().unwrap();
        assert!(!old.live());
        assert!(current.live());
        old.ended.set(true);
        assert!(current.live());
        gate.release();
        pump(&mut driver, &mut tasks).await;
        assert_eq!(
            second.await.unwrap(),
            RunResult::Blocked(BlockReason::MissingDefinition)
        );
        assert!(!current.live());
        let close = runner.close();
        tick(&mut tasks).await;
        close.await.unwrap();
        let close = session.close();
        tick(&mut driver).await;
        close.await.unwrap();
    });
}

#[test]
fn tree_guard_covers_reconcile_gap_after_invocation_ends() {
    block_on(async {
        let probe = Rc::new(Probe::default());
        let gate = Gate::default();
        let definition = definition(Rc::new({
            let probe = probe.clone();
            let gate = gate.clone();
            move |_, runtime| {
                let probe = probe.clone();
                let gate = gate.clone();
                Box::pin(async move {
                    runtime
                        .commit(|_, _| {
                            Box::pin(async {
                                Ok(Some(TaskUpdate::Wait {
                                    checkpoint: json!({"phase":"run"}),
                                    on: vec![id(100)],
                                    policy: JoinPolicy::AllSettled,
                                }))
                            })
                        })
                        .await
                        .unwrap();
                    *probe.scan_gate.borrow_mut() = Some((probe.scan_reads.get() + 1, gate));
                    Ok(())
                })
            }
        }));
        let mut external = record(100, None);
        external.kind = "external".into();
        let storage = seeded(probe.clone(), vec![record(10, None), external]).await;
        let (session, mut driver) = Session::new(storage);
        let (runner, mut tasks) =
            TaskRunner::attach(&session, TaskRegistry::new([definition]).unwrap()).unwrap();
        let run = runner.run(id(10));
        pump(&mut driver, &mut tasks).await;
        assert!(
            runner
                .control
                .0
                .borrow()
                .actives
                .values()
                .filter_map(Weak::upgrade)
                .all(|inv| inv.ended.get())
        );
        assert!(runner.control.0.borrow().drive.upgrade().unwrap().live());
        let ownership = session.commit(|tx| {
            Box::pin(async { tx.create_conversation_owned(Owner::Task(id(10))).await })
        });
        gate.release();
        // Drain Session first, before TaskDriver can observe quiescence and release
        // its drive. Owned conversations remain valid while the drive is live.
        tick(&mut driver).await;
        ownership.await.unwrap();
        tick(&mut tasks).await;
        assert!(matches!(run.await.unwrap(), RunResult::Suspended(_)));
        let released = session.commit(|tx| {
            Box::pin(async { tx.create_conversation_owned(Owner::Task(id(10))).await })
        });
        tick(&mut driver).await;
        released.await.unwrap();
        let close = runner.close();
        tick(&mut tasks).await;
        close.await.unwrap();
        let close = session.close();
        tick(&mut driver).await;
        close.await.unwrap();
    });
}

#[test]
fn cascade_ack_wakers_can_reenter_session_admission() {
    thread_local! { static REENTER: RefCell<Option<Session>> = const { RefCell::new(None) }; }
    struct Reenter;
    impl std::task::Wake for Reenter {
        fn wake(self: std::sync::Arc<Self>) {
            REENTER.with(|slot| {
                drop(
                    slot.borrow()
                        .as_ref()
                        .unwrap()
                        .commit(|_| Box::pin(async { Ok(()) })),
                );
            });
        }
    }
    block_on(async {
        let probe = Rc::new(Probe::default());
        let (definition, context) = captured();
        let mut root = record(10, None);
        root.kind = "missing".into();
        waiting(&mut root, &[20], JoinPolicy::AllSettled);
        let storage = seeded(probe, vec![root, record(20, Some(10))]).await;
        let (session, mut driver) = Session::new(storage);
        let (runner, mut tasks) =
            TaskRunner::attach(&session, TaskRegistry::new([definition]).unwrap()).unwrap();
        let run = runner.run(id(10));
        pump(&mut driver, &mut tasks).await;
        let runtime = context.await.unwrap();
        REENTER.with(|slot| *slot.borrow_mut() = Some(session.clone()));
        let waker = Waker::from(std::sync::Arc::new(Reenter));
        let mut cancelled = Box::pin(runtime.cancelled());
        assert!(
            cancelled
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let abort = runner.abort(id(10));
        tick(&mut driver).await;
        assert_eq!(abort.await.unwrap(), AbortResult::Marked);
        assert!(runtime.is_cancelled());
        REENTER.with(|slot| slot.borrow_mut().take());
        pump(&mut driver, &mut tasks).await;
        assert!(matches!(run.await.unwrap(), RunResult::Terminal(_)));
        let close = runner.close();
        tick(&mut tasks).await;
        close.await.unwrap();
        let close = session.close();
        tick(&mut driver).await;
        close.await.unwrap();
    });
}

#[test]
fn direct_orphan_overtaking_selected_reservation_returns_durable_terminal_receipt() {
    block_on(async {
        let probe = Rc::new(Probe::default());
        let mut root = record(10, None);
        root.kind = "missing".into();
        root.abort_requested = true;
        let storage = seeded(probe, vec![root]).await;
        let (session, mut driver) = Session::new(storage);
        let (runner, mut tasks) = TaskRunner::attach(&session, TaskRegistry::default()).unwrap();
        let run = runner.run(id(10));
        tick(&mut tasks).await;
        tick(&mut driver).await; // Drive selected the marked leaf, not yet reserved.
        let abort = runner.abort(id(10));
        tick(&mut tasks).await; // Reservation admitted behind the orphan command.
        tick(&mut driver).await;
        tick(&mut tasks).await;
        assert_eq!(abort.await.unwrap(), AbortResult::Marked);
        let RunResult::Terminal(root) = run.await.unwrap() else {
            panic!()
        };
        assert!(matches!(
            root.state,
            TaskState::Terminal {
                outcome: TaskOutcome::Orphaned { .. }
            }
        ));
        let close = runner.close();
        tick(&mut tasks).await;
        close.await.unwrap();
        let close = session.close();
        tick(&mut driver).await;
        close.await.unwrap();
    });
}

#[test]
fn queued_reconciliation_does_not_finalize_or_cascade_after_close() {
    for cascade in [false, true] {
        for session_close in [false, true] {
            block_on(async {
                let probe = Rc::new(Probe::default());
                let mut root = record(10, None);
                root.state = TaskState::Completing {
                    outcome: if cascade {
                        TaskOutcome::Failed {
                            error: fault("held failure"),
                            result: None,
                        }
                    } else {
                        TaskOutcome::Completed {
                            result: json!("held success"),
                        }
                    },
                };
                let mut records = vec![root];
                if cascade {
                    records.push(record(20, Some(10)));
                }
                let storage = seeded(probe.clone(), records.clone()).await;
                let (session, mut driver) = Session::new(storage);
                let (runner, mut tasks) =
                    TaskRunner::attach(&session, TaskRegistry::new([complete("leaf")]).unwrap())
                        .unwrap();
                let run = runner.run(id(10));
                tick(&mut tasks).await; // Reconciliation admitted, but not started.
                assert_eq!(probe.scan_reads.get(), 0);
                let runner_closed = (!session_close).then(|| runner.close());
                let session_closed = session_close.then(|| session.close());
                pump(&mut driver, &mut tasks).await;
                assert_eq!(run.await.unwrap(), RunResult::Interrupted);
                if let Some(close) = runner_closed {
                    close.await.unwrap();
                }
                assert_eq!(
                    probe.scan_reads.get(),
                    0,
                    "closed entry gate must precede reads"
                );
                assert_eq!(
                    probe
                        .events
                        .borrow()
                        .iter()
                        .filter(|&&e| e == "persisted")
                        .count(),
                    1
                );
                if let Some(close) = session_closed {
                    close.await.unwrap();
                } else {
                    let close = session.close();
                    tick(&mut driver).await;
                    close.await.unwrap();
                }
                assert_eq!(*probe.closed_tasks.borrow(), records);
            });
        }
    }
}

#[test]
fn started_reconciliation_settles_after_close_without_dispatching_fresh_handlers() {
    for cascade in [false, true] {
        for in_commit in [false, true] {
            for session_close in [false, true] {
                block_on(async {
                    let probe = Rc::new(Probe::default());
                    let mut root = record(10, None);
                    root.state = TaskState::Completing {
                        outcome: if cascade {
                            TaskOutcome::Failed {
                                error: fault("held failure"),
                                result: None,
                            }
                        } else {
                            TaskOutcome::Completed {
                                result: json!("held success"),
                            }
                        },
                    };
                    let mut records = vec![root.clone()];
                    if cascade {
                        records.push(record(20, Some(10)));
                    }
                    let storage = seeded(probe.clone(), records).await;
                    let gate = Gate::default();
                    if in_commit {
                        *probe.commit_gate.borrow_mut() = Some(gate.clone());
                    } else {
                        *probe.scan_gate.borrow_mut() = Some((1, gate.clone()));
                    }
                    let (session, mut driver) = Session::new(storage);
                    let (runner, mut tasks) = TaskRunner::attach(
                        &session,
                        TaskRegistry::new([complete("leaf")]).unwrap(),
                    )
                    .unwrap();
                    let run = runner.run(id(10));
                    tick(&mut tasks).await;
                    tick(&mut driver).await; // Callback passed entry, now blocked in Storage.
                    if in_commit {
                        assert_eq!(probe.events.borrow().last(), Some(&"commit entered"));
                    } else {
                        assert_eq!(probe.scan_reads.get(), 1);
                    }
                    let mut runner_closed = (!session_close).then(|| runner.close());
                    let mut session_closed = session_close.then(|| session.close());
                    tick(&mut tasks).await;
                    if let Some(close) = &mut runner_closed {
                        assert!(poll_once(close).await.is_none());
                    }
                    if let Some(close) = &mut session_closed {
                        assert!(poll_once(close).await.is_none());
                    }
                    gate.release();
                    pump(&mut driver, &mut tasks).await;
                    if cascade {
                        assert_eq!(run.await.unwrap(), RunResult::Interrupted);
                    } else {
                        let TaskState::Completing { outcome } = root.state else {
                            unreachable!()
                        };
                        root.state = TaskState::Terminal { outcome };
                        assert_eq!(run.await.unwrap(), RunResult::Terminal(root.clone()));
                    }
                    if let Some(close) = runner_closed {
                        close.await.unwrap();
                    }
                    assert_eq!(
                        probe
                            .events
                            .borrow()
                            .iter()
                            .filter(|&&e| e == "persisted")
                            .count(),
                        2
                    );
                    if let Some(close) = session_closed {
                        close.await.unwrap();
                    } else {
                        let close = session.close();
                        tick(&mut driver).await;
                        close.await.unwrap();
                    }
                    let records = probe.closed_tasks.borrow();
                    assert_eq!(records[0], root);
                    if cascade {
                        let mut child = record(20, Some(10));
                        child.abort_requested = true;
                        assert_eq!(
                            records[1], child,
                            "started pass marks, but close prevents cleanup dispatch"
                        );
                    }
                });
            }
        }
    }
}
