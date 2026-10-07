//! Public, host-polled bounded-tree workflows. No executor or provider is involved.
use futures_lite::future::{block_on, zip};
use publicworks_runtime::*;
use serde_json::{Value, json};
use std::{
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
    task::{Poll, Waker},
};

#[derive(Clone, Default)]
struct Gate(Rc<RefCell<(bool, Option<Waker>)>>);
impl Gate {
    async fn wait(&self) {
        std::future::poll_fn(|cx| {
            let mut state = self.0.borrow_mut();
            if state.0 {
                Poll::Ready(())
            } else {
                state.1 = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
    fn release(&self) {
        let wake = {
            let mut state = self.0.borrow_mut();
            state.0 = true;
            state.1.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}
// A regression should fail, not hang CI. Waking this wrapper also keeps both
// ownerless drivers polled while a deliberately withheld gate stays pending.
async fn bounded<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut polls = 0;
    std::future::poll_fn(|cx| {
        polls += 1;
        assert!(
            polls < 20_000,
            "tree workflow did not quiesce within poll budget"
        );
        let result = future.as_mut().poll(cx);
        if result.is_pending() {
            cx.waker().wake_by_ref();
        }
        result
    })
    .await
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
    phases: impl IntoIterator<Item = (&'static str, PhaseHandler)>,
) -> TaskDefinition {
    TaskDefinition::new(
        kind,
        1,
        |_| Ok(json!({"phase":"work"})),
        phases.into_iter().map(|(k, v)| (k.into(), v)).collect(),
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
async fn create(
    session: &Session,
    def: TaskDefinition,
    input: Value,
    owner: Option<Id>,
) -> TaskRecord {
    session
        .commit(move |tx| Box::pin(async move { tx.create_task(def, input, options(owner)).await }))
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
fn outcome(task: &TaskRecord) -> &TaskOutcome {
    match &task.state {
        TaskState::Terminal { outcome } => outcome,
        state => panic!("expected terminal, got {state:?}"),
    }
}
fn terminal(result: RunResult) -> TaskRecord {
    match result {
        RunResult::Terminal(task) => task,
        result => panic!("expected terminal, got {result:?}"),
    }
}
fn checkpoint(task: &TaskRecord) -> &Value {
    match &task.state {
        TaskState::Running { checkpoint }
        | TaskState::Waiting { checkpoint, .. }
        | TaskState::Pending { checkpoint } => checkpoint,
        _ => panic!("no checkpoint"),
    }
}
fn ids(task: &TaskRecord) -> Vec<Id> {
    serde_json::from_value(checkpoint(task)["ids"].clone()).unwrap()
}
async fn update(runtime: &TaskRuntime, update: TaskUpdate) {
    runtime
        .commit(move |_, _| Box::pin(async move { Ok(Some(update)) }))
        .await
        .unwrap();
}
fn leaf() -> TaskDefinition {
    definition(
        "leaf",
        [(
            "work",
            phase(|task, runtime| async move {
                assert!(!task.abort_requested);
                update(&runtime, TaskUpdate::Complete(task.input)).await;
                Ok(())
            }),
        )],
    )
}
fn join_phase() -> PhaseHandler {
    phase(|task, runtime| async move {
        let ids = ids(&task);
        runtime
            .commit(move |tx, _| {
                Box::pin(async move {
                    let mut outcomes = Vec::new();
                    for id in ids {
                        outcomes.push(outcome(&tx.task(id).await?.unwrap()).clone());
                    }
                    Ok(Some(TaskUpdate::Complete(
                        serde_json::to_value(outcomes).unwrap(),
                    )))
                })
            })
            .await
            .unwrap();
        Ok(())
    })
}
fn spawning_parent(child: TaskDefinition, policy: JoinPolicy) -> TaskDefinition {
    definition(
        "parent",
        [
            (
                "work",
                phase(move |_, runtime| {
                    let child = child.clone();
                    async move {
                        runtime
                            .commit(move |tx, task| {
                                Box::pin(async move {
                                    let a = tx
                                        .create_task(
                                            child.clone(),
                                            json!("a"),
                                            options(Some(task.id)),
                                        )
                                        .await?;
                                    let b = tx
                                        .create_task(child, json!("b"), options(Some(task.id)))
                                        .await?;
                                    let on = vec![b.id, a.id, b.id];
                                    Ok(Some(TaskUpdate::Wait {
                                        checkpoint: json!({"phase":"join", "ids":on}),
                                        on,
                                        policy,
                                    }))
                                })
                            })
                            .await
                            .unwrap();
                        Ok(())
                    }
                }),
            ),
            ("join", join_phase()),
        ],
    )
}

#[test]
fn atomic_spawn_wait_preserves_order_duplicates_and_empty_wait_runs_next_phase() {
    block_on(bounded(async {
        let active = Gate::default();
        let release = Gate::default();
        let child = definition(
            "leaf",
            [(
                "work",
                phase({
                    let active = active.clone();
                    let release = release.clone();
                    move |task, rt| {
                        let active = active.clone();
                        let release = release.clone();
                        async move {
                            active.release();
                            release.wait().await;
                            update(&rt, TaskUpdate::Complete(task.input)).await;
                            Ok(())
                        }
                    }
                }),
            )],
        );
        let parent = spawning_parent(child.clone(), JoinPolicy::AllSettled);
        let empty = definition(
            "empty",
            [
                (
                    "work",
                    phase(|_, rt| async move {
                        update(
                            &rt,
                            TaskUpdate::Wait {
                                checkpoint: json!({"phase":"join", "ids":[]}),
                                on: vec![],
                                policy: JoinPolicy::FailFast,
                            },
                        )
                        .await;
                        Ok(())
                    }),
                ),
                ("join", join_phase()),
            ],
        );
        let (session, sd) = Session::new(memory().await);
        let (runner, td) = TaskRunner::attach(
            &session,
            TaskRegistry::new([parent.clone(), child, empty.clone()]).unwrap(),
        )
        .unwrap();
        zip(async {
            let root = create(&session, parent, Value::Null, None).await;
            let waiter = runner.run(root.id);
            active.wait().await;
            let waiting = read(&session, root.id).await;
            let TaskState::Waiting { on, policy: JoinPolicy::AllSettled, .. } = &waiting.state else { panic!("parent did not durably wait") };
            assert_eq!(on.len(), 3);
            assert_eq!(on[0], on[2], "duplicate wait targets must be retained");
            assert_eq!(on, &ids(&waiting));
            release.release();
            let done = terminal(waiter.await.unwrap());
            assert_eq!(outcome(&done), &TaskOutcome::Completed { result: json!([
                {"status":"completed", "result":"b"}, {"status":"completed", "result":"a"}, {"status":"completed", "result":"b"}
            ]) });
            let tasks = session.commit(|tx| Box::pin(async { tx.scan_tasks(TaskQuery::default(), 20, None).await })).await.unwrap().value.items;
            assert_eq!(tasks.len(), 3);
            for task in tasks.iter().filter(|t| t.id != root.id) {
                assert_eq!(task.owner, Some(root.id));
                assert_eq!(task.conversation_id, root.conversation_id);
                assert!(!task.background);
            }
            let root = create(&session, empty, Value::Null, None).await;
            assert_eq!(outcome(&terminal(runner.run(root.id).await.unwrap())), &TaskOutcome::Completed { result: json!([]) });
            runner.close().await.unwrap(); session.close().await.unwrap();
        }, zip(sd, td)).await;
    }));
}

#[test]
fn fail_fast_held_failure_marks_only_named_live_sibling_and_parent_still_joins() {
    block_on(bounded(async {
        let cleanup_started = Gate::default();
        let cleanup_release = Gate::default();
        let grand = leaf().with_abort_handler(phase({
            let started = cleanup_started.clone();
            let release = cleanup_release.clone();
            move |_, rt| {
                let started = started.clone();
                let release = release.clone();
                async move {
                    started.release();
                    release.wait().await;
                    update(
                        &rt,
                        TaskUpdate::Abort {
                            reason: None,
                            result: None,
                        },
                    )
                    .await;
                    Ok(())
                }
            }
        }));
        let failing = definition(
            "failing",
            [
                (
                    "work",
                    phase({
                        let grand = grand.clone();
                        move |_, rt| {
                            let grand = grand.clone();
                            async move {
                                rt.commit(move |tx, task| {
                                    Box::pin(async move {
                                        tx.create_task(grand, Value::Null, options(Some(task.id)))
                                            .await?;
                                        Ok(Some(TaskUpdate::Checkpoint(json!({"phase":"fail"}))))
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
                    "fail",
                    phase(|_, rt| async move {
                        update(
                            &rt,
                            TaskUpdate::Fail(
                                TaskOutcomeError {
                                    message: "held failure".into(),
                                    detail: None,
                                },
                                Some(json!(7)),
                            ),
                        )
                        .await;
                        Ok(())
                    }),
                ),
            ],
        );
        let normal = definition(
            "normal",
            [(
                "work",
                phase(|task, rt| async move {
                    assert!(!task.abort_requested);
                    update(&rt, TaskUpdate::Complete(task.input)).await;
                    Ok(())
                }),
            )],
        );
        let parent = definition(
            "parent",
            [
                (
                    "work",
                    phase({
                        let failing = failing.clone();
                        let normal = normal.clone();
                        move |_, rt| {
                            let failing = failing.clone();
                            let normal = normal.clone();
                            async move {
                                rt.commit(move |tx, task| Box::pin(async move {
                    let a = tx.create_task(failing, Value::Null, options(Some(task.id))).await?;
                    let b = tx.create_task(normal.clone(), json!("named"), options(Some(task.id))).await?;
                    let c = tx.create_task(normal, json!("unlisted"), options(Some(task.id))).await?;
                    Ok(Some(TaskUpdate::Wait { checkpoint: json!({"phase":"join", "ids":[a.id,b.id], "unlisted":c.id}), on: vec![a.id,b.id], policy: JoinPolicy::FailFast }))
                })).await.unwrap();
                                Ok(())
                            }
                        }
                    }),
                ),
                ("join", join_phase()),
            ],
        );
        let (session, sd) = Session::new(memory().await);
        let (runner, td) = TaskRunner::attach(
            &session,
            TaskRegistry::new([parent.clone(), failing, normal, grand]).unwrap(),
        )
        .unwrap();
        zip(
            async {
                let root = create(&session, parent, Value::Null, None).await;
                let waiter = runner.run(root.id);
                cleanup_started.wait().await;
                let waiting = read(&session, root.id).await;
                assert_eq!(waiting.status(), TaskStatus::Waiting);
                let children = ids(&waiting);
                let failure = read(&session, children[0]).await;
                assert!(matches!(
                    failure.state,
                    TaskState::Completing {
                        outcome: TaskOutcome::Failed { .. }
                    }
                ));
                assert!(
                    !failure.abort_requested,
                    "held failure must keep its own outcome"
                );
                assert!(read(&session, children[1]).await.abort_requested);
                let unlisted: Id =
                    serde_json::from_value(checkpoint(&waiting)["unlisted"].clone()).unwrap();
                assert!(!read(&session, unlisted).await.abort_requested);
                cleanup_release.release();
                let done = terminal(waiter.await.unwrap());
                assert!(!done.abort_requested);
                let TaskOutcome::Completed { result } = outcome(&done) else {
                    panic!("parent failed automatically")
                };
                assert_eq!(result[0]["status"], "failed");
                assert_eq!(result[1]["status"], "aborted");
                assert_eq!(
                    outcome(&read(&session, unlisted).await),
                    &TaskOutcome::Completed {
                        result: json!("unlisted")
                    }
                );
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    }));
}

#[test]
fn held_completed_parent_drives_children_and_abort_preserves_outcome_and_version() {
    for abort in [false, true] {
        block_on(bounded(async {
            let active = Gate::default();
            let release = Gate::default();
            let cancelled = Gate::default();
            let child = definition(
                "child",
                [(
                    "work",
                    phase({
                        let active = active.clone();
                        let release = release.clone();
                        let cancelled = cancelled.clone();
                        move |_, rt| {
                            let active = active.clone();
                            let release = release.clone();
                            let cancelled = cancelled.clone();
                            async move {
                                active.release();
                                if abort {
                                    rt.cancelled().await;
                                    cancelled.release();
                                    release.wait().await;
                                } else {
                                    release.wait().await;
                                    update(&rt, TaskUpdate::Complete(json!("child"))).await;
                                }
                                Ok(())
                            }
                        }
                    }),
                )],
            );
            let parent = definition(
                "parent",
                [
                    (
                        "work",
                        phase({
                            let child = child.clone();
                            move |_, rt| {
                                let child = child.clone();
                                async move {
                                    rt.commit(move |tx, task| {
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
                        phase(|_, rt| async move {
                            update(&rt, TaskUpdate::Complete(json!("original"))).await;
                            Ok(())
                        }),
                    ),
                ],
            )
            .with_abort_handler(phase(|_, _| async {
                panic!("held parent must never run abort hook")
            }));
            let (session, sd) = Session::new(memory().await);
            let (runner, td) = TaskRunner::attach(
                &session,
                TaskRegistry::new([parent.clone(), child]).unwrap(),
            )
            .unwrap();
            zip(
                async {
                    let root = create(&session, parent, Value::Null, None).await;
                    let waiter = runner.run(root.id);
                    active.wait().await;
                    let held = read(&session, root.id).await;
                    assert_eq!(
                        held.state,
                        TaskState::Completing {
                            outcome: TaskOutcome::Completed {
                                result: json!("original")
                            }
                        }
                    );
                    if abort {
                        assert_eq!(runner.abort(root.id).await.unwrap(), AbortResult::Marked);
                        cancelled.wait().await;
                    }
                    release.release();
                    let done = terminal(waiter.await.unwrap());
                    assert_eq!(done.version, held.version);
                    assert_eq!(
                        outcome(&done),
                        &TaskOutcome::Completed {
                            result: json!("original")
                        }
                    );
                    runner.close().await.unwrap();
                    session.close().await.unwrap();
                },
                zip(sd, td),
            )
            .await;
        }));
    }
}

#[test]
fn abort_waiting_root_ack_does_not_join_descendant_and_cleanup_is_bottom_up() {
    block_on(bounded(async {
        let active = Gate::default();
        let cancelled = Gate::default();
        let release = Gate::default();
        let log = Rc::new(RefCell::new(Vec::new()));
        let cleanup = |name: &'static str| {
            phase({
                let log = log.clone();
                move |_, rt| {
                    let log = log.clone();
                    async move {
                        log.borrow_mut().push(name);
                        update(
                            &rt,
                            TaskUpdate::Abort {
                                reason: None,
                                result: None,
                            },
                        )
                        .await;
                        Ok(())
                    }
                }
            })
        };
        let grand = definition(
            "grand",
            [(
                "work",
                phase({
                    let active = active.clone();
                    let cancelled = cancelled.clone();
                    let release = release.clone();
                    move |_, rt| {
                        let active = active.clone();
                        let cancelled = cancelled.clone();
                        let release = release.clone();
                        async move {
                            active.release();
                            rt.cancelled().await;
                            cancelled.release();
                            release.wait().await;
                            Ok(())
                        }
                    }
                }),
            )],
        )
        .with_abort_handler(cleanup("grand"));
        let child = spawning_parent(grand.clone(), JoinPolicy::AllSettled)
            .with_abort_handler(cleanup("child"));
        // Use a distinct kind for the root and an external, never-driven wait target.
        let external = leaf();
        let (session, sd) = Session::new(memory().await);
        zip(async {
            let external_task = create(&session, external.clone(), json!("external"), None).await;
            let root = definition("root", [("work", phase({ let child = child.clone(); move |_, rt| { let child = child.clone(); async move {
                rt.commit(move |tx, task| Box::pin(async move {
                    let child = tx.create_task(child, Value::Null, options(Some(task.id))).await?;
                    Ok(Some(TaskUpdate::Wait { checkpoint: json!({"phase":"never", "ids":[child.id]}), on: vec![external_task.id], policy: JoinPolicy::AllSettled }))
                })).await.unwrap(); Ok(())
            }}}))]).with_abort_handler(cleanup("root"));
            let (runner, td) = TaskRunner::attach(&session, TaskRegistry::new([root.clone(), child, grand, external]).unwrap()).unwrap();
            zip(async {
                let root = create(&session, root, Value::Null, None).await;
                let waiter = runner.run(root.id); active.wait().await;
                assert_eq!(runner.abort(root.id).await.unwrap(), AbortResult::Marked);
                // The descendant has not returned, so acknowledging this cannot join it.
                cancelled.wait().await;
                assert!(log.borrow().is_empty());
                assert_eq!(read(&session, external_task.id).await, external_task);
                release.release();
                assert!(matches!(outcome(&terminal(waiter.await.unwrap())), TaskOutcome::Aborted { .. }));
                // There are two grandchildren; both clean before their owner.
                assert_eq!(&*log.borrow(), &["grand", "grand", "child", "root"]);
                runner.close().await.unwrap(); session.close().await.unwrap();
            }, td).await;
        }, sd).await;
    }));
}

#[test]
fn abort_missing_definition_cancels_active_fail_fast_sibling_before_handler_returns() {
    block_on(bounded(async {
        let active = Gate::default();
        let cancelled = Gate::default();
        let release = Gate::default();
        let known = definition(
            "known",
            [(
                "work",
                phase({
                    let active = active.clone();
                    let cancelled = cancelled.clone();
                    let release = release.clone();
                    move |_, rt| {
                        let active = active.clone();
                        let cancelled = cancelled.clone();
                        let release = release.clone();
                        async move {
                            active.release();
                            rt.cancelled().await;
                            cancelled.release();
                            release.wait().await;
                            Ok(())
                        }
                    }
                }),
            )],
        );
        let unknown = definition("unregistered", []);
        let parent = definition(
            "parent",
            [
                (
                    "work",
                    phase({
                        let known = known.clone();
                        move |_, rt| {
                            let known = known.clone();
                            let unknown = unknown.clone();
                            async move {
                                rt.commit(move |tx, task| Box::pin(async move {
                let missing = tx.create_task(unknown, Value::Null, options(Some(task.id))).await?;
                let active = tx.create_task(known, Value::Null, options(Some(task.id))).await?;
                Ok(Some(TaskUpdate::Wait { checkpoint: json!({"phase":"join", "ids":[missing.id,active.id]}), on: vec![missing.id,active.id], policy: JoinPolicy::FailFast }))
            })).await.unwrap();
                                Ok(())
                            }
                        }
                    }),
                ),
                ("join", join_phase()),
            ],
        );
        let (session, sd) = Session::new(memory().await);
        let (runner, td) = TaskRunner::attach(
            &session,
            TaskRegistry::new([parent.clone(), known]).unwrap(),
        )
        .unwrap();
        zip(
            async {
                let root = create(&session, parent, Value::Null, None).await;
                let waiter = runner.run(root.id);
                active.wait().await;
                let children = ids(&read(&session, root.id).await);
                assert_eq!(
                    runner.abort(children[0]).await.unwrap(),
                    AbortResult::Marked
                );
                assert!(matches!(
                    outcome(&read(&session, children[0]).await),
                    TaskOutcome::Orphaned { .. }
                ));
                cancelled.wait().await; // Deadlock trap: the sole active handler has not returned.
                assert!(read(&session, children[1]).await.abort_requested);
                release.release();
                let done = terminal(waiter.await.unwrap());
                let TaskOutcome::Completed { result } = outcome(&done) else {
                    panic!("parent did not resume")
                };
                assert_eq!(result[0]["status"], "orphaned");
                assert_eq!(result[1]["status"], "aborted");
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    }));
}

#[test]
fn invalid_waits_and_spawn_finish_roll_back_and_abort_hooks_cannot_wait() {
    block_on(bounded(async {
        let checked = Rc::new(Cell::new(0));
        let child = definition(
            "validator",
            [(
                "work",
                phase({
                    let checked = checked.clone();
                    move |task, rt| {
                        let checked = checked.clone();
                        async move {
                            // Self, ownership ancestor, missing task, and non-direct failFast target.
                            for (target, policy) in [
                                (task.id, JoinPolicy::AllSettled),
                                (task.owner.unwrap(), JoinPolicy::AllSettled),
                                (Id::new(999_999).unwrap(), JoinPolicy::AllSettled),
                                (task.owner.unwrap(), JoinPolicy::FailFast),
                            ] {
                                let error = rt
                                    .commit(move |tx, task| {
                                        Box::pin(async move {
                                            tx.append_entry(
                                                task.conversation_id,
                                                EntryDraft::new("must roll back"),
                                            )
                                            .await?;
                                            Ok(Some(TaskUpdate::Wait {
                                                checkpoint: json!({"phase":"bad"}),
                                                on: vec![target],
                                                policy,
                                            }))
                                        })
                                    })
                                    .await
                                    .unwrap_err();
                                assert!(
                                    matches!(error, SessionError::Invalid(_)),
                                    "unexpected validation error: {error:?}"
                                );
                                checked.set(checked.get() + 1);
                            }
                            update(&rt, TaskUpdate::Complete(json!("validated"))).await;
                            Ok(())
                        }
                    }
                }),
            )],
        )
        .with_abort_handler(phase(|_, rt| async move {
            let error = rt
                .commit(|_, _| {
                    Box::pin(async {
                        Ok(Some(TaskUpdate::Wait {
                            checkpoint: json!({"phase":"bad"}),
                            on: vec![],
                            policy: JoinPolicy::AllSettled,
                        }))
                    })
                })
                .await
                .unwrap_err();
            assert!(matches!(error, SessionError::Invalid(_)));
            update(
                &rt,
                TaskUpdate::Abort {
                    reason: None,
                    result: None,
                },
            )
            .await;
            Ok(())
        }));
        let root = definition(
            "parent",
            [
                (
                    "work",
                    phase({
                        let child = child.clone();
                        move |_, rt| {
                            let child = child.clone();
                            async move {
                                let rejected_child = child.clone();
                                let error = rt
                                    .commit(move |tx, task| {
                                        Box::pin(async move {
                                            tx.create_task(
                                                rejected_child,
                                                json!("rejected"),
                                                options(Some(task.id)),
                                            )
                                            .await?;
                                            tx.append_entry(
                                                task.conversation_id,
                                                EntryDraft::new("must roll back"),
                                            )
                                            .await?;
                                            Ok(Some(TaskUpdate::Complete(Value::Null)))
                                        })
                                    })
                                    .await
                                    .unwrap_err();
                                assert!(
                                    matches!(error, SessionError::Invalid(_)),
                                    "spawn+finish must retain final-owner validation: {error:?}"
                                );
                                rt.commit(move |tx, task| {
                                    Box::pin(async move {
                                        let child = tx
                                            .create_task(child, Value::Null, options(Some(task.id)))
                                            .await?;
                                        Ok(Some(TaskUpdate::Wait {
                                            checkpoint: json!({"phase":"join", "ids":[child.id]}),
                                            on: vec![child.id],
                                            policy: JoinPolicy::AllSettled,
                                        }))
                                    })
                                })
                                .await
                                .unwrap();
                                Ok(())
                            }
                        }
                    }),
                ),
                ("join", join_phase()),
            ],
        );
        let (session, sd) = Session::new(memory().await);
        let (runner, td) = TaskRunner::attach(
            &session,
            TaskRegistry::new([root.clone(), child.clone()]).unwrap(),
        )
        .unwrap();
        zip(
            async {
                // A sibling is neither self nor ancestor: failFast must reject it too.
                let external = create(&session, child.clone(), Value::Null, None).await;
                let invalid = definition(
                    "invalid-sibling",
                    [(
                        "work",
                        phase(move |_, rt| async move {
                            let error = rt
                                .commit(move |_, _| {
                                    Box::pin(async move {
                                        Ok(Some(TaskUpdate::Wait {
                                            checkpoint: json!({"phase":"work"}),
                                            on: vec![external.id],
                                            policy: JoinPolicy::FailFast,
                                        }))
                                    })
                                })
                                .await
                                .unwrap_err();
                            assert!(matches!(error, SessionError::Invalid(_)));
                            update(&rt, TaskUpdate::Complete(Value::Null)).await;
                            Ok(())
                        }),
                    )],
                );
                // The registry is immutable, so this independent definition is exercised
                // after the first runner closes below.
                let root = create(&session, root, Value::Null, None).await;
                terminal(runner.run(root.id).await.unwrap());
                assert_eq!(checked.get(), 4);
                let snapshot = session
                    .commit(|tx| {
                        Box::pin(async {
                            let entries = tx
                                .scan_entries(EntryQuery::new(ROOT_CONVERSATION), 20, None)
                                .await?;
                            let tasks = tx.scan_tasks(TaskQuery::default(), 20, None).await?;
                            Ok((entries, tasks))
                        })
                    })
                    .await
                    .unwrap()
                    .value;
                assert!(snapshot.0.items.is_empty());
                assert_eq!(snapshot.1.items.len(), 3);
                assert_eq!(
                    runner.abort(external.id).await.unwrap(),
                    AbortResult::Marked
                );
                assert!(matches!(
                    outcome(&terminal(runner.run(external.id).await.unwrap())),
                    TaskOutcome::Aborted { .. }
                ));
                runner.close().await.unwrap();
                let (next, driver) =
                    TaskRunner::attach(&session, TaskRegistry::new([invalid.clone()]).unwrap())
                        .unwrap();
                zip(
                    async {
                        let task = create(&session, invalid, Value::Null, None).await;
                        terminal(next.run(task.id).await.unwrap());
                        next.close().await.unwrap();
                    },
                    driver,
                )
                .await;
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    }));
}

#[test]
fn external_cross_conversation_wait_is_observed_not_driven_and_cycles_suspend() {
    block_on(bounded(async {
        let child = leaf();
        let (session, sd) = Session::new(memory().await);
        zip(
            async {
                let external = session
                    .commit({
                        let child = child.clone();
                        move |tx| {
                            Box::pin(async move {
                                let conversation = tx.create_conversation().await?;
                                tx.create_task(
                                    child,
                                    json!("external"),
                                    TaskOptions {
                                        conversation_id: Some(conversation.id),
                                        ownership: TaskOwnership::Conversation,
                                        background: false,
                                    },
                                )
                                .await
                            })
                        }
                    })
                    .await
                    .unwrap()
                    .value;
                let parent = definition(
                    "observer",
                    [
                        (
                            "work",
                            phase(move |_, rt| async move {
                                update(
                                    &rt,
                                    TaskUpdate::Wait {
                                        checkpoint: json!({"phase":"join", "ids":[external.id]}),
                                        on: vec![external.id],
                                        policy: JoinPolicy::AllSettled,
                                    },
                                )
                                .await;
                                Ok(())
                            }),
                        ),
                        ("join", join_phase()),
                    ],
                );
                let (runner, td) = TaskRunner::attach(
                    &session,
                    TaskRegistry::new([parent.clone(), child]).unwrap(),
                )
                .unwrap();
                zip(
                    async {
                        let root = create(&session, parent, Value::Null, None).await;
                        let RunResult::Suspended(waiting) = runner.run(root.id).await.unwrap()
                        else {
                            panic!("external target must suspend")
                        };
                        assert_eq!(waiting, read(&session, root.id).await);
                        assert_eq!(waiting.status(), TaskStatus::Waiting);
                        assert_eq!(read(&session, external.id).await, external);
                        terminal(runner.run(external.id).await.unwrap());
                        terminal(runner.run(root.id).await.unwrap());
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

        // Public storage fixtures model existing sibling waits. There is no
        // general cycle prohibition and no polling loop when neither is ready.
        let root_id = Id::new(100).unwrap();
        let a = Id::new(101).unwrap();
        let b = Id::new(102).unwrap();
        let mut storage = memory().await;
        let root = record(
            root_id,
            None,
            TaskState::Waiting {
                checkpoint: json!({"phase":"join", "ids":[a,b]}),
                on: vec![a, b],
                policy: JoinPolicy::AllSettled,
            },
        );
        storage
            .commit(vec![
                StorageWrite::Task(root.clone()),
                StorageWrite::Task(record(
                    a,
                    Some(root_id),
                    TaskState::Waiting {
                        checkpoint: json!({"phase":"join", "ids":[b]}),
                        on: vec![b],
                        policy: JoinPolicy::AllSettled,
                    },
                )),
                StorageWrite::Task(record(
                    b,
                    Some(root_id),
                    TaskState::Waiting {
                        checkpoint: json!({"phase":"join", "ids":[a]}),
                        on: vec![a],
                        policy: JoinPolicy::AllSettled,
                    },
                )),
            ])
            .await
            .unwrap();
        let (session, sd) = Session::new(storage);
        let (runner, td) = TaskRunner::attach(
            &session,
            TaskRegistry::new([definition("seed", [("join", join_phase())])]).unwrap(),
        )
        .unwrap();
        zip(
            async {
                assert_eq!(
                    runner.run(root_id).await.unwrap(),
                    RunResult::Suspended(root)
                );
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    }));
}

fn record(id: Id, owner: Option<Id>, state: TaskState) -> TaskRecord {
    TaskRecord {
        id,
        conversation_id: ROOT_CONVERSATION,
        kind: "seed".into(),
        version: 1,
        input: Value::Null,
        owner,
        background: false,
        abort_requested: false,
        state,
        memos: None,
    }
}

#[test]
fn expanded_scopes_drive_owned_conversations_and_background_anchors() {
    block_on(bounded(async {
        let active = Gate::default();
        let release = Gate::default();
        let child = leaf();
        let parent = definition(
            "parent",
            [(
                "work",
                phase({
                    let active = active.clone();
                    let release = release.clone();
                    move |_, rt| {
                        let active = active.clone();
                        let release = release.clone();
                        async move {
                            active.release();
                            release.wait().await;
                            update(&rt, TaskUpdate::Complete(Value::Null)).await;
                            Ok(())
                        }
                    }
                }),
            )],
        );
        let (session, sd) = Session::new(memory().await);
        let (runner, td) = TaskRunner::attach(
            &session,
            TaskRegistry::new([parent.clone(), child.clone()]).unwrap(),
        )
        .unwrap();
        zip(
            async {
                let root = create(&session, parent.clone(), Value::Null, None).await;
                let waiter = runner.run(root.id);
                active.wait().await;
                session
                    .commit({
                        let child = child.clone();
                        move |tx| {
                            Box::pin(async move {
                                let child = tx
                                    .create_task(
                                        child,
                                        json!("owned-conversation"),
                                        options(Some(root.id)),
                                    )
                                    .await?;
                                tx.create_conversation_owned(Owner::Task(child.id)).await
                            })
                        }
                    })
                    .await
                    .unwrap();
                let tasks = session
                    .commit(|tx| {
                        Box::pin(async { tx.scan_tasks(TaskQuery::default(), 20, None).await })
                    })
                    .await
                    .unwrap()
                    .value;
                assert_eq!(tasks.items.len(), 2);
                let added = create(&session, child.clone(), json!("allowed"), Some(root.id)).await;
                release.release();
                terminal(waiter.await.unwrap());
                assert_eq!(
                    outcome(&read(&session, added.id).await),
                    &TaskOutcome::Completed {
                        result: json!("allowed")
                    }
                );

                let owner = create(&session, parent, Value::Null, None).await;
                let owned = session
                    .commit({
                        let child = child.clone();
                        move |tx| {
                            Box::pin(async move {
                                let conversation =
                                    tx.create_conversation_owned(Owner::Task(owner.id)).await?;
                                tx.create_task(
                                    child,
                                    Value::Null,
                                    TaskOptions {
                                        conversation_id: Some(conversation.id),
                                        ownership: TaskOwnership::Conversation,
                                        background: false,
                                    },
                                )
                                .await
                            })
                        }
                    })
                    .await
                    .unwrap()
                    .value;
                let background = session
                    .commit({
                        let child = child.clone();
                        move |tx| {
                            Box::pin(async move {
                                tx.create_task(
                                    child,
                                    Value::Null,
                                    TaskOptions {
                                        background: true,
                                        ..options(None)
                                    },
                                )
                                .await
                            })
                        }
                    })
                    .await
                    .unwrap()
                    .value;
                let internal = create(&session, child, Value::Null, Some(owner.id)).await;
                assert_eq!(
                    runner.run(owned.id).await.unwrap(),
                    RunResult::Blocked(BlockReason::UnsupportedScope)
                );
                terminal(runner.run(background.id).await.unwrap());
                terminal(runner.run(owner.id).await.unwrap());
                for task in [owned.id, internal.id] {
                    assert!(matches!(
                        read(&session, task).await.state,
                        TaskState::Terminal { .. }
                    ));
                    assert_eq!(
                        runner.run(task).await.unwrap(),
                        RunResult::Blocked(BlockReason::NotPending)
                    );
                }
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    }));
}

#[test]
fn dropping_root_waiter_does_not_drop_the_admitted_workflow() {
    block_on(bounded(async {
        let finished = Gate::default();
        let child = leaf();
        let parent = spawning_parent(child.clone(), JoinPolicy::AllSettled);
        // A second root supplies a gate so the host can observe the entire first
        // admitted tree finishing without holding its waiter.
        let observer = definition(
            "observer",
            [(
                "work",
                phase({
                    let finished = finished.clone();
                    move |_, rt| {
                        let finished = finished.clone();
                        async move {
                            update(&rt, TaskUpdate::Complete(Value::Null)).await;
                            finished.release();
                            Ok(())
                        }
                    }
                }),
            )],
        );
        // Keep the parent value owned independently of the registry.
        let (session, sd) = Session::new(memory().await);
        let (runner, td) = TaskRunner::attach(
            &session,
            TaskRegistry::new([parent.clone(), child, observer.clone()]).unwrap(),
        )
        .unwrap();
        zip(
            async {
                let root = create(&session, parent.clone(), Value::Null, None).await;
                drop(runner.run(root.id));
                let other = create(&session, observer, Value::Null, None).await;
                let next = runner.run(other.id);
                finished.wait().await;
                terminal(next.await.unwrap());
                assert!(matches!(
                    outcome(&read(&session, root.id).await),
                    TaskOutcome::Completed { .. }
                ));
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    }));
}

#[test]
fn persisted_missing_wait_target_and_terminal_intermediate_do_not_invent_cancellation() {
    block_on(bounded(async {
        let root = Id::new(100).unwrap();
        let middle = Id::new(101).unwrap();
        let grand = Id::new(102).unwrap();
        let missing = Id::new(999).unwrap();
        let active = Gate::default();
        let release = Gate::default();
        let def = definition(
            "seed",
            [
                (
                    "work",
                    phase({
                        let active = active.clone();
                        let release = release.clone();
                        move |task, rt| {
                            let active = active.clone();
                            let release = release.clone();
                            async move {
                                assert!(
                                    !task.abort_requested,
                                    "terminal failed owner must not cascade"
                                );
                                active.release();
                                release.wait().await;
                                update(&rt, TaskUpdate::Complete(json!("grandchild"))).await;
                                Ok(())
                            }
                        }
                    }),
                ),
                (
                    "join",
                    phase(|_, rt| async move {
                        update(&rt, TaskUpdate::Complete(json!("root"))).await;
                        Ok(())
                    }),
                ),
            ],
        );
        let mut storage = memory().await;
        let mut terminal_middle = record(
            middle,
            Some(root),
            TaskState::Terminal {
                outcome: TaskOutcome::Failed {
                    error: TaskOutcomeError {
                        message: "old failure".into(),
                        detail: None,
                    },
                    result: None,
                },
            },
        );
        terminal_middle.abort_requested = true;
        storage
            .commit(vec![
                StorageWrite::Task(record(
                    root,
                    None,
                    TaskState::Waiting {
                        checkpoint: json!({"phase":"join"}),
                        on: vec![middle, missing],
                        policy: JoinPolicy::AllSettled,
                    },
                )),
                StorageWrite::Task(terminal_middle.clone()),
                StorageWrite::Task(record(
                    grand,
                    Some(middle),
                    TaskState::Pending {
                        checkpoint: json!({"phase":"work"}),
                    },
                )),
            ])
            .await
            .unwrap();
        let (session, sd) = Session::new(storage);
        let (runner, td) = TaskRunner::attach(&session, TaskRegistry::new([def]).unwrap()).unwrap();
        zip(
            async {
                let waiter = runner.run(root);
                active.wait().await;
                // Missing persisted targets don't block readiness; after its next
                // phase, the root still holds for its live transitive grandchild.
                assert_eq!(
                    read(&session, root).await.state,
                    TaskState::Completing {
                        outcome: TaskOutcome::Completed {
                            result: json!("root")
                        }
                    }
                );
                assert_eq!(read(&session, middle).await, terminal_middle);
                release.release();
                terminal(waiter.await.unwrap());
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    }));
}

#[test]
fn all_noncompleted_held_outcomes_trigger_fail_fast_but_not_all_settled() {
    let error = TaskOutcomeError {
        message: "durable failure".into(),
        detail: None,
    };
    // Valid durable outcome fixtures complement the live-handler failure and
    // direct-orphan tests above; no test-only scheduler interface is needed.
    let outcomes = [
        TaskOutcome::Failed {
            error: error.clone(),
            result: Some(json!(7)),
        },
        TaskOutcome::Faulted { error },
        TaskOutcome::Aborted {
            reason: Some("requested".into()),
            result: None,
        },
        TaskOutcome::Orphaned {
            reason: "definition removed".into(),
        },
    ];
    for held_outcome in outcomes {
        for policy in [JoinPolicy::AllSettled, JoinPolicy::FailFast] {
            block_on(bounded(async {
                let root = Id::new(100).unwrap();
                let held = Id::new(101).unwrap();
                let sibling = Id::new(102).unwrap();
                let grand = Id::new(103).unwrap();
                let mut storage = memory().await;
                storage
                    .commit(vec![
                        StorageWrite::Task(record(
                            root,
                            None,
                            TaskState::Waiting {
                                checkpoint: json!({"phase":"join", "ids":[held,sibling]}),
                                on: vec![held, sibling],
                                policy,
                            },
                        )),
                        StorageWrite::Task(record(
                            held,
                            Some(root),
                            TaskState::Completing {
                                outcome: held_outcome.clone(),
                            },
                        )),
                        StorageWrite::Task(record(
                            sibling,
                            Some(root),
                            TaskState::Pending {
                                checkpoint: json!({"phase":"work"}),
                            },
                        )),
                        StorageWrite::Task(record(
                            grand,
                            Some(held),
                            TaskState::Pending {
                                checkpoint: json!({"phase":"work"}),
                            },
                        )),
                    ])
                    .await
                    .unwrap();
                let def = definition(
                    "seed",
                    [
                        (
                            "work",
                            phase(|task, rt| async move {
                                assert!(!task.abort_requested);
                                update(&rt, TaskUpdate::Complete(json!(task.id))).await;
                                Ok(())
                            }),
                        ),
                        ("join", join_phase()),
                    ],
                );
                let (session, sd) = Session::new(storage);
                let (runner, td) =
                    TaskRunner::attach(&session, TaskRegistry::new([def]).unwrap()).unwrap();
                zip(
                    async {
                        let done = terminal(runner.run(root).await.unwrap());
                        assert!(!done.abort_requested);
                        let TaskOutcome::Completed { result } = outcome(&done) else {
                            panic!("wait policy must not fail its parent")
                        };
                        assert_eq!(result[0], serde_json::to_value(&held_outcome).unwrap());
                        let failed_fast = policy == JoinPolicy::FailFast;
                        assert_eq!(
                            result[1]["status"],
                            if failed_fast { "aborted" } else { "completed" }
                        );
                        assert_eq!(read(&session, sibling).await.abort_requested, failed_fast);
                        assert!(
                            !read(&session, held).await.abort_requested,
                            "already-held nonCompleted outcomes are not re-aborted"
                        );
                        assert!(matches!(
                            outcome(&read(&session, grand).await),
                            TaskOutcome::Aborted { .. }
                        ));
                        runner.close().await.unwrap();
                        session.close().await.unwrap();
                    },
                    zip(sd, td),
                )
                .await;
            }));
        }
    }
}
