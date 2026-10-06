//! Restart coverage for bounded, explicitly driven task trees.
use futures_lite::future::{block_on, zip};
use publicworks_runtime::*;
use publicworks_storage_sqlite::SqliteStorage;
use serde_json::{Value, json};
use std::{
    cell::{Cell, RefCell},
    future::Future,
    path::PathBuf,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
    task::{Poll, Waker},
};

struct Database(PathBuf);
impl Database {
    fn new() -> Self {
        static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "publicworks-tree-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_DATABASE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn open(&self) -> SqliteStorage {
        SqliteStorage::open(self.0.join("tree.db")).unwrap()
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
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
async fn bounded<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut polls = 0;
    std::future::poll_fn(|cx| {
        polls += 1;
        assert!(polls < 20_000, "tree restart exceeded poll budget");
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
fn checkpoint(task: &TaskRecord) -> &Value {
    match &task.state {
        TaskState::Waiting { checkpoint, .. }
        | TaskState::Running { checkpoint }
        | TaskState::Pending { checkpoint } => checkpoint,
        _ => panic!("no checkpoint"),
    }
}
fn terminal(result: RunResult) -> TaskRecord {
    match result {
        RunResult::Terminal(task) => task,
        result => panic!("expected terminal, got {result:?}"),
    }
}
async fn read(session: &Session, id: Id) -> TaskRecord {
    session
        .commit(move |tx| Box::pin(async move { Ok(tx.task(id).await?.unwrap()) }))
        .await
        .unwrap()
        .value
}
fn options(conversation_id: Id, owner: Option<Id>) -> TaskOptions {
    TaskOptions {
        conversation_id: Some(conversation_id),
        ownership: owner.map_or(TaskOwnership::Conversation, TaskOwnership::Task),
        background: false,
    }
}

#[test]
fn waiting_tree_reopens_without_reinitializing_or_repeating_completed_child() {
    block_on(bounded(async {
        let db = Database::new();
        let initialized = Rc::new(Cell::new(0));
        let started = Rc::new(Cell::new(0));
        let checkpointed = Gate::default();
        let child = TaskDefinition::new(
            "child",
            3,
            {
                let initialized = initialized.clone();
                move |_| {
                    initialized.set(initialized.get() + 1);
                    Ok(json!({"phase":"work"}))
                }
            },
            [
                (
                    "work".into(),
                    phase({
                        let started = started.clone();
                        let checkpointed = checkpointed.clone();
                        move |task, rt| {
                            started.set(started.get() + 1);
                            let checkpointed = checkpointed.clone();
                            async move {
                                let second = task.input["name"] == "second";
                                rt.commit(move |tx, task| {
                                    Box::pin(async move {
                                        tx.append_entry(
                                            task.conversation_id,
                                            EntryDraft::new(if second {
                                                "second checkpoint"
                                            } else {
                                                "first complete"
                                            }),
                                        )
                                        .await?;
                                        Ok(Some(if second {
                                            TaskUpdate::Checkpoint(
                                                json!({"phase":"finish", "answer":42}),
                                            )
                                        } else {
                                            TaskUpdate::Complete(json!(21))
                                        }))
                                    })
                                })
                                .await
                                .unwrap();
                                if second {
                                    checkpointed.release();
                                    rt.cancelled().await;
                                }
                                Ok(())
                            }
                        }
                    }),
                ),
                (
                    "finish".into(),
                    phase(|task, rt| async move {
                        assert_eq!(checkpoint(&task), &json!({"phase":"finish", "answer":42}));
                        rt.commit(|tx, task| {
                            Box::pin(async move {
                                tx.append_entry(
                                    task.conversation_id,
                                    EntryDraft::new("second complete"),
                                )
                                .await?;
                                Ok(Some(TaskUpdate::Complete(json!(42))))
                            })
                        })
                        .await
                        .unwrap();
                        Ok(())
                    }),
                ),
            ]
            .into_iter()
            .collect(),
        );
        let parent = TaskDefinition::new("parent", 5, {
            let initialized = initialized.clone(); move |_| { initialized.set(initialized.get() + 1); Ok(json!({"phase":"spawn"})) }
        }, [
            ("spawn".into(), phase({ let child = child.clone(); move |_, rt| { let child = child.clone(); async move {
                rt.commit(move |tx, task| Box::pin(async move {
                    let first = tx.create_task(child.clone(), json!({"name":"first", "opaque":u64::MAX}), options(task.conversation_id, Some(task.id))).await?;
                    let second = tx.create_task(child, json!({"name":"second", "opaque":u64::MAX}), options(task.conversation_id, Some(task.id))).await?;
                    Ok(Some(TaskUpdate::Wait { checkpoint: json!({"phase":"join", "ids":[first.id, second.id]}), on: vec![first.id, second.id], policy: JoinPolicy::AllSettled }))
                })).await.unwrap(); Ok(())
            }}})),
            ("join".into(), phase(|task, rt| async move {
                let ids: Vec<Id> = serde_json::from_value(checkpoint(&task)["ids"].clone()).unwrap();
                rt.commit(move |tx, task| Box::pin(async move {
                    let mut results = Vec::new();
                    for id in ids {
                        let child = tx.task(id).await?.unwrap();
                        assert_eq!(child.owner, Some(task.id));
                        let TaskState::Terminal { outcome: TaskOutcome::Completed { result } } = child.state else { panic!("join ran before all terminals") };
                        results.push(result);
                    }
                    tx.append_entry(task.conversation_id, EntryDraft::new("parent joined")).await?;
                    Ok(Some(TaskUpdate::Complete(json!(results))))
                })).await.unwrap(); Ok(())
            })),
        ].into_iter().collect());
        let (session, sd) = Session::new(db.open());
        let (runner, td) = TaskRunner::attach(
            &session,
            TaskRegistry::new([parent.clone(), child.clone()]).unwrap(),
        )
        .unwrap();
        let (snapshot, _) = zip(
            async {
                let create_parent = parent.clone();
                let root = session
                    .commit(move |tx| {
                        Box::pin(async move {
                            let conversation = tx.create_conversation().await?;
                            tx.create_task(
                                create_parent,
                                json!({"workflow":7}),
                                options(conversation.id, None),
                            )
                            .await
                        })
                    })
                    .await
                    .unwrap()
                    .value;
                let waiter = runner.run(root.id);
                checkpointed.wait().await;
                let root = read(&session, root.id).await;
                let TaskState::Waiting {
                    on,
                    policy: JoinPolicy::AllSettled,
                    ..
                } = &root.state
                else {
                    panic!("root not durably waiting")
                };
                let first = read(&session, on[0]).await;
                let second = read(&session, on[1]).await;
                assert_eq!(first.status(), TaskStatus::Terminal);
                assert_eq!(second.status(), TaskStatus::Running);
                assert_eq!(initialized.get(), 3);
                assert_eq!(started.get(), 2);
                runner.close().await.unwrap();
                assert_eq!(waiter.await.unwrap(), RunResult::Interrupted);
                assert_eq!(read(&session, root.id).await, root);
                assert_eq!(read(&session, second.id).await, second);
                session.close().await.unwrap();
                (root, first, second)
            },
            zip(sd, td),
        )
        .await;
        let (root, first, second) = snapshot;
        let mut storage = db.open();
        assert_eq!(storage.task(root.id).await.unwrap().unwrap(), root);
        assert_eq!(storage.task(first.id).await.unwrap().unwrap(), first);
        assert_eq!(storage.task(second.id).await.unwrap().unwrap(), second);
        storage.close().await.unwrap();

        let (opening, sd) = Session::open_recovered(db.open());
        let (done, _) = zip(
            async {
                let session = opening.await.unwrap().value;
                assert_eq!(
                    read(&session, root.id).await,
                    root,
                    "open_recovered must not reconcile Waiting"
                );
                assert_eq!(read(&session, first.id).await, first);
                let mut expected = second.clone();
                expected.state = TaskState::Pending {
                    checkpoint: checkpoint(&second).clone(),
                };
                assert_eq!(read(&session, second.id).await, expected);
                let (runner, td) =
                    TaskRunner::attach(&session, TaskRegistry::new([parent, child]).unwrap())
                        .unwrap();
                zip(
                    async {
                        let done = terminal(runner.run(root.id).await.unwrap());
                        assert_eq!(
                            done.state,
                            TaskState::Terminal {
                                outcome: TaskOutcome::Completed {
                                    result: json!([21, 42])
                                }
                            }
                        );
                        assert_eq!(initialized.get(), 3);
                        assert_eq!(started.get(), 2);
                        assert_eq!(read(&session, first.id).await, first);
                        runner.close().await.unwrap();
                        session.close().await.unwrap();
                        done
                    },
                    td,
                )
                .await
                .0
            },
            sd,
        )
        .await;
        let mut storage = db.open();
        assert_eq!(storage.task(root.id).await.unwrap().unwrap(), done);
        let entries = storage
            .scan_entries(EntryQuery::new(root.conversation_id), 20, None)
            .await
            .unwrap()
            .items;
        assert_eq!(
            entries
                .iter()
                .rev()
                .map(|e| (e.kind.as_str(), e.by_task_id))
                .collect::<Vec<_>>(),
            vec![
                ("first complete", Some(first.id)),
                ("second checkpoint", Some(second.id)),
                ("second complete", Some(second.id)),
                ("parent joined", Some(root.id)),
            ]
        );
        storage.close().await.unwrap();
    }));
}

fn seeded(id: u64, owner: Option<u64>, kind: &str, state: TaskState) -> TaskRecord {
    TaskRecord {
        id: Id::new(id).unwrap(),
        conversation_id: ROOT_CONVERSATION,
        kind: kind.into(),
        version: 7,
        input: json!({"name":id, "opaque":u64::MAX}),
        owner: owner.map(|id| Id::new(id).unwrap()),
        background: false,
        abort_requested: false,
        state,
        memos: None,
    }
}

#[test]
fn recovery_preserves_unknown_held_outcomes_then_reconciles_unapplied_cascade_bottom_up() {
    block_on(bounded(async {
        let db = Database::new();
        let completed = |value| TaskState::Completing {
            outcome: TaskOutcome::Completed {
                result: json!(value),
            },
        };
        let mut root = seeded(100, None, "unknown-root", completed("root result"));
        root.abort_requested = true;
        let middle = seeded(101, Some(100), "unknown-middle", completed("middle result"));
        let mut child = seeded(
            102,
            Some(101),
            "cleanup",
            TaskState::Pending {
                checkpoint: json!({"phase":"work"}),
            },
        );
        child.memos = Some(
            [("opaque".into(), json!({"kept":true}))]
                .into_iter()
                .collect(),
        );
        let mut grand = seeded(
            103,
            Some(102),
            "cleanup",
            TaskState::Running {
                checkpoint: json!({"phase":"work", "saved":42}),
            },
        );
        grand.memos = child.memos.clone();
        let drained = seeded(
            104,
            Some(100),
            "unknown-drained",
            TaskState::Completing {
                outcome: TaskOutcome::Faulted {
                    error: TaskOutcomeError {
                        message: "original fault".into(),
                        detail: Some(json!({"opaque":u64::MAX})),
                    },
                },
            },
        );
        let terminal_child = seeded(
            105,
            Some(104),
            "unknown-terminal",
            TaskState::Terminal {
                outcome: TaskOutcome::Aborted {
                    reason: Some("already drained".into()),
                    result: None,
                },
            },
        );
        // This is a valid durable crash boundary: cancellation intent exists but
        // its cascade and drained nested held outcomes have not been applied.
        let mut storage = db.open();
        storage
            .commit(vec![
                StorageWrite::Conversation(ConversationRecord {
                    id: ROOT_CONVERSATION,
                    parent: None,
                    owner: None,
                }),
                StorageWrite::Task(root.clone()),
                StorageWrite::Task(middle.clone()),
                StorageWrite::Task(child.clone()),
                StorageWrite::Task(grand.clone()),
                StorageWrite::Task(drained.clone()),
                StorageWrite::Task(terminal_child.clone()),
            ])
            .await
            .unwrap();
        storage.close().await.unwrap();
        let cleanup_order = Rc::new(RefCell::new(Vec::new()));
        let cleanup = TaskDefinition::new(
            "cleanup",
            7,
            |_| panic!("restart must not initialize"),
            [(
                "work".into(),
                phase(|_, _| async { panic!("cascade must suppress normal work") }),
            )]
            .into_iter()
            .collect(),
        )
        .with_abort_handler(phase({
            let cleanup_order = cleanup_order.clone();
            move |task, rt| {
                let cleanup_order = cleanup_order.clone();
                async move {
                    assert!(task.abort_requested);
                    assert!(!rt.is_cancelled());
                    assert_eq!(task.memos.as_ref().unwrap()["opaque"], json!({"kept":true}));
                    cleanup_order.borrow_mut().push(task.id);
                    rt.commit(|tx, task| {
                        Box::pin(async move {
                            tx.append_entry(task.conversation_id, EntryDraft::new("cleanup"))
                                .await?;
                            Ok(Some(TaskUpdate::Abort {
                                reason: Some("cascade".into()),
                                result: Some(json!(task.id)),
                            }))
                        })
                    })
                    .await
                    .unwrap();
                    Ok(())
                }
            }
        }));
        let (opening, sd) = Session::open_recovered(db.open());
        zip(
            async {
                let session = opening.await.unwrap().value;
                // Opening normalizes Running only; no definition lookup or cascade.
                for expected in [&root, &middle, &child, &drained, &terminal_child] {
                    assert_eq!(read(&session, expected.id).await, *expected);
                }
                let mut normalized = grand.clone();
                normalized.state = TaskState::Pending {
                    checkpoint: checkpoint(&grand).clone(),
                };
                assert_eq!(read(&session, grand.id).await, normalized);
                let (runner, td) =
                    TaskRunner::attach(&session, TaskRegistry::new([cleanup]).unwrap()).unwrap();
                zip(
                    async {
                        let done = terminal(runner.run(root.id).await.unwrap());
                        assert_eq!(done.version, root.version);
                        assert_eq!(done.kind, root.kind);
                        assert_eq!(
                            done.state,
                            TaskState::Terminal {
                                outcome: TaskOutcome::Completed {
                                    result: json!("root result")
                                }
                            }
                        );
                        let settled = read(&session, middle.id).await;
                        assert_eq!(settled.version, middle.version);
                        assert_eq!(
                            settled.state,
                            TaskState::Terminal {
                                outcome: TaskOutcome::Completed {
                                    result: json!("middle result")
                                }
                            }
                        );
                        let settled = read(&session, drained.id).await;
                        let TaskState::Completing { outcome } = &drained.state else {
                            unreachable!()
                        };
                        assert_eq!(
                            settled.state,
                            TaskState::Terminal {
                                outcome: outcome.clone()
                            }
                        );
                        assert_eq!(settled.version, drained.version);
                        assert_eq!(read(&session, terminal_child.id).await, terminal_child);
                        assert_eq!(&*cleanup_order.borrow(), &[grand.id, child.id]);
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
        let mut storage = db.open();
        let entries = storage
            .scan_entries(EntryQuery::new(ROOT_CONVERSATION), 20, None)
            .await
            .unwrap()
            .items;
        assert_eq!(
            entries
                .iter()
                .rev()
                .map(|e| e.by_task_id)
                .collect::<Vec<_>>(),
            vec![Some(grand.id), Some(child.id)]
        );
        for id in [
            root.id,
            middle.id,
            child.id,
            grand.id,
            drained.id,
            terminal_child.id,
        ] {
            assert_eq!(
                storage.task(id).await.unwrap().unwrap().status(),
                TaskStatus::Terminal
            );
        }
        storage.close().await.unwrap();
    }));
}
