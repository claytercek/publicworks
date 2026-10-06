use futures_lite::future::{block_on, zip};
use publicworks_runtime::*;
use publicworks_storage_sqlite::SqliteStorage;
use serde_json::json;
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    path::PathBuf,
    rc::Rc,
    task::Waker,
};

struct Database(PathBuf);
impl Database {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "publicworks-abort-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
    }

    fn open(&self) -> SqliteStorage {
        SqliteStorage::open(self.0.join("abort.db")).unwrap()
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

fn definition(
    initializations: Rc<Cell<usize>>,
    normal_calls: Rc<Cell<usize>>,
    cleanup_calls: Rc<Cell<usize>>,
    normal_checkpointed: Gate,
    cleanup_checkpointed: Gate,
) -> TaskDefinition {
    let normal: PhaseHandler = Rc::new(move |task, runtime| {
        normal_calls.set(normal_calls.get() + 1);
        let normal_checkpointed = normal_checkpointed.clone();
        Box::pin(async move {
            assert_eq!(
                task.memos.as_ref().unwrap()["opaque"],
                json!({"survives":true})
            );
            runtime
                .commit(|tx, task| {
                    Box::pin(async move {
                        tx.append_entry(task.conversation_id, EntryDraft::new("normal checkpoint"))
                            .await?;
                        Ok(Some(TaskUpdate::Checkpoint(
                            json!({"phase":"resume", "normal":true}),
                        )))
                    })
                })
                .await
                .unwrap();
            normal_checkpointed.release();
            runtime.cancelled().await;
            Ok(())
        })
    });

    let cleanup: PhaseHandler = Rc::new(move |task, runtime| {
        cleanup_calls.set(cleanup_calls.get() + 1);
        assert!(task.abort_requested);
        assert!(!runtime.is_cancelled());
        let cleanup_checkpointed = cleanup_checkpointed.clone();
        Box::pin(async move {
            let checkpoint = match &task.state {
                TaskState::Running { checkpoint } => checkpoint,
                state => panic!("cleanup received non-running state: {state:?}"),
            };
            if checkpoint.get("phase") == Some(&json!("resume")) {
                runtime
                    .commit(|tx, task| {
                        Box::pin(async move {
                            tx.append_entry(
                                task.conversation_id,
                                EntryDraft::new("cleanup checkpoint"),
                            )
                            .await?;
                            Ok(Some(TaskUpdate::Checkpoint(
                                json!({"phase":"cleanup", "started":true}),
                            )))
                        })
                    })
                    .await
                    .unwrap();
                cleanup_checkpointed.release();
                runtime.cancelled().await;
                Ok(())
            } else {
                assert_eq!(checkpoint, &json!({"phase":"cleanup", "started":true}));
                assert_eq!(
                    task.memos.as_ref().unwrap()["opaque"],
                    json!({"survives":true})
                );
                runtime
                    .commit(|tx, task| {
                        Box::pin(async move {
                            tx.append_entry(
                                task.conversation_id,
                                EntryDraft::new("cleanup finished"),
                            )
                            .await?;
                            Ok(Some(TaskUpdate::Abort {
                                reason: Some("operator request".into()),
                                result: Some(json!(null)),
                            }))
                        })
                    })
                    .await
                    .unwrap();
                Ok(())
            }
        })
    });

    TaskDefinition::new(
        "durable-abort",
        4,
        move |input| {
            initializations.set(initializations.get() + 1);
            assert_eq!(input, &json!({"job":7}));
            Ok(json!({"phase":"work"}))
        },
        [(String::from("work"), normal)].into_iter().collect(),
    )
    .with_abort_handler(cleanup)
}

#[test]
fn abort_mark_cleanup_checkpoint_and_attribution_survive_restart() {
    block_on(async {
        let db = Database::new();
        let initializations = Rc::new(Cell::new(0));
        let normal_calls = Rc::new(Cell::new(0));
        let cleanup_calls = Rc::new(Cell::new(0));
        let normal_checkpointed = Gate::default();
        let cleanup_checkpointed = Gate::default();
        let definition = definition(
            initializations.clone(),
            normal_calls.clone(),
            cleanup_calls.clone(),
            normal_checkpointed.clone(),
            cleanup_checkpointed.clone(),
        );

        // Create through the public task API, then add an opaque memo through the
        // public Storage boundary. Creation is the only initializer call.
        let (session, session_driver) = Session::new(db.open());
        let id = zip(
            async {
                let definition = definition.clone();
                let task = session
                    .commit(move |tx| {
                        Box::pin(async move {
                            let conversation = tx.create_conversation().await?;
                            tx.create_task(
                                definition,
                                json!({"job":7}),
                                TaskOptions {
                                    conversation_id: Some(conversation.id),
                                    ownership: TaskOwnership::Conversation,
                                    background: false,
                                },
                            )
                            .await
                        })
                    })
                    .await
                    .unwrap()
                    .value;
                session.close().await.unwrap();
                task.id
            },
            session_driver,
        )
        .await
        .0;
        let mut storage = db.open();
        let mut task = storage.task(id).await.unwrap().unwrap();
        task.memos = Some(BTreeMap::from([(
            "opaque".into(),
            json!({"survives":true}),
        )]));
        storage
            .commit(vec![StorageWrite::Task(task)])
            .await
            .unwrap();
        storage.close().await.unwrap();

        // Start normally, durably mark abort, then interrupt cleanup only after
        // its own checkpoint and attributed effect are durable.
        let (session, session_driver) = Session::new(db.open());
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        zip(
            async {
                let run = runner.run(id);
                normal_checkpointed.wait().await;
                assert_eq!(runner.abort(id).await.unwrap(), AbortResult::Marked);
                cleanup_checkpointed.wait().await;
                let during_cleanup = session
                    .commit(move |tx| Box::pin(async move { Ok(tx.task(id).await?.unwrap()) }))
                    .await
                    .unwrap()
                    .value;
                assert!(during_cleanup.abort_requested);
                assert_eq!(
                    during_cleanup.state,
                    TaskState::Running {
                        checkpoint: json!({"phase":"cleanup", "started":true})
                    }
                );
                assert!(during_cleanup.memos.is_some());

                runner.close().await.unwrap();
                assert_eq!(run.await.unwrap(), RunResult::Interrupted);
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;

        let mut storage = db.open();
        let interrupted = storage.task(id).await.unwrap().unwrap();
        assert!(interrupted.abort_requested);
        assert_eq!(
            interrupted.state,
            TaskState::Running {
                checkpoint: json!({"phase":"cleanup", "started":true})
            }
        );
        assert!(interrupted.memos.is_some());
        let effects = storage
            .scan_entries(EntryQuery::new(interrupted.conversation_id), 10, None)
            .await
            .unwrap();
        assert_eq!(effects.items.len(), 2);
        assert_eq!(effects.items[0].kind, "cleanup checkpoint");
        assert_eq!(effects.items[1].kind, "normal checkpoint");
        assert!(
            effects
                .items
                .iter()
                .all(|entry| entry.by_task_id == Some(id))
        );
        storage.close().await.unwrap();

        // Recovery retains the sticky mark, cleanup checkpoint, and memos. An
        // explicit run dispatches cleanup directly; it does not initialize or
        // replay the normal phase.
        let (opening, session_driver) = Session::open_recovered(db.open());
        let completed = zip(
            async {
                let session = opening.await.unwrap().value;
                let recovered = session
                    .commit(move |tx| Box::pin(async move { Ok(tx.task(id).await?.unwrap()) }))
                    .await
                    .unwrap()
                    .value;
                assert!(recovered.abort_requested);
                assert_eq!(
                    recovered.state,
                    TaskState::Pending {
                        checkpoint: json!({"phase":"cleanup", "started":true})
                    }
                );
                assert!(recovered.memos.is_some());

                let (runner, task_driver) =
                    TaskRunner::attach(&session, TaskRegistry::new([definition]).unwrap()).unwrap();
                zip(
                    async {
                        let completed = match runner.run(id).await.unwrap() {
                            RunResult::Terminal(task) => task,
                            other => panic!("expected terminal task, got {other:?}"),
                        };
                        assert_eq!(
                            completed.state,
                            TaskState::Terminal {
                                outcome: TaskOutcome::Aborted {
                                    reason: Some("operator request".into()),
                                    result: Some(json!(null)),
                                }
                            }
                        );
                        assert!(completed.abort_requested);
                        assert!(completed.memos.is_none());
                        runner.close().await.unwrap();
                        session.close().await.unwrap();
                        completed
                    },
                    task_driver,
                )
                .await
                .0
            },
            session_driver,
        )
        .await
        .0;

        assert_eq!(initializations.get(), 1);
        assert_eq!(normal_calls.get(), 1);
        assert_eq!(cleanup_calls.get(), 2);
        let mut storage = db.open();
        assert_eq!(storage.task(id).await.unwrap(), Some(completed.clone()));
        let effects = storage
            .scan_entries(EntryQuery::new(completed.conversation_id), 10, None)
            .await
            .unwrap();
        assert_eq!(effects.items.len(), 3);
        assert_eq!(effects.items[0].kind, "cleanup finished");
        assert_eq!(effects.items[1].kind, "cleanup checkpoint");
        assert_eq!(effects.items[2].kind, "normal checkpoint");
        assert!(
            effects
                .items
                .iter()
                .all(|entry| entry.by_task_id == Some(id))
        );
        storage.close().await.unwrap();
    });
}
