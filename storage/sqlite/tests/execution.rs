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
            "publicworks-execution-{}-{}",
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
        SqliteStorage::open(self.0.join("execution.db")).unwrap()
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

#[test]
fn cooperative_close_and_reopen_resumes_checkpoint_without_reinitializing() {
    block_on(async {
        let db = Database::new();
        let initializations = Rc::new(Cell::new(0));
        let first_effects = Rc::new(Cell::new(0));
        let checkpointed = Gate::default();
        let mut phases: BTreeMap<String, PhaseHandler> = BTreeMap::new();
        phases.insert(
            "first".into(),
            Rc::new({
                let first_effects = first_effects.clone();
                let checkpointed = checkpointed.clone();
                move |task, runtime| {
                    first_effects.set(first_effects.get() + 1);
                    let checkpointed = checkpointed.clone();
                    Box::pin(async move {
                        assert_eq!(
                            task.memos.as_ref().unwrap()["saved"],
                            json!({"opaque":true})
                        );
                        runtime
                            .commit(|tx, task| {
                                Box::pin(async move {
                                    tx.append_entry(
                                        task.conversation_id,
                                        EntryDraft::new("first effect"),
                                    )
                                    .await?;
                                    Ok(Some(TaskUpdate::Checkpoint(
                                        json!({"phase":"finish", "answer":42}),
                                    )))
                                })
                            })
                            .await
                            .unwrap();
                        checkpointed.release();
                        runtime.cancelled().await;
                        Ok(())
                    })
                }
            }),
        );
        phases.insert(
            "finish".into(),
            Rc::new(|task, runtime| {
                Box::pin(async move {
                    assert_eq!(
                        task.state,
                        TaskState::Running {
                            checkpoint: json!({"phase":"finish", "answer":42})
                        }
                    );
                    assert_eq!(
                        task.memos.as_ref().unwrap()["saved"],
                        json!({"opaque":true})
                    );
                    runtime
                        .commit(|tx, task| {
                            Box::pin(async move {
                                let entries = tx
                                    .scan_entries(EntryQuery::new(task.conversation_id), 10, None)
                                    .await?;
                                assert_eq!(entries.items.len(), 1);
                                assert_eq!(entries.items[0].kind, "first effect");
                                assert_eq!(entries.items[0].by_task_id, Some(task.id));
                                Ok(Some(TaskUpdate::Complete(json!(42))))
                            })
                        })
                        .await
                        .unwrap();
                    Ok(())
                })
            }),
        );
        let def = TaskDefinition::new(
            "durable-example",
            3,
            {
                let initializations = initializations.clone();
                move |input| {
                    initializations.set(initializations.get() + 1);
                    assert_eq!(input, &json!("input"));
                    Ok(json!({"phase":"first"}))
                }
            },
            phases,
        );

        // Public Storage seeding adds an opaque memo, without private runtime
        // APIs or legacy-format fixtures. Only task creation calls the initializer.
        let (session, sd) = Session::new(db.open());
        let id = zip(
            async {
                let def = def.clone();
                let task = session
                    .commit(move |tx| {
                        Box::pin(async move {
                            let conversation = tx.create_conversation().await?;
                            tx.create_task(
                                def,
                                json!("input"),
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
            sd,
        )
        .await
        .0;
        let mut storage = db.open();
        let mut task = storage.task(id).await.unwrap().unwrap();
        task.memos = Some(BTreeMap::from([("saved".into(), json!({"opaque":true}))]));
        storage
            .commit(vec![StorageWrite::Task(task)])
            .await
            .unwrap();
        storage.close().await.unwrap();

        let (session, sd) = Session::new(db.open());
        let (runner, td) =
            TaskRunner::attach(&session, TaskRegistry::new([def.clone()]).unwrap()).unwrap();
        zip(
            async {
                let waiter = runner.run(id);
                checkpointed.wait().await;
                let read = session
                    .commit(move |tx| {
                        Box::pin(async move {
                            let task = tx.task(id).await?.unwrap();
                            let entries = tx
                                .scan_entries(EntryQuery::new(task.conversation_id), 10, None)
                                .await?;
                            assert_eq!(entries.items.len(), 1);
                            assert_eq!(entries.items[0].by_task_id, Some(id));
                            assert_eq!(
                                tx.entry(entries.items[0].id)
                                    .await?
                                    .unwrap()
                                    .commit_seq
                                    .get(),
                                4
                            ); // creation, memo, reservation, checkpoint+entry
                            Ok(task)
                        })
                    })
                    .await
                    .unwrap()
                    .value;
                assert_eq!(
                    read.state,
                    TaskState::Running {
                        checkpoint: json!({"phase":"finish", "answer":42})
                    }
                );
                assert!(read.memos.is_some());
                runner.close().await.unwrap();
                assert!(matches!(waiter.await.unwrap(), RunResult::Interrupted));
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;

        // Closing did not claim completion; the durable checkpoint is Running.
        let mut storage = db.open();
        assert!(matches!(
            storage.task(id).await.unwrap().unwrap().state,
            TaskState::Running { .. }
        ));
        storage.close().await.unwrap();
        let (opening, sd) = Session::open_recovered(db.open());
        let completed = zip(
            async {
                let session = opening.await.unwrap().value;
                let pending = session
                    .commit(move |tx| Box::pin(async move { Ok(tx.task(id).await?.unwrap()) }))
                    .await
                    .unwrap()
                    .value;
                assert_eq!(
                    pending.state,
                    TaskState::Pending {
                        checkpoint: json!({"phase":"finish", "answer":42})
                    }
                );
                let (runner, td) =
                    TaskRunner::attach(&session, TaskRegistry::new([def]).unwrap()).unwrap();
                zip(
                    async {
                        let completed = match runner.run(id).await.unwrap() {
                            RunResult::Terminal(task) => task,
                            other => panic!("expected terminal, got {other:?}"),
                        };
                        assert_eq!(
                            completed.state,
                            TaskState::Terminal {
                                outcome: TaskOutcome::Completed { result: json!(42) }
                            }
                        );
                        assert!(completed.memos.is_none());
                        runner.close().await.unwrap();
                        session.close().await.unwrap();
                        completed
                    },
                    td,
                )
                .await
                .0
            },
            sd,
        )
        .await
        .0;
        assert_eq!(initializations.get(), 1);
        assert_eq!(first_effects.get(), 1);

        let mut storage = db.open();
        assert_eq!(storage.task(id).await.unwrap(), Some(completed.clone()));
        let entries = storage
            .scan_entries(EntryQuery::new(completed.conversation_id), 10, None)
            .await
            .unwrap();
        assert_eq!(entries.items.len(), 1);
        assert_eq!(entries.items[0].by_task_id, Some(id));
        storage.close().await.unwrap();
    });
}
