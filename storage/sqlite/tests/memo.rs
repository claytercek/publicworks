use futures_lite::future::{block_on, zip};
use publicworks_runtime::{
    test_support::{Gate, TempDatabase},
    *,
};
use publicworks_storage_sqlite::SqliteStorage;
use serde_json::{Value, json};
use std::{cell::Cell, collections::BTreeMap, rc::Rc};

struct Database(TempDatabase);
impl Database {
    fn new(label: &str) -> Self {
        Self(TempDatabase::new(
            &format!("publicworks-memo-{label}-"),
            "memo.db",
        ))
    }

    fn open(&self) -> SqliteStorage {
        SqliteStorage::open(self.0.path()).unwrap()
    }
}

fn phase<F, Fut>(f: F) -> PhaseHandler
where
    F: Fn(TaskRecord, TaskRuntime) -> Fut + 'static,
    Fut: std::future::Future<Output = Result<(), TaskOutcomeError>> + 'static,
{
    Rc::new(move |task, runtime| Box::pin(f(task, runtime)))
}

fn options(conversation_id: Id) -> TaskOptions {
    TaskOptions {
        conversation_id: Some(conversation_id),
        ownership: TaskOwnership::Conversation,
        background: false,
    }
}

async fn create(session: &Session, definition: TaskDefinition, input: Value) -> TaskRecord {
    session
        .commit(move |tx| {
            Box::pin(async move {
                let conversation = tx.create_conversation().await?;
                tx.create_task(definition, input, options(conversation.id))
                    .await
            })
        })
        .await
        .unwrap()
        .value
}

async fn stored(db: &Database, id: Id) -> TaskRecord {
    let mut storage = db.open();
    let task = storage.task(id).await.unwrap().unwrap();
    storage.close().await.unwrap();
    task
}

#[test]
fn checkpoint_restart_preserves_the_winner_and_completion_clears_it() {
    block_on(async {
        let db = Database::new("checkpoint");
        let checkpointed = Gate::default();
        let initializations = Rc::new(Cell::new(0));
        let definition = TaskDefinition::new(
            "sqlite-memo-checkpoint",
            1,
            {
                let initializations = initializations.clone();
                move |_| {
                    initializations.set(initializations.get() + 1);
                    Ok(json!({"phase":"write"}))
                }
            },
            BTreeMap::from([
                (
                    "write".into(),
                    phase({
                        let checkpointed = checkpointed.clone();
                        move |_, runtime| {
                            let checkpointed = checkpointed.clone();
                            async move {
                                let winner = json!({
                                    "signed": i64::MIN,
                                    "unsigned": u64::MAX,
                                    "$serde_json::private::Number": "ordinary"
                                });
                                assert_eq!(
                                    runtime
                                        .memo_or_insert("winner", winner.clone())
                                        .await
                                        .unwrap()
                                        .value,
                                    winner
                                );
                                assert_eq!(
                                    runtime
                                        .memo_or_insert("winner", json!("loser"))
                                        .await
                                        .unwrap()
                                        .value,
                                    winner
                                );
                                runtime
                                    .commit(|_, _| {
                                        Box::pin(async {
                                            Ok(Some(TaskUpdate::Checkpoint(json!({
                                                "phase":"finish"
                                            }))))
                                        })
                                    })
                                    .await
                                    .unwrap();
                                checkpointed.release();
                                runtime.cancelled().await;
                                Ok(())
                            }
                        }
                    }),
                ),
                (
                    "finish".into(),
                    phase(|task, runtime| async move {
                        let winner = task.memos.as_ref().unwrap()["winner"].clone();
                        assert_eq!(
                            runtime.memo("winner").await.unwrap().value,
                            Some(winner.clone())
                        );
                        assert_eq!(
                            runtime
                                .memo_or_insert("winner", json!("replacement"))
                                .await
                                .unwrap()
                                .value,
                            winner
                        );
                        runtime
                            .commit(move |_, _| {
                                Box::pin(async move { Ok(Some(TaskUpdate::Complete(winner))) })
                            })
                            .await
                            .unwrap();
                        Ok(())
                    }),
                ),
            ]),
        );

        let (session, session_driver) = Session::new(db.open());
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        let id = zip(
            async {
                let task = create(&session, definition.clone(), Value::Null).await;
                let run = runner.run(task.id);
                checkpointed.wait().await;
                runner.close().await.unwrap();
                assert_eq!(run.await.unwrap(), RunResult::Interrupted);
                session.close().await.unwrap();
                task.id
            },
            zip(session_driver, task_driver),
        )
        .await
        .0;

        let interrupted = stored(&db, id).await;
        assert!(matches!(interrupted.state, TaskState::Running { .. }));
        assert_eq!(
            interrupted.memos.as_ref().unwrap()["winner"]["unsigned"],
            json!(u64::MAX)
        );

        let (opening, session_driver) = Session::open_recovered(db.open());
        zip(
            async {
                let session = opening.await.unwrap().value;
                let recovered = session
                    .commit(move |tx| Box::pin(async move { Ok(tx.task(id).await?.unwrap()) }))
                    .await
                    .unwrap()
                    .value;
                assert!(matches!(recovered.state, TaskState::Pending { .. }));
                assert!(recovered.memos.is_some());
                let (runner, task_driver) =
                    TaskRunner::attach(&session, TaskRegistry::new([definition]).unwrap()).unwrap();
                zip(
                    async {
                        let RunResult::Terminal(done) = runner.run(id).await.unwrap() else {
                            panic!("expected terminal task")
                        };
                        assert!(done.memos.is_none());
                        runner.close().await.unwrap();
                        session.close().await.unwrap();
                    },
                    task_driver,
                )
                .await;
            },
            session_driver,
        )
        .await;

        assert_eq!(initializations.get(), 1);
        assert!(stored(&db, id).await.memos.is_none());
    });
}

#[test]
fn abort_checkpoint_restart_can_read_the_normal_phase_memo() {
    block_on(async {
        let db = Database::new("abort");
        let normal_checkpointed = Gate::default();
        let cleanup_checkpointed = Gate::default();
        let normal_calls = Rc::new(Cell::new(0));
        let cleanup_calls = Rc::new(Cell::new(0));

        let normal: PhaseHandler = phase({
            let normal_checkpointed = normal_checkpointed.clone();
            let normal_calls = normal_calls.clone();
            move |_, runtime| {
                normal_calls.set(normal_calls.get() + 1);
                let normal_checkpointed = normal_checkpointed.clone();
                async move {
                    runtime
                        .memo_or_insert("request", json!({"id":7, "stable":true}))
                        .await
                        .unwrap();
                    runtime
                        .commit(|_, _| {
                            Box::pin(async {
                                Ok(Some(TaskUpdate::Checkpoint(json!({"phase":"resume"}))))
                            })
                        })
                        .await
                        .unwrap();
                    normal_checkpointed.release();
                    runtime.cancelled().await;
                    Ok(())
                }
            }
        });
        let cleanup: PhaseHandler = phase({
            let cleanup_checkpointed = cleanup_checkpointed.clone();
            let cleanup_calls = cleanup_calls.clone();
            move |task, runtime| {
                cleanup_calls.set(cleanup_calls.get() + 1);
                let cleanup_checkpointed = cleanup_checkpointed.clone();
                async move {
                    assert!(task.abort_requested);
                    assert_eq!(
                        runtime.memo("request").await.unwrap().value,
                        Some(json!({"id":7, "stable":true}))
                    );
                    let checkpoint = match task.state {
                        TaskState::Running { checkpoint } => checkpoint,
                        state => panic!("unexpected cleanup state: {state:?}"),
                    };
                    if checkpoint["phase"] == "resume" {
                        assert_eq!(
                            runtime
                                .memo_or_insert("request", json!("cannot replace"))
                                .await
                                .unwrap()
                                .value,
                            json!({"id":7, "stable":true})
                        );
                        runtime
                            .commit(|_, _| {
                                Box::pin(async {
                                    Ok(Some(TaskUpdate::Checkpoint(json!({
                                        "phase":"cleanup"
                                    }))))
                                })
                            })
                            .await
                            .unwrap();
                        cleanup_checkpointed.release();
                        runtime.cancelled().await;
                    } else {
                        assert_eq!(checkpoint, json!({"phase":"cleanup"}));
                        runtime
                            .commit(|_, _| {
                                Box::pin(async {
                                    Ok(Some(TaskUpdate::Abort {
                                        reason: Some("operator".into()),
                                        result: None,
                                    }))
                                })
                            })
                            .await
                            .unwrap();
                    }
                    Ok(())
                }
            }
        });
        let definition = TaskDefinition::new(
            "sqlite-memo-abort",
            1,
            |_| Ok(json!({"phase":"work"})),
            BTreeMap::from([("work".into(), normal)]),
        )
        .with_abort_handler(cleanup);

        let (session, session_driver) = Session::new(db.open());
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
        let id = zip(
            async {
                let task = create(&session, definition.clone(), json!({"job":7})).await;
                let run = runner.run(task.id);
                normal_checkpointed.wait().await;
                assert_eq!(runner.abort(task.id).await.unwrap(), AbortResult::Marked);
                cleanup_checkpointed.wait().await;
                runner.close().await.unwrap();
                assert_eq!(run.await.unwrap(), RunResult::Interrupted);
                session.close().await.unwrap();
                task.id
            },
            zip(session_driver, task_driver),
        )
        .await
        .0;

        let interrupted = stored(&db, id).await;
        assert!(interrupted.abort_requested);
        assert!(interrupted.memos.is_some());
        assert!(matches!(interrupted.state, TaskState::Running { .. }));

        let (opening, session_driver) = Session::open_recovered(db.open());
        zip(
            async {
                let session = opening.await.unwrap().value;
                let recovered = session
                    .commit(move |tx| Box::pin(async move { Ok(tx.task(id).await?.unwrap()) }))
                    .await
                    .unwrap()
                    .value;
                assert!(recovered.abort_requested);
                assert!(recovered.memos.is_some());
                assert!(matches!(recovered.state, TaskState::Pending { .. }));
                let (runner, task_driver) =
                    TaskRunner::attach(&session, TaskRegistry::new([definition]).unwrap()).unwrap();
                zip(
                    async {
                        let RunResult::Terminal(done) = runner.run(id).await.unwrap() else {
                            panic!("expected terminal task")
                        };
                        assert!(matches!(
                            done.state,
                            TaskState::Terminal {
                                outcome: TaskOutcome::Aborted { .. }
                            }
                        ));
                        assert!(done.memos.is_none());
                        runner.close().await.unwrap();
                        session.close().await.unwrap();
                    },
                    task_driver,
                )
                .await;
            },
            session_driver,
        )
        .await;

        assert_eq!(normal_calls.get(), 1);
        assert_eq!(cleanup_calls.get(), 2);
        assert!(stored(&db, id).await.memos.is_none());
    });
}

#[path = "../../../runtime/tests/support/phase_lifetime.rs"]
mod phase_lifetime;

#[test]
fn phase_handoff_joins_dropped_mutations_and_fences_retained_runtime() {
    let db = Database::new("phase-lifetime");
    block_on(phase_lifetime::phase_handoff(db.open()));
}
