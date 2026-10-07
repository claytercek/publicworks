use futures_lite::future::zip;
use publicworks_runtime::*;
use serde_json::json;
use std::{cell::RefCell, future::Future, rc::Rc};

fn phase<F, Fut>(f: F) -> PhaseHandler
where
    F: Fn(TaskRecord, TaskRuntime) -> Fut + 'static,
    Fut: Future<Output = Result<(), TaskOutcomeError>> + 'static,
{
    Rc::new(move |task, runtime| Box::pin(f(task, runtime)))
}

pub async fn phase_handoff(storage: impl Storage + 'static) {
    let retained = Rc::new(RefCell::new(None::<TaskRuntime>));
    let definition = TaskDefinition::new(
        "memo-phase",
        1,
        |_| Ok(json!({"phase":"work"})),
        [
            (
                "work",
                phase({
                    let retained = retained.clone();
                    move |_, runtime| {
                        *retained.borrow_mut() = Some(runtime.clone());
                        async move {
                            drop(runtime.memo_or_insert("winner", json!(42)));
                            drop(runtime.commit(|_, _| {
                                Box::pin(async {
                                    Ok(Some(TaskUpdate::Checkpoint(json!({"phase":"next"}))))
                                })
                            }));
                            Ok(())
                        }
                    }
                }),
            ),
            (
                "next",
                phase(move |_, runtime| {
                    let old = retained.borrow().clone().unwrap();
                    async move {
                        assert!(old.is_cancelled());
                        old.cancelled().await;
                        assert!(matches!(
                            old.memo("winner").await,
                            Err(SessionError::Invalid(_))
                        ));
                        assert!(matches!(
                            old.memo_or_insert("late", json!(true)).await,
                            Err(SessionError::Invalid(_))
                        ));
                        assert!(matches!(
                            old.read::<_, ()>(|_, _| Box::pin(async {
                                panic!("stale read callback")
                            }))
                            .await,
                            Err(SessionError::Invalid(_))
                        ));
                        assert_eq!(runtime.memo("winner").await.unwrap().value, Some(json!(42)));
                        assert_eq!(runtime.memo("late").await.unwrap().value, None);
                        runtime
                            .commit(|_, _| {
                                Box::pin(async { Ok(Some(TaskUpdate::Complete(json!("done")))) })
                            })
                            .await
                            .unwrap();
                        Ok(())
                    }
                }),
            ),
        ]
        .into_iter()
        .map(|(name, handler)| (name.into(), handler))
        .collect(),
    );
    let (session, session_driver) = Session::new(storage);
    let (runner, task_driver) =
        TaskRunner::attach(&session, TaskRegistry::new([definition.clone()]).unwrap()).unwrap();
    zip(
        async {
            let task = session
                .commit(move |tx| {
                    Box::pin(async move {
                        let conversation = tx.create_conversation().await?;
                        tx.create_task(
                            definition,
                            json!(null),
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
            let RunResult::Terminal(task) = runner.run(task.id).await.unwrap() else {
                panic!("expected terminal task")
            };
            assert!(matches!(
                task.state,
                TaskState::Terminal {
                    outcome: TaskOutcome::Completed { .. }
                }
            ));
            runner.close().await.unwrap();
            session.close().await.unwrap();
        },
        zip(session_driver, task_driver),
    )
    .await;
}
