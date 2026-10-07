use futures_lite::future::{block_on, zip};
use futures_util::future::join_all;
use publicworks_runtime::*;
use serde_json::json;
use std::{cell::Cell, collections::BTreeMap, rc::Rc};

fn completing_definition(
    kind: &str,
    expected_running: usize,
    calls: Rc<Cell<usize>>,
) -> TaskDefinition {
    let handler: PhaseHandler = Rc::new(move |_, runtime| {
        let calls = calls.clone();
        Box::pin(async move {
            let running = runtime
                .read(move |tx, _| {
                    Box::pin(async move {
                        Ok(tx
                            .scan_tasks(
                                TaskQuery {
                                    status: Some(TaskStatus::Running),
                                    ..TaskQuery::default()
                                },
                                128,
                                None,
                            )
                            .await?
                            .items
                            .len())
                    })
                })
                .await
                .map_err(|error| TaskOutcomeError {
                    message: error.to_string(),
                    detail: None,
                })?
                .value;
            assert_eq!(running, expected_running);
            calls.set(calls.get() + 1);
            runtime
                .commit(|_, _| Box::pin(async { Ok(Some(TaskUpdate::Complete(json!("done")))) }))
                .await
                .map_err(|error| TaskOutcomeError {
                    message: error.to_string(),
                    detail: None,
                })?;
            Ok(())
        })
    });
    TaskDefinition::new(
        kind,
        1,
        |_| Ok(json!({"phase":"run"})),
        BTreeMap::from([("run".into(), handler)]),
    )
}

#[test]
fn resume_reserves_all_roots_and_runs_them_concurrently() {
    block_on(async {
        let calls = Rc::new(Cell::new(0));
        let definition = completing_definition("work", 3, calls.clone());
        let (opening, driver) = Harness::open(
            MemoryStorage::new(),
            TaskRegistry::new([definition.clone()]).unwrap(),
        );
        let command = async move {
            let harness = opening.await.unwrap();
            let tasks = harness
                .commit(move |tx| {
                    Box::pin(async move {
                        let mut tasks = Vec::new();
                        for value in 0..3 {
                            let conversation = tx.create_conversation().await?;
                            tasks.push(
                                tx.create_task(
                                    definition.clone(),
                                    json!(value),
                                    TaskOptions {
                                        ownership: TaskOwnership::Conversation,
                                        conversation_id: Some(conversation.id),
                                        background: false,
                                    },
                                )
                                .await?,
                            );
                        }
                        Ok(tasks)
                    })
                })
                .await
                .unwrap()
                .value;

            let paused = harness.inspect().await.unwrap();
            assert!(!paused.progress_enabled);
            assert_eq!(paused.tasks.len(), 3);
            assert_eq!(calls.get(), 0);

            let waiters = tasks
                .iter()
                .map(|task| harness.wait_task(task.id))
                .collect::<Vec<_>>();
            harness.resume().unwrap();
            let settled = join_all(waiters).await;
            assert!(settled.iter().all(|result| matches!(
                result,
                Ok(TaskRecord {
                    state: TaskState::Terminal {
                        outcome: TaskOutcome::Completed { .. }
                    },
                    ..
                })
            )));
            assert_eq!(calls.get(), 3);
            assert!(harness.inspect().await.unwrap().tasks.is_empty());
            harness.close().await.unwrap();
        };
        let ((), ()) = zip(command, driver).await;
    });
}

#[test]
fn registry_replacement_wakes_blocked_work_after_resume() {
    block_on(async {
        let calls = Rc::new(Cell::new(0));
        let definition = completing_definition("late", 1, calls.clone());
        let (opening, driver) = Harness::open(MemoryStorage::new(), TaskRegistry::default());
        let command = async move {
            let harness = opening.await.unwrap();
            let task = harness
                .commit(move |tx| {
                    Box::pin(async move {
                        let conversation = tx.create_conversation().await?;
                        tx.create_task(
                            definition.clone(),
                            json!(null),
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
                .value;
            harness.resume().unwrap();
            // The missing definition remains committed pending work.
            let inspection = harness.inspect().await.unwrap();
            assert_eq!(
                inspection.tasks[0].blocked,
                Some(BlockReason::MissingDefinition)
            );
            assert_eq!(calls.get(), 0);

            harness
                .replace_registry(
                    TaskRegistry::new([completing_definition("late", 1, calls.clone())]).unwrap(),
                )
                .unwrap();
            let terminal = harness.wait_task(task.id).await.unwrap();
            assert_eq!(terminal.status(), TaskStatus::Terminal);
            assert_eq!(calls.get(), 1);
            harness.close().await.unwrap();
        };
        let ((), ()) = zip(command, driver).await;
    });
}
