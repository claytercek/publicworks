//! Runtime-owned transition tests still go through the Session transaction seam.
use super::*;
use futures_lite::future::{block_on, zip};
use serde_json::json;

fn initial() -> TaskDefinition {
    TaskDefinition::new(
        "test",
        9,
        |input| Ok(json!({"input":input})),
        Default::default(),
    )
}
fn options(conversation: Id) -> TaskOptions {
    TaskOptions {
        ownership: TaskOwnership::Conversation,
        conversation_id: Some(conversation),
        background: false,
    }
}
async fn seed(session: &Session) -> TaskRecord {
    session
        .commit(|tx| {
            Box::pin(async move {
                let c = tx.create_conversation().await?;
                tx.create_task(initial(), json!(null), options(c.id)).await
            })
        })
        .await
        .unwrap()
        .value
}
fn finished() -> TaskState {
    TaskState::Terminal {
        outcome: TaskOutcome::Completed {
            result: json!(null),
        },
    }
}
#[test]
fn owners_checked_against_final_candidates_in_both_orders() {
    block_on(async {
        let (session, driver) = Session::new(MemoryStorage::new());
        zip(
            async {
                let parent = seed(&session).await;
                for conversation in [false, true] {
                    for owner_first in [false, true] {
                        for status in [
                            TaskStatus::Completing,
                            TaskStatus::Terminal,
                            TaskStatus::Pending,
                        ] {
                            let mut parent = parent.clone();
                            let result = session
                                .commit(move |tx| {
                                    Box::pin(async move {
                                        parent.state = match status {
                                            TaskStatus::Completing => TaskState::Completing {
                                                outcome: TaskOutcome::Completed {
                                                    result: json!(null),
                                                },
                                            },
                                            TaskStatus::Terminal => finished(),
                                            _ => {
                                                parent.abort_requested = true;
                                                parent.state.clone()
                                            }
                                        };
                                        if owner_first {
                                            tx.set_task(parent.clone()).await?;
                                        }
                                        if conversation {
                                            tx.create_conversation_owned(Owner::Task(parent.id))
                                                .await?;
                                        } else {
                                            tx.create_task(
                                                initial(),
                                                json!(null),
                                                TaskOptions {
                                                    ownership: TaskOwnership::Task(parent.id),
                                                    conversation_id: None,
                                                    background: false,
                                                },
                                            )
                                            .await?;
                                        }
                                        if !owner_first {
                                            tx.set_task(parent).await?;
                                        }
                                        Ok(())
                                    })
                                })
                                .await;
                            assert!(matches!(result, Err(SessionError::Invalid(_))));
                        }
                    }
                }
                let id = parent.id;
                let read = session
                    .commit(move |tx| {
                        Box::pin(async move {
                            assert_eq!(
                                tx.scan_tasks(TaskQuery::default(), 100, None)
                                    .await?
                                    .items
                                    .len(),
                                1
                            );
                            assert_eq!(
                                tx.scan_conversations(ConversationQuery::default(), 100, None)
                                    .await?
                                    .items
                                    .len(),
                                1
                            );
                            tx.task(id).await
                        })
                    })
                    .await
                    .unwrap();
                assert_eq!(read.value, Some(parent));
                assert!(read.seq.is_none());
                session.close().await.unwrap();
            },
            driver,
        )
        .await;
    });
}
#[test]
fn replacement_guards_coalescing_and_created_class_survive() {
    block_on(async {
        let (session, driver) = Session::new(MemoryStorage::new());
        zip(
            async {
                let parent = seed(&session).await;
                // Newly created owner remains a creation after replacement; assembly must
                // still check its owner and must not require a committed row for itself.
                let id = parent.id;
                let child = session
                    .commit(move |tx| {
                        Box::pin(async move {
                            let mut c = tx
                                .create_task(
                                    initial(),
                                    json!(null),
                                    TaskOptions {
                                        ownership: TaskOwnership::Task(id),
                                        conversation_id: None,
                                        background: false,
                                    },
                                )
                                .await?;
                            c.state = TaskState::Running {
                                checkpoint: json!(1),
                            };
                            tx.set_task(c.clone()).await?;
                            c.state = TaskState::Pending {
                                checkpoint: json!(2),
                            };
                            tx.set_task(c.clone()).await?;
                            Ok(c)
                        })
                    })
                    .await
                    .unwrap()
                    .value;
                let original = child.clone();
                for staged in [false, true] {
                    let mut child = child.clone();
                    let result = session
                        .commit(move |tx| {
                            Box::pin(async move {
                                let other = tx.create_conversation().await?;
                                if staged {
                                    tx.set_task(child.clone()).await?;
                                }
                                child.conversation_id = other.id;
                                tx.set_task(child).await
                            })
                        })
                        .await;
                    assert!(matches!(result, Err(SessionError::Invalid(_))));
                }
                let mut terminal = child.clone();
                terminal.state = finished();
                let terminal_copy = terminal.clone();
                // Terminal staged candidates cannot be overwritten even in the same callback.
                let result = session
                    .commit(move |tx| {
                        Box::pin(async move {
                            tx.set_task(terminal_copy).await?;
                            tx.set_task(original).await
                        })
                    })
                    .await;
                assert!(matches!(result, Err(SessionError::Invalid(_))));
                let terminal_copy = terminal.clone();
                session
                    .commit(move |tx| Box::pin(async move { tx.set_task(terminal_copy).await }))
                    .await
                    .unwrap();
                let result = session
                    .commit(move |tx| Box::pin(async move { tx.set_task(child).await }))
                    .await;
                assert!(matches!(result, Err(SessionError::Invalid(_))));
                let id = terminal.id;
                assert_eq!(
                    session
                        .commit(move |tx| Box::pin(async move { tx.task(id).await }))
                        .await
                        .unwrap()
                        .value,
                    Some(terminal)
                );
                // No upsert through the private Session replacement seam.
                let mut missing = parent;
                missing.id = Id::new(999).unwrap();
                assert!(
                    session
                        .commit(move |tx| Box::pin(async move { tx.set_task(missing).await }))
                        .await
                        .is_err()
                );
                session.close().await.unwrap();
            },
            driver,
        )
        .await;
    });
}
#[test]
fn replaced_creation_still_validates_its_owner() {
    block_on(async {
        let (session, driver) = Session::new(MemoryStorage::new());
        zip(
            async {
                let mut parent = seed(&session).await;
                let result = session
                    .commit(move |tx| {
                        Box::pin(async move {
                            let mut child = tx
                                .create_task(
                                    initial(),
                                    json!(null),
                                    TaskOptions {
                                        ownership: TaskOwnership::Task(parent.id),
                                        conversation_id: None,
                                        background: false,
                                    },
                                )
                                .await?;
                            child.state = TaskState::Running {
                                checkpoint: json!(null),
                            };
                            tx.set_task(child).await?;
                            parent.abort_requested = true;
                            tx.set_task(parent).await
                        })
                    })
                    .await;
                assert!(matches!(result, Err(SessionError::Invalid(_))));
                session.close().await.unwrap();
            },
            driver,
        )
        .await;
    });
}
