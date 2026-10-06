//! An explicitly driven parent/child workflow with a durable join.
//! Run with: cargo run -p publicworks-runtime --example task_tree
//!
//! The host polls both drivers. Only one handler is active at a time; this is
//! bounded foreground orchestration, not a global or concurrent scheduler.
use futures_lite::future::{block_on, zip};
use publicworks_runtime::*;
use serde_json::json;
use std::{collections::BTreeMap, rc::Rc};

fn handler_error(error: SessionError) -> TaskOutcomeError {
    TaskOutcomeError {
        message: error.to_string(),
        detail: None,
    }
}

fn worker() -> TaskDefinition {
    let mut phases: BTreeMap<String, PhaseHandler> = BTreeMap::new();
    phases.insert(
        "work".into(),
        Rc::new(|_, runtime| {
            Box::pin(async move {
                runtime
                    .commit(|tx, task| {
                        Box::pin(async move {
                            // Effects are attributed to this child, not its parent.
                            tx.append_entry(task.conversation_id, EntryDraft::new("child result"))
                                .await?;
                            Ok(Some(TaskUpdate::Complete(task.input)))
                        })
                    })
                    .await
                    .map_err(handler_error)?;
                Ok(())
            })
        }),
    );
    TaskDefinition::new("example.worker", 1, |_| Ok(json!({"phase":"work"})), phases)
}

fn parent(worker: TaskDefinition) -> TaskDefinition {
    let mut phases: BTreeMap<String, PhaseHandler> = BTreeMap::new();
    phases.insert(
        "spawn".into(),
        Rc::new(move |_, runtime| {
            let worker = worker.clone();
            Box::pin(async move {
                runtime
                    .commit(move |tx, task| {
                        Box::pin(async move {
                            let mut ids = Vec::new();
                            for result in [21, 42] {
                                let child = tx
                                    .create_task(
                                        worker.clone(),
                                        json!(result),
                                        TaskOptions {
                                            conversation_id: Some(task.conversation_id),
                                            ownership: TaskOwnership::Task(task.id),
                                            background: false,
                                        },
                                    )
                                    .await?;
                                ids.push(child.id);
                            }
                            // Child creation, IDs, and the next phase are one atomic commit.
                            // Both policies wait for ALL named tasks to become Terminal.
                            Ok(Some(TaskUpdate::Wait {
                                checkpoint: json!({"phase":"join", "children":ids}),
                                on: ids,
                                policy: JoinPolicy::AllSettled,
                            }))
                        })
                    })
                    .await
                    .map_err(handler_error)?;
                Ok(())
            })
        }),
    );
    phases.insert(
        "join".into(),
        Rc::new(|task, runtime| {
            Box::pin(async move {
                let TaskState::Running { checkpoint } = task.state else {
                    return Err(handler_error(SessionError::Invalid(
                        "Expected running parent".into(),
                    )));
                };
                let ids: Vec<Id> = serde_json::from_value(checkpoint["children"].clone())
                    .map_err(|error| handler_error(SessionError::Invalid(error.to_string())))?;
                runtime
                    .commit(move |tx, task| {
                        Box::pin(async move {
                            let mut outcomes = Vec::new();
                            // Read in checkpoint order. allSettled doesn't implicitly fail this
                            // parent or cancel siblings; the parent decides how to use outcomes.
                            for id in ids {
                                let child = tx.task(id).await?.ok_or_else(|| {
                                    SessionError::Invalid("Child disappeared".into())
                                })?;
                                let TaskState::Terminal { outcome } = child.state else {
                                    return Err(SessionError::Invalid(
                                        "Join target is not terminal".into(),
                                    ));
                                };
                                outcomes.push(outcome);
                            }
                            tx.append_entry(task.conversation_id, EntryDraft::new("parent joined"))
                                .await?;
                            Ok(Some(TaskUpdate::Complete(json!({"children":outcomes}))))
                        })
                    })
                    .await
                    .map_err(handler_error)?;
                Ok(())
            })
        }),
    );
    TaskDefinition::new(
        "example.parent",
        1,
        |_| Ok(json!({"phase":"spawn"})),
        phases,
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    block_on(async {
        let worker = worker();
        let parent = parent(worker.clone());
        let (session, session_driver) = Session::new(MemoryStorage::new());
        let (result, ()) = zip(
            async {
                let work = async {
                    let registry = TaskRegistry::new([parent.clone(), worker])?;
                    let (runner, task_driver) = TaskRunner::attach(&session, registry)?;
                    let (result, ()) = zip(
                        async {
                            let work = async {
                                let root = session
                                    .commit(move |tx| {
                                        Box::pin(async move {
                                            let conversation = tx.create_conversation().await?;
                                            tx.create_task(
                                                parent,
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
                                    .map_err(RunError::Session)?
                                    .value;
                                // Children are internal: run the conversation-owned root.
                                // A quiescent nonterminal tree returns Suspended(root).
                                runner.run(root.id).await
                            }
                            .await;
                            // Always close the runner first, even if creation/run failed.
                            // Its cooperative shutdown still needs both drivers polled.
                            let closed = runner.close().await;
                            let result = work?;
                            closed?;
                            Ok::<_, RunError>(result)
                        },
                        task_driver,
                    )
                    .await;
                    result
                }
                .await;
                let closed = session.close().await;
                let result = work?;
                closed.map_err(RunError::Session)?;
                println!("{result:#?}");
                Ok::<_, RunError>(())
            },
            session_driver,
        )
        .await;
        result.map_err(Into::into)
    })
}
