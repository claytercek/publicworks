//! Explicit foreground leaf execution. No scheduler or provider is involved.
//! Run with: cargo run -p publicworks-runtime --example task_execution
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
fn definition() -> TaskDefinition {
    let mut phases: BTreeMap<String, PhaseHandler> = BTreeMap::new();
    phases.insert(
        "write".into(),
        Rc::new(|_, runtime| {
            Box::pin(async move {
                runtime
                    .commit(|tx, task| {
                        Box::pin(async move {
                            // This entry and the checkpoint are one storage commit. Entries
                            // written here are automatically attributed to the running task.
                            tx.append_entry(
                                task.conversation_id,
                                EntryDraft::new("example effect"),
                            )
                            .await?;
                            Ok(Some(TaskUpdate::Checkpoint(
                                json!({"phase":"finish", "answer":42}),
                            )))
                        })
                    })
                    .await
                    .map_err(handler_error)?;
                Ok(())
            })
        }),
    );
    phases.insert(
        "finish".into(),
        Rc::new(|_, runtime| {
            Box::pin(async move {
                runtime
                    .commit(|_, _| {
                        Box::pin(async { Ok(Some(TaskUpdate::Complete(json!({"answer":42})))) })
                    })
                    .await
                    .map_err(handler_error)?;
                Ok(())
            })
        }),
    );
    TaskDefinition::new("example", 1, |_| Ok(json!({"phase":"write"})), phases)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    block_on(async {
        let definition = definition();
        let (session, session_driver) = Session::new(MemoryStorage::new());
        let (result, ()) = zip(
            async {
                let work = async {
                    let registry = TaskRegistry::new([definition.clone()])?;
                    let (runner, task_driver) = TaskRunner::attach(&session, registry)?;
                    let (result, ()) = zip(
                        async {
                            let result = async {
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
                                    .map_err(RunError::Session)?
                                    .value;
                                runner.run(task.id).await
                            }
                            .await;
                            // Cleanup runs on both success and command failure. Keep
                            // polling both drivers while closing the runner first.
                            let closed = runner.close().await;
                            let result = result?;
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
