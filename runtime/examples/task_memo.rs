//! Durable first-writer-wins task data using only the in-memory storage adapter.
//! Run with: cargo run -p publicworks-runtime --example task_memo
use futures_lite::future::{block_on, zip};
use publicworks_runtime::*;
use serde_json::{Value, json};
use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

fn handler_error(error: SessionError) -> TaskOutcomeError {
    TaskOutcomeError {
        message: error.to_string(),
        detail: None,
    }
}

fn definition(saved: Rc<RefCell<Option<Value>>>) -> TaskDefinition {
    let mut phases: BTreeMap<String, PhaseHandler> = BTreeMap::new();
    phases.insert(
        "choose".into(),
        Rc::new(|_, runtime| {
            Box::pin(async move {
                let winner = runtime
                    .memo_or_insert("stable-value", json!({"chosen":42}))
                    .await
                    .map_err(handler_error)?
                    .value;
                // Retrying with another candidate returns the durable first value.
                assert_eq!(
                    runtime
                        .memo_or_insert("stable-value", json!({"chosen":99}))
                        .await
                        .map_err(handler_error)?
                        .value,
                    winner
                );
                runtime
                    .commit(|_, _| {
                        Box::pin(async {
                            Ok(Some(TaskUpdate::Checkpoint(json!({"phase":"finish"}))))
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
        Rc::new(move |_, runtime| {
            let saved = saved.clone();
            Box::pin(async move {
                let winner = runtime
                    .memo("stable-value")
                    .await
                    .map_err(handler_error)?
                    .value
                    .expect("the first phase stored a value");
                *saved.borrow_mut() = Some(winner.clone());
                runtime
                    .commit(move |_, _| {
                        Box::pin(async move { Ok(Some(TaskUpdate::Complete(winner))) })
                    })
                    .await
                    .map_err(handler_error)?;
                Ok(())
            })
        }),
    );
    TaskDefinition::new("memo-example", 1, |_| Ok(json!({"phase":"choose"})), phases)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    block_on(async {
        let saved = Rc::new(RefCell::new(None));
        let definition = definition(saved.clone());
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
                                                Value::Null,
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
                println!(
                    "saved winner: {}",
                    saved.borrow().as_ref().expect("finish phase ran")
                );
                println!("{result:#?}");
                Ok::<_, RunError>(())
            },
            session_driver,
        )
        .await;
        result.map_err(Into::into)
    })
}
