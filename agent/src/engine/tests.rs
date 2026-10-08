use super::*;
mod checkpoints;
mod receipts;
use futures_lite::future::{block_on, zip};
use receipts::fixture;

fn agent() -> Agent {
    Agent::new(
        |_: ModelRequest, _: Cancellation| -> ModelFuture {
            Box::pin(async { panic!("validation must not invoke the model") })
        },
        vec![],
    )
    .unwrap()
}

#[test]
fn invalid_config_admission_has_no_user_or_task_writes() {
    block_on(async {
        let (session, driver) = Session::new(MemoryStorage::new());
        let command = async {
            let conversation = session
                .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
                .await
                .unwrap()
                .value
                .id;
            let installation = agent();
            let result = session
                .commit(move |tx| {
                    installation.admit_input(
                        tx,
                        conversation,
                        "hello",
                        TurnConfig {
                            model: "fake".into(),
                            instructions: "".into(),
                            max_model_rounds: 0,
                        },
                        crate::SubmitOptions::default(),
                    )
                })
                .await;
            assert!(matches!(result, Err(SessionError::Invalid(_))));
            session
                .commit(move |tx| {
                    Box::pin(async move {
                        assert!(
                            tx.scan_entries(EntryQuery::new(conversation), 128, None)
                                .await?
                                .items
                                .is_empty()
                        );
                        assert!(
                            tx.scan_tasks(TaskQuery::default(), 128, None)
                                .await?
                                .items
                                .is_empty()
                        );
                        Ok(())
                    })
                })
                .await
                .unwrap();
            session.close().await.unwrap();
        };
        zip(command, driver).await;
    });
}

#[test]
fn initial_factories_reject_malformed_raw_task_input() {
    block_on(async {
        let (session, driver) = Session::new(MemoryStorage::new());
        let command = async {
            let conversation = session
                .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
                .await
                .unwrap()
                .value
                .id;
            for definition in agent().definitions() {
                let result = session
                    .commit(move |tx| {
                        Box::pin(async move {
                            tx.create_task(
                                definition,
                                json!({}),
                                TaskOptions {
                                    ownership: TaskOwnership::Conversation,
                                    conversation_id: Some(conversation),
                                    background: false,
                                },
                            )
                            .await
                        })
                    })
                    .await;
                assert!(matches!(result, Err(SessionError::Invalid(_))));
            }
            session.close().await.unwrap();
        };
        zip(command, driver).await;
    });
}

#[test]
fn response_validation_rejects_invalid_calls_before_children() {
    let call = ToolCall {
        id: "a".into(),
        name: "t".into(),
        arguments: json!({}),
    };
    assert!(
        validate_response(&ModelResponse {
            text: "".into(),
            tool_calls: vec![call.clone()],
            usage: None
        })
        .is_ok()
    );
    assert!(
        validate_response(&ModelResponse {
            text: "".into(),
            tool_calls: vec![call.clone(), call],
            usage: None
        })
        .is_err()
    );
    assert!(
        validate_response(&ModelResponse {
            text: "".into(),
            tool_calls: vec![ToolCall {
                id: "".into(),
                name: "t".into(),
                arguments: Value::Null
            }],
            usage: None
        })
        .is_err()
    );
}
