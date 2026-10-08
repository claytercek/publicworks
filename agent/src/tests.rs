use super::*;
use serde_json::json;

#[test]
fn message_wire_preserves_opaque_arguments_without_serde_content_buffers() {
    let arguments = json!({
        "signed": i64::MIN, "unsigned": u64::MAX, "fraction": 0.12345678901234568,
        "$serde_json::private::Number": "not a numeric envelope", "nested": {"role":"future"}
    });
    let message = ModelMessage::Assistant {
        text: "".into(),
        tool_calls: vec![ToolCall {
            id: "call-1".into(),
            name: "lookup".into(),
            arguments,
        }],
    };
    assert_eq!(decode_message(&encode_message(&message)).unwrap(), message);
}

#[test]
fn rejects_unsupported_and_malformed_messages() {
    for value in [
        json!({"role":"system","text":"no"}),
        json!({"role":"user","text":"ok","image":"no"}),
        json!({"role":"assistant","text":"ok","toolCalls":[],"reasoning":"no"}),
        json!({"role":"assistant","text":"ok","toolCalls":[{"id":"","name":"x","arguments":{}}]}),
        json!({"role":"assistant","text":"ok","toolCalls":[{"id":"same","name":"x","arguments":{}},{"id":"same","name":"x","arguments":{}}]}),
        json!({"role":"toolResult","callId":"x","content":"ok","isError":"false"}),
    ] {
        assert!(decode_message(&value).is_err(), "accepted {value}");
    }
}

#[test]
fn model_bearing_payloads_enforce_native_numbers_and_depth() {
    let mut nested = Value::Null;
    for _ in 0..publicworks_runtime::MAX_JSON_DEPTH {
        nested = json!([nested]);
    }
    let message = ModelMessage::Assistant {
        text: "".into(),
        tool_calls: vec![ToolCall {
            id: "x".into(),
            name: "x".into(),
            arguments: nested,
        }],
    };
    assert!(decode_message(&encode_message(&message)).is_err());
    // Finite values still use the runtime's native f64 contract when features unify.
    let value: Value = serde_json::from_str(
        r#"{"role":"assistant","text":"","toolCalls":[{"id":"x","name":"x","arguments":1.25}]}"#,
    )
    .unwrap();
    assert!(decode_message(&value).is_ok());
}

#[test]
fn installation_rejects_duplicate_names_even_for_distinct_versions() {
    let tool = |version| {
        Tool::new(
            ToolDeclaration {
                name: "same".into(),
                version,
                description: "".into(),
                parameters: json!({}),
            },
            |_| Ok(()),
            |_, _| Box::pin(async { unreachable!() }),
        )
    };
    let model =
        |_: ModelRequest, _: Cancellation| -> ModelFuture { Box::pin(async { unreachable!() }) };
    assert!(Agent::new(model, vec![tool(1), tool(2)]).is_err());
}

#[test]
fn model_failure_preserves_valid_usage_independently_of_partial_payloads() {
    use futures_lite::future::{block_on, zip};
    use publicworks_runtime::{
        EntryQuery, MAX_JSON_DEPTH, MemoryStorage, RunResult, Session, TaskOutcome, TaskRegistry,
        TaskRunner, TaskState,
    };

    fn nested(depth: usize) -> Value {
        (0..depth).fold(Value::Null, |value, _| json!([value]))
    }
    fn partial(arguments: Value, usage: Option<Value>) -> ModelResponse {
        ModelResponse {
            text: "partial".into(),
            tool_calls: vec![ToolCall {
                id: "call".into(),
                name: "tool".into(),
                arguments,
            }],
            usage,
        }
    }
    fn diagnostic(error: ModelError) -> Value {
        block_on(async {
            let agent = Agent::new(
                move |_: ModelRequest, _: Cancellation| -> ModelFuture {
                    let error = error.clone();
                    Box::pin(async move { Err(error) })
                },
                vec![],
            )
            .unwrap();
            let (session, driver) = Session::new(MemoryStorage::new());
            let (runner, task_driver) =
                TaskRunner::attach(&session, TaskRegistry::new(agent.definitions()).unwrap())
                    .unwrap();
            let command = async {
                let conversation = session
                    .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
                    .await
                    .unwrap()
                    .value
                    .id;
                let _submission = session
                    .commit(move |tx| {
                        agent.admit_input(
                            tx,
                            conversation,
                            "hello",
                            TurnConfig {
                                model: "fake".into(),
                                instructions: "".into(),
                                max_model_rounds: 1,
                            },
                            crate::SubmitOptions::default(),
                        )
                    })
                    .await
                    .unwrap()
                    .value;
                let task_id = session
                    .commit(move |tx| {
                        Box::pin(async move {
                            Ok(tx
                                .conversation_state(conversation)
                                .await?
                                .unwrap()
                                .run
                                .unwrap()
                                .task_id)
                        })
                    })
                    .await
                    .unwrap()
                    .value;
                let RunResult::Terminal(task) = runner.run(task_id).await.unwrap() else {
                    panic!("turn did not settle");
                };
                assert!(matches!(
                    task.state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Failed { .. }
                    }
                ));
                let data = session
                    .commit(move |tx| {
                        Box::pin(async move {
                            Ok(tx
                                .scan_entries(EntryQuery::new(conversation), 128, None)
                                .await?
                                .items
                                .into_iter()
                                .find(|entry| entry.kind == "agent.diagnostic")
                                .unwrap()
                                .data
                                .unwrap())
                        })
                    })
                    .await
                    .unwrap()
                    .value;
                runner.close().await.unwrap();
                session.close().await.unwrap();
                data
            };
            zip(command, zip(driver, task_driver)).await.0
        })
    }

    for (explicit, partial_usage, expected) in [
        (
            Some(json!({"billed":17})),
            Some(json!({"billed":9})),
            Some(json!({"billed":17})),
        ),
        (None, Some(json!({"billed":9})), Some(json!({"billed":9}))),
        (
            Some(Value::Null),
            Some(json!({"billed":9})),
            Some(Value::Null),
        ),
        (None, Some(Value::Null), Some(Value::Null)),
        (None, None, None),
    ] {
        let data = diagnostic(ModelError {
            message: "provider failed".into(),
            usage: explicit,
            partial_response: Some(partial(nested(MAX_JSON_DEPTH), partial_usage)),
        });
        assert_eq!(data.get("usage"), expected.as_ref());
        assert!(data.get("partialResponse").is_none());
        assert!(data.get("partialResponseOmitted").is_some());
    }

    // An invalid usage value must not discard otherwise valid partial output.
    let data = diagnostic(ModelError {
        message: "provider failed".into(),
        usage: Some(nested(MAX_JSON_DEPTH)),
        partial_response: Some(partial(json!({}), Some(nested(MAX_JSON_DEPTH)))),
    });
    assert!(data.get("usage").is_none());
    assert!(data.get("usageOmitted").is_some());
    assert!(data["partialResponse"].get("message").is_some());
    assert!(data["partialResponse"].get("usage").is_none());
    assert!(data["partialResponse"].get("usageOmitted").is_some());

    // This value fits top-level diagnostic usage but not the deeper partial envelope.
    let usage = nested(MAX_JSON_DEPTH - 1);
    let data = diagnostic(ModelError {
        message: "provider failed".into(),
        usage: None,
        partial_response: Some(partial(json!({}), Some(usage.clone()))),
    });
    assert_eq!(data.get("usage"), Some(&usage));
    assert!(data["partialResponse"].get("message").is_some());
    assert!(data["partialResponse"].get("usage").is_none());
    assert!(data["partialResponse"].get("usageOmitted").is_some());
}
