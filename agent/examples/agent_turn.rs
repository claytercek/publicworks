//! A complete durable agent turn with a deterministic local model and calculator.
//! No network access or API key is required.
//!
//! Run with: `cargo run -p publicworks-agent --example agent_turn`
use futures_lite::future::{block_on, zip};
use publicworks_agent::*;
use publicworks_runtime::*;
use serde_json::json;

#[derive(Clone)]
struct CalculatorModel;
impl Model for CalculatorModel {
    fn complete(&self, request: ModelRequest, _: Cancellation) -> ModelFuture {
        Box::pin(async move {
            let result = request.messages.iter().find_map(|message| match message {
                ModelMessage::ToolResult { content, .. } => Some(content.clone()),
                _ => None,
            });
            Ok(match result {
                Some(result) => ModelResponse {
                    message: ModelMessage::Assistant {
                        text: format!("The answer is {result}."),
                        tool_calls: vec![],
                    },
                    finish_reason: FinishReason::Stop,
                    usage: None,
                },
                None => ModelResponse {
                    message: ModelMessage::Assistant {
                        text: "I'll calculate it.".into(),
                        tool_calls: vec![ToolCall {
                            id: "calculation-1".into(),
                            name: "add".into(),
                            arguments: json!({"left":20,"right":22}),
                        }],
                    },
                    finish_reason: FinishReason::ToolCalls,
                    usage: None,
                },
            })
        })
    }
}

fn calculator() -> Tool {
    Tool::new(
        ToolDeclaration {
            name: "add".into(),
            version: 1,
            description: "Add two signed integers.".into(),
            parameters: json!({
                "type":"object",
                "properties":{
                    "left":{"type":"integer"},
                    "right":{"type":"integer"}
                },
                "required":["left","right"],
                "additionalProperties":false
            }),
        },
        |arguments| {
            arguments
                .get("left")
                .and_then(|value| value.as_i64())
                .ok_or("left must be an integer")?;
            arguments
                .get("right")
                .and_then(|value| value.as_i64())
                .ok_or("right must be an integer")?;
            Ok(())
        },
        |call, _| {
            Box::pin(async move {
                let left = call.arguments["left"].as_i64().expect("validated left");
                let right = call.arguments["right"].as_i64().expect("validated right");
                let sum = left.checked_add(right).ok_or_else(|| ToolError {
                    message: "integer overflow".into(),
                    usage: None,
                })?;
                Ok(ToolResult {
                    content: sum.to_string(),
                    is_error: false,
                    usage: None,
                })
            })
        },
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    block_on(async {
        let agent = Agent::new(CalculatorModel, vec![calculator()])?;
        let (session, session_driver) = Session::new(MemoryStorage::new());
        let (result, ()) = zip(
            async {
                let work = async {
                    let registry = TaskRegistry::new(agent.definitions())?;
                    let (runner, task_driver) = TaskRunner::attach(&session, registry)?;
                    let (result, ()) = zip(
                        async {
                            let result = async {
                                let conversation = session
                                    .commit(|tx| {
                                        Box::pin(async move { tx.create_conversation().await })
                                    })
                                    .await
                                    .map_err(RunError::Session)?
                                    .value;
                                let admitted_agent = agent.clone();
                                let handle = session
                                    .commit(move |tx| {
                                        admitted_agent.admit_turn(
                                            tx,
                                            conversation.id,
                                            "What is 20 + 22?",
                                            TurnConfig {
                                                model: "local-calculator-demo".into(),
                                                instructions:
                                                    "Use the calculator, then answer briefly."
                                                        .into(),
                                                max_model_rounds: 3,
                                            },
                                        )
                                    })
                                    .await
                                    .map_err(RunError::Session)?
                                    .value;
                                let outcome = runner.run(handle.task_id).await?;
                                let mut transcript = session
                                    .commit(move |tx| {
                                        Box::pin(async move {
                                            Ok(tx
                                                .scan_entries(
                                                    EntryQuery::new(conversation.id),
                                                    32,
                                                    None,
                                                )
                                                .await?
                                                .items)
                                        })
                                    })
                                    .await
                                    .map_err(RunError::Session)?
                                    .value;
                                transcript.reverse();
                                for entry in transcript {
                                    if let Some(messages) = entry.model {
                                        for message in messages {
                                            println!(
                                                "{}: {:?}",
                                                entry.kind,
                                                decode_message(&message)?
                                            );
                                        }
                                    }
                                }
                                Ok::<_, RunError>(outcome)
                            }
                            .await;
                            // Always close the runner while its driver is still polled.
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
                // Session cleanup also runs when setup or execution returns an error.
                let closed = session.close().await;
                let result = work?;
                closed.map_err(RunError::Session)?;
                Ok::<_, RunError>(result)
            },
            session_driver,
        )
        .await;
        println!("Turn outcome: {:#?}", result?);
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
