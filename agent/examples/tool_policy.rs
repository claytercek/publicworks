//! Block tool calls with a host-owned policy using the agent extension API.
//! No network access or API key is required.
//!
//! Run with: `cargo run -p publicworks-agent --example tool_policy`
use futures_lite::future::{block_on, zip};
use publicworks_agent::*;
use publicworks_runtime::*;
use serde_json::json;
use std::rc::Rc;

// Copy and adapt this function in your host. It is not a security boundary:
// omitting/deselecting the extension leaves tools unguarded. Later hooks can
// rewrite allowed calls, so the host also owns hook ordering and installation.
fn host_policy() -> Extension {
    Extension::new("host-policy", vec![]).with_hooks(LifecycleHooks {
        before_tool: Some(Rc::new(|call, _| {
            Box::pin(async move {
                Ok(if call.name == "add" {
                    BeforeTool::Block("Calculator disabled by host policy".into())
                } else {
                    BeforeTool::Continue
                })
            })
        })),
        ..LifecycleHooks::default()
    })
}

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
                    text: format!("The answer is {result}."),
                    tool_calls: vec![],
                    usage: None,
                },
                None => ModelResponse {
                    text: "I'll calculate it.".into(),
                    tool_calls: vec![ToolCall {
                        id: "calculation-1".into(),
                        name: "add".into(),
                        arguments: json!({"left":20,"right":22}),
                    }],
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
        let mut registry = AgentRegistry::new();
        registry.install(Extension::new("calculator", vec![calculator()]))?;
        registry.install(host_policy())?;
        // Reinstall executable extensions on every host restart.
        let agent = Agent::with_registry(CalculatorModel, registry.snapshot(), None);
        let (opening, driver) = Harness::open(
            MemoryStorage::new(),
            TaskRegistry::new(agent.definitions())?,
        );
        let (result, ()) = zip(
            async {
                let harness = opening.await?;
                let work = async {
                    let conversation = harness
                        .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
                        .await?
                        .value
                        .id;
                    agent
                        .configure_extensions(
                            &harness,
                            conversation,
                            ExtensionConfig {
                                selection: ExtensionSelection::Exact(vec![
                                    "calculator".into(),
                                    "host-policy".into(),
                                ]),
                                ..ExtensionConfig::default()
                            },
                        )
                        .await?;
                    let submission = agent
                        .submit(
                            &harness,
                            conversation,
                            "What is 20 + 22?",
                            TurnConfig {
                                model: "local-calculator-demo".into(),
                                instructions: "Use the calculator, then answer briefly.".into(),
                                max_model_rounds: 3,
                            },
                            SubmitOptions::default(),
                        )
                        .await?;
                    let receipt = submission.wait().await?;
                    let mut transcript = harness
                        .commit(move |tx| {
                            Box::pin(async move {
                                Ok(tx
                                    .scan_entries(EntryQuery::new(conversation), 32, None)
                                    .await?
                                    .items)
                            })
                        })
                        .await?
                        .value;
                    assert!(transcript.iter().any(|entry| {
                        entry
                            .data
                            .as_ref()
                            .is_some_and(|data| data["code"] == "tool_blocked")
                    }));
                    transcript.reverse();
                    for entry in transcript {
                        for message in entry.model.into_iter().flatten() {
                            println!("{}: {:?}", entry.kind, decode_message(&message)?);
                        }
                    }
                    println!("Submission: {receipt:#?}");
                    Ok::<_, Box<dyn std::error::Error>>(())
                }
                .await;
                let closed = harness.close().await;
                work?;
                closed?;
                Ok::<_, Box<dyn std::error::Error>>(())
            },
            driver,
        )
        .await;
        result
    })
}

#[test]
fn selected_policy_blocks_tool_call() {
    main().unwrap();
}
