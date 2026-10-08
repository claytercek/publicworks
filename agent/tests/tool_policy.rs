use futures_lite::future::{block_on, zip};
use publicworks_agent::*;
use publicworks_runtime::{test_support::TempDatabase, *};
use publicworks_storage_sqlite::SqliteStorage;
use serde_json::json;
use std::{cell::Cell, rc::Rc};

struct Database(TempDatabase);
impl Database {
    fn new() -> Self {
        Self(TempDatabase::new("publicworks-permissions-", "db"))
    }
    fn open(&self) -> SqliteStorage {
        SqliteStorage::open(self.0.path()).unwrap()
    }
}

fn declaration() -> ToolDeclaration {
    ToolDeclaration {
        name: "dangerous".into(),
        version: 1,
        description: "test effect".into(),
        parameters: json!({}),
    }
}

async fn exercise(storage: impl Storage + 'static, decision: Option<BeforeTool>) {
    let executions = Rc::new(Cell::new(0));
    let tool = Tool::new(declaration(), |_| Ok(()), {
        let executions = executions.clone();
        move |_, _| {
            executions.set(executions.get() + 1);
            Box::pin(async {
                Ok(ToolResult {
                    content: "executed".into(),
                    is_error: false,
                    usage: None,
                })
            })
        }
    });
    let mut registry = AgentRegistry::new();
    registry
        .install(Extension::new("tools", vec![tool]))
        .unwrap();
    registry
        .install(
            Extension::new("host-policy", vec![]).with_hooks(LifecycleHooks {
                before_tool: Some(Rc::new({
                    let decision = decision.clone();
                    move |call: ToolCall, _| {
                        assert_eq!(call.name, "dangerous");
                        let decision = decision
                            .clone()
                            .expect("deselected host policy must not run");
                        Box::pin(async move { Ok(decision) })
                    }
                })),
                ..LifecycleHooks::default()
            }),
        )
        .unwrap();

    let round = Cell::new(0);
    let agent = Agent::with_registry(
        move |request: ModelRequest, _| {
            assert_eq!(request.tools, vec![declaration()]);
            let current = round.get();
            round.set(current + 1);
            Box::pin(async move {
                Ok(ModelResponse {
                    text: if current == 0 { "" } else { "done" }.into(),
                    tool_calls: if current == 0 {
                        vec![ToolCall {
                            id: "call-1".into(),
                            name: "dangerous".into(),
                            arguments: json!({}),
                        }]
                    } else {
                        vec![]
                    },
                    usage: None,
                })
            }) as ModelFuture
        },
        registry.snapshot(),
        None,
    );
    let (opening, driver) = Harness::open(storage, TaskRegistry::new(agent.definitions()).unwrap());
    zip(
        async {
            let harness = opening.await.unwrap();
            let conversation = harness
                .commit(|tx| Box::pin(async move { Ok(tx.create_conversation().await?.id) }))
                .await
                .unwrap()
                .value;
            let mut selected = vec!["tools".into()];
            if decision.is_some() {
                selected.push("host-policy".into());
            }
            agent
                .configure_extensions(
                    &harness,
                    conversation,
                    ExtensionConfig {
                        selection: ExtensionSelection::Exact(selected),
                        ..ExtensionConfig::default()
                    },
                )
                .await
                .unwrap();
            agent
                .submit(
                    &harness,
                    conversation,
                    "test",
                    TurnConfig {
                        model: "fake".into(),
                        instructions: "".into(),
                        max_model_rounds: 3,
                    },
                    SubmitOptions::default(),
                )
                .await
                .unwrap()
                .wait()
                .await
                .unwrap();
            let entries = harness
                .commit(move |tx| {
                    Box::pin(async move {
                        Ok(tx
                            .scan_entries(EntryQuery::new(conversation), 64, None)
                            .await?
                            .items)
                    })
                })
                .await
                .unwrap()
                .value;
            let result = entries
                .iter()
                .find(|entry| entry.kind == "agent.toolResult")
                .unwrap();
            let ModelMessage::ToolResult {
                content, is_error, ..
            } = decode_message(&result.model.as_ref().unwrap()[0]).unwrap()
            else {
                panic!("expected tool result")
            };
            match decision {
                Some(BeforeTool::Block(_)) => {
                    assert_eq!(executions.get(), 0);
                    assert!(is_error);
                    assert_eq!(content, "Blocked by host policy");
                    assert_eq!(result.data.as_ref().unwrap()["code"], "tool_blocked");
                }
                Some(BeforeTool::Rewrite(_)) => panic!("this policy does not rewrite arguments"),
                Some(BeforeTool::Continue) | None => {
                    assert_eq!(executions.get(), 1);
                    assert!(!is_error);
                    assert_eq!(content, "executed");
                }
            }
            harness.close().await.unwrap();
        },
        driver,
    )
    .await;
}

#[test]
fn memory_policy_allows_denies_and_only_runs_when_selected() {
    for decision in [
        Some(BeforeTool::Block("Blocked by host policy".into())),
        Some(BeforeTool::Continue),
        None,
    ] {
        block_on(exercise(MemoryStorage::new(), decision));
    }
}

#[test]
fn sqlite_policy_allows_denies_and_only_runs_when_selected() {
    for decision in [
        Some(BeforeTool::Block("Blocked by host policy".into())),
        Some(BeforeTool::Continue),
        None,
    ] {
        let db = Database::new();
        block_on(exercise(db.open(), decision));
    }
}
