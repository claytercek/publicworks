use super::*;
use std::cell::Cell;

pub(super) type Trace = Rc<RefCell<Vec<String>>>;
pub(super) fn log(trace: &Trace, label: impl Into<String>) {
    trace.borrow_mut().push(label.into());
}
pub(super) fn output(content: &str) -> ToolResult {
    ToolResult {
        content: content.into(),
        is_error: false,
        usage: Some(json!({"n":u64::MAX})),
    }
}
pub(super) fn model() -> impl Model {
    let round = Cell::new(0);
    move |_: ModelRequest, _: Cancellation| -> ModelFuture {
        let n = round.get();
        round.set(n + 1);
        Box::pin(async move { Ok(response(if n == 0 { &["effect"] } else { &[] })) })
    }
}
pub(super) async fn entries(h: &Harness, c: Id) -> Vec<EntryRecord> {
    h.commit(move |tx| {
        Box::pin(async move { Ok(tx.scan_entries(EntryQuery::new(c), 128, None).await?.items) })
    })
    .await
    .unwrap()
    .value
}
fn recording_hooks(name: &'static str, trace: Trace) -> LifecycleHooks {
    LifecycleHooks {
        before_request: Some(Rc::new({
            let trace = trace.clone();
            move |mut request, ctx| {
                log(&trace, format!("request-{name}:{}", request.instructions));
                Box::pin(async move {
                    assert_eq!(ctx.extension_name(), name);
                    let winner = json!({"name":name,"number":u64::MAX});
                    // Even an invalid later candidate must return the durable
                    // winner instead of being validated or overwriting it.
                    let loser = (0..MAX_JSON_DEPTH).fold(Value::Null, |v, _| json!([v]));
                    let (first, second) = zip(
                        ctx.memo_or_insert("winner", winner.clone()),
                        ctx.memo_or_insert("winner", loser),
                    )
                    .await;
                    assert_eq!(first.unwrap(), winner);
                    assert_eq!(second.unwrap(), winner);
                    assert_eq!(ctx.memo("winner").await.unwrap(), Some(winner));
                    request.instructions.push_str(name);
                    Ok(Some(request))
                })
            }
        })),
        after_response: Some(Rc::new({
            let trace = trace.clone();
            move |_, ctx| {
                log(&trace, format!("response-{name}"));
                Box::pin(async move {
                    assert_eq!(ctx.memo("winner").await.unwrap().unwrap()["name"], name);
                    Ok(())
                })
            }
        })),
        before_tool: Some(Rc::new({
            let trace = trace.clone();
            move |call, ctx| {
                log(&trace, format!("before-{name}:{}", call.arguments));
                Box::pin(async move {
                    // Tool and turn have distinct memo namespaces.
                    assert_eq!(ctx.memo("winner").await.unwrap(), None);
                    let mut args = call.arguments;
                    args[name] = json!(true);
                    Ok(BeforeTool::Rewrite(args))
                })
            }
        })),
        after_tool: Some(Rc::new({
            let trace = trace.clone();
            move |mut outcome, _| {
                log(&trace, format!("after-{name}:{}", outcome.result.content));
                assert_eq!(outcome.call.arguments, json!({"a":true,"b":true}));
                outcome.result.content.push_str(name);
                Box::pin(async move { Ok(Some(outcome.result)) })
            }
        })),
        after_tools: Some(Rc::new(move |outcomes, _| {
            log(&trace, format!("batch-{name}"));
            assert_eq!(outcomes.len(), 1);
            assert_eq!(outcomes[0].call.arguments, json!({})); // model's call
            assert_eq!(outcomes[0].result, output("effectba"));
            Box::pin(async { Ok(()) })
        })),
    }
}
async fn compositions(storage: impl Storage + 'static) {
    let trace = Trace::default();
    let tool = Tool::new(
        declaration("effect", 1),
        {
            let trace = trace.clone();
            move |args| {
                log(&trace, format!("validate:{args}"));
                Ok(())
            }
        },
        {
            let trace = trace.clone();
            move |call, _| {
                assert_eq!(call.arguments, json!({"a":true,"b":true}));
                log(&trace, "effect");
                Box::pin(async { Ok(output("effect")) })
            }
        },
    );
    let mut registry = AgentRegistry::new();
    registry
        .install(Extension::new("a", vec![tool]).with_hooks(recording_hooks("a", trace.clone())))
        .unwrap();
    registry
        .install(Extension::new("b", vec![]).with_hooks(recording_hooks("b", trace.clone())))
        .unwrap();
    // Ordinary errors in chains/observers retain the last winner and continue.
    registry
        .install(Extension::new("errors", vec![]).with_hooks(LifecycleHooks {
            before_request: Some(Rc::new(|_, _| {
                Box::pin(async { Err(HookError::Failed("request error".into())) })
            })),
            after_response: Some(Rc::new(|_, _| {
                Box::pin(async { Err(HookError::Failed("response error".into())) })
            })),
            after_tool: Some(Rc::new(|_, _| {
                Box::pin(async { Err(HookError::Failed("tool error".into())) })
            })),
            after_tools: Some(Rc::new(|_, _| {
                Box::pin(async { Err(HookError::Failed("batch error".into())) })
            })),
            ..LifecycleHooks::default()
        }))
        .unwrap();
    let a = Agent::with_registry(
        {
            let model = model();
            move |request: ModelRequest, cancel: Cancellation| {
                assert_eq!(request.instructions, "ba");
                model.complete(request, cancel)
            }
        },
        registry.snapshot(),
        None,
    );
    let (open, driver) = Harness::open(storage, TaskRegistry::new(a.definitions()).unwrap());
    zip(
        async {
            let h = open.await.unwrap();
            let c = conversation(&h).await;
            a.configure_extensions(
                &h,
                c,
                settings(ExtensionSelection::Exact(vec![
                    "missing".into(),
                    "b".into(),
                    "errors".into(),
                    "a".into(),
                    "b".into(),
                ])),
            )
            .await
            .unwrap();
            a.submit(&h, c, "test", turn_config(), SubmitOptions::default())
                .await
                .unwrap()
                .wait()
                .await
                .unwrap();
            assert_eq!(
                *trace.borrow(),
                [
                    "request-b:",
                    "request-a:b",
                    "response-b",
                    "response-a",
                    "validate:{}",
                    "before-b:{}",
                    "before-a:{\"b\":true}",
                    "validate:{\"a\":true,\"b\":true}",
                    "effect",
                    "after-b:effect",
                    "after-a:effectb",
                    "batch-b",
                    "batch-a",
                    "request-b:",
                    "request-a:b",
                    "response-b",
                    "response-a",
                ]
            );
            let entries = entries(&h, c).await;
            let errors: Vec<_> = entries
                .iter()
                .filter(|e| e.kind == "agent.hookError")
                .collect();
            assert_eq!(errors.len(), 6);
            assert!(errors.iter().all(
                |e| e.by_task_id.is_some() && e.data.as_ref().unwrap()["extension"] == "errors"
            ));
            h.close().await.unwrap();
        },
        driver,
    )
    .await;
}
#[test]
fn memory_compositions_order_errors_and_memos() {
    block_on(bounded(compositions(MemoryStorage::new())));
}
#[test]
fn sqlite_compositions_order_errors_and_memos() {
    let db = Database::new();
    block_on(bounded(compositions(db.open())));
}

async fn rejects(storage: impl Storage + 'static, mode: &'static str) {
    let trace = Trace::default();
    let validated = Rc::new(Cell::new(0));
    let mut registry = AgentRegistry::new();
    registry
        .install(
            Extension::new(
                "first",
                vec![Tool::new(
                    declaration("effect", 1),
                    {
                        let validated = validated.clone();
                        move |args| {
                            validated.set(validated.get() + 1);
                            if args.is_object() {
                                Ok(())
                            } else {
                                Err("object required".into())
                            }
                        }
                    },
                    |_, _| panic!("rejected calls must not execute"),
                )],
            )
            .with_hooks(LifecycleHooks {
                before_tool: Some(Rc::new(move |_, _| {
                    Box::pin(async move {
                        match mode {
                            "block" => Ok(BeforeTool::Block("first block".into())),
                            "error" => Err(HookError::Failed("ordinary failure".into())),
                            "invalid" => Ok(BeforeTool::Rewrite(json!(42))),
                            "deep" => Ok(BeforeTool::Rewrite(
                                (0..MAX_JSON_DEPTH).fold(Value::Null, |v, _| json!([v])),
                            )),
                            _ => Err(HookError::Cancelled),
                        }
                    })
                })),
                after_tool: Some(Rc::new(|_, _| panic!("no afterTool without execution"))),
                ..LifecycleHooks::default()
            }),
        )
        .unwrap();
    registry
        .install(Extension::new("last", vec![]).with_hooks(LifecycleHooks {
            before_tool: Some(Rc::new({
                let trace = trace.clone();
                move |_, _| {
                    log(&trace, "last");
                    Box::pin(async { Ok(BeforeTool::Continue) })
                }
            })),
            ..LifecycleHooks::default()
        }))
        .unwrap();
    let a = Agent::with_registry(model(), registry.snapshot(), None);
    let (open, driver) = Harness::open(storage, TaskRegistry::new(a.definitions()).unwrap());
    zip(
        async {
            let h = open.await.unwrap();
            let c = conversation(&h).await;
            a.submit(&h, c, "test", turn_config(), SubmitOptions::default())
                .await
                .unwrap()
                .wait()
                .await
                .unwrap();
            let entries = entries(&h, c).await;
            let errors = entries
                .iter()
                .filter(|e| e.kind == "agent.hookError")
                .count();
            assert_eq!(errors, usize::from(mode == "error"));
            assert_eq!(validated.get(), if mode == "invalid" { 2 } else { 1 });
            assert_eq!(
                trace.borrow().len(),
                usize::from(mode == "invalid" || mode == "deep")
            );
            let result = entries
                .iter()
                .find(|e| e.kind == "agent.toolResult")
                .unwrap();
            let code = result.data.as_ref().unwrap()["code"].as_str().unwrap();
            if mode == "block" || mode == "error" {
                let message = decode_message(&result.model.as_ref().unwrap()[0]).unwrap();
                let ModelMessage::ToolResult { content, .. } = message else {
                    panic!("expected result")
                };
                assert_eq!(
                    content,
                    if mode == "block" {
                        "first block"
                    } else {
                        "ordinary failure"
                    }
                );
            }
            assert_eq!(
                code,
                match mode {
                    "block" | "error" => "tool_blocked",
                    "cancel" => "missing_result",
                    _ => "invalid_tool_call",
                }
            );
            h.close().await.unwrap();
        },
        driver,
    )
    .await;
}
#[test]
fn memory_before_tool_first_block_errors_and_revalidation() {
    for mode in ["block", "error", "invalid", "deep", "cancel"] {
        block_on(bounded(rejects(MemoryStorage::new(), mode)));
    }
}
#[test]
fn sqlite_before_tool_first_block_errors_and_revalidation() {
    for mode in ["block", "error", "invalid", "deep", "cancel"] {
        let db = Database::new();
        block_on(bounded(rejects(db.open(), mode)));
    }
}

async fn batch_and_offers(storage: impl Storage + 'static, remove_offer: bool) {
    let effects = Rc::new(Cell::new(0));
    let batches = Rc::new(Cell::new(0));
    let after = Rc::new(Cell::new(0));
    let mut registry = AgentRegistry::new();
    registry
        .install(
            Extension::new(
                "local",
                vec![Tool::new(declaration("effect", 1), |_| Ok(()), {
                    let effects = effects.clone();
                    move |_, _| {
                        effects.set(effects.get() + 1);
                        Box::pin(async {
                            Err(ToolError {
                                message: "execution failed".into(),
                                usage: Some(json!({"n":u64::MAX})),
                            })
                        })
                    }
                })],
            )
            .with_hooks(LifecycleHooks {
                before_request: Some(Rc::new(move |mut request, _| {
                    if remove_offer {
                        request.tools.clear();
                    }
                    Box::pin(async move { Ok(Some(request)) })
                })),
                before_tool: Some(Rc::new(move |call, _| {
                    assert!(!remove_offer, "unoffered calls cannot resolve hooks");
                    Box::pin(async move {
                        Ok(if call.id == "call-1" {
                            BeforeTool::Block("second blocked".into())
                        } else {
                            BeforeTool::Continue
                        })
                    })
                })),
                after_tool: Some(Rc::new({
                    let after = after.clone();
                    move |mut outcome, _| {
                        after.set(after.get() + 1);
                        assert!(outcome.result.is_error);
                        assert_eq!(outcome.result.content, "execution failed");
                        outcome.result.content = "replacement failure".into();
                        Box::pin(async move { Ok(Some(outcome.result)) })
                    }
                })),
                after_tools: Some(Rc::new({
                    let batches = batches.clone();
                    move |outcomes, _| {
                        batches.set(batches.get() + 1);
                        assert_eq!(outcomes.len(), 2);
                        assert_eq!(
                            outcomes
                                .iter()
                                .map(|o| o.call.id.as_str())
                                .collect::<Vec<_>>(),
                            ["call-0", "call-1"]
                        );
                        assert!(outcomes.iter().all(|o| o.result.is_error));
                        if remove_offer {
                            assert!(
                                outcomes
                                    .iter()
                                    .all(|o| o.result.content == "Tool was not offered")
                            );
                        } else {
                            assert_eq!(outcomes[0].result.content, "replacement failure");
                            assert_eq!(outcomes[0].result.usage, Some(json!({"n":u64::MAX})));
                            assert_eq!(outcomes[1].result.content, "second blocked");
                        }
                        Box::pin(async { Ok(()) })
                    }
                })),
                ..LifecycleHooks::default()
            }),
        )
        .unwrap();
    let round = Cell::new(0);
    let a = Agent::with_registry(
        move |request: ModelRequest, _: Cancellation| -> ModelFuture {
            assert_eq!(request.tools.is_empty(), remove_offer);
            let n = round.get();
            round.set(n + 1);
            Box::pin(async move { Ok(response(if n == 0 { &["effect", "effect"] } else { &[] })) })
        },
        registry.snapshot(),
        None,
    );
    let (open, driver) = Harness::open(storage, TaskRegistry::new(a.definitions()).unwrap());
    zip(
        async {
            let h = open.await.unwrap();
            let c = conversation(&h).await;
            a.submit(&h, c, "test", turn_config(), SubmitOptions::default())
                .await
                .unwrap()
                .wait()
                .await
                .unwrap();
            assert_eq!(effects.get(), usize::from(!remove_offer));
            assert_eq!(after.get(), effects.get());
            assert_eq!(batches.get(), 1);
            h.close().await.unwrap();
        },
        driver,
    )
    .await;
}
#[test]
fn memory_rewritten_offers_and_batch_error_results() {
    for remove in [false, true] {
        block_on(bounded(batch_and_offers(MemoryStorage::new(), remove)));
    }
}
#[test]
fn sqlite_rewritten_offers_and_batch_error_results() {
    for remove in [false, true] {
        let db = Database::new();
        block_on(bounded(batch_and_offers(db.open(), remove)));
    }
}

async fn invalid_request_replacement(storage: impl Storage + 'static, invalid: &'static str) {
    let constructed = Rc::new(Cell::new(0));
    let mut registry = AgentRegistry::new();
    registry
        .install(
            Extension::new("invalid", vec![]).with_hooks(LifecycleHooks {
                before_request: Some(Rc::new(move |mut request, _| {
                    let call = ToolCall {
                        id: "call".into(),
                        name: "effect".into(),
                        arguments: json!({}),
                    };
                    match invalid {
                        "model" => request.model.clear(),
                        "tool-name" => request.tools = vec![declaration("", 1)],
                        "duplicate-tools" => request.tools = vec![declaration("effect", 1); 2],
                        "call-id" | "call-name" | "duplicate-calls" | "deep-message" => {
                            let mut call = call;
                            match invalid {
                                "call-id" => call.id.clear(),
                                "call-name" => call.name.clear(),
                                "deep-message" => {
                                    call.arguments =
                                        (0..MAX_JSON_DEPTH).fold(Value::Null, |v, _| json!([v]))
                                }
                                _ => {}
                            }
                            request.messages.push(ModelMessage::Assistant {
                                text: String::new(),
                                tool_calls: vec![
                                    call;
                                    if invalid == "duplicate-calls" { 2 } else { 1 }
                                ],
                            });
                        }
                        "result-id" => request.messages.push(ModelMessage::ToolResult {
                            call_id: String::new(),
                            content: String::new(),
                            is_error: false,
                        }),
                        "deep-tools" => {
                            let mut tool = declaration("effect", 1);
                            tool.parameters =
                                (0..MAX_JSON_DEPTH).fold(Value::Null, |v, _| json!([v]));
                            request.tools = vec![tool];
                        }
                        _ => unreachable!(),
                    }
                    Box::pin(async move { Ok(Some(request)) })
                })),
                ..LifecycleHooks::default()
            }),
        )
        .unwrap();
    let a = Agent::with_registry(
        {
            let constructed = constructed.clone();
            move |_: ModelRequest, _: Cancellation| -> ModelFuture {
                // Count construction, not polling; a caught panic alone could mask this bug.
                constructed.set(constructed.get() + 1);
                Box::pin(async { Ok(response(&[])) })
            }
        },
        registry.snapshot(),
        None,
    );
    let (open, driver) = Harness::open(storage, TaskRegistry::new(a.definitions()).unwrap());
    zip(
        async {
            let h = open.await.unwrap();
            let c = conversation(&h).await;
            a.submit(&h, c, "test", turn_config(), SubmitOptions::default())
                .await
                .unwrap()
                .wait()
                .await
                .unwrap();
            h.wait_for_idle().await.unwrap();
            assert_eq!(constructed.get(), 0, "invalid replacement: {invalid}");
            assert!(
                !entries(&h, c)
                    .await
                    .iter()
                    .any(|entry| entry.kind == "agent.assistant")
            );
            h.close().await.unwrap();
        },
        driver,
    )
    .await;
}

const INVALID_REQUESTS: &[&str] = &[
    "model",
    "tool-name",
    "duplicate-tools",
    "call-id",
    "call-name",
    "duplicate-calls",
    "result-id",
    "deep-tools",
    "deep-message",
];

#[test]
fn memory_invalid_before_request_never_constructs_model() {
    for invalid in INVALID_REQUESTS {
        block_on(bounded(invalid_request_replacement(
            MemoryStorage::new(),
            invalid,
        )));
    }
}

#[test]
fn sqlite_invalid_before_request_never_constructs_model() {
    for invalid in INVALID_REQUESTS {
        let db = Database::new();
        block_on(bounded(invalid_request_replacement(db.open(), invalid)));
    }
}
