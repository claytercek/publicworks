use super::hooks::{Trace, entries, log, model, output};
use super::*;
use std::cell::Cell;

async fn lifetime(storage: impl Storage + 'static) {
    let entered = Gate::default();
    let release = Gate::default();
    let before = Gate::default();
    let before_release = Gate::default();
    let executing = Gate::default();
    let effect_release = Gate::default();
    let trace = Trace::default();
    let retained = Rc::new(RefCell::new(None::<HookContext>));
    let mut registry = AgentRegistry::new();
    registry
        .install(
            Extension::new("local", vec![inert("effect", 1)]).with_hooks(LifecycleHooks {
                before_request: Some(Rc::new({
                    let entered = entered.clone();
                    let release = release.clone();
                    let retained = retained.clone();
                    move |_, ctx| {
                        let entered = entered.clone();
                        let release = release.clone();
                        *retained.borrow_mut() = Some(ctx.clone());
                        Box::pin(async move {
                            assert_eq!(
                                ctx.memo_or_insert("winner", json!("old")).await.unwrap(),
                                json!("old")
                            );
                            entered.release();
                            release.wait().await;
                            Ok(None)
                        })
                    }
                })),
                after_response: Some(Rc::new({
                    let trace = trace.clone();
                    move |_, _| {
                        log(&trace, "old response");
                        Box::pin(async { Ok(()) })
                    }
                })),
                ..LifecycleHooks::default()
            }),
        )
        .unwrap();
    registry
        .install(Extension::new("later", vec![]).with_hooks(LifecycleHooks {
            after_response: Some(Rc::new({
                let trace = trace.clone();
                move |_, _| {
                    log(&trace, "selected response");
                    Box::pin(async { Ok(()) })
                }
            })),
            ..LifecycleHooks::default()
        }))
        .unwrap();
    let a = Agent::with_registry(model(), registry.snapshot(), Some(vec!["local".into()]));
    let (open, driver) = Harness::open(storage, TaskRegistry::new(a.definitions()).unwrap());
    zip(
        async {
            let h = open.await.unwrap();
            let c = conversation(&h).await;
            let receipt = a
                .submit(&h, c, "test", turn_config(), SubmitOptions::default())
                .await
                .unwrap();
            entered.wait().await;
            a.configure_extensions(
                &h,
                c,
                settings(ExtensionSelection::Exact(vec![
                    "local".into(),
                    "later".into(),
                ])),
            )
            .await
            .unwrap();
            // Callback is off the Session line; configuration and registry writes
            // can settle while it waits. Already-selected response observers stay old.
            registry
                .install(
                    Extension::new(
                        "local",
                        vec![Tool::new(declaration("effect", 1), |_| Ok(()), {
                            let trace = trace.clone();
                            let executing = executing.clone();
                            let release = effect_release.clone();
                            move |_, _| {
                                log(&trace, "new effect");
                                executing.release();
                                let release = release.clone();
                                Box::pin(async move {
                                    release.wait().await;
                                    Ok(output("new"))
                                })
                            }
                        })],
                    )
                    .with_hooks(LifecycleHooks {
                        before_tool: Some(Rc::new({
                            let before = before.clone();
                            let release = before_release.clone();
                            move |_, _| {
                                before.release();
                                let release = release.clone();
                                Box::pin(async move {
                                    release.wait().await;
                                    Ok(BeforeTool::Continue)
                                })
                            }
                        })),
                        after_tool: Some(Rc::new({
                            let trace = trace.clone();
                            move |_, _| {
                                log(&trace, "new after");
                                Box::pin(async { Ok(None) })
                            }
                        })),
                        before_request: Some(Rc::new({
                            let trace = trace.clone();
                            move |_, ctx| {
                                let trace = trace.clone();
                                Box::pin(async move {
                                    assert_eq!(
                                        ctx.memo_or_insert("winner", json!("new")).await.unwrap(),
                                        json!("old")
                                    );
                                    log(&trace, "new request");
                                    Ok(None)
                                })
                            }
                        })),
                        after_response: Some(Rc::new({
                            let trace = trace.clone();
                            move |_, _| {
                                log(&trace, "new response");
                                Box::pin(async { Ok(()) })
                            }
                        })),
                        ..LifecycleHooks::default()
                    }),
                )
                .unwrap();
            let new = a.publish_snapshot(&h, registry.snapshot(), []).unwrap();
            release.release();
            before.wait().await;
            assert_eq!(*trace.borrow(), ["old response"]);
            // Inspect durable work while beforeTool waits: effect intent is absent.
            h.commit(move |tx| {
                Box::pin(async move {
                    let run = tx.conversation_state(c).await?.unwrap().run.unwrap();
                    let root = tx.task(run.task_id).await?.unwrap();
                    let TaskState::Waiting { checkpoint, .. } = root.state else {
                        panic!("root must wait")
                    };
                    let child = tx
                        .task(Id::new(checkpoint["child"].as_u64().unwrap()).unwrap())
                        .await?
                        .unwrap();
                    let TaskState::Running { checkpoint, .. } = child.state else {
                        panic!("child must run")
                    };
                    assert_eq!(checkpoint["phase"], "call");
                    Ok(())
                })
            })
            .await
            .unwrap();
            // Uninstall and publish while beforeTool is suspended; the selected
            // implementation and afterTool remain pinned through effect settlement.
            let snapshot = registry.snapshot();
            registry.uninstall("local").unwrap();
            new.publish_snapshot(&h, registry.snapshot(), []).unwrap();
            before_release.release();
            executing.wait().await;
            // Execution started while uninstalled. Reinstall only request hooks,
            // so afterTool must still come from the removed phase snapshot.
            let saved = snapshot.get("local").unwrap().hooks();
            registry
                .install(Extension::new("local", vec![]).with_hooks(LifecycleHooks {
                    before_request: saved.before_request.clone(),
                    after_response: saved.after_response.clone(),
                    ..LifecycleHooks::default()
                }))
                .unwrap();
            new.publish_snapshot(&h, registry.snapshot(), []).unwrap();
            effect_release.release();
            receipt.wait().await.unwrap();
            assert_eq!(
                *trace.borrow(),
                [
                    "old response",
                    "new effect",
                    "new after",
                    "new request",
                    "new response",
                    "selected response"
                ]
            );
            let context = retained.borrow().clone().unwrap();
            assert!(context.cancellation().is_cancelled());
            assert!(context.memo_or_insert("late", json!(true)).await.is_err());
            h.close().await.unwrap();
        },
        driver,
    )
    .await;
}
#[test]
fn memory_hook_phase_lifetime_and_memo_replacement() {
    block_on(bounded(lifetime(MemoryStorage::new())));
}
#[test]
fn sqlite_hook_phase_lifetime_and_memo_replacement() {
    let db = Database::new();
    block_on(bounded(lifetime(db.open())));
}

// Exercise cancellation at every real lifecycle site, both explicit hook
// cancellation errors and invocation cancellation while a hook is pending.
async fn cancellation(storage: impl Storage + 'static, site: &'static str, explicit: bool) {
    let entered = Gate::default();
    let held = Rc::new(RefCell::new(None::<HookContext>));
    let effects = Rc::new(Cell::new(0));
    fn cancel<I: 'static, O: 'static>(
        entered: Gate,
        held: Rc<RefCell<Option<HookContext>>>,
        explicit: bool,
    ) -> Hook<I, O> {
        Rc::new(move |_, context| {
            *held.borrow_mut() = Some(context);
            entered.release();
            Box::pin(async move {
                if explicit {
                    Err(HookError::Cancelled)
                } else {
                    std::future::pending().await
                }
            })
        })
    }
    fn unexpected<I, O>() -> Hook<I, O> {
        Rc::new(|_, _| panic!("dispatch must stop at cancellation"))
    }
    let mut hooks = LifecycleHooks::default();
    let mut later = LifecycleHooks::default();
    match site {
        "request" => {
            hooks.before_request = Some(cancel(entered.clone(), held.clone(), explicit));
            later.before_request = Some(unexpected());
        }
        "response" => {
            hooks.after_response = Some(cancel(entered.clone(), held.clone(), explicit));
            later.after_response = Some(unexpected());
        }
        "before" => {
            hooks.before_tool = Some(cancel(entered.clone(), held.clone(), explicit));
            later.before_tool = Some(unexpected());
        }
        "after" => {
            hooks.after_tool = Some(cancel(entered.clone(), held.clone(), explicit));
            later.after_tool = Some(unexpected());
        }
        "batch" => {
            hooks.after_tools = Some(cancel(entered.clone(), held.clone(), explicit));
            later.after_tools = Some(unexpected());
        }
        _ => unreachable!(),
    }
    let mut registry = AgentRegistry::new();
    registry
        .install(
            Extension::new(
                "first",
                vec![Tool::new(declaration("effect", 1), |_| Ok(()), {
                    let effects = effects.clone();
                    move |_, _| {
                        effects.set(effects.get() + 1);
                        Box::pin(async { Ok(output("effect")) })
                    }
                })],
            )
            .with_hooks(hooks),
        )
        .unwrap();
    registry
        .install(Extension::new("later", vec![]).with_hooks(later))
        .unwrap();
    let a = Agent::with_registry(model(), registry.snapshot(), None);
    let (open, driver) = Harness::open(storage, TaskRegistry::new(a.definitions()).unwrap());
    zip(
        async {
            let h = open.await.unwrap();
            let c = conversation(&h).await;
            a.submit(&h, c, "test", turn_config(), SubmitOptions::default())
                .await
                .unwrap();
            entered.wait().await;
            if explicit {
                h.wait_for_idle().await.unwrap();
            }
            let entries = entries(&h, c).await;
            assert!(!entries.iter().any(|e| e.kind == "agent.hookError"));
            assert!(
                !entries
                    .iter()
                    .any(|e| e.data.as_ref().is_some_and(|d| d["code"] == "tool_blocked"))
            );
            assert_eq!(
                effects.get(),
                usize::from(site == "after" || site == "batch")
            );
            // Even a hook that never polls its cancellation future cannot hold close.
            registry.uninstall("first").unwrap();
            a.publish_snapshot(&h, registry.snapshot(), []).unwrap();
            h.close().await.unwrap();
            assert!(
                held.borrow()
                    .as_ref()
                    .unwrap()
                    .cancellation()
                    .is_cancelled()
            );
        },
        driver,
    )
    .await;
}
#[test]
fn memory_hook_cancellation_stops_every_composition() {
    for site in ["request", "response", "before", "after", "batch"] {
        for explicit in [false, true] {
            block_on(bounded(cancellation(MemoryStorage::new(), site, explicit)));
        }
    }
}
#[test]
fn sqlite_hook_cancellation_stops_every_composition() {
    for site in ["request", "response", "before", "after", "batch"] {
        for explicit in [false, true] {
            let db = Database::new();
            block_on(bounded(cancellation(db.open(), site, explicit)));
        }
    }
}

#[test]
fn sqlite_reopen_reuses_hook_memo_after_unsettled_request() {
    block_on(bounded(async {
        let db = Database::new();
        let entered = Gate::default();
        let mut registry = AgentRegistry::new();
        registry
            .install(Extension::new("stable", vec![]).with_hooks(LifecycleHooks {
                before_request: Some(Rc::new({
                    let entered = entered.clone();
                    move |_, context| {
                        let entered = entered.clone();
                        Box::pin(async move {
                            assert_eq!(
                                context
                                    .memo_or_insert("value", json!({"winner":u64::MAX}))
                                    .await
                                    .unwrap(),
                                json!({"winner":u64::MAX})
                            );
                            entered.release();
                            std::future::pending().await
                        })
                    }
                })),
                ..LifecycleHooks::default()
            }))
            .unwrap();
        let a = Agent::with_registry(
            |_: ModelRequest, _: Cancellation| -> ModelFuture {
                panic!("pending hook must precede model")
            },
            registry.snapshot(),
            None,
        );
        let (open, driver) = Harness::open(db.open(), TaskRegistry::new(a.definitions()).unwrap());
        let (c, root) = zip(
            async {
                let h = open.await.unwrap();
                let c = conversation(&h).await;
                a.submit(&h, c, "test", turn_config(), SubmitOptions::default())
                    .await
                    .unwrap();
                entered.wait().await;
                let root = h
                    .commit(move |tx| {
                        Box::pin(async move {
                            let root = tx
                                .conversation_state(c)
                                .await?
                                .unwrap()
                                .run
                                .unwrap()
                                .task_id;
                            assert!(tx.task(root).await?.unwrap().memos.is_some());
                            Ok(root)
                        })
                    })
                    .await
                    .unwrap()
                    .value;
                h.close().await.unwrap();
                (c, root)
            },
            driver,
        )
        .await
        .0;
        // Restart installs new code, not serialized callbacks. Memo identity is
        // extension name + key on the recovered task, independent of object identity.
        let mut registry = AgentRegistry::new();
        registry
            .install(Extension::new("stable", vec![]).with_hooks(LifecycleHooks {
                before_request: Some(Rc::new(|mut request, context| {
                    Box::pin(async move {
                        let winner = context
                            .memo_or_insert("value", json!("loser"))
                            .await
                            .unwrap();
                        assert_eq!(winner, json!({"winner":u64::MAX}));
                        request.instructions = winner.to_string();
                        Ok(Some(request))
                    })
                })),
                ..LifecycleHooks::default()
            }))
            .unwrap();
        let a = Agent::with_registry(
            |request: ModelRequest, _: Cancellation| -> ModelFuture {
                assert_eq!(request.instructions, json!({"winner":u64::MAX}).to_string());
                Box::pin(async { Ok(response(&[])) })
            },
            registry.snapshot(),
            None,
        );
        let (open, driver) = Harness::open(db.open(), TaskRegistry::new(a.definitions()).unwrap());
        zip(
            async {
                let h = open.await.unwrap();
                h.resume().unwrap();
                let done = h.wait_task(root).await.unwrap();
                assert!(matches!(
                    done.state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Completed { .. }
                    }
                ));
                assert!(done.memos.is_none());
                assert_eq!(
                    entries(&h, c)
                        .await
                        .iter()
                        .filter(|e| e.kind == "agent.assistant")
                        .count(),
                    1
                );
                h.close().await.unwrap();
            },
            driver,
        )
        .await;
    }));
}

async fn retained_context_next_phase(storage: impl Storage + 'static) {
    let retained = Rc::new(RefCell::new(None::<HookContext>));
    let next_phase = Gate::default();
    let release = Gate::default();
    let mut registry = AgentRegistry::new();
    registry
        .install(
            Extension::new(
                "local",
                vec![Tool::new(
                    declaration("effect", 1),
                    |_| Ok(()),
                    |_, _| Box::pin(async { Ok(output("effect")) }),
                )],
            )
            .with_hooks(LifecycleHooks {
                after_tools: Some(Rc::new({
                    let retained = retained.clone();
                    move |_, ctx| {
                        *retained.borrow_mut() = Some(ctx.clone());
                        Box::pin(async move {
                            ctx.memo_or_insert("winner", json!(42)).await.unwrap();
                            Ok(())
                        })
                    }
                })),
                before_request: Some(Rc::new({
                    let retained = retained.clone();
                    let next_phase = next_phase.clone();
                    let release = release.clone();
                    move |_, ctx| {
                        let second = retained.borrow().is_some();
                        let next_phase = next_phase.clone();
                        let release = release.clone();
                        Box::pin(async move {
                            if second {
                                assert_eq!(ctx.memo("winner").await.unwrap(), Some(json!(42)));
                                next_phase.release();
                                release.wait().await;
                                assert_eq!(ctx.memo("late").await.unwrap(), None);
                            }
                            Ok(None)
                        })
                    }
                })),
                ..LifecycleHooks::default()
            }),
        )
        .unwrap();
    let a = Agent::with_registry(model(), registry.snapshot(), None);
    let (open, driver) = Harness::open(storage, TaskRegistry::new(a.definitions()).unwrap());
    zip(
        async {
            let h = open.await.unwrap();
            let c = conversation(&h).await;
            let receipt = a
                .submit(&h, c, "test", turn_config(), SubmitOptions::default())
                .await
                .unwrap();
            // No publication, wait, or terminal transition separates the tools,
            // prepare, and request phases on this turn task.
            next_phase.wait().await;
            let old = retained.borrow().clone().unwrap();
            assert!(old.cancellation().is_cancelled());
            assert!(matches!(
                old.memo("winner").await,
                Err(SessionError::Invalid(_))
            ));
            assert!(matches!(
                old.memo_or_insert("late", json!(true)).await,
                Err(SessionError::Invalid(_))
            ));
            release.release();
            receipt.wait().await.unwrap();
            h.wait_for_idle().await.unwrap();
            h.close().await.unwrap();
        },
        driver,
    )
    .await;
}

#[test]
fn memory_retained_hook_context_is_fenced_during_next_phase() {
    block_on(bounded(retained_context_next_phase(MemoryStorage::new())));
}

#[test]
fn sqlite_retained_hook_context_is_fenced_during_next_phase() {
    let db = Database::new();
    block_on(bounded(retained_context_next_phase(db.open())));
}
