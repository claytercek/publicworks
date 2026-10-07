use super::*;
#[path = "probe.rs"]
mod probe;
use probe::Store;
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cut {
    BeforeTool,
    BeforeIntentCommit,
    IntentAck,
    Effect,
    AfterTool,
    ResultAck,
    None,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Current {
    Safe,
    Unsafe,
    Missing,
    Deselected,
    VersionMismatch,
    ReplacedIncompatible,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stop {
    Crash,
    Close,
    Abort,
}
fn effective() -> Value {
    json!({"rewritten":true,"max":u64::MAX,"min":i64::MIN,"float":1.25,
        "opaque":{"$serde_json::private::Number":"not a number"},"null":null,
        "deep":(0..58).fold(json!(u64::MAX), |value, _| json!([value]))})
}
fn output() -> ToolResult {
    ToolResult {
        content: "effective result".into(),
        is_error: false,
        usage: Some(effective()),
    }
}
fn installation(
    model: impl Model + 'static,
    tools: Vec<Tool>,
    hooks: LifecycleHooks,
    replacement: bool,
) -> Agent {
    let mut registry = AgentRegistry::new();
    registry
        .install(Extension::new("local", tools).with_hooks(hooks))
        .unwrap();
    if replacement {
        registry
            .install(Extension::new(
                "replacement",
                vec![
                    Tool::new(
                        declaration("effect", 99),
                        |_| panic!("incompatible validator"),
                        |_, _| panic!("incompatible effect"),
                    )
                    .with_replay_policy(ReplayPolicy::Safe),
                ],
            ))
            .unwrap();
    }
    Agent::with_registry(model, registry.snapshot(), None)
}

async fn replay_case(sqlite: bool, stored: ReplayPolicy, current: Current, cut: Cut, stop: Stop) {
    let store = Store::new(sqlite);
    let reached = Gate::default();
    let durable_intent = Rc::new(Cell::new(false));
    let before = Rc::new(Cell::new(0));
    let effects = Rc::new(Cell::new(0));
    let after = Rc::new(Cell::new(0));
    let tool = Tool::new(declaration("effect", 7), |_| Ok(()), {
        let reached = reached.clone();
        let effects = effects.clone();
        let durable_intent = durable_intent.clone();
        move |call, _| {
            assert!(
                durable_intent.get(),
                "commit must precede callback, even callback construction"
            );
            assert_eq!(call.arguments, effective());
            effects.set(effects.get() + 1);
            let reached = reached.clone();
            Box::pin(async move {
                if cut == Cut::Effect {
                    reached.release();
                    std::future::pending::<()>().await;
                }
                Ok(output())
            })
        }
    });
    assert_eq!(tool.replay_policy(), ReplayPolicy::Unsafe);
    assert_eq!(ReplayPolicy::default(), ReplayPolicy::Unsafe);
    let tool = if stored == ReplayPolicy::Safe {
        tool.with_replay_policy(stored)
    } else {
        tool
    };
    let hooks = LifecycleHooks {
        before_tool: Some(Rc::new({
            let reached = reached.clone();
            let before = before.clone();
            move |call, ctx| {
                assert_eq!(call.arguments, json!({"opaque":u64::MAX}));
                before.set(before.get() + 1);
                let reached = reached.clone();
                Box::pin(async move {
                    assert_eq!(
                        ctx.memo_or_insert("arguments", effective()).await.unwrap(),
                        effective()
                    );
                    if cut == Cut::BeforeTool {
                        reached.release();
                        std::future::pending::<()>().await;
                    }
                    Ok(BeforeTool::Rewrite(effective()))
                })
            }
        })),
        after_tool: Some(Rc::new({
            let reached = reached.clone();
            let after = after.clone();
            move |outcome, _| {
                assert_eq!(outcome.call.arguments, effective());
                after.set(after.get() + 1);
                let reached = reached.clone();
                Box::pin(async move {
                    if cut == Cut::AfterTool {
                        reached.release();
                        std::future::pending::<()>().await;
                    }
                    Ok(None)
                })
            }
        })),
        ..LifecycleHooks::default()
    };
    let agent = installation(
        FakeModel::new([Ok(tool_response("call", "effect"))]),
        vec![tool],
        hooks,
        false,
    );
    let storage = store.open(cut, reached.clone(), durable_intent.clone());
    let release_commit = storage.release.clone();
    let (session, mut sd) = Session::new(storage);
    let (runner, mut td) =
        TaskRunner::attach(&session, TaskRegistry::new(agent.definitions()).unwrap()).unwrap();
    let pause_tasks = Cell::new(false);
    let (conversation, root) = or(
        async {
            let conversation = create_conversation(&session).await;
            let root = admit(&session, agent, conversation, "replay probe").await;
            let run = runner.run(root.task_id);
            reached.wait().await;
            if stop == Stop::Close {
                let close = runner.close();
                release_commit.release();
                close.await.unwrap();
                assert_eq!(run.await.unwrap(), RunResult::Interrupted);
            } else if stop == Stop::Abort {
                // Let the Session acknowledge abort before polling the tool's
                // checkpoint receipt. Abort admission alone is not cancellation.
                pause_tasks.set(true);
                let abort = runner.abort(root.task_id);
                release_commit.release();
                abort.await.unwrap();
                pause_tasks.set(false);
                terminal(run.await.unwrap());
                runner.close().await.unwrap();
            }
            (conversation, root)
        },
        async {
            zip(
                &mut sd,
                std::future::poll_fn(|cx| {
                    if pause_tasks.get() {
                        Poll::Pending
                    } else {
                        std::pin::Pin::new(&mut td).poll(cx)
                    }
                }),
            )
            .await;
            std::future::pending().await
        },
    )
    .await;
    drop(runner);
    drop(session);
    drop(td);
    drop(sd);
    assert_eq!(before.get(), 1);
    let initial_effects = usize::from(matches!(cut, Cut::Effect | Cut::AfterTool | Cut::ResultAck));
    assert_eq!(effects.get(), initial_effects, "{cut:?} {stop:?}");

    let pre_intent = !durable_intent.get();
    let replay = !pre_intent
        && cut != Cut::ResultAck
        && stop != Stop::Abort
        && stored == ReplayPolicy::Safe
        && current == Current::Safe;
    let pre_intent_executable = pre_intent && matches!(current, Current::Safe | Current::Unsafe);
    let execute = stop != Stop::Abort && (replay || pre_intent_executable);
    let model = FakeModel::new([Ok(final_response("continued"))]);
    let current_tool = Tool::new(
        declaration(
            "effect",
            if current == Current::VersionMismatch {
                8
            } else {
                7
            },
        ),
        move |_| {
            assert!(
                pre_intent,
                "post-intent recovery must skip current argument validation"
            );
            Ok(())
        },
        {
            let effects = effects.clone();
            move |call, _| {
                assert!(execute, "non-replay case invoked tool");
                assert_eq!(call.id, "call");
                assert_eq!(call.name, "effect");
                assert_eq!(call.arguments, effective());
                effects.set(effects.get() + 1);
                Box::pin(async { Ok(output()) })
            }
        },
    )
    .with_replay_policy(if current == Current::Unsafe {
        ReplayPolicy::Unsafe
    } else {
        ReplayPolicy::Safe
    });
    let current_agent = installation(
        model.clone(),
        if current == Current::Missing {
            vec![]
        } else {
            vec![current_tool]
        },
        LifecycleHooks {
            before_tool: Some(Rc::new({
                let before = before.clone();
                move |call, ctx| {
                    assert!(pre_intent, "post-intent replay cannot run beforeTool");
                    assert_eq!(call.arguments, json!({"opaque":u64::MAX}));
                    before.set(before.get() + 1);
                    Box::pin(async move {
                        // First writer survives a hook/intent commit interruption.
                        let winner = ctx
                            .memo_or_insert("arguments", json!("new config loser"))
                            .await
                            .unwrap();
                        assert_eq!(winner, effective());
                        Ok(BeforeTool::Rewrite(winner))
                    })
                }
            })),
            after_tool: Some(Rc::new({
                let after = after.clone();
                move |outcome, _| {
                    assert!(execute);
                    assert_eq!(outcome.call.arguments, effective());
                    after.set(after.get() + 1);
                    Box::pin(async { Ok(None) })
                }
            })),
            ..LifecycleHooks::default()
        },
        current == Current::ReplacedIncompatible,
    );
    let (opening, sd) =
        Session::open_recovered(store.open(Cut::None, Gate::default(), durable_intent));
    zip(async {
        let session = opening.await.unwrap().value;
        if current == Current::Deselected {
            session.commit(move |tx| Box::pin(async move {
                tx.update_conversation_state(conversation, |s| s.agent_config = Some(json!({"extensions":[]}))).await?;
                Ok(())
            })).await.unwrap();
        }
        // Inspect the actual recovered intent before any current callback runs.
        session.commit(move |tx| Box::pin(async move {
            let children = tx.scan_tasks(TaskQuery {kind: Some(TOOL_KIND.into()), ..TaskQuery::default()}, 128, None).await?.items;
            assert_eq!(children.len(), 1);
            let child = &children[0];
            if stop == Stop::Abort || cut == Cut::ResultAck {
                assert_eq!(child.status(), TaskStatus::Terminal);
            } else {
                let TaskState::Pending { checkpoint } = &child.state else { panic!("expected recovered pending child") };
                if pre_intent { assert_eq!(checkpoint, &json!({"phase":"call"})); }
                else {
                    assert_eq!(checkpoint, &json!({"phase":"execute","callId":"call","name":"effect","version":7,"arguments":effective(),"replay":if stored == ReplayPolicy::Safe {"safe"} else {"unsafe"}}));
                }
            }
            Ok(())
        })).await.unwrap();
        let (runner, td) = TaskRunner::attach(&session, TaskRegistry::new(current_agent.definitions()).unwrap()).unwrap();
        zip(async {
            if stop != Stop::Abort { terminal(runner.run(root.task_id).await.unwrap()); }
            assert_eq!(effects.get(), initial_effects + usize::from(execute));
            assert_eq!(before.get(), 1 + usize::from(pre_intent_executable && stop != Stop::Abort));
            assert_eq!(after.get(), usize::from(matches!(cut, Cut::AfterTool | Cut::ResultAck)) + usize::from(execute));
            let log = entries(&session, conversation).await;
            let assistant = log.iter().find(|e| e.kind == "agent.assistant").unwrap();
            assert_eq!(decode_message(&assistant.model.as_ref().unwrap()[0]).unwrap(), tool_response("call", "effect").message, "assistant arguments are immutable");
            let results: Vec<_> = log.iter().filter(|e| e.kind == "agent.toolResult").collect();
            assert_eq!(results.len(), 1);
            let data = results[0].data.as_ref().unwrap();
            assert_eq!(data["callId"], "call");
            let result = decode_message(&results[0].model.as_ref().unwrap()[0]).unwrap();
            if stop == Stop::Abort && cut != Cut::ResultAck {
                assert_eq!(data["code"], "aborted");
                let ModelMessage::ToolResult {content, is_error:true, ..} = result else { panic!("expected abort result") };
                assert_eq!(content.contains("may have"), !pre_intent);
            } else if execute || cut == Cut::ResultAck {
                assert!(data.get("code").is_none());
                assert_eq!(data["usage"], effective());
                assert!(matches!(result, ModelMessage::ToolResult {is_error:false, ..}));
            } else if pre_intent {
                assert_eq!(data["code"], "invalid_tool_call");
                assert!(matches!(result, ModelMessage::ToolResult {is_error:true, ..}));
            } else {
                assert_eq!(data["code"], "interrupted_effect");
                let ModelMessage::ToolResult {content, is_error:true, ..} = result else { panic!("expected uncertain result") };
                assert!(content.contains("effect") && content.contains("may have partially or fully run") && content.contains("not replayed"));
            }
            let result_id = results[0].id;
            session.commit(move |tx| Box::pin(async move {
                let child = tx.scan_tasks(TaskQuery {kind:Some(TOOL_KIND.into()), ..TaskQuery::default()}, 128, None).await?.items.remove(0);
                let TaskState::Terminal {outcome} = child.state else { panic!("child must settle atomically with result") };
                match outcome {
                    TaskOutcome::Completed {result} => {
                        assert!(execute || cut == Cut::ResultAck || pre_intent);
                        assert_eq!(result["resultEntryId"], result_id.get());
                    }
                    TaskOutcome::Failed {error, result:Some(result)} => {
                        assert!(!execute && !pre_intent && stop != Stop::Abort);
                        assert_eq!(error.message, "interrupted_effect");
                        assert_eq!(result["resultEntryId"], result_id.get());
                    }
                    TaskOutcome::Aborted {result:Some(result), ..} => {
                        assert_eq!(stop, Stop::Abort);
                        assert_eq!(result["resultEntryId"], result_id.get());
                    }
                    other => panic!("unexpected child outcome: {other:?}"),
                }
                Ok(())
            })).await.unwrap();
            if stop != Stop::Abort {
                assert_eq!(model.requests.borrow().len(), 1);
                assert!(model.requests.borrow()[0].messages.iter().any(|m| matches!(m, ModelMessage::ToolResult {call_id, ..} if call_id == "call")));
            }
            runner.close().await.unwrap();
            session.close().await.unwrap();
        }, td).await;
    }, sd).await;
}

fn matrix(sqlite: bool) {
    for stored in [ReplayPolicy::Unsafe, ReplayPolicy::Safe] {
        for current in [
            Current::Safe,
            Current::Unsafe,
            Current::Missing,
            Current::Deselected,
            Current::VersionMismatch,
            Current::ReplacedIncompatible,
        ] {
            block_on(bounded(replay_case(
                sqlite,
                stored,
                current,
                Cut::Effect,
                Stop::Close,
            )));
            // The same current installation change before intent is a known
            // no-effect rejection, not an uncertain interrupted operation.
            block_on(bounded(replay_case(
                sqlite,
                stored,
                current,
                Cut::BeforeTool,
                Stop::Close,
            )));
        }
    }
}
#[test]
fn memory_live_policy_matrix_and_effect_recovery() {
    matrix(false);
}
#[test]
fn sqlite_reopen_policy_matrix_and_effect_recovery() {
    matrix(true);
}
fn boundaries(sqlite: bool) {
    for stored in [ReplayPolicy::Unsafe, ReplayPolicy::Safe] {
        for cut in [
            Cut::BeforeTool,
            Cut::BeforeIntentCommit,
            Cut::IntentAck,
            Cut::AfterTool,
            Cut::ResultAck,
        ] {
            block_on(bounded(replay_case(
                sqlite,
                stored,
                Current::Safe,
                cut,
                Stop::Crash,
            )));
        }
        for cut in [
            Cut::BeforeTool,
            Cut::BeforeIntentCommit,
            Cut::IntentAck,
            Cut::Effect,
            Cut::AfterTool,
            Cut::ResultAck,
        ] {
            for stop in [Stop::Close, Stop::Abort] {
                block_on(bounded(replay_case(
                    sqlite,
                    stored,
                    Current::Safe,
                    cut,
                    stop,
                )));
            }
        }
    }
}
#[test]
fn memory_intent_effect_result_and_cancellation_boundaries() {
    boundaries(false);
}
#[test]
fn sqlite_reopen_intent_effect_result_and_cancellation_boundaries() {
    boundaries(true);
}
