use futures_lite::future::{block_on, zip};
use publicworks_agent::*;
use publicworks_runtime::*;
use serde_json::{Value, json};
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    rc::Rc,
    task::{Poll, Waker},
};

#[derive(Clone, Default)]
struct Gate(Rc<RefCell<(bool, Option<Waker>)>>);
impl Gate {
    async fn wait(&self) {
        std::future::poll_fn(|cx| {
            let mut state = self.0.borrow_mut();
            if state.0 {
                Poll::Ready(())
            } else {
                state.1 = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
    fn release(&self) {
        let wake = {
            let mut state = self.0.borrow_mut();
            state.0 = true;
            state.1.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}

#[derive(Clone)]
struct FakeModel {
    replies: Rc<RefCell<VecDeque<Result<ModelResponse, ModelError>>>>,
    requests: Rc<RefCell<Vec<ModelRequest>>>,
}
impl FakeModel {
    fn new(replies: impl IntoIterator<Item = Result<ModelResponse, ModelError>>) -> Self {
        Self {
            replies: Rc::new(RefCell::new(replies.into_iter().collect())),
            requests: Rc::new(RefCell::new(Vec::new())),
        }
    }
}
impl Model for FakeModel {
    fn complete(&self, request: ModelRequest, _: Cancellation) -> ModelFuture {
        self.requests.borrow_mut().push(request);
        let reply = self
            .replies
            .borrow_mut()
            .pop_front()
            .expect("unexpected model invocation");
        Box::pin(async move { reply })
    }
}

fn response(text: &str) -> ModelResponse {
    ModelResponse {
        message: ModelMessage::Assistant {
            text: text.into(),
            tool_calls: vec![],
        },
        finish_reason: FinishReason::Stop,
        usage: None,
    }
}
fn calls(calls: Vec<ToolCall>, usage: Option<Value>) -> ModelResponse {
    ModelResponse {
        message: ModelMessage::Assistant {
            text: String::new(),
            tool_calls: calls,
        },
        finish_reason: FinishReason::ToolCalls,
        usage,
    }
}
fn call(id: &str, name: &str, arguments: Value) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: name.into(),
        arguments,
    }
}
fn config(rounds: u64) -> TurnConfig {
    TurnConfig {
        model: "fake-v1".into(),
        instructions: "Be exact.".into(),
        max_model_rounds: rounds,
    }
}
fn declaration(name: &str, version: u64) -> ToolDeclaration {
    ToolDeclaration {
        name: name.into(),
        version,
        description: format!("the {name} tool"),
        parameters: json!({"type":"object"}),
    }
}
fn tool(
    name: &str,
    version: u64,
    validate: impl Fn(&Value) -> Result<(), String> + 'static,
    execute: impl Fn(ToolCall, Cancellation) -> ToolFuture + 'static,
) -> Tool {
    Tool::new(declaration(name, version), validate, execute)
}
async fn conversation(session: &Session) -> Id {
    session
        .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
        .await
        .unwrap()
        .value
        .id
}
#[derive(Clone)]
struct TestTurn {
    task_id: Id,
    user_entry_id: Id,
}
async fn admit(
    session: &Session,
    agent: Agent,
    conversation: Id,
    text: &str,
    cfg: TurnConfig,
) -> TestTurn {
    let text = text.to_owned();
    let id = session
        .commit(move |tx| {
            agent.admit_input(
                tx,
                conversation,
                text,
                cfg,
                publicworks_agent::SubmitOptions::default(),
            )
        })
        .await
        .unwrap()
        .value;
    session
        .commit(move |tx| {
            Box::pin(async move {
                let run = tx
                    .conversation_state(conversation)
                    .await?
                    .unwrap()
                    .run
                    .unwrap();
                let record = tx.submission(id).await?.unwrap();
                let SubmissionState::InputPlaced { entry } = record.state else {
                    panic!("not placed")
                };
                Ok(TestTurn {
                    task_id: run.task_id,
                    user_entry_id: entry,
                })
            })
        })
        .await
        .unwrap()
        .value
}
async fn task(session: &Session, id: Id) -> TaskRecord {
    session
        .commit(move |tx| Box::pin(async move { Ok(tx.task(id).await?.unwrap()) }))
        .await
        .unwrap()
        .value
}
async fn entries(session: &Session, conversation: Id) -> Vec<EntryRecord> {
    let mut items = session
        .commit(move |tx| {
            Box::pin(async move {
                Ok(tx
                    .scan_entries(EntryQuery::new(conversation), 128, None)
                    .await?
                    .items)
            })
        })
        .await
        .unwrap()
        .value;
    items.reverse();
    items
}
fn terminal(result: RunResult) -> TaskRecord {
    match result {
        RunResult::Terminal(task) => task,
        other => panic!("expected terminal task, got {other:?}"),
    }
}

#[test]
fn final_answer_is_atomic_attributed_and_preserves_json_precision() {
    block_on(async {
        let usage = json!({"input":u64::MAX,"signed":i64::MIN,"opaque":{"$reserved":true}});
        let model = FakeModel::new([Ok(ModelResponse {
            message: ModelMessage::Assistant {
                text: "forty-two".into(),
                tool_calls: vec![],
            },
            finish_reason: FinishReason::Stop,
            usage: Some(usage.clone()),
        })]);
        let agent = Agent::new(model.clone(), vec![]).unwrap();
        let (session, session_driver) = Session::new(MemoryStorage::new());
        let (runner, task_driver) =
            TaskRunner::attach(&session, TaskRegistry::new(agent.definitions()).unwrap()).unwrap();
        zip(
            async {
                let conversation = conversation(&session).await;
                let handle = admit(&session, agent, conversation, "question", config(2)).await;
                let done = terminal(runner.run(handle.task_id).await.unwrap());
                assert!(matches!(
                    done.state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Completed { .. }
                    }
                ));
                let log = entries(&session, conversation).await;
                assert_eq!(log.len(), 2);
                assert_eq!(log[0].id, handle.user_entry_id);
                assert_eq!(log[0].kind, "agent.user");
                assert_eq!(log[0].by_task_id, None);
                assert_eq!(
                    log[0].model,
                    Some(vec![json!({"role":"user","text":"question"})])
                );
                assert_eq!(log[1].kind, "agent.assistant");
                assert_eq!(log[1].by_task_id, Some(handle.task_id));
                assert_eq!(
                    log[1].data,
                    Some(json!({"status":"completed","usage":usage}))
                );
                assert_eq!(
                    decode_message(&log[1].model.as_ref().unwrap()[0]).unwrap(),
                    ModelMessage::Assistant {
                        text: "forty-two".into(),
                        tool_calls: vec![]
                    }
                );
                {
                    let requests = model.requests.borrow();
                    let request = &requests[0];
                    assert_eq!(request.model, "fake-v1");
                    assert_eq!(request.instructions, "Be exact.");
                    assert_eq!(
                        request.messages,
                        vec![ModelMessage::User {
                            text: "question".into()
                        }]
                    );
                    assert!(request.tools.is_empty());
                }
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    });
}

#[test]
fn tools_run_sequentially_and_tool_failures_continue_to_the_next_round() {
    block_on(async {
        let first_call = call("first-id", "first", json!({"n":u64::MAX,"$opaque":1}));
        let second_call = call("second-id", "second", json!({"n":i64::MIN}));
        let third_call = call("third-id", "third", json!({}));
        let model = FakeModel::new([
            Ok(calls(
                vec![first_call.clone(), second_call.clone(), third_call.clone()],
                Some(json!({"round":1})),
            )),
            Ok(response("finished")),
        ]);
        let order = Rc::new(RefCell::new(Vec::new()));
        let first = tool("first", 7, |_| Ok(()), {
            let order = order.clone();
            move |call, _| {
                order.borrow_mut().push(call.id);
                Box::pin(async {
                    Ok(ToolResult {
                        content: "21".into(),
                        is_error: false,
                        usage: None,
                    })
                })
            }
        });
        let second = tool("second", 9, |_| Ok(()), {
            let order = order.clone();
            move |call, _| {
                order.borrow_mut().push(call.id);
                Box::pin(async {
                    Ok(ToolResult {
                        content: "domain rejection".into(),
                        is_error: true,
                        usage: Some(json!(null)),
                    })
                })
            }
        });
        let third = tool("third", 11, |_| Ok(()), {
            let order = order.clone();
            move |call, _| {
                order.borrow_mut().push(call.id);
                Box::pin(async {
                    Err(ToolError {
                        message: "executor failed".into(),
                        usage: Some(json!({"attempts":1})),
                    })
                })
            }
        });
        let agent = Agent::new(model.clone(), vec![third, second, first]).unwrap();
        let (session, sd) = Session::new(MemoryStorage::new());
        let (runner, td) =
            TaskRunner::attach(&session, TaskRegistry::new(agent.definitions()).unwrap()).unwrap();
        zip(
            async {
                let conversation = conversation(&session).await;
                let handle = admit(&session, agent, conversation, "calculate", config(3)).await;
                terminal(runner.run(handle.task_id).await.unwrap());
                assert_eq!(&*order.borrow(), &["first-id", "second-id", "third-id"]);
                {
                    let requests = model.requests.borrow();
                    assert_eq!(requests.len(), 2);
                    assert_eq!(
                        requests[0]
                            .tools
                            .iter()
                            .map(|d| (&d.name, d.version))
                            .collect::<Vec<_>>(),
                        vec![
                            (&"first".to_owned(), 7),
                            (&"second".to_owned(), 9),
                            (&"third".to_owned(), 11),
                        ]
                    );
                    assert_eq!(
                        requests[1].messages,
                        vec![
                            ModelMessage::User {
                                text: "calculate".into()
                            },
                            ModelMessage::Assistant {
                                text: String::new(),
                                tool_calls: vec![first_call, second_call, third_call]
                            },
                            ModelMessage::ToolResult {
                                call_id: "first-id".into(),
                                content: "21".into(),
                                is_error: false
                            },
                            ModelMessage::ToolResult {
                                call_id: "second-id".into(),
                                content: "domain rejection".into(),
                                is_error: true
                            },
                            ModelMessage::ToolResult {
                                call_id: "third-id".into(),
                                content: "executor failed".into(),
                                is_error: true
                            },
                        ]
                    );
                }
                let log = entries(&session, conversation).await;
                assert_eq!(
                    log.iter().map(|e| e.kind.as_str()).collect::<Vec<_>>(),
                    vec![
                        "agent.user",
                        "agent.assistant",
                        "agent.toolResult",
                        "agent.toolResult",
                        "agent.toolResult",
                        "agent.assistant"
                    ]
                );
                assert_eq!(
                    log[1].data,
                    Some(json!({"status":"completed","usage":{"round":1}}))
                );
                assert_eq!(log[2].data.as_ref().unwrap().get("usage"), None);
                assert_eq!(log[3].data.as_ref().unwrap()["usage"], Value::Null);
                assert_eq!(
                    log[4].data.as_ref().unwrap()["usage"],
                    json!({"attempts":1})
                );
                assert_eq!(log[4].data.as_ref().unwrap()["code"], "tool_error");
                for result in &log[2..=4] {
                    assert_eq!(
                        result.data.as_ref().unwrap()["assistantEntryId"],
                        json!(log[1].id.get())
                    );
                }
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    });
}

#[test]
fn admission_rejects_invalid_config_and_busy_turn_without_partial_writes() {
    block_on(async {
        let entered = Gate::default();
        let release = Gate::default();
        let model = {
            let entered = entered.clone();
            let release = release.clone();
            move |_: ModelRequest, _: Cancellation| -> ModelFuture {
                entered.release();
                let release = release.clone();
                Box::pin(async move {
                    release.wait().await;
                    Ok(response("done"))
                })
            }
        };
        let agent = Agent::new(model, vec![]).unwrap();
        let (session, sd) = Session::new(MemoryStorage::new());
        let (runner, td) =
            TaskRunner::attach(&session, TaskRegistry::new(agent.definitions()).unwrap()).unwrap();
        zip(
            async {
                let conversation = conversation(&session).await;
                let invalid_agent = agent.clone();
                let invalid = session
                    .commit(move |tx| {
                        invalid_agent.admit_input(
                            tx,
                            conversation,
                            "bad",
                            config(0),
                            publicworks_agent::SubmitOptions::default(),
                        )
                    })
                    .await;
                assert!(matches!(invalid, Err(SessionError::Invalid(_))));
                assert!(entries(&session, conversation).await.is_empty());
                // Unrelated tasks fill the first 128-item page. The agent turn
                // admitted afterward is only visible if admission follows the cursor.
                let padding = TaskDefinition::new(
                    "padding",
                    1,
                    |_| Ok(json!({"phase":"unused"})),
                    Default::default(),
                );
                session
                    .commit(move |tx| {
                        Box::pin(async move {
                            for _ in 0..129 {
                                tx.create_task(
                                    padding.clone(),
                                    json!(null),
                                    TaskOptions {
                                        conversation_id: Some(conversation),
                                        ownership: TaskOwnership::Conversation,
                                        background: false,
                                    },
                                )
                                .await?;
                            }
                            Ok(())
                        })
                    })
                    .await
                    .unwrap();
                let first = admit(&session, agent.clone(), conversation, "first", config(2)).await;
                let run = runner.run(first.task_id);
                entered.wait().await;
                let first_page = session
                    .commit(move |tx| {
                        Box::pin(async move {
                            tx.scan_tasks(
                                TaskQuery {
                                    conversation_id: Some(conversation),
                                    ..TaskQuery::default()
                                },
                                128,
                                None,
                            )
                            .await
                        })
                    })
                    .await
                    .unwrap()
                    .value;
                assert!(first_page.next.is_some());
                assert!(first_page.items.iter().all(|task| task.id != first.task_id));
                let busy_agent = agent.clone();
                let busy = session
                    .commit(move |tx| {
                        busy_agent.admit_input(
                            tx,
                            conversation,
                            "must not append",
                            config(2),
                            SubmitOptions {
                                busy: BusyMode::Reject,
                                ..Default::default()
                            },
                        )
                    })
                    .await;
                assert!(matches!(busy, Err(SessionError::Invalid(_))));
                assert_eq!(entries(&session, conversation).await.len(), 1);
                release.release();
                terminal(run.await.unwrap());
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    });
}

#[test]
fn malformed_model_envelopes_fail_without_children_or_tool_effects() {
    block_on(async {
        let duplicate = call("same", "effect", json!({}));
        let replies = vec![
            Ok(ModelResponse {
                message: ModelMessage::User {
                    text: "wrong role".into(),
                },
                finish_reason: FinishReason::Stop,
                usage: None,
            }),
            Ok(ModelResponse {
                message: ModelMessage::Assistant {
                    text: String::new(),
                    tool_calls: vec![call("a", "effect", json!({}))],
                },
                finish_reason: FinishReason::Stop,
                usage: None,
            }),
            Ok(ModelResponse {
                message: ModelMessage::Assistant {
                    text: String::new(),
                    tool_calls: vec![],
                },
                finish_reason: FinishReason::ToolCalls,
                usage: None,
            }),
            Ok(ModelResponse {
                message: ModelMessage::Assistant {
                    text: String::new(),
                    tool_calls: vec![duplicate.clone(), duplicate],
                },
                finish_reason: FinishReason::ToolCalls,
                usage: None,
            }),
            Ok(ModelResponse {
                message: ModelMessage::Assistant {
                    text: String::new(),
                    tool_calls: vec![call("", "effect", json!({}))],
                },
                finish_reason: FinishReason::ToolCalls,
                usage: Some(json!({"bad":true})),
            }),
        ];
        let model = FakeModel::new(replies);
        let effects = Rc::new(Cell::new(0));
        let effect = tool("effect", 1, |_| Ok(()), {
            let effects = effects.clone();
            move |_, _| {
                effects.set(effects.get() + 1);
                Box::pin(async {
                    Ok(ToolResult {
                        content: "ran".into(),
                        is_error: false,
                        usage: None,
                    })
                })
            }
        });
        let agent = Agent::new(model, vec![effect]).unwrap();
        let (session, sd) = Session::new(MemoryStorage::new());
        let (runner, td) =
            TaskRunner::attach(&session, TaskRegistry::new(agent.definitions()).unwrap()).unwrap();
        zip(
            async {
                for n in 0..5 {
                    let conversation = conversation(&session).await;
                    let handle = admit(
                        &session,
                        agent.clone(),
                        conversation,
                        &format!("case {n}"),
                        config(2),
                    )
                    .await;
                    let done = terminal(runner.run(handle.task_id).await.unwrap());
                    assert!(matches!(
                        done.state,
                        TaskState::Terminal {
                            outcome: TaskOutcome::Failed { .. }
                        }
                    ));
                    let log = entries(&session, conversation).await;
                    assert_eq!(
                        log.iter()
                            .map(|entry| entry.kind.as_str())
                            .collect::<Vec<_>>(),
                        vec!["agent.user", "agent.diagnostic"]
                    );
                }
                assert_eq!(effects.get(), 0);
                let all = session
                    .commit(|tx| {
                        Box::pin(async move {
                            Ok(tx.scan_tasks(TaskQuery::default(), 128, None).await?.items)
                        })
                    })
                    .await
                    .unwrap()
                    .value;
                assert_eq!(
                    all.len(),
                    5,
                    "malformed responses must create no tool children"
                );
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    });
}

#[test]
fn unknown_and_invalid_tool_calls_are_durable_results_without_execution() {
    block_on(async {
        let model = FakeModel::new([
            Ok(calls(
                vec![
                    call("unknown", "not-offered", json!({})),
                    call("invalid", "installed", json!({"valid":false})),
                ],
                None,
            )),
            Ok(response("continued after errors")),
        ]);
        let executions = Rc::new(Cell::new(0));
        let installed = tool(
            "installed",
            3,
            |arguments| {
                if arguments["valid"] == json!(true) {
                    Ok(())
                } else {
                    Err("invalid arguments".into())
                }
            },
            {
                let executions = executions.clone();
                move |_, _| {
                    executions.set(executions.get() + 1);
                    Box::pin(async {
                        Ok(ToolResult {
                            content: "unexpected".into(),
                            is_error: false,
                            usage: None,
                        })
                    })
                }
            },
        );
        let agent = Agent::new(model.clone(), vec![installed]).unwrap();
        let (session, sd) = Session::new(MemoryStorage::new());
        let (runner, td) =
            TaskRunner::attach(&session, TaskRegistry::new(agent.definitions()).unwrap()).unwrap();
        zip(
            async {
                let conversation = conversation(&session).await;
                let handle = admit(&session, agent, conversation, "try tools", config(3)).await;
                terminal(runner.run(handle.task_id).await.unwrap());
                assert_eq!(executions.get(), 0);
                {
                    let requests = model.requests.borrow();
                    let request = &requests[1];
                    let results = request
                        .messages
                        .iter()
                        .filter_map(|message| match message {
                            ModelMessage::ToolResult {
                                call_id,
                                content,
                                is_error,
                            } => Some((call_id, content, is_error)),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(results.len(), 2);
                    assert_eq!(results[0].0, "unknown");
                    assert!(*results[0].2);
                    assert!(results[0].1.contains("not offered"));
                    assert_eq!(results[1].0, "invalid");
                    assert!(*results[1].2);
                    assert_eq!(results[1].1, "invalid arguments");
                }
                let log = entries(&session, conversation).await;
                for entry in log.iter().filter(|entry| entry.kind == "agent.toolResult") {
                    assert_eq!(entry.data.as_ref().unwrap()["code"], "invalid_tool_call");
                }
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    });
}

#[test]
fn max_round_exhaustion_and_provider_partial_failure_are_persisted() {
    block_on(async {
        let model = FakeModel::new([
            Ok(calls(vec![call("one", "ok", json!({}))], None)),
            Err(ModelError {
                message: "provider unavailable".into(),
                partial_response: Some(ModelResponse {
                    message: ModelMessage::Assistant {
                        text: "partial".into(),
                        tool_calls: vec![],
                    },
                    finish_reason: FinishReason::Stop,
                    usage: Some(json!({"partialTokens":u64::MAX})),
                }),
                usage: Some(json!({"billed":17})),
            }),
        ]);
        let ok = tool(
            "ok",
            1,
            |_| Ok(()),
            |_, _| {
                Box::pin(async {
                    Ok(ToolResult {
                        content: "ok".into(),
                        is_error: false,
                        usage: None,
                    })
                })
            },
        );
        let agent = Agent::new(model, vec![ok]).unwrap();
        let (session, sd) = Session::new(MemoryStorage::new());
        let (runner, td) =
            TaskRunner::attach(&session, TaskRegistry::new(agent.definitions()).unwrap()).unwrap();
        zip(async {
            let first_conversation = conversation(&session).await;
            let first = admit(&session, agent.clone(), first_conversation, "one round", config(1)).await;
            let exhausted = terminal(runner.run(first.task_id).await.unwrap());
            assert!(matches!(exhausted.state, TaskState::Terminal { outcome: TaskOutcome::Failed { ref error, .. } } if error.message.contains("Maximum model rounds")));

            let second_conversation = conversation(&session).await;
            let second = admit(&session, agent, second_conversation, "provider", config(2)).await;
            let failed = terminal(runner.run(second.task_id).await.unwrap());
            assert!(matches!(failed.state, TaskState::Terminal { outcome: TaskOutcome::Failed { ref error, .. } } if error.message == "provider unavailable"));
            let log = entries(&session, second_conversation).await;
            assert_eq!(log.iter().map(|entry| entry.kind.as_str()).collect::<Vec<_>>(), vec!["agent.user", "agent.diagnostic"]);
            assert_eq!(log[1].data.as_ref().unwrap()["usage"], json!({"billed":17}));
            assert_eq!(log[1].data.as_ref().unwrap()["partialResponse"]["usage"], json!({"partialTokens":u64::MAX}));
            assert_eq!(log[1].data.as_ref().unwrap()["partialResponse"]["message"], json!({"role":"assistant","text":"partial","toolCalls":[]}));
            runner.close().await.unwrap();
            session.close().await.unwrap();
        }, zip(sd, td)).await;
    });
}

#[test]
fn aborting_a_pending_tool_records_started_and_unstarted_results_once() {
    block_on(async {
        let tool_entered = Gate::default();
        let model = FakeModel::new([Ok(calls(
            vec![
                call("started", "slow", json!({})),
                call("unstarted", "slow", json!({})),
            ],
            None,
        ))]);
        let slow = tool("slow", 1, |_| Ok(()), {
            let tool_entered = tool_entered.clone();
            move |_, cancellation| {
                tool_entered.release();
                Box::pin(async move {
                    cancellation.cancelled().await;
                    std::future::pending().await
                })
            }
        });
        let agent = Agent::new(model, vec![slow]).unwrap();
        let (session, sd) = Session::new(MemoryStorage::new());
        let (runner, td) =
            TaskRunner::attach(&session, TaskRegistry::new(agent.definitions()).unwrap()).unwrap();
        zip(
            async {
                let conversation = conversation(&session).await;
                let handle = admit(&session, agent, conversation, "abort", config(2)).await;
                let run = runner.run(handle.task_id);
                tool_entered.wait().await;
                assert_eq!(
                    runner.abort(handle.task_id).await.unwrap(),
                    AbortResult::Marked
                );
                let done = terminal(run.await.unwrap());
                assert!(matches!(
                    done.state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Aborted { .. }
                    }
                ));
                let log = entries(&session, conversation).await;
                let results = log
                    .iter()
                    .filter(|entry| entry.kind == "agent.toolResult")
                    .collect::<Vec<_>>();
                assert_eq!(results.len(), 2);
                assert_eq!(
                    results
                        .iter()
                        .map(|entry| entry.data.as_ref().unwrap()["callId"].as_str().unwrap())
                        .collect::<Vec<_>>(),
                    vec!["started", "unstarted"]
                );
                assert_eq!(
                    results
                        .iter()
                        .filter(|entry| entry.data.as_ref().unwrap()["callId"] == "started")
                        .count(),
                    1
                );
                assert!(
                    decode_message(&results[0].model.as_ref().unwrap()[0])
                        .unwrap()
                        .eq(&ModelMessage::ToolResult {
                            call_id: "started".into(),
                            content: "Tool aborted; the operation may have partially or fully run."
                                .into(),
                            is_error: true
                        })
                );
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    });
}

#[test]
fn pending_model_does_not_block_session_and_dropped_run_waiter_does_not_stop_work() {
    block_on(async {
        let entered = Gate::default();
        let release = Gate::default();
        let agent = Agent::new(
            {
                let entered = entered.clone();
                let release = release.clone();
                move |_: ModelRequest, _: Cancellation| -> ModelFuture {
                    entered.release();
                    let release = release.clone();
                    Box::pin(async move {
                        release.wait().await;
                        Ok(response("eventually"))
                    })
                }
            },
            vec![],
        )
        .unwrap();
        let (session, sd) = Session::new(MemoryStorage::new());
        let (runner, td) =
            TaskRunner::attach(&session, TaskRegistry::new(agent.definitions()).unwrap()).unwrap();
        zip(
            async {
                let conversation = conversation(&session).await;
                let handle = admit(&session, agent, conversation, "wait", config(2)).await;
                drop(runner.run(handle.task_id));
                entered.wait().await;
                let marker = session
                    .commit(move |tx| {
                        Box::pin(async move {
                            tx.append_entry(conversation, EntryDraft::new("unrelated"))
                                .await
                        })
                    })
                    .await
                    .unwrap()
                    .value;
                assert_eq!(marker.kind, "unrelated");
                release.release();
                let mut polls = 0;
                loop {
                    let current = task(&session, handle.task_id).await;
                    if current.status() == TaskStatus::Terminal {
                        break;
                    }
                    polls += 1;
                    assert!(
                        polls < 100,
                        "dropped waiter did not leave the drive running"
                    );
                    futures_lite::future::yield_now().await;
                }
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    });
}

#[test]
fn malformed_supported_history_faults_before_constructing_a_model_future() {
    block_on(async {
        let calls = Rc::new(Cell::new(0));
        let agent = Agent::new(
            {
                let calls = calls.clone();
                move |_: ModelRequest, _: Cancellation| -> ModelFuture {
                    calls.set(calls.get() + 1);
                    Box::pin(async { Ok(response("must not run")) })
                }
            },
            vec![],
        )
        .unwrap();
        let (session, sd) = Session::new(MemoryStorage::new());
        let (runner, td) =
            TaskRunner::attach(&session, TaskRegistry::new(agent.definitions()).unwrap()).unwrap();
        zip(
            async {
                let conversation = conversation(&session).await;
                let mut bad = EntryDraft::new("agent.assistant");
                bad.model = Some(vec![json!({
                    "role":"assistant",
                    "text":"bad history",
                    "toolCalls":[],
                    "unsupported":true
                })]);
                session
                    .commit(move |tx| {
                        Box::pin(async move { tx.append_entry(conversation, bad).await })
                    })
                    .await
                    .unwrap();
                let handle = admit(&session, agent, conversation, "new user", config(2)).await;
                let done = terminal(runner.run(handle.task_id).await.unwrap());
                assert!(matches!(
                    done.state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Faulted { .. }
                    }
                ));
                assert_eq!(calls.get(), 0);
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    });
}

#[test]
fn panicking_tool_faults_the_child_and_parent_supplies_an_uncertain_result() {
    block_on(async {
        let model = FakeModel::new([
            Ok(calls(vec![call("panic-id", "panic", json!({}))], None)),
            Ok(response("continued safely")),
        ]);
        let agent = Agent::new(
            model.clone(),
            vec![tool(
                "panic",
                1,
                |_| Ok(()),
                |_, _| panic!("tool constructor panic"),
            )],
        )
        .unwrap();
        let (session, sd) = Session::new(MemoryStorage::new());
        let (runner, td) =
            TaskRunner::attach(&session, TaskRegistry::new(agent.definitions()).unwrap()).unwrap();
        zip(
            async {
                let conversation = conversation(&session).await;
                let handle = admit(&session, agent, conversation, "panic probe", config(3)).await;
                let done = terminal(runner.run(handle.task_id).await.unwrap());
                assert!(matches!(
                    done.state,
                    TaskState::Terminal {
                        outcome: TaskOutcome::Completed { .. }
                    }
                ));
                let log = entries(&session, conversation).await;
                let results = log
                    .iter()
                    .filter(|entry| entry.kind == "agent.toolResult")
                    .collect::<Vec<_>>();
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].data.as_ref().unwrap()["code"], "missing_result");
                let ModelMessage::ToolResult {
                    content, is_error, ..
                } = decode_message(&results[0].model.as_ref().unwrap()[0]).unwrap()
                else {
                    panic!("not a result")
                };
                assert!(is_error);
                assert!(content.contains("may have run"));
                assert!(model.requests.borrow()[1].messages.iter().any(|message| {
                    matches!(message, ModelMessage::ToolResult { call_id, .. } if call_id == "panic-id")
                }));
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(sd, td),
        )
        .await;
    });
}
