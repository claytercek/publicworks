use futures_lite::future::{block_on, or, zip};
use publicworks_agent::*;
use publicworks_runtime::*;
mod support;
use publicworks_storage_sqlite::SqliteStorage;
use serde_json::json;
use std::{
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
    task::Poll,
};
use support::{FakeModel, Gate, TempDatabase, bounded};

struct Database(TempDatabase);
impl Database {
    fn new() -> Self {
        Self(TempDatabase::new("publicworks-agent-", "agent.db"))
    }
    fn open(&self) -> SqliteStorage {
        SqliteStorage::open(self.0.path()).unwrap()
    }
}

fn final_response(text: &str) -> ModelResponse {
    ModelResponse {
        text: text.into(),
        tool_calls: vec![],
        usage: None,
    }
}
fn tool_response(id: &str, name: &str) -> ModelResponse {
    ModelResponse {
        text: String::new(),
        tool_calls: vec![ToolCall {
            id: id.into(),
            name: name.into(),
            arguments: json!({"opaque":u64::MAX}),
        }],
        usage: None,
    }
}
fn declaration(name: &str, version: u64) -> ToolDeclaration {
    ToolDeclaration {
        name: name.into(),
        version,
        description: "recovery probe".into(),
        parameters: json!({"type":"object"}),
    }
}
fn installed_tool(
    name: &str,
    version: u64,
    execute: impl Fn(ToolCall, Cancellation) -> ToolFuture + 'static,
) -> Tool {
    Tool::new(declaration(name, version), |_| Ok(()), execute)
}
fn config() -> TurnConfig {
    TurnConfig {
        model: "pinned-model".into(),
        instructions: "pinned instructions".into(),
        max_model_rounds: 4,
    }
}
async fn create_conversation(session: &Session) -> Id {
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
async fn admit(session: &Session, agent: Agent, conversation: Id, text: &str) -> TestTurn {
    let text = text.to_owned();
    let id = session
        .commit(move |tx| {
            agent.admit_input(
                tx,
                conversation,
                text,
                config(),
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
async fn entries(session: &Session, conversation: Id) -> Vec<EntryRecord> {
    let mut result = session
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
    result.reverse();
    result
}
fn terminal(result: RunResult) -> TaskRecord {
    match result {
        RunResult::Terminal(task) => task,
        other => panic!("expected terminal, got {other:?}"),
    }
}

#[test]
fn restart_before_model_commit_replays_the_pinned_request_not_later_context_or_installation() {
    block_on(bounded(async {
        let db = Database::new();
        let entered = Gate::default();
        let old_calls = Rc::new(Cell::new(0));
        let old_model = {
            let entered = entered.clone();
            let old_calls = old_calls.clone();
            move |request: ModelRequest, cancellation: Cancellation| -> ModelFuture {
                assert_eq!(request.model, "hook-selected-model");
                assert_eq!(request.instructions, "hook instructions");
                assert!(request.tools.is_empty());
                assert_eq!(
                    request.messages,
                    vec![ModelMessage::User {
                        text: "hook context".into()
                    }]
                );
                old_calls.set(old_calls.get() + 1);
                entered.release();
                Box::pin(async move {
                    cancellation.cancelled().await;
                    std::future::pending().await
                })
            }
        };
        let mut old_registry = AgentRegistry::new();
        old_registry
            .install(
                Extension::new(
                    "default",
                    vec![installed_tool("original", 7, |_, _| {
                        Box::pin(async { panic!("no tool call expected") })
                    })],
                )
                .with_hooks(LifecycleHooks {
                    before_request: Some(Rc::new(|mut request, _| {
                        Box::pin(async move {
                            assert_eq!(request.model, "pinned-model");
                            assert_eq!(request.instructions, "pinned instructions");
                            request.model = "hook-selected-model".into();
                            request.instructions = "hook instructions".into();
                            request.tools.clear();
                            request.messages = vec![ModelMessage::User {
                                text: "hook context".into(),
                            }];
                            Ok(Some(request))
                        })
                    })),
                    ..LifecycleHooks::default()
                }),
            )
            .unwrap();
        let old_agent = Agent::with_registry(old_model, old_registry.snapshot(), None);
        let (session, sd) = Session::new(db.open());
        let (runner, td) = TaskRunner::attach(
            &session,
            TaskRegistry::new(old_agent.definitions()).unwrap(),
        )
        .unwrap();
        let (conversation, handle) = zip(
            async {
                let conversation = create_conversation(&session).await;
                let handle =
                    admit(&session, old_agent.clone(), conversation, "original user").await;
                let run = runner.run(handle.task_id);
                entered.wait().await;
                let mut later = EntryDraft::new("agent.user");
                later.model = Some(vec![encode_message(&ModelMessage::User {
                    text: "later user".into(),
                })]);
                later.edits = Some(vec![ContextEdit::Replace {
                    target: handle.user_entry_id,
                    messages: vec![encode_message(&ModelMessage::User {
                        text: "edited original".into(),
                    })],
                }]);
                session
                    .commit(move |tx| {
                        Box::pin(async move { tx.append_entry(conversation, later).await })
                    })
                    .await
                    .unwrap();
                runner.close().await.unwrap();
                assert_eq!(run.await.unwrap(), RunResult::Interrupted);
                session.close().await.unwrap();
                (conversation, handle)
            },
            zip(sd, td),
        )
        .await
        .0;
        assert_eq!(old_calls.get(), 1);

        let resumed_model = FakeModel::new([Ok(final_response("resumed"))]);
        let resumed_agent = Agent::new(
            resumed_model.clone(),
            vec![
                installed_tool("original", 99, |_, _| {
                    Box::pin(async { panic!("version changed") })
                }),
                installed_tool("added-later", 1, |_, _| {
                    Box::pin(async { panic!("not offered") })
                }),
            ],
        )
        .unwrap();
        let (opening, sd) = Session::open_recovered(db.open());
        zip(
            async {
                let session = opening.await.unwrap().value;
                let (runner, td) = TaskRunner::attach(
                    &session,
                    TaskRegistry::new(resumed_agent.definitions()).unwrap(),
                )
                .unwrap();
                zip(
                    async {
                        let done = terminal(runner.run(handle.task_id).await.unwrap());
                        assert!(matches!(
                            done.state,
                            TaskState::Terminal {
                                outcome: TaskOutcome::Completed { .. }
                            }
                        ));
                        {
                            let requests = resumed_model.requests.borrow();
                            assert_eq!(requests.len(), 1);
                            let request = &requests[0];
                            assert_eq!(request.model, "pinned-model");
                            assert_eq!(request.instructions, "pinned instructions");
                            assert_eq!(request.tools, vec![declaration("original", 7)]);
                            assert_eq!(
                                request.messages,
                                vec![ModelMessage::User {
                                    text: "original user".into()
                                }]
                            );
                        }
                        assert_eq!(
                            entries(&session, conversation)
                                .await
                                .iter()
                                .filter(|entry| entry.kind == "agent.assistant")
                                .count(),
                            1
                        );
                        runner.close().await.unwrap();
                        session.close().await.unwrap();
                    },
                    td,
                )
                .await;
            },
            sd,
        )
        .await;
    }));
}

#[test]
fn default_unsafe_recovery_never_reexecutes_and_later_requests_resolve_current_offers() {
    block_on(bounded(async {
        let db = Database::new();
        let entered = Gate::default();
        let effects = Rc::new(Cell::new(0));
        let first_model = FakeModel::new([Ok(tool_response("same-call", "effect"))]);
        let old_agent = Agent::new(
            first_model,
            vec![installed_tool("effect", 1, {
                let entered = entered.clone();
                let effects = effects.clone();
                move |_, cancellation| {
                    effects.set(effects.get() + 1);
                    entered.release();
                    Box::pin(async move {
                        cancellation.cancelled().await;
                        std::future::pending().await
                    })
                }
            })],
        )
        .unwrap();
        let (session, sd) = Session::new(db.open());
        let (runner, td) = TaskRunner::attach(
            &session,
            TaskRegistry::new(old_agent.definitions()).unwrap(),
        )
        .unwrap();
        let (conversation, handle) = zip(
            async {
                let conversation = create_conversation(&session).await;
                let handle = admit(&session, old_agent.clone(), conversation, "run effect").await;
                let run = runner.run(handle.task_id);
                entered.wait().await;
                assert_eq!(
                    effects.get(),
                    1,
                    "effect starts only after the execute intent ACK"
                );
                runner.close().await.unwrap();
                assert_eq!(run.await.unwrap(), RunResult::Interrupted);
                session.close().await.unwrap();
                (conversation, handle)
            },
            zip(sd, td),
        )
        .await
        .0;

        let current_effects = Rc::new(Cell::new(0));
        let resumed_model = FakeModel::new([
            Ok(final_response("continued from uncertainty")),
            Ok(tool_response("new-id", "effect")),
            Ok(final_response("continued from current tools")),
        ]);
        let current_agent = Agent::new(
            resumed_model.clone(),
            vec![installed_tool("effect", 2, {
                let current_effects = current_effects.clone();
                move |_, _| {
                    current_effects.set(current_effects.get() + 1);
                    Box::pin(async {
                        Ok(ToolResult {
                            content: "current effect".into(),
                            is_error: false,
                            usage: None,
                        })
                    })
                }
            })],
        )
        .unwrap();
        let (opening, sd) = Session::open_recovered(db.open());
        zip(async {
            let session = opening.await.unwrap().value;
            let (runner, td) = TaskRunner::attach(&session, TaskRegistry::new(current_agent.definitions()).unwrap()).unwrap();
            zip(async {
                terminal(runner.run(handle.task_id).await.unwrap());
                assert_eq!(effects.get(), 1, "the interrupted call must not run a second time");
                assert_eq!(current_effects.get(), 0, "registry changes cannot turn uncertainty into execution");
                let log = entries(&session, conversation).await;
                let uncertain = log.iter().find(|entry| entry.kind == "agent.toolResult").unwrap();
                assert_eq!(uncertain.data.as_ref().unwrap()["callId"], "same-call");
                assert_eq!(uncertain.data.as_ref().unwrap()["code"], "interrupted_effect");
                let ModelMessage::ToolResult { content, is_error, .. } = decode_message(&uncertain.model.as_ref().unwrap()[0]).unwrap() else { panic!("not a result") };
                assert!(is_error);
                assert!(content.contains("may have partially or fully run"));
                assert!(content.contains("not replayed"));
                assert!(resumed_model.requests.borrow()[0].messages.iter().any(|message| matches!(message, ModelMessage::ToolResult { call_id, .. } if call_id == "same-call")));

                // Admission no longer pins tools. A later preparation uses this
                // runner's version 2 installation, even when admitted by an old Agent.
                let next = admit(&session, old_agent, conversation, "new request").await;
                terminal(runner.run(next.task_id).await.unwrap());
                assert_eq!(current_effects.get(), 1);
                let current_result = entries(&session, conversation).await.into_iter()
                    .filter(|entry| entry.kind == "agent.toolResult")
                    .find(|entry| entry.data.as_ref().unwrap()["callId"] == "new-id")
                    .unwrap();
                assert!(current_result.data.as_ref().unwrap().get("code").is_none());
                let ModelMessage::ToolResult { content, .. } = decode_message(&current_result.model.as_ref().unwrap()[0]).unwrap() else { panic!("not a result") };
                assert_eq!(content, "current effect");
                assert_eq!(resumed_model.requests.borrow()[1].tools[0].version, 2);
                runner.close().await.unwrap();
                session.close().await.unwrap();
            }, td).await;
        }, sd).await;
    }));
}

/// Commits the selected batch to SQLite, then deliberately withholds its ACK.
/// Dropping the drivers simulates a process loss at that durable boundary.
struct ResultCommitGate {
    inner: SqliteStorage,
    committed: Gate,
    armed: bool,
}
impl ResultCommitGate {
    fn new(inner: SqliteStorage, committed: Gate) -> Self {
        Self {
            inner,
            committed,
            armed: true,
        }
    }
}
impl Storage for ResultCommitGate {
    publicworks_runtime::forward_storage_methods!(inner;
        mint_id,
        conversation,
        scan_conversations,
        task,
        scan_tasks,
        submission,
        scan_submissions,
        submission_by_request,
        conversation_state,
        entry,
        visible_entry,
        scan_entries,
        find_latest_head_marker,
        close,
    );
    fn commit(&mut self, writes: Vec<StorageWrite>) -> StorageFuture<'_, Seq> {
        let is_result = self.armed && writes.iter().any(|write| matches!(write, StorageWrite::Entry(entry) if entry.kind == "agent.toolResult"));
        if is_result {
            self.armed = false;
        }
        let committed = self.committed.clone();
        Box::pin(async move {
            let seq = self.inner.commit(writes).await?;
            if is_result {
                committed.release();
                std::future::pending::<()>().await;
            }
            Ok(seq)
        })
    }
}

#[test]
fn restart_after_result_commit_before_parent_resume_does_not_repeat_effect_or_result() {
    block_on(bounded(async {
        let db = Database::new();
        let committed = Gate::default();
        let effects = Rc::new(Cell::new(0));
        let model = FakeModel::new([Ok(tool_response("once", "effect"))]);
        let agent = Agent::new(
            model,
            vec![installed_tool("effect", 1, {
                let effects = effects.clone();
                move |_, _| {
                    effects.set(effects.get() + 1);
                    Box::pin(async {
                        Ok(ToolResult {
                            content: "durable result".into(),
                            is_error: false,
                            usage: Some(json!({"charged":1})),
                        })
                    })
                }
            })],
        )
        .unwrap();
        let storage = ResultCommitGate::new(db.open(), committed.clone());
        let (session, mut sd) = Session::new(storage);
        let (runner, mut td) =
            TaskRunner::attach(&session, TaskRegistry::new(agent.definitions()).unwrap()).unwrap();
        let (conversation, root) = or(
            async {
                let conversation = create_conversation(&session).await;
                let root = admit(&session, agent, conversation, "durable result probe").await;
                drop(runner.run(root.task_id));
                committed.wait().await;
                (conversation, root)
            },
            async {
                zip(&mut sd, &mut td).await;
                std::future::pending().await
            },
        )
        .await;
        assert_eq!(effects.get(), 1);
        // Do not close: the test point is after SQLite committed but before Session
        // observed the ACK. Driver drop models process loss rather than abort intent.
        drop(runner);
        drop(session);
        drop(td);
        drop(sd);

        let resumed_model = FakeModel::new([Ok(final_response("after durable result"))]);
        let resumed_agent = Agent::new(
            resumed_model.clone(),
            vec![installed_tool("effect", 1, {
                let effects = effects.clone();
                move |_, _| {
                    effects.set(effects.get() + 1);
                    Box::pin(async {
                        Ok(ToolResult {
                            content: "duplicate".into(),
                            is_error: false,
                            usage: None,
                        })
                    })
                }
            })],
        )
        .unwrap();
        let (opening, sd) = Session::open_recovered(db.open());
        zip(async {
            let session = opening.await.unwrap().value;
            let (runner, td) = TaskRunner::attach(&session, TaskRegistry::new(resumed_agent.definitions()).unwrap()).unwrap();
            zip(async {
                terminal(runner.run(root.task_id).await.unwrap());
                assert_eq!(effects.get(), 1, "durably completed tool must not execute again");
                let log = entries(&session, conversation).await;
                let results = log.iter().filter(|entry| entry.kind == "agent.toolResult").collect::<Vec<_>>();
                assert_eq!(results.len(), 1, "parent resume must not duplicate the result entry");
                assert_eq!(results[0].data.as_ref().unwrap()["callId"], "once");
                assert_eq!(results[0].data.as_ref().unwrap()["usage"], json!({"charged":1}));
                assert!(resumed_model.requests.borrow()[0].messages.iter().any(|message| matches!(message, ModelMessage::ToolResult { call_id, content, is_error:false } if call_id == "once" && content == "durable result")));
                runner.close().await.unwrap();
                session.close().await.unwrap();
            }, td).await;
        }, sd).await;
    }));
}

#[path = "recovery/replay.rs"]
mod replay;
