use super::*;
use std::cell::RefCell;
fn root_conversation() -> StorageWrite {
    StorageWrite::Conversation(ConversationRecord {
        id: ROOT_CONVERSATION,
        parent: None,
        owner: None,
    })
}

pub(super) fn fixture() -> (TaskRecord, ToolBatch, TaskRecord, EntryRecord, EntryRecord) {
    let batch = ToolBatch {
        round: 1,
        assistant: Id::new(10).unwrap(),
        index: 0,
        child: Id::new(11).unwrap(),
        offers: BTreeMap::from([("t".into(), 7)]),
    };
    let root = TaskRecord {
        id: Id::new(2).unwrap(),
        conversation_id: ROOT_CONVERSATION,
        kind: TURN_KIND.into(),
        version: TURN_VERSION,
        input: json!({"config":{"model":"m","instructions":"","maxModelRounds":2}}),
        owner: None,
        background: false,
        abort_requested: false,
        memos: None,
        state: TaskState::Waiting {
            checkpoint: TurnCheckpoint::Tools(batch.clone()).encode().unwrap(),
            on: vec![batch.child],
            policy: JoinPolicy::AllSettled,
        },
    };
    let child = TaskRecord {
        id: batch.child,
        conversation_id: ROOT_CONVERSATION,
        kind: TOOL_KIND.into(),
        version: TOOL_VERSION,
        input: ToolInput {
            assistant: batch.assistant,
            index: 0,
        }
        .encode(),
        owner: Some(root.id),
        background: false,
        abort_requested: false,
        memos: None,
        state: TaskState::Terminal {
            outcome: TaskOutcome::Completed {
                result: json!({"resultEntryId":12}),
            },
        },
    };
    let mut assistant = EntryRecord::new(batch.assistant, ROOT_CONVERSATION, "agent.assistant");
    assistant.by_task_id = Some(root.id);
    assistant.model = Some(vec![encode_message(&ModelMessage::Assistant {
        text: "".into(),
        tool_calls: vec![ToolCall {
            id: "call".into(),
            name: "t".into(),
            arguments: json!({"original":true}),
        }],
    })]);
    let mut result = EntryRecord::new(Id::new(12).unwrap(), ROOT_CONVERSATION, "agent.toolResult");
    result.data = Some(json!({"assistantEntryId":10,"callId":"call","usage":null}));
    result.model = Some(vec![encode_message(&ModelMessage::ToolResult {
        call_id: "call".into(),
        content: "host supplied".into(),
        is_error: false,
    })]);
    (root, batch, child, assistant, result)
}

struct Scans {
    inner: MemoryStorage,
    queries: Rc<RefCell<Vec<EntryQuery>>>,
}
impl Storage for Scans {
    publicworks_runtime::forward_storage_methods!(inner; mint_id,commit,conversation,scan_conversations,task,scan_tasks,submission,scan_submissions,submission_by_request,conversation_state,entry,visible_entry,find_latest_head_marker,close);
    fn scan_entries(
        &mut self,
        query: EntryQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<EntryRecord>> {
        self.queries.borrow_mut().push(query.clone());
        self.inner.scan_entries(query, limit, cursor)
    }
}

#[test]
fn host_inserted_result_is_preserved_in_terminal_child_receipt() {
    use publicworks_runtime::test_support::{Gate, bounded};
    block_on(bounded(async {
        let entered = Gate::default();
        let resume = Gate::default();
        let invoked = Rc::new(std::cell::Cell::new(false));
        let model_invoked = invoked.clone();
        let tool = Tool::new(
            ToolDeclaration {
                name: "t".into(),
                version: 7,
                description: "".into(),
                parameters: json!({}),
            },
            |_| Ok(()),
            {
                let entered = entered.clone();
                let resume = resume.clone();
                move |_, _| {
                    let entered = entered.clone();
                    let resume = resume.clone();
                    Box::pin(async move {
                        entered.release();
                        resume.wait().await;
                        Ok(ToolResult {
                            content: "callback result".into(),
                            is_error: false,
                            usage: None,
                        })
                    })
                }
            },
        );
        let agent = Agent::new(
            move |_: ModelRequest, _: Cancellation| -> ModelFuture {
                let calls = if model_invoked.replace(true) {
                    vec![]
                } else {
                    vec![ToolCall {
                        id: "call".into(),
                        name: "t".into(),
                        arguments: Value::Null,
                    }]
                };
                Box::pin(async move {
                    Ok(ModelResponse {
                        text: "answer".into(),
                        tool_calls: calls,
                        usage: None,
                    })
                })
            },
            vec![tool],
        )
        .unwrap();
        let (open, driver) = Harness::open(
            MemoryStorage::new(),
            TaskRegistry::new(agent.definitions()).unwrap(),
        );
        zip(
            async {
                let harness = open.await.unwrap();
                let conversation = harness
                    .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
                    .await
                    .unwrap()
                    .value
                    .id;
                let submission = agent
                    .submit(
                        &harness,
                        conversation,
                        "go",
                        TurnConfig {
                            model: "m".into(),
                            instructions: "".into(),
                            max_model_rounds: 2,
                        },
                        SubmitOptions::default(),
                    )
                    .await
                    .unwrap();
                entered.wait().await;
                let existing = harness
                    .commit(move |tx| {
                        Box::pin(async move {
                            let entries = tx
                                .scan_entries(EntryQuery::new(conversation), 128, None)
                                .await?
                                .items;
                            let assistant = entries
                                .iter()
                                .find(|e| e.kind == "agent.assistant")
                                .unwrap()
                                .id;
                            append_result(
                                tx,
                                conversation,
                                assistant,
                                "call",
                                ToolResult {
                                    content: "host supplied".into(),
                                    is_error: false,
                                    usage: Some(Value::Null),
                                },
                                None,
                            )
                            .await
                        })
                    })
                    .await
                    .unwrap()
                    .value;
                resume.release();
                submission.wait().await.unwrap();
                harness
                    .commit(move |tx| {
                        Box::pin(async move {
                            let child = tx
                                .scan_tasks(
                                    TaskQuery {
                                        conversation_id: Some(conversation),
                                        kind: Some(TOOL_KIND.into()),
                                        ..TaskQuery::default()
                                    },
                                    128,
                                    None,
                                )
                                .await?
                                .items
                                .remove(0);
                            assert_eq!(
                                child.state,
                                TaskState::Terminal {
                                    outcome: TaskOutcome::Completed {
                                        result: json!({"resultEntryId":existing.get()})
                                    }
                                }
                            );
                            let entries = tx
                                .scan_entries(EntryQuery::new(conversation), 128, None)
                                .await?
                                .items;
                            assert_eq!(
                                entries
                                    .iter()
                                    .filter(|e| e.kind == "agent.toolResult")
                                    .count(),
                                1
                            );
                            Ok(())
                        })
                    })
                    .await
                    .unwrap();
                harness.close().await.unwrap();
            },
            driver,
        )
        .await;
    }));
}

#[test]
fn completing_owner_recovery_aborts_child_using_immutable_provenance() {
    use publicworks_runtime::test_support::bounded;
    block_on(bounded(async {
        for executing in [false, true] {
            let (mut root, batch, mut child, assistant, _) = fixture();
            let outcome = TaskOutcome::Faulted {
                error: TaskOutcomeError {
                    message: "owner fault".into(),
                    detail: None,
                },
            };
            root.state = TaskState::Completing {
                outcome: outcome.clone(),
            };
            child.state = TaskState::Pending {
                checkpoint: if executing {
                    ToolCheckpoint::Execute {
                        arguments: json!({"effective":true}),
                        policy: ReplayPolicy::Safe,
                    }
                    .encode()
                    .unwrap()
                } else {
                    ToolCheckpoint::Call.encode().unwrap()
                },
            };
            let root_id = root.id;
            let mut storage = MemoryStorage::new();
            // Keep newly appended result IDs above the seeded immutable records.
            for _ in 0..12 {
                storage.mint_id().await.unwrap();
            }
            storage
                .commit(vec![
                    root_conversation(),
                    StorageWrite::Task(root),
                    StorageWrite::Task(child),
                    StorageWrite::Entry(assistant),
                ])
                .await
                .unwrap();
            let agent = agent();
            let (open, driver) =
                Harness::open(storage, TaskRegistry::new(agent.definitions()).unwrap());
            zip(
                async {
                    let harness = open.await.unwrap();
                    harness.resume().unwrap();
                    let settled = harness.wait_task(root_id).await.unwrap();
                    assert_eq!(settled.state, TaskState::Terminal { outcome });
                    harness
                        .commit(move |tx| {
                            Box::pin(async move {
                                let child = tx.task(batch.child).await?.unwrap();
                                let TaskState::Terminal {
                                    outcome:
                                        TaskOutcome::Aborted {
                                            result: Some(receipt),
                                            ..
                                        },
                                } = child.state
                                else {
                                    panic!("{:?}", child.state)
                                };
                                let result = tx
                                    .visible_entry(
                                        ROOT_CONVERSATION,
                                        id(&receipt, "resultEntryId")?,
                                    )
                                    .await?
                                    .unwrap()
                                    .entry;
                                assert_eq!(result.by_task_id, Some(batch.child));
                                assert_eq!(result.data.as_ref().unwrap()["code"], "aborted");
                                let ModelMessage::ToolResult { content, .. } =
                                    decode_message(&result.model.unwrap()[0])?
                                else {
                                    unreachable!()
                                };
                                assert_eq!(content.contains("may have"), executing);
                                Ok(())
                            })
                        })
                        .await
                        .unwrap();
                    harness.close().await.unwrap();
                },
                driver,
            )
            .await;
        }
    }));
}

#[test]
fn receipt_is_direct_and_validates_pointer_child_and_message_associations() {
    block_on(async {
        for case in 0..14 {
            let (root, batch, mut child, assistant, mut result) = fixture();
            let expect_fallback = case == 12 || case == 13;
            match case {
                0 => {} // external host entry, with a real entry ID and null usage
                1 => child.owner = None,
                2 => child.input["index"] = json!(1),
                3 => child.input["assistantEntryId"] = json!(9),
                4 => child.version -= 1,
                5 => result.data.as_mut().unwrap()["callId"] = json!("other"),
                6 => result.model.as_mut().unwrap()[0]["callId"] = json!("other"),
                7 => result.data.as_mut().unwrap()["assistantEntryId"] = json!(9),
                8 => result.kind = "host.note".into(),
                9..=11 => {
                    child.state = TaskState::Terminal {
                        outcome: TaskOutcome::Completed {
                            result: match case {
                                9 => json!({"resultEntryId":null}),
                                10 => json!({"resultEntryId":999}),
                                _ => json!({}),
                            },
                        },
                    }
                }
                12 => {
                    child.state = TaskState::Terminal {
                        outcome: TaskOutcome::Faulted {
                            error: TaskOutcomeError {
                                message: "panic".into(),
                                detail: None,
                            },
                        },
                    }
                }
                13 => {
                    child.state = TaskState::Terminal {
                        outcome: TaskOutcome::Orphaned {
                            reason: "missing code".into(),
                        },
                    }
                }
                _ => unreachable!(),
            }
            let mut inner = MemoryStorage::new();
            inner
                .commit(vec![
                    root_conversation(),
                    StorageWrite::Task(root.clone()),
                    StorageWrite::Task(child),
                    StorageWrite::Entry(assistant),
                    StorageWrite::Entry(result),
                ])
                .await
                .unwrap();
            let queries = Rc::new(RefCell::new(Vec::new()));
            let (session, driver) = Session::new(Scans {
                inner,
                queries: queries.clone(),
            });
            zip(
                async {
                    let result = session
                        .commit(move |tx| {
                            Box::pin(async move {
                                let calls = assistant_calls(tx, &root, batch.assistant).await?;
                                child_receipt(tx, &root, &batch, &calls[0]).await
                            })
                        })
                        .await;
                    if case == 0 {
                        let receipt = result.unwrap().value.unwrap();
                        assert_eq!(receipt.id, Id::new(12).unwrap());
                        assert_eq!(receipt.result.usage, Some(Value::Null));
                        assert_eq!(receipt.result.content, "host supplied");
                    } else if expect_fallback {
                        assert!(result.unwrap().value.is_none());
                    } else {
                        assert!(result.is_err(), "accepted case {case}");
                    }
                    assert!(
                        queries.borrow().is_empty(),
                        "receipt must not scan transcript"
                    );
                    session.close().await.unwrap();
                },
                driver,
            )
            .await;
        }
    });
}

#[test]
fn tool_context_uses_immutable_calls_and_rejects_invalid_provenance_before_callbacks() {
    block_on(async {
        for case in 0..10 {
            let (mut root, _, mut child, mut assistant, _) = fixture();
            match case {
                0 => {}
                1 => assistant.by_task_id = None,
                2 => assistant.kind = "host.note".into(),
                3 => assistant.conversation_id = Id::new(99).unwrap(),
                4 => child.owner = None,
                5 => child.input["index"] = json!(1),
                6 => child.background = true,
                7 => root.owner = Some(Id::new(3).unwrap()),
                8 => root.version -= 1,
                9 => {
                    let TaskState::Waiting { checkpoint, .. } = &mut root.state else {
                        unreachable!()
                    };
                    checkpoint["child"] = json!(99);
                }
                _ => unreachable!(),
            }
            let mut storage = MemoryStorage::new();
            storage
                .commit(vec![
                    root_conversation(),
                    StorageWrite::Task(root),
                    StorageWrite::Entry(assistant),
                ])
                .await
                .unwrap();
            let (session, driver) = Session::new(storage);
            zip(
                async {
                    let found = session
                        .commit(move |tx| Box::pin(async move { tool_context(tx, &child).await }))
                        .await;
                    if case == 0 {
                        let (_, call, version) = found.unwrap().value;
                        assert_eq!(call.arguments, json!({"original":true}));
                        assert_eq!(version, Some(7));
                    } else {
                        assert!(found.is_err(), "accepted case {case}");
                    }
                    session.close().await.unwrap();
                },
                driver,
            )
            .await;
        }
    });
}

#[test]
fn collector_bounds_history_uses_first_duplicate_and_ignores_projection_edits() {
    block_on(async {
        let (_, _, _, mut assistant, mut result) = fixture();
        assistant.id = Id::new(300).unwrap();
        result.id = Id::new(301).unwrap();
        result.data.as_mut().unwrap()["assistantEntryId"] = json!(300);
        let mut duplicate = result.clone();
        duplicate.id = Id::new(302).unwrap();
        duplicate.model.as_mut().unwrap()[0]["content"] = json!("later duplicate");
        let mut edit = EntryRecord::new(Id::new(303).unwrap(), ROOT_CONVERSATION, "host.edit");
        edit.edits = Some(vec![ContextEdit::Omit { target: result.id }]);
        edit.head = Some(edit.id);
        let mut inner = MemoryStorage::new();
        let mut writes: Vec<_> = (2..300)
            .map(|n| {
                StorageWrite::Entry(EntryRecord::new(
                    Id::new(n).unwrap(),
                    ROOT_CONVERSATION,
                    "unrelated",
                ))
            })
            .collect();
        writes.extend([
            root_conversation(),
            StorageWrite::Entry(assistant),
            StorageWrite::Entry(result),
            StorageWrite::Entry(duplicate),
            StorageWrite::Entry(edit),
        ]);
        inner.commit(writes).await.unwrap();
        let queries = Rc::new(RefCell::new(Vec::new()));
        let (session, driver) = Session::new(Scans {
            inner,
            queries: queries.clone(),
        });
        zip(
            async {
                let found = session
                    .commit(|tx| {
                        Box::pin(async move {
                            collect_results(tx, ROOT_CONVERSATION, Id::new(300).unwrap()).await
                        })
                    })
                    .await
                    .unwrap()
                    .value;
                assert_eq!(found.len(), 1);
                assert_eq!(found["call"].id, Id::new(301).unwrap());
                assert_eq!(found["call"].result.content, "host supplied");
                assert_eq!(queries.borrow().len(), 1);
                assert_eq!(
                    queries.borrow()[0].min_entry_id,
                    Some(Id::new(300).unwrap())
                );
                session.close().await.unwrap();
            },
            driver,
        )
        .await;
    });
}
