use crate::{Gate, forward_storage_methods};
use futures_lite::future::zip;
use publicworks_runtime::*;
use serde_json::{Value, json};
use std::{cell::RefCell, rc::Rc};

async fn wait(harness: &Harness, id: Id) -> Result<SubmissionRecord, HarnessError> {
    harness.submission(id).await?.unwrap().wait().await
}

async fn initialized<S: Storage>(mut storage: S) -> S {
    storage
        .commit(vec![StorageWrite::Conversation(ConversationRecord {
            id: ROOT_CONVERSATION,
            parent: None,
            owner: None,
        })])
        .await
        .unwrap();
    storage
}
fn options(conversation: Id) -> TaskOptions {
    TaskOptions {
        conversation_id: Some(conversation),
        ownership: TaskOwnership::Conversation,
        background: false,
    }
}
fn definition(mode: &'static str) -> TaskDefinition {
    let handler: PhaseHandler = Rc::new(move |task, runtime| {
        Box::pin(async move {
            match mode {
                "panic" => panic!("opaque handler panic"),
                "no-progress" => return Ok(()),
                "error" => {
                    return Err(TaskOutcomeError {
                        message: "handler error".into(),
                        detail: None,
                    });
                }
                _ => {}
            }
            runtime
                .commit(move |tx, _| {
                    Box::pin(async move {
                        if matches!(mode, "specific" | "successor" | "answered") {
                            let state = tx.conversation_state(task.conversation_id).await?.unwrap();
                            let settlement = if mode == "answered" {
                                SubmissionSettlement::Done {
                                    answer: tx
                                        .append_entry(
                                            task.conversation_id,
                                            EntryDraft::new("answer"),
                                        )
                                        .await?
                                        .id,
                                }
                            } else {
                                SubmissionSettlement::Unanswered {
                                    reason: "specific".into(),
                                    detail: Some(Value::Null),
                                }
                            };
                            for id in &state.run.as_ref().unwrap().input_submission_ids {
                                tx.settle_submission(*id, settlement.clone()).await?;
                            }
                            if mode == "successor" {
                                let successor = tx
                                    .create_task(
                                        definition("unregistered"),
                                        Value::Null,
                                        options(task.conversation_id),
                                    )
                                    .await?;
                                let entry = tx
                                    .append_entry(
                                        task.conversation_id,
                                        EntryDraft::new("successor"),
                                    )
                                    .await?;
                                let input = tx
                                    .create_submission(
                                        task.conversation_id,
                                        SubmissionType::Input,
                                        None,
                                    )
                                    .await?;
                                tx.place_submission(input.id, entry.id).await?;
                                tx.update_conversation_state(task.conversation_id, move |state| {
                                    state.run = Some(ConversationRun {
                                        task_id: successor.id,
                                        input_submission_ids: vec![input.id],
                                    });
                                })
                                .await?;
                            }
                        }
                        Ok(Some(match mode {
                            "failed" => TaskUpdate::Fail(
                                TaskOutcomeError {
                                    message: "failed".into(),
                                    detail: None,
                                },
                                Some(json!({"answer": "not interpreted"})),
                            ),
                            "aborted" => TaskUpdate::Abort {
                                reason: Some("custom".into()),
                                result: Some(json!({"answer": "not interpreted"})),
                            },
                            _ => TaskUpdate::Complete(
                                json!({"status": "done", "answer": "not interpreted"}),
                            ),
                        }))
                    })
                })
                .await
                .unwrap();
            Ok(())
        })
    });
    TaskDefinition::new(
        mode,
        1,
        move |_| {
            Ok(match mode {
                "malformed" => Value::Null,
                "unknown-phase" => json!({"phase": "missing"}),
                _ => json!({"phase": "run"}),
            })
        },
        [("run".into(), handler)].into(),
    )
}

#[derive(Clone, Debug)]
struct Seed {
    task: Id,
    placed: Vec<Id>,
    queued: Id,
    write: Id,
}
async fn seed(tx: &Tx, conversation: Id, def: TaskDefinition) -> Result<Seed, SessionError> {
    let task = tx
        .create_task(def, Value::Null, options(conversation))
        .await?
        .id;
    let mut placed = Vec::new();
    for _ in 0..2 {
        let entry = tx
            .append_entry(conversation, EntryDraft::new("input"))
            .await?
            .id;
        let input = tx
            .create_submission(conversation, SubmissionType::Input, None)
            .await?
            .id;
        tx.place_submission(input, entry).await?;
        placed.push(input);
    }
    let (queued, write) = queue(tx, conversation).await?;
    let ids = placed.clone();
    tx.update_conversation_state(conversation, move |state| {
        state.run = Some(ConversationRun {
            task_id: task,
            input_submission_ids: ids,
        });
    })
    .await?;
    Ok(Seed {
        task,
        placed,
        queued,
        write,
    })
}
async fn queue(tx: &Tx, conversation: Id) -> Result<(Id, Id), SessionError> {
    let input = tx
        .create_submission(conversation, SubmissionType::Input, None)
        .await?
        .id;
    let write = tx
        .create_submission(conversation, SubmissionType::Write, None)
        .await?
        .id;
    tx.update_conversation_state(conversation, move |state| {
        state.agent_config = Some(json!({"opaque": true}));
        state
            .inbox
            .extend([input, write].map(|submission_id| InboxItem {
                submission_id,
                payload: json!({"not": "an agent payload"}),
            }));
    })
    .await?;
    Ok((input, write))
}
fn unanswered(record: &SubmissionRecord, expected: &str, placed: bool) {
    assert!(
        matches!(&record.state, SubmissionState::InputUnanswered { reason, entry, .. }
        if reason == expected && entry.is_some() == placed),
        "{record:?}"
    );
}

pub async fn terminal_paths<S: Storage + 'static>(make: impl Fn() -> S) {
    for mode in [
        "panic",
        "no-progress",
        "error",
        "malformed",
        "unknown-phase",
        "completed",
        "failed",
        "aborted",
        "default-abort",
        "orphan",
        "version-mismatch",
        "specific",
        "successor",
        "answered",
    ] {
        let def = definition(mode);
        let registry = if mode == "orphan" {
            TaskRegistry::default()
        } else if mode == "version-mismatch" {
            TaskRegistry::new([TaskDefinition::new(
                mode,
                2,
                |_| Ok(Value::Null),
                Default::default(),
            )])
            .unwrap()
        } else {
            TaskRegistry::new([def.clone()]).unwrap()
        };
        let (opening, driver) = Harness::open(initialized(make()).await, registry);
        zip(
            async {
                let harness = opening.await.unwrap();
                let ids = harness
                    .commit(move |tx| {
                        Box::pin(async move { seed(tx, ROOT_CONVERSATION, def).await })
                    })
                    .await
                    .unwrap()
                    .value;
                if matches!(mode, "default-abort" | "orphan" | "version-mismatch") {
                    harness.abort(ids.task).await.unwrap();
                }
                let expected = match mode {
                    "aborted" | "default-abort" => "aborted",
                    "specific" | "successor" => "specific",
                    _ => "faulted",
                };
                // Waiters register before the scheduler can settle the run. They must
                // see the assembly-derived receipts, not the pre-assembly candidates.
                let (first, second) =
                    zip(wait(&harness, ids.placed[0]), wait(&harness, ids.placed[1])).await;
                for receipt in [first.unwrap(), second.unwrap()] {
                    if mode == "answered" {
                        assert!(matches!(receipt.state, SubmissionState::InputDone { .. }));
                    } else {
                        unanswered(&receipt, expected, true);
                    }
                    if matches!(mode, "specific" | "successor") {
                        assert!(matches!(
                            receipt.state,
                            SubmissionState::InputUnanswered {
                                detail: Some(Value::Null),
                                ..
                            }
                        ));
                    }
                }
                let task = harness.wait_task(ids.task).await.unwrap();
                assert_eq!(task.status(), TaskStatus::Terminal);
                harness
                    .commit(move |tx| {
                        Box::pin(async move {
                            let state = tx.conversation_state(ROOT_CONVERSATION).await?.unwrap();
                            if mode == "successor" {
                                let run = state.run.unwrap();
                                assert_ne!(run.task_id, ids.task);
                                assert_eq!(
                                    tx.submission(run.input_submission_ids[0])
                                        .await?
                                        .unwrap()
                                        .status(),
                                    SubmissionStatus::Placed
                                );
                            } else {
                                assert!(state.run.is_none());
                            }
                            assert_eq!(
                                state
                                    .inbox
                                    .iter()
                                    .map(|item| item.submission_id)
                                    .collect::<Vec<_>>(),
                                [ids.queued, ids.write]
                            );
                            assert_eq!(
                                tx.submission(ids.queued).await?.unwrap().state,
                                SubmissionState::InputQueued
                            );
                            assert_eq!(
                                tx.submission(ids.write).await?.unwrap().state,
                                SubmissionState::WriteQueued
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
    }
}

pub async fn held_outcome<S: Storage + 'static>(storage: S) {
    let received = Gate::default();
    let sent = received.clone();
    let handler: PhaseHandler = Rc::new(move |_, runtime| {
        let sent = sent.clone();
        Box::pin(async move {
            runtime
                .commit(|_, _| Box::pin(async { Ok(Some(TaskUpdate::Complete(Value::Null))) }))
                .await
                .unwrap();
            sent.release();
            Ok(())
        })
    });
    let def = TaskDefinition::new(
        "held",
        1,
        |_| Ok(json!({"phase":"run"})),
        [("run".into(), handler)].into(),
    );
    let (opening, driver) = Harness::open(
        initialized(storage).await,
        TaskRegistry::new([def.clone()]).unwrap(),
    );
    zip(
        async {
            let harness = opening.await.unwrap();
            let ids = harness
                .commit(move |tx| Box::pin(async move { seed(tx, ROOT_CONVERSATION, def).await }))
                .await
                .unwrap()
                .value;
            let parent = ids.task;
            let child = harness
                .commit(move |tx| {
                    Box::pin(async move {
                        tx.create_task(
                            definition("unregistered"),
                            Value::Null,
                            TaskOptions {
                                ownership: TaskOwnership::Task(parent),
                                ..options(ROOT_CONVERSATION)
                            },
                        )
                        .await
                    })
                })
                .await
                .unwrap()
                .value
                .id;
            harness.resume().unwrap();
            received.wait().await;
            let observed = ids.clone();
            harness
                .commit(move |tx| {
                    Box::pin(async move {
                        assert_eq!(
                            tx.task(parent).await?.unwrap().status(),
                            TaskStatus::Completing
                        );
                        assert_eq!(
                            tx.conversation_state(ROOT_CONVERSATION)
                                .await?
                                .unwrap()
                                .run
                                .unwrap()
                                .task_id,
                            parent
                        );
                        for id in observed.placed {
                            assert_eq!(
                                tx.submission(id).await?.unwrap().status(),
                                SubmissionStatus::Placed
                            );
                        }
                        Ok(())
                    })
                })
                .await
                .unwrap();
            harness.abort(child).await.unwrap();
            for id in ids.placed {
                unanswered(&wait(&harness, id).await.unwrap(), "faulted", true);
            }
            assert_eq!(
                harness.wait_task(parent).await.unwrap().status(),
                TaskStatus::Terminal
            );
            harness.close().await.unwrap();
        },
        driver,
    )
    .await;
}

pub async fn conversation_scopes<S: Storage + 'static>(make: impl Fn() -> S) {
    for background in [false, true] {
        let (opening, driver) = Harness::open(initialized(make()).await, TaskRegistry::default());
        zip(
            async {
                let harness = opening.await.unwrap();
                let scopes = harness
                    .commit(|tx| {
                        Box::pin(async move {
                            let owner = tx
                                .create_task(
                                    definition("missing"),
                                    Value::Null,
                                    options(ROOT_CONVERSATION),
                                )
                                .await?
                                .id;
                            let leaf = tx.create_conversation_owned(Owner::Task(owner)).await?.id;
                            let nested = tx.create_conversation_owned(Owner::Task(owner)).await?.id;
                            let nested_owner = tx
                                .create_task(definition("missing"), Value::Null, options(nested))
                                .await?
                                .id;
                            let nested_leaf = tx
                                .create_conversation_owned(Owner::Task(nested_owner))
                                .await?
                                .id;
                            let bg = tx
                                .create_task(
                                    definition("missing"),
                                    Value::Null,
                                    TaskOptions {
                                        background: true,
                                        ..options(ROOT_CONVERSATION)
                                    },
                                )
                                .await?
                                .id;
                            let bg_leaf = tx.create_conversation_owned(Owner::Task(bg)).await?.id;
                            let unrelated = tx.create_conversation().await?.id;
                            let mut scopes = Vec::new();
                            for c in [
                                ROOT_CONVERSATION,
                                leaf,
                                nested,
                                nested_leaf,
                                bg_leaf,
                                unrelated,
                            ] {
                                let (input, write) = queue(tx, c).await?;
                                scopes.push((c, input, write));
                            }
                            Ok(scopes)
                        })
                    })
                    .await
                    .unwrap()
                    .value;
                harness
                    .conversation(ROOT_CONVERSATION)
                    .await
                    .unwrap()
                    .unwrap()
                    .abort(ConversationAbortOptions { background })
                    .await
                    .unwrap();
                harness
                    .commit(move |tx| {
                        Box::pin(async move {
                            for (index, (c, input, write)) in scopes.into_iter().enumerate() {
                                let withdrawn = index < 4 || (index == 4 && background);
                                let receipt = tx.submission(input).await?.unwrap();
                                if withdrawn {
                                    unanswered(&receipt, "aborted", false);
                                } else {
                                    assert_eq!(receipt.state, SubmissionState::InputQueued);
                                }
                                assert_eq!(
                                    tx.submission(write).await?.unwrap().state,
                                    SubmissionState::WriteQueued
                                );
                                let state = tx.conversation_state(c).await?.unwrap();
                                let expected = if withdrawn {
                                    vec![write]
                                } else {
                                    vec![input, write]
                                };
                                assert_eq!(
                                    state
                                        .inbox
                                        .iter()
                                        .map(|item| item.submission_id)
                                        .collect::<Vec<_>>(),
                                    expected
                                );
                                assert_eq!(state.agent_config, Some(json!({"opaque":true})));
                            }
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
}

pub async fn scheduler_cascade<S: Storage + 'static>(make: impl Fn() -> S) {
    for mode in ["failed", "panic", "default-abort"] {
        let def = definition(mode);
        let (opening, driver) = Harness::open(
            initialized(make()).await,
            TaskRegistry::new([def.clone()]).unwrap(),
        );
        zip(
            async {
                let harness = opening.await.unwrap();
                let root = harness
                    .commit(move |tx| {
                        Box::pin(async move { seed(tx, ROOT_CONVERSATION, def).await })
                    })
                    .await
                    .unwrap()
                    .value;
                let parent = root.task;
                let (leaf, input, write, bg_input) = harness
                    .commit(move |tx| {
                        Box::pin(async move {
                            let leaf = tx.create_conversation_owned(Owner::Task(parent)).await?.id;
                            let (input, write) = queue(tx, leaf).await?;
                            let bg = tx
                                .create_task(
                                    definition("missing"),
                                    Value::Null,
                                    TaskOptions {
                                        background: true,
                                        ..options(leaf)
                                    },
                                )
                                .await?
                                .id;
                            let bg_leaf = tx.create_conversation_owned(Owner::Task(bg)).await?.id;
                            let (bg_input, _) = queue(tx, bg_leaf).await?;
                            Ok((leaf, input, write, bg_input))
                        })
                    })
                    .await
                    .unwrap()
                    .value;
                if mode == "default-abort" {
                    harness.abort(parent).await.unwrap();
                }
                unanswered(&wait(&harness, input).await.unwrap(), "aborted", false);
                harness.wait_task(parent).await.unwrap();
                harness
                    .commit(move |tx| {
                        Box::pin(async move {
                            assert_eq!(
                                tx.conversation_state(leaf)
                                    .await?
                                    .unwrap()
                                    .inbox
                                    .iter()
                                    .map(|item| item.submission_id)
                                    .collect::<Vec<_>>(),
                                [write]
                            );
                            assert_eq!(
                                tx.submission(root.queued).await?.unwrap().state,
                                SubmissionState::InputQueued
                            );
                            assert_eq!(
                                tx.submission(root.write).await?.unwrap().state,
                                SubmissionState::WriteQueued
                            );
                            assert_eq!(
                                tx.submission(bg_input).await?.unwrap().state,
                                SubmissionState::InputQueued
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
    }
}

pub async fn rollback<S: Storage + 'static>(storage: S) {
    let received = Gate::default();
    let sent = received.clone();
    let handler: PhaseHandler = Rc::new(move |task, runtime| {
        let sent = sent.clone();
        Box::pin(async move {
            // A rejected final batch must discard derived cleanup too. The bad
            // queued inbox reference is discovered after terminal fallback.
            let error = runtime
                .commit(move |tx, _| {
                    Box::pin(async move {
                        tx.update_conversation_state(task.conversation_id, |state| {
                            state.inbox.push(InboxItem {
                                submission_id: Id::new(MAX_NUMBER).unwrap(),
                                payload: Value::Null,
                            });
                        })
                        .await?;
                        Ok(Some(TaskUpdate::Abort {
                            reason: None,
                            result: None,
                        }))
                    })
                })
                .await
                .unwrap_err();
            assert!(matches!(error, SessionError::Invalid(_)));
            runtime
                .read(move |tx, _| {
                    Box::pin(async move {
                        let state = tx.conversation_state(task.conversation_id).await?.unwrap();
                        assert_eq!(state.inbox.len(), 2);
                        for id in state.run.unwrap().input_submission_ids {
                            assert_eq!(
                                tx.submission(id).await?.unwrap().status(),
                                SubmissionStatus::Placed
                            );
                        }
                        assert_eq!(
                            tx.task(task.id).await?.unwrap().status(),
                            TaskStatus::Running
                        );
                        for conversation in tx
                            .scan_conversations(ConversationQuery::default(), 128, None)
                            .await?
                            .items
                        {
                            if conversation
                                .owner
                                .as_ref()
                                .is_some_and(|owner| owner.task_id == task.id)
                            {
                                let state = tx.conversation_state(conversation.id).await?.unwrap();
                                assert_eq!(state.inbox.len(), 2);
                                assert_eq!(
                                    tx.submission(state.inbox[0].submission_id)
                                        .await?
                                        .unwrap()
                                        .state,
                                    SubmissionState::InputQueued
                                );
                            }
                        }
                        Ok(())
                    })
                })
                .await
                .unwrap();
            sent.release();
            runtime
                .commit(|_, _| Box::pin(async { Ok(Some(TaskUpdate::Complete(Value::Null))) }))
                .await
                .unwrap();
            Ok(())
        })
    });
    let def = TaskDefinition::new(
        "rollback",
        1,
        |_| Ok(json!({"phase":"run"})),
        [("run".into(), handler)].into(),
    );
    let (opening, driver) = Harness::open(
        initialized(storage).await,
        TaskRegistry::new([def.clone()]).unwrap(),
    );
    zip(
        async {
            let harness = opening.await.unwrap();
            let ids = harness
                .commit(move |tx| Box::pin(async move { seed(tx, ROOT_CONVERSATION, def).await }))
                .await
                .unwrap()
                .value;
            let parent = ids.task;
            harness
                .commit(move |tx| {
                    Box::pin(async move {
                        let owned = tx.create_conversation_owned(Owner::Task(parent)).await?.id;
                        queue(tx, owned).await?;
                        Ok(())
                    })
                })
                .await
                .unwrap();
            harness.resume().unwrap();
            received.wait().await;
            for id in ids.placed {
                unanswered(&wait(&harness, id).await.unwrap(), "faulted", true);
            }
            harness.close().await.unwrap();
        },
        driver,
    )
    .await;
}

/// Seed the storage-valid legacy/crash shape directly: a terminal task with a
/// stale active marker. Opening must normalize it before returning a handle.
pub async fn stale_terminal(store: &mut dyn Storage, aborted: bool) {
    let c = ROOT_CONVERSATION;
    let task = Id::new(2).unwrap();
    let entry = Id::new(3).unwrap();
    let input = Id::new(4).unwrap();
    store
        .commit(vec![
            StorageWrite::Conversation(ConversationRecord {
                id: c,
                parent: None,
                owner: None,
            }),
            StorageWrite::Task(TaskRecord {
                id: task,
                conversation_id: c,
                kind: "absent".into(),
                version: 1,
                input: Value::Null,
                owner: None,
                background: false,
                abort_requested: false,
                state: TaskState::Terminal {
                    outcome: if aborted {
                        TaskOutcome::Aborted {
                            reason: None,
                            result: None,
                        }
                    } else {
                        TaskOutcome::Completed {
                            result: json!({"answer":entry}),
                        }
                    },
                },
                memos: None,
            }),
            StorageWrite::Entry(EntryRecord::new(entry, c, "input")),
            StorageWrite::Submission(SubmissionRecord {
                id: input,
                conversation_id: c,
                request_id: None,
                state: SubmissionState::InputPlaced { entry },
            }),
            StorageWrite::ConversationState(ConversationStateRecord {
                id: Id::new(5).unwrap(),
                conversation_id: c,
                run: Some(ConversationRun {
                    task_id: task,
                    input_submission_ids: vec![input],
                }),
                inbox: Vec::new(),
                agent_config: Some(Value::Null),
            }),
            StorageWrite::Conversation(ConversationRecord {
                id: Id::new(6).unwrap(),
                parent: None,
                owner: Some(OwnerLink {
                    task_id: task,
                    conversation_id: c,
                }),
            }),
            StorageWrite::Submission(SubmissionRecord {
                id: Id::new(7).unwrap(),
                conversation_id: Id::new(6).unwrap(),
                request_id: None,
                state: SubmissionState::InputQueued,
            }),
            StorageWrite::Submission(SubmissionRecord {
                id: Id::new(8).unwrap(),
                conversation_id: Id::new(6).unwrap(),
                request_id: None,
                state: SubmissionState::WriteQueued,
            }),
            StorageWrite::ConversationState(ConversationStateRecord {
                id: Id::new(9).unwrap(),
                conversation_id: Id::new(6).unwrap(),
                run: None,
                inbox: [7, 8]
                    .map(|id| InboxItem {
                        submission_id: Id::new(id).unwrap(),
                        payload: Value::Null,
                    })
                    .into(),
                agent_config: None,
            }),
        ])
        .await
        .unwrap();
}
async fn assert_recovered(tx: &Tx, aborted: bool) -> Result<(), SessionError> {
    unanswered(
        &tx.submission(Id::new(4).unwrap()).await?.unwrap(),
        if aborted { "aborted" } else { "faulted" },
        true,
    );
    assert!(
        tx.conversation_state(ROOT_CONVERSATION)
            .await?
            .unwrap()
            .run
            .is_none()
    );
    let queued = tx.submission(Id::new(7).unwrap()).await?.unwrap();
    if aborted {
        unanswered(&queued, "aborted", false);
    } else {
        assert_eq!(queued.state, SubmissionState::InputQueued);
    }
    assert_eq!(
        tx.submission(Id::new(8).unwrap()).await?.unwrap().state,
        SubmissionState::WriteQueued
    );
    assert_eq!(
        tx.conversation_state(Id::new(6).unwrap())
            .await?
            .unwrap()
            .inbox
            .len(),
        if aborted { 1 } else { 2 }
    );
    Ok(())
}
pub async fn reopened<S: Storage + 'static>(storage: S, harness: bool, aborted: bool) {
    if harness {
        let (opening, driver) = Harness::open(storage, TaskRegistry::default());
        zip(
            async {
                let harness = opening.await.unwrap();
                harness
                    .commit(move |tx| Box::pin(async move { assert_recovered(tx, aborted).await }))
                    .await
                    .unwrap();
                harness.close().await.unwrap();
            },
            driver,
        )
        .await;
    } else {
        let (opening, driver) = Session::open_recovered(storage);
        zip(
            async {
                let session = opening.await.unwrap().value;
                session
                    .commit(move |tx| Box::pin(async move { assert_recovered(tx, aborted).await }))
                    .await
                    .unwrap();
                session.close().await.unwrap();
            },
            driver,
        )
        .await;
    }
}

#[derive(Clone, Copy, Debug)]
pub enum CommitFault {
    Rejected,
    Before,
    After,
}
struct FaultStore<S> {
    inner: S,
    fault: Option<CommitFault>,
    attempted: Rc<RefCell<Vec<StorageWrite>>>,
}
impl<S: Storage> Storage for FaultStore<S> {
    forward_storage_methods!(inner;
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
        Box::pin(async move {
            let terminal = writes.iter().any(|write| matches!(write, StorageWrite::Task(task) if task.status() == TaskStatus::Terminal));
            if terminal && let Some(fault) = self.fault.take() {
                *self.attempted.borrow_mut() = writes.clone();
                if matches!(fault, CommitFault::After) {
                    self.inner.commit(writes).await?;
                }
                return Err(match fault {
                    CommitFault::Rejected => StorageError::Rejected("no effect".into()),
                    _ => StorageError::Other("uncertain".into()),
                });
            }
            self.inner.commit(writes).await
        })
    }
}

pub async fn storage_failures<S: Storage + 'static>(make: impl Fn() -> S) {
    for fault in [
        CommitFault::Rejected,
        CommitFault::Before,
        CommitFault::After,
    ] {
        let attempted = Rc::new(RefCell::new(Vec::new()));
        let store = FaultStore {
            inner: initialized(make()).await,
            fault: Some(fault),
            attempted: attempted.clone(),
        };
        let handler: PhaseHandler = Rc::new(move |task, runtime| {
            Box::pin(async move {
                let error = runtime
                    .commit(|_, _| {
                        Box::pin(async {
                            Ok(Some(TaskUpdate::Abort {
                                reason: None,
                                result: None,
                            }))
                        })
                    })
                    .await
                    .unwrap_err();
                if matches!(fault, CommitFault::Rejected) {
                    assert!(matches!(
                        error,
                        SessionError::Storage(StorageError::Rejected(_))
                    ));
                    runtime
                        .read(move |tx, _| {
                            Box::pin(async move {
                                let state =
                                    tx.conversation_state(task.conversation_id).await?.unwrap();
                                assert_eq!(state.run.as_ref().unwrap().task_id, task.id);
                                for id in state.run.unwrap().input_submission_ids {
                                    assert_eq!(
                                        tx.submission(id).await?.unwrap().status(),
                                        SubmissionStatus::Placed
                                    );
                                }
                                Ok(())
                            })
                        })
                        .await
                        .unwrap();
                } else {
                    assert!(matches!(
                        error,
                        SessionError::Storage(StorageError::Other(_))
                    ));
                }
                // Rejection permits a no-progress fault; uncertainty must never
                // publish the attempted aborted receipts, even if storage applied.
                Ok(())
            })
        });
        let def = TaskDefinition::new(
            "storage-fault",
            1,
            |_| Ok(json!({"phase":"run"})),
            [("run".into(), handler)].into(),
        );
        let (opening, driver) = Harness::open(store, TaskRegistry::new([def.clone()]).unwrap());
        zip(async {
            let harness = opening.await.unwrap();
            let ids = harness.commit(move |tx| Box::pin(async move { seed(tx, ROOT_CONVERSATION, def).await })).await.unwrap().value;
            let result = wait(&harness, ids.placed[0]).await;
            if matches!(fault, CommitFault::Rejected) {
                unanswered(&result.unwrap(), "faulted", true);
                harness.close().await.unwrap();
            } else {
                assert!(result.is_err());
                assert!(harness.close().await.is_err());
            }
            let writes = attempted.borrow();
            for id in ids.placed {
                let record = writes.iter().find_map(|write| match write { StorageWrite::Submission(record) if record.id == id => Some(record), _ => None }).unwrap();
                unanswered(record, "aborted", true);
            }
            assert!(writes.iter().any(|write| matches!(write, StorageWrite::ConversationState(state) if state.run.is_none())));
        }, driver).await;
    }
}

pub async fn queued_only_pagination<S: Storage + 'static>(storage: S) {
    let (opening, driver) = Harness::open(initialized(storage).await, TaskRegistry::default());
    zip(
        async {
            let harness = opening.await.unwrap();
            let inputs = harness
                .commit(|tx| {
                    Box::pin(async move {
                        let mut ids = Vec::new();
                        for _ in 0..130 {
                            ids.push(queue(tx, ROOT_CONVERSATION).await?);
                        }
                        Ok(ids)
                    })
                })
                .await
                .unwrap()
                .value;
            harness
                .conversation(ROOT_CONVERSATION)
                .await
                .unwrap()
                .unwrap()
                .abort(ConversationAbortOptions::default())
                .await
                .unwrap();
            harness
                .commit(move |tx| {
                    Box::pin(async move {
                        let state = tx.conversation_state(ROOT_CONVERSATION).await?.unwrap();
                        assert_eq!(state.inbox.len(), 130);
                        for (input, write) in inputs {
                            unanswered(&tx.submission(input).await?.unwrap(), "aborted", false);
                            assert_eq!(
                                tx.submission(write).await?.unwrap().state,
                                SubmissionState::WriteQueued
                            );
                        }
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

pub async fn explicit_runner<S: Storage + 'static>(make: impl Fn() -> S) {
    for orphan in [false, true] {
        let def = definition("default-abort");
        let registry = if orphan {
            TaskRegistry::default()
        } else {
            TaskRegistry::new([def.clone()]).unwrap()
        };
        let (session, session_driver) = Session::new(initialized(make()).await);
        let (runner, task_driver) = TaskRunner::attach(&session, registry).unwrap();
        zip(
            async {
                let ids = session
                    .commit(move |tx| {
                        Box::pin(async move { seed(tx, ROOT_CONVERSATION, def).await })
                    })
                    .await
                    .unwrap()
                    .value;
                if orphan {
                    assert!(matches!(
                        runner.run(ids.task).await.unwrap(),
                        RunResult::Blocked(BlockReason::MissingDefinition)
                    ));
                    let id = ids.placed[0];
                    session
                        .commit(move |tx| {
                            Box::pin(async move {
                                assert_eq!(
                                    tx.submission(id).await?.unwrap().status(),
                                    SubmissionStatus::Placed
                                );
                                assert!(
                                    tx.conversation_state(ROOT_CONVERSATION)
                                        .await?
                                        .unwrap()
                                        .run
                                        .is_some()
                                );
                                Ok(())
                            })
                        })
                        .await
                        .unwrap();
                }
                assert_eq!(runner.abort(ids.task).await.unwrap(), AbortResult::Marked);
                if !orphan {
                    assert!(matches!(
                        runner.run(ids.task).await.unwrap(),
                        RunResult::Terminal(_)
                    ));
                }
                session
                    .commit(move |tx| {
                        Box::pin(async move {
                            for id in ids.placed {
                                unanswered(
                                    &tx.submission(id).await?.unwrap(),
                                    if orphan { "faulted" } else { "aborted" },
                                    true,
                                );
                            }
                            let state = tx.conversation_state(ROOT_CONVERSATION).await?.unwrap();
                            assert!(state.run.is_none());
                            assert_eq!(
                                state
                                    .inbox
                                    .iter()
                                    .map(|item| item.submission_id)
                                    .collect::<Vec<_>>(),
                                [ids.queued, ids.write]
                            );
                            assert_eq!(
                                tx.submission(ids.queued).await?.unwrap().state,
                                SubmissionState::InputQueued
                            );
                            assert_eq!(
                                tx.submission(ids.write).await?.unwrap().state,
                                SubmissionState::WriteQueued
                            );
                            Ok(())
                        })
                    })
                    .await
                    .unwrap();
                runner.close().await.unwrap();
                session.close().await.unwrap();
            },
            zip(session_driver, task_driver),
        )
        .await;
    }
}
