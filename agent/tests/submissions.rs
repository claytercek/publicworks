use futures_lite::future::{block_on, zip};
use publicworks_agent::*;
use publicworks_runtime::*;
mod support;
use serde_json::json;
use std::{cell::RefCell, collections::VecDeque, future::Future, rc::Rc};
use support::{Gate, TempDatabase, bounded_with_budget};

async fn bounded<F: Future>(future: F) -> F::Output {
    bounded_with_budget(30_000, future).await
}

fn config() -> TurnConfig {
    TurnConfig {
        model: "fake".into(),
        instructions: "".into(),
        max_model_rounds: 4,
    }
}
fn options(request: &str, busy: BusyMode) -> SubmitOptions {
    SubmitOptions {
        request_id: Some(request.into()),
        busy,
    }
}
fn answer(text: &str) -> ModelResponse {
    ModelResponse {
        text: text.into(),
        tool_calls: vec![],
        usage: None,
    }
}
struct Step {
    entered: Gate,
    release: Gate,
    reply: Result<ModelResponse, ModelError>,
}
fn step(reply: Result<ModelResponse, ModelError>) -> (Step, Gate, Gate) {
    let entered = Gate::default();
    let release = Gate::default();
    (
        Step {
            entered: entered.clone(),
            release: release.clone(),
            reply,
        },
        entered,
        release,
    )
}
fn agent(steps: Vec<Step>) -> (Agent, Rc<RefCell<Vec<ModelRequest>>>) {
    let steps = Rc::new(RefCell::new(VecDeque::from(steps)));
    let requests = Rc::new(RefCell::new(Vec::new()));
    let captured = requests.clone();
    let agent = Agent::new(
        move |request: ModelRequest, _: Cancellation| -> ModelFuture {
            captured.borrow_mut().push(request);
            let step = steps
                .borrow_mut()
                .pop_front()
                .expect("unexpected model call");
            step.entered.release();
            Box::pin(async move {
                step.release.wait().await;
                step.reply
            })
        },
        vec![],
    )
    .unwrap();
    (agent, requests)
}
async fn conversation(h: &Harness) -> Id {
    h.commit(|tx| Box::pin(async move { Ok(tx.create_conversation().await?.id) }))
        .await
        .unwrap()
        .value
}
async fn state(h: &Harness, c: Id) -> ConversationStateRecord {
    h.commit(move |tx| Box::pin(async move { Ok(tx.conversation_state(c).await?.unwrap()) }))
        .await
        .unwrap()
        .value
}
async fn log(h: &Harness, c: Id) -> Vec<EntryRecord> {
    let mut entries = h
        .commit(move |tx| {
            Box::pin(async move { Ok(tx.scan_entries(EntryQuery::new(c), 128, None).await?.items) })
        })
        .await
        .unwrap()
        .value;
    entries.reverse();
    entries
}
async fn input(a: &Agent, h: &Harness, c: Id, text: &str, busy: BusyMode) -> Submission {
    a.submit(h, c, text, config(), options(text, busy))
        .await
        .unwrap()
}
fn texts(entries: &[EntryRecord]) -> Vec<String> {
    entries
        .iter()
        .filter_map(|e| {
            e.model
                .as_ref()?
                .first()?
                .get("text")?
                .as_str()
                .map(str::to_owned)
        })
        .collect()
}
fn done_answer(record: SubmissionRecord) -> Id {
    let SubmissionState::InputDone { answer, .. } = record.state else {
        panic!("not done: {record:?}")
    };
    answer
}
fn assert_unanswered(record: SubmissionRecord, reason: &str) {
    assert!(
        matches!(record.state, SubmissionState::InputUnanswered { reason: r, .. } | SubmissionState::WriteUnanswered { reason: r, .. } if r == reason)
    );
}

#[test]
fn typed_dedup_precedes_validation_busy_and_configuration_without_sequence() {
    block_on(bounded(async {
        let (a, _) = agent(vec![]);
        let (opening, driver) = Harness::open(
            MemoryStorage::new(),
            TaskRegistry::new(a.definitions()).unwrap(),
        );
        zip(
            async {
                let h = opening.await.unwrap();
                let c = conversation(&h).await;
                let a1 = a.clone();
                let admitted = h
                    .commit(move |tx| {
                        a1.admit_input(tx, c, "first", config(), options("key", BusyMode::Reject))
                    })
                    .await
                    .unwrap();
                assert!(admitted.seq.is_some());
                let id = admitted.value;
                let run = state(&h, c).await.run.unwrap();
                let receipt = h.submission(id).await.unwrap().unwrap();
                assert_eq!(run.input_submission_ids, vec![id]);
                assert_eq!(
                    receipt.status().await.unwrap().status(),
                    SubmissionStatus::Placed
                );
                assert_eq!(log(&h, c).await.len(), 1);
                // A corrupted later queue configuration must not be read on a retry.
                h.commit(move |tx| {
                    Box::pin(async move {
                        tx.update_conversation_state(c, |s| s.agent_config = Some(json!(null)))
                            .await?;
                        Ok(())
                    })
                })
                .await
                .unwrap();
                let seq = h.inspect().await.unwrap().last_commit_seq;
                let a1 = a.clone();
                let mut bad = config();
                bad.model.clear();
                let retry = h
                    .commit(move |tx| {
                        a1.admit_input(tx, c, "ignored", bad, options("key", BusyMode::Reject))
                    })
                    .await
                    .unwrap();
                assert_eq!(retry.value, id);
                assert_eq!(retry.seq, None);
                let a1 = a.clone();
                let conflict = h
                    .commit(move |tx| {
                        a1.admit_write(
                            tx,
                            c,
                            EntryDraft::new("ignored"),
                            WriteOptions {
                                request_id: Some("key".into()),
                                ..Default::default()
                            },
                        )
                    })
                    .await;
                let Err(SessionError::Invalid(message)) = conflict else {
                    panic!("expected conflicting request type");
                };
                assert!(message.contains("different submission type"));
                assert_eq!(h.inspect().await.unwrap().last_commit_seq, seq);
                // Partial configuration updates reject malformed envelopes rather
                // than silently discarding potentially unrelated settings.
                assert!(
                    a.configure_queues(&h, c, QueueConfig::default())
                        .await
                        .is_err()
                );
                h.commit(move |tx| {
                    Box::pin(async move {
                        tx.update_conversation_state(c, |s| s.agent_config = None)
                            .await?;
                        Ok(())
                    })
                })
                .await
                .unwrap();
                a.configure_queues(&h, c, QueueConfig::default())
                    .await
                    .unwrap();
                let seq = h.inspect().await.unwrap().last_commit_seq;
                let a1 = a.clone();
                assert!(
                    h.commit(move |tx| a1.admit_input(
                        tx,
                        c,
                        "rejected",
                        config(),
                        options("new", BusyMode::Reject)
                    ))
                    .await
                    .is_err()
                );
                assert_eq!(h.inspect().await.unwrap().last_commit_seq, seq);
                assert!(!h.inspect().await.unwrap().progress_enabled);
                let other = conversation(&h).await;
                let write = a
                    .write(
                        &h,
                        other,
                        EntryDraft::new("note"),
                        WriteOptions {
                            request_id: Some("key".into()),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                assert_ne!(write.id(), id);
                assert_eq!(
                    write.status().await.unwrap().status(),
                    SubmissionStatus::Done
                );
                // Same-type terminal retry ignores an invalid payload too.
                let mut draft = EntryDraft::new("ignored");
                let mut deep = json!(0);
                for _ in 0..200 {
                    deep = json!([deep]);
                }
                draft.data = Some(deep);
                let a1 = a.clone();
                let retry = h
                    .commit(move |tx| {
                        a1.admit_write(
                            tx,
                            other,
                            draft,
                            WriteOptions {
                                request_id: Some("key".into()),
                                ..Default::default()
                            },
                        )
                    })
                    .await
                    .unwrap();
                assert_eq!(retry.value, write.id());
                assert_eq!(retry.seq, None);
                h.close().await.unwrap();
            },
            driver,
        )
        .await;
    }));
}

#[test]
fn boundaries_order_writes_first_select_one_or_all_and_handover_atomically() {
    for all_steers in [false, true] {
        block_on(bounded(async {
            let tools = Ok(ModelResponse {
                text: "tools".into(),
                tool_calls: vec![ToolCall {
                    id: "missing".into(),
                    name: "not-installed".into(),
                    arguments: json!({}),
                }],
                usage: None,
            });
            let (s1, entered1, release1) = step(tools);
            let (s2, entered2, release2) = step(Ok(answer("answer-current")));
            let (s3, entered3, release3) = step(Ok(answer("answer-next")));
            let (s4, _, release4) = step(Ok(answer("answer-last")));
            release4.release();
            let (a, requests) = agent(vec![s1, s2, s3, s4]);
            let (opening, driver) = Harness::open(
                MemoryStorage::new(),
                TaskRegistry::new(a.definitions()).unwrap(),
            );
            zip(
                async {
                    let h = opening.await.unwrap();
                    let c = conversation(&h).await;
                    let first = input(&a, &h, c, "initial", BusyMode::FollowUp).await;
                    entered1.wait().await;
                    let original = state(&h, c).await.run.unwrap();
                    let f1 = input(&a, &h, c, "f1", BusyMode::FollowUp).await;
                    let w1 = a
                        .write(&h, c, EntryDraft::new("w1"), WriteOptions::default())
                        .await
                        .unwrap();
                    let s1 = input(&a, &h, c, "s1", BusyMode::Steer).await;
                    let withdrawn = input(&a, &h, c, "withdrawn", BusyMode::FollowUp).await;
                    let f2 = input(&a, &h, c, "f2", BusyMode::FollowUp).await;
                    let w2 = a
                        .write(&h, c, EntryDraft::new("w2"), WriteOptions::default())
                        .await
                        .unwrap();
                    let s2 = input(&a, &h, c, "s2", BusyMode::Steer).await;
                    assert_eq!(withdrawn.abort().await.unwrap(), WithdrawalResult::Aborted);
                    assert_eq!(
                        first.abort().await.unwrap(),
                        WithdrawalResult::AlreadyPlaced
                    );
                    assert_eq!(
                        state(&h, c)
                            .await
                            .inbox
                            .iter()
                            .map(|i| i.submission_id)
                            .collect::<Vec<_>>(),
                        vec![f1.id(), w1.id(), s1.id(), f2.id(), w2.id(), s2.id()]
                    );
                    // Policy is changed after enqueue, before the boundary.
                    a.configure_queues(
                        &h,
                        c,
                        QueueConfig {
                            steer: if all_steers {
                                QueueMode::All
                            } else {
                                QueueMode::One
                            },
                            follow_up: QueueMode::One,
                        },
                    )
                    .await
                    .unwrap();
                    let mut stale_reset = EntryDraft::new("agent.reset");
                    stale_reset.head = Some(Head::SelfEntry);
                    let stale_reset = a
                        .write(
                            &h,
                            c,
                            stale_reset,
                            WriteOptions {
                                expected_head: ExpectedHead::Exact(Some(c)),
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap();
                    release1.release();
                    entered2.wait().await;
                    assert_unanswered(stale_reset.status().await.unwrap(), "stale");
                    let current = state(&h, c).await;
                    let mut expected = vec![first.id(), s1.id()];
                    if all_steers {
                        expected.push(s2.id());
                    }
                    assert_eq!(current.run.unwrap().input_submission_ids, expected);
                    assert_eq!(w1.status().await.unwrap().status(), SubmissionStatus::Done);
                    assert_eq!(w2.status().await.unwrap().status(), SubmissionStatus::Done);
                    assert_eq!(
                        f1.status().await.unwrap().status(),
                        SubmissionStatus::Queued
                    );
                    let entries = log(&h, c).await;
                    let kinds = entries.iter().map(|e| e.kind.as_str()).collect::<Vec<_>>();
                    assert_eq!(&kinds[3..5], &["w1", "w2"]);
                    assert_eq!(
                        texts(&entries),
                        if all_steers {
                            vec!["initial", "tools", "s1", "s2"]
                        } else {
                            vec!["initial", "tools", "s1"]
                        }
                    );
                    assert!(
                        requests.borrow()[1]
                            .messages
                            .contains(&ModelMessage::User { text: "s1".into() })
                    );
                    if !all_steers {
                        assert_eq!(s2.abort().await.unwrap(), WithdrawalResult::Aborted);
                    }
                    // Exercise All at final in one iteration, One across successors in the other.
                    a.configure_queues(
                        &h,
                        c,
                        QueueConfig {
                            steer: QueueMode::All,
                            follow_up: if all_steers {
                                QueueMode::All
                            } else {
                                QueueMode::One
                            },
                        },
                    )
                    .await
                    .unwrap();
                    let wf = a
                        .write(
                            &h,
                            c,
                            EntryDraft::new("final-write"),
                            WriteOptions::default(),
                        )
                        .await
                        .unwrap();
                    release2.release();
                    entered3.wait().await;
                    let answer = done_answer(first.status().await.unwrap());
                    assert_eq!(done_answer(s1.status().await.unwrap()), answer);
                    let successor = state(&h, c).await.run.unwrap();
                    assert_ne!(successor.task_id, original.task_id);
                    assert_eq!(
                        successor.input_submission_ids,
                        if all_steers {
                            vec![f1.id(), f2.id()]
                        } else {
                            vec![f1.id()]
                        }
                    );
                    assert_eq!(wf.status().await.unwrap().status(), SubmissionStatus::Done);
                    let entries = log(&h, c).await;
                    let write_pos = entries
                        .iter()
                        .position(|e| e.kind == "final-write")
                        .unwrap();
                    assert_eq!(entries[write_pos - 1].id, answer);
                    assert_eq!(
                        entries[write_pos + 1].model.as_ref().unwrap()[0]["text"],
                        "f1"
                    );
                    // The settled receipt is never observable without successor placement.
                    h.commit(move |tx| {
                        Box::pin(async move {
                            assert_eq!(
                                tx.task(original.task_id).await?.unwrap().status(),
                                TaskStatus::Terminal
                            );
                            assert!(tx.task(successor.task_id).await?.is_some());
                            Ok(())
                        })
                    })
                    .await
                    .unwrap();
                    release3.release();
                    let f1_answer = done_answer(f1.wait().await.unwrap());
                    let f2_answer = done_answer(f2.wait().await.unwrap());
                    assert_eq!(f1_answer == f2_answer, all_steers);
                    h.wait_for_idle().await.unwrap();
                    assert!(state(&h, c).await.run.is_none());
                    assert!(state(&h, c).await.inbox.is_empty());
                    assert_unanswered(withdrawn.status().await.unwrap(), "aborted");
                    h.close().await.unwrap();
                },
                driver,
            )
            .await;
        }));
    }
}

#[test]
fn abort_and_model_error_clear_current_run_but_preserve_queued_work_for_restart() {
    for abort in [false, true] {
        block_on(bounded(async {
            let (s1, entered, release) = step(Err(ModelError {
                message: "failed".into(),
                partial_response: None,
                usage: None,
            }));
            let (s2, next_entered, next_release) = step(Ok(answer("recovered")));
            let (s3, _, release3) = step(Ok(answer("latest")));
            release3.release();
            let (a, _) = agent(vec![s1, s2, s3]);
            let (opening, driver) = Harness::open(
                MemoryStorage::new(),
                TaskRegistry::new(a.definitions()).unwrap(),
            );
            zip(
                async {
                    let h = opening.await.unwrap();
                    let c = conversation(&h).await;
                    let first = input(&a, &h, c, "initial", BusyMode::FollowUp).await;
                    entered.wait().await;
                    let queued = input(&a, &h, c, "queued", BusyMode::FollowUp).await;
                    let steer = input(&a, &h, c, "steer", BusyMode::Steer).await;
                    let write = a
                        .write(
                            &h,
                            c,
                            EntryDraft::new("later-write"),
                            WriteOptions::default(),
                        )
                        .await
                        .unwrap();
                    if abort {
                        h.abort(state(&h, c).await.run.unwrap().task_id)
                            .await
                            .unwrap();
                    } else {
                        release.release();
                    }
                    assert_unanswered(
                        first.wait().await.unwrap(),
                        if abort { "aborted" } else { "model_error" },
                    );
                    let after = state(&h, c).await;
                    assert!(after.run.is_none());
                    assert_eq!(after.inbox.len(), 3);
                    assert_eq!(
                        queued.status().await.unwrap().status(),
                        SubmissionStatus::Queued
                    );
                    assert_eq!(
                        steer.status().await.unwrap().status(),
                        SubmissionStatus::Queued
                    );
                    assert_eq!(
                        write.status().await.unwrap().status(),
                        SubmissionStatus::Queued
                    );
                    assert_eq!(steer.withdraw().await.unwrap(), WithdrawalResult::Aborted);
                    let latest = input(&a, &h, c, "latest", BusyMode::FollowUp).await;
                    next_entered.wait().await;
                    assert_eq!(
                        state(&h, c).await.run.unwrap().input_submission_ids,
                        vec![queued.id()]
                    );
                    assert_eq!(
                        latest.status().await.unwrap().status(),
                        SubmissionStatus::Queued
                    );
                    let entries = log(&h, c).await;
                    let w = entries
                        .iter()
                        .position(|e| e.kind == "later-write")
                        .unwrap();
                    assert_eq!(entries[w + 1].model.as_ref().unwrap()[0]["text"], "queued");
                    next_release.release();
                    done_answer(queued.wait().await.unwrap());
                    done_answer(latest.wait().await.unwrap());
                    h.close().await.unwrap();
                },
                driver,
            )
            .await;
        }));
    }
}

#[test]
fn passive_writes_compare_explicit_heads_at_placement_and_preserve_null() {
    block_on(bounded(async {
        let (s, entered, release) = step(Ok(answer("done")));
        let (a, _) = agent(vec![s]);
        let (opening, driver) = Harness::open(
            MemoryStorage::new(),
            TaskRegistry::new(a.definitions()).unwrap(),
        );
        zip(
            async {
                let h = opening.await.unwrap();
                let c = conversation(&h).await;
                let mut draft = EntryDraft::new("head");
                draft.head = Some(Head::SelfEntry);
                draft.data = Some(json!(null));
                let head = a
                    .write(
                        &h,
                        c,
                        draft,
                        WriteOptions {
                            expected_head: ExpectedHead::Exact(None),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                let SubmissionState::WriteDone { entry: head } = head.status().await.unwrap().state
                else {
                    panic!()
                };
                assert!(!h.inspect().await.unwrap().progress_enabled);
                assert_eq!(log(&h, c).await[0].data, Some(json!(null)));
                let stale = a
                    .write(
                        &h,
                        c,
                        EntryDraft::new("stale-idle"),
                        WriteOptions {
                            expected_head: ExpectedHead::Exact(None),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                assert_unanswered(stale.status().await.unwrap(), "stale");
                assert_eq!(log(&h, c).await.len(), 1);
                let first = input(&a, &h, c, "initial", BusyMode::FollowUp).await;
                entered.wait().await;
                let mut draft = EntryDraft::new("new-head");
                draft.head = Some(Head::SelfEntry);
                let fresh = a
                    .write(
                        &h,
                        c,
                        draft,
                        WriteOptions {
                            expected_head: ExpectedHead::Exact(Some(head)),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                let stale = a
                    .write(
                        &h,
                        c,
                        EntryDraft::new("stale-busy"),
                        WriteOptions {
                            expected_head: ExpectedHead::Exact(Some(head)),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                let any = a
                    .write(&h, c, EntryDraft::new("unguarded"), WriteOptions::default())
                    .await
                    .unwrap();
                release.release();
                done_answer(first.wait().await.unwrap());
                assert_eq!(
                    fresh.status().await.unwrap().status(),
                    SubmissionStatus::Done
                );
                assert_unanswered(stale.status().await.unwrap(), "stale");
                assert_eq!(any.status().await.unwrap().status(), SubmissionStatus::Done);
                assert_eq!(
                    log(&h, c)
                        .await
                        .iter()
                        .map(|e| e.kind.as_str())
                        .collect::<Vec<_>>(),
                    vec![
                        "head",
                        "agent.user",
                        "agent.assistant",
                        "new-head",
                        "unguarded"
                    ]
                );
                h.close().await.unwrap();
            },
            driver,
        )
        .await;
    }));
}

#[test]
fn dropping_unpolled_admission_observer_keeps_atomic_admission_and_progress() {
    block_on(bounded(async {
        let (s, entered, release) = step(Ok(answer("done")));
        let (a, _) = agent(vec![s]);
        let (opening, driver) = Harness::open(
            MemoryStorage::new(),
            TaskRegistry::new(a.definitions()).unwrap(),
        );
        zip(
            async {
                let h = opening.await.unwrap();
                let c = conversation(&h).await;
                drop(a.submit(
                    &h,
                    c,
                    "initial",
                    config(),
                    options("durable", BusyMode::FollowUp),
                ));
                entered.wait().await;
                let record = h
                    .commit(move |tx| {
                        Box::pin(async move {
                            let receipt = tx.submission_by_request(c, "durable").await?.unwrap();
                            let run = tx.conversation_state(c).await?.unwrap().run.unwrap();
                            assert_eq!(run.input_submission_ids, vec![receipt.id]);
                            assert!(tx.task(run.task_id).await?.is_some());
                            let SubmissionState::InputPlaced { entry } = receipt.state else {
                                panic!()
                            };
                            assert!(tx.visible_entry(c, entry).await?.is_some());
                            Ok(receipt)
                        })
                    })
                    .await
                    .unwrap()
                    .value;
                release.release();
                let receipt = h.submission(record.id).await.unwrap().unwrap();
                done_answer(receipt.wait().await.unwrap());
                let mut bad = config();
                bad.max_model_rounds = 0;
                let retry = a
                    .submit(&h, c, "ignored", bad, options("durable", BusyMode::Reject))
                    .await
                    .unwrap();
                assert_eq!(retry.id(), receipt.id());
                assert_eq!(
                    retry.status().await.unwrap(),
                    receipt.status().await.unwrap()
                );
                h.close().await.unwrap();
            },
            driver,
        )
        .await;
    }));
}

#[test]
fn task_owned_conversation_admits_and_completes_without_taskstate_busy_policy() {
    block_on(bounded(async {
        let (s, entered, release) = step(Ok(answer("owned answer")));
        let (next, _, next_release) = step(Ok(answer("owned successor")));
        next_release.release();
        let (a, _) = agent(vec![s, next]);
        let owner = TaskDefinition::new(
            "owner",
            1,
            |_| Ok(json!({"phase":"hold"})),
            std::collections::BTreeMap::from([
                (
                    "hold".into(),
                    Rc::new(|_, runtime: TaskRuntime| {
                        Box::pin(async move {
                            let child = runtime
                                .read(|tx, _| {
                                    Box::pin(async move {
                                        Ok(tx
                                            .scan_tasks(TaskQuery::default(), 128, None)
                                            .await?
                                            .items
                                            .into_iter()
                                            .find(|task| task.kind == TURN_KIND)
                                            .unwrap()
                                            .id)
                                    })
                                })
                                .await
                                .unwrap()
                                .value;
                            runtime
                                .commit(move |_, _| {
                                    Box::pin(async move {
                                        Ok(Some(TaskUpdate::Wait {
                                            checkpoint: json!({"phase":"finish"}),
                                            on: vec![child],
                                            policy: JoinPolicy::AllSettled,
                                        }))
                                    })
                                })
                                .await
                                .unwrap();
                            Ok(())
                        }) as PhaseFuture
                    }) as PhaseHandler,
                ),
                (
                    "finish".into(),
                    Rc::new(|_, runtime: TaskRuntime| {
                        Box::pin(async move {
                            runtime
                                .commit(|_, _| {
                                    Box::pin(async { Ok(Some(TaskUpdate::Complete(json!(null)))) })
                                })
                                .await
                                .unwrap();
                            Ok(())
                        }) as PhaseFuture
                    }) as PhaseHandler,
                ),
            ]),
        );
        let registry =
            TaskRegistry::new(a.definitions().into_iter().chain([owner.clone()])).unwrap();
        let (opening, driver) = Harness::open(MemoryStorage::new(), registry);
        zip(
            async {
                let h = opening.await.unwrap();
                let root = conversation(&h).await;
                let owned = h
                    .commit(move |tx| {
                        Box::pin(async move {
                            let task = tx
                                .create_task(
                                    owner,
                                    json!({}),
                                    TaskOptions {
                                        ownership: TaskOwnership::Conversation,
                                        conversation_id: Some(root),
                                        background: false,
                                    },
                                )
                                .await?;
                            Ok(tx.create_conversation_owned(Owner::Task(task.id)).await?.id)
                        })
                    })
                    .await
                    .unwrap()
                    .value;
                let receipt = input(&a, &h, owned, "question", BusyMode::Reject).await;
                entered.wait().await;
                let next = input(&a, &h, owned, "follow up", BusyMode::FollowUp).await;
                release.release();
                done_answer(receipt.wait().await.unwrap());
                done_answer(next.wait().await.unwrap());
                assert!(state(&h, owned).await.run.is_none());
                assert_eq!(
                    texts(&log(&h, owned).await),
                    vec!["question", "owned answer", "follow up", "owned successor"]
                );
                h.close().await.unwrap();
            },
            driver,
        )
        .await;
    }));
}

#[test]
fn reset_write_at_post_tools_settles_current_and_places_followups_in_new_context() {
    block_on(bounded(async {
        let tools = Ok(ModelResponse {
            text: "tools".into(),
            tool_calls: vec![ToolCall {
                id: "missing".into(),
                name: "not-installed".into(),
                arguments: json!({}),
            }],
            usage: None,
        });
        let (s1, entered1, release1) = step(tools);
        let (s2, entered2, release2) = step(Ok(answer("new answer")));
        let (a, requests) = agent(vec![s1, s2]);
        let (opening, driver) = Harness::open(
            MemoryStorage::new(),
            TaskRegistry::new(a.definitions()).unwrap(),
        );
        zip(
            async {
                let h = opening.await.unwrap();
                let c = conversation(&h).await;
                let first = input(&a, &h, c, "old", BusyMode::FollowUp).await;
                entered1.wait().await;
                let next = input(&a, &h, c, "new", BusyMode::FollowUp).await;
                let mut reset = EntryDraft::new("agent.reset");
                reset.head = Some(Head::SelfEntry);
                let write = a
                    .write(&h, c, reset, WriteOptions::default())
                    .await
                    .unwrap();
                release1.release();
                entered2.wait().await;
                assert_unanswered(first.status().await.unwrap(), "reset");
                assert_eq!(
                    write.status().await.unwrap().status(),
                    SubmissionStatus::Done
                );
                assert_eq!(
                    state(&h, c).await.run.unwrap().input_submission_ids,
                    vec![next.id()]
                );
                assert_eq!(
                    requests.borrow()[1].messages,
                    vec![ModelMessage::User { text: "new".into() }]
                );
                release2.release();
                done_answer(next.wait().await.unwrap());
                h.close().await.unwrap();
            },
            driver,
        )
        .await;
    }));
}

#[test]
fn sqlite_reopen_preserves_placed_queued_configuration_and_typed_dedup() {
    use publicworks_storage_sqlite::SqliteStorage;
    block_on(bounded(async {
        let db = TempDatabase::new("publicworks-submissions-", "reopen.db");
        let path = db.path().to_owned();
        let (s1, entered, _) = step(Ok(answer("interrupted")));
        let (a, _) = agent(vec![s1]);
        let (opening, driver) = Harness::open(
            SqliteStorage::open(&path).unwrap(),
            TaskRegistry::new(a.definitions()).unwrap(),
        );
        let ((c, first_id, f1_id, f2_id, w_id), ()) = zip(
            async {
                let h = opening.await.unwrap();
                let c = conversation(&h).await;
                let first = input(&a, &h, c, "initial", BusyMode::FollowUp).await;
                entered.wait().await;
                let f1 = input(&a, &h, c, "f1", BusyMode::FollowUp).await;
                let write = a
                    .write(
                        &h,
                        c,
                        EntryDraft::new("persisted-write"),
                        WriteOptions::default(),
                    )
                    .await
                    .unwrap();
                let f2 = input(&a, &h, c, "f2", BusyMode::FollowUp).await;
                a.configure_queues(
                    &h,
                    c,
                    QueueConfig {
                        steer: QueueMode::One,
                        follow_up: QueueMode::All,
                    },
                )
                .await
                .unwrap();
                let ids = (c, first.id(), f1.id(), f2.id(), write.id());
                h.close().await.unwrap();
                ids
            },
            driver,
        )
        .await;
        let (s1, entered, release) = step(Ok(answer("replayed")));
        let (s2, _, release2) = step(Ok(answer("successor")));
        release2.release();
        let (a, requests) = agent(vec![s1, s2]);
        let (opening, driver) = Harness::open(
            SqliteStorage::open(&path).unwrap(),
            TaskRegistry::new(a.definitions()).unwrap(),
        );
        zip(
            async {
                let h = opening.await.unwrap();
                assert!(requests.borrow().is_empty());
                assert!(!h.inspect().await.unwrap().progress_enabled);
                let first = h.submission(first_id).await.unwrap().unwrap();
                assert_eq!(
                    first.status().await.unwrap().status(),
                    SubmissionStatus::Placed
                );
                let before = state(&h, c).await;
                assert_eq!(
                    before
                        .inbox
                        .iter()
                        .map(|i| i.submission_id)
                        .collect::<Vec<_>>(),
                    vec![f1_id, w_id, f2_id]
                );
                let a1 = a.clone();
                let mut bad = config();
                bad.model.clear();
                let retry = h
                    .commit(move |tx| {
                        a1.admit_input(tx, c, "ignored", bad, options("f1", BusyMode::Reject))
                    })
                    .await
                    .unwrap();
                assert_eq!(retry.value, f1_id);
                assert_eq!(retry.seq, None);
                let wait = first.wait();
                entered.wait().await;
                release.release();
                done_answer(wait.await.unwrap());
                let f1 = h.submission(f1_id).await.unwrap().unwrap();
                let f2 = h.submission(f2_id).await.unwrap().unwrap();
                assert_eq!(
                    done_answer(f1.wait().await.unwrap()),
                    done_answer(f2.wait().await.unwrap())
                );
                assert_eq!(
                    h.submission(w_id)
                        .await
                        .unwrap()
                        .unwrap()
                        .status()
                        .await
                        .unwrap()
                        .status(),
                    SubmissionStatus::Done
                );
                assert!(state(&h, c).await.inbox.is_empty());
                assert!(state(&h, c).await.run.is_none());
                let retry = a
                    .submit(&h, c, "ignored", config(), options("f1", BusyMode::Reject))
                    .await
                    .unwrap();
                assert_eq!(retry.id(), f1_id);
                let entries = log(&h, c).await;
                assert_eq!(
                    entries.iter().map(|e| e.kind.as_str()).collect::<Vec<_>>(),
                    vec![
                        "agent.user",
                        "agent.assistant",
                        "persisted-write",
                        "agent.user",
                        "agent.user",
                        "agent.assistant"
                    ]
                );
                assert_eq!(
                    texts(&entries),
                    vec!["initial", "replayed", "f1", "f2", "successor"]
                );
                h.close().await.unwrap();
            },
            driver,
        )
        .await;
    }));
}

#[test]
fn passive_draft_roundtrip_retains_native_payloads_and_context_edits() {
    block_on(bounded(async {
        let (a, _) = agent(vec![]);
        let (opening, driver) = Harness::open(
            MemoryStorage::new(),
            TaskRegistry::new(a.definitions()).unwrap(),
        );
        zip(async {
            let h = opening.await.unwrap();
            let c = conversation(&h).await;
            let payload = json!({"max":u64::MAX,"min":i64::MIN,"reserved":{"$serde_json::private::Number":"not a number"}});
            let mut draft = EntryDraft::new("opaque");
            draft.model = Some(vec![payload.clone()]);
            draft.data = Some(payload.clone());
            draft.head = Some(Head::SelfEntry);
            draft.edits = Some(vec![ContextEdit::Replace { target: c, messages: vec![payload.clone()] }, ContextEdit::Omit { target: c }]);
            let expected_edits = draft.edits.clone();
            let receipt = a.write(&h, c, draft, WriteOptions::default()).await.unwrap();
            assert_eq!(receipt.status().await.unwrap().status(), SubmissionStatus::Done);
            let entries = log(&h, c).await;
            assert_eq!(entries[0].model, Some(vec![payload.clone()]));
            assert_eq!(entries[0].data, Some(payload));
            assert_eq!(entries[0].edits, expected_edits);
            assert_eq!(entries[0].head, Some(entries[0].id));
            h.close().await.unwrap();
        }, driver).await;
    }));
}

#[test]
fn invalid_queued_payload_rolls_back_receipt_inbox_and_sequence() {
    block_on(bounded(async {
        let (a, _) = agent(vec![]);
        let (opening, driver) = Harness::open(
            MemoryStorage::new(),
            TaskRegistry::new(a.definitions()).unwrap(),
        );
        zip(
            async {
                let h = opening.await.unwrap();
                let c = conversation(&h).await;
                let a1 = a.clone();
                h.commit(move |tx| {
                    a1.admit_input(tx, c, "initial", config(), SubmitOptions::default())
                })
                .await
                .unwrap();
                // The encoded write object plus the inbox array and item object
                // leave room for 61 containers in data, but not 62.
                let boundary = (0..61).fold(json!(0), |value, _| json!([value]));
                let mut accepted = EntryDraft::new("exact-queue-boundary");
                accepted.data = Some(boundary.clone());
                let receipt = a
                    .write(
                        &h,
                        c,
                        accepted,
                        WriteOptions {
                            request_id: Some("good".into()),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    receipt.status().await.unwrap().status(),
                    SubmissionStatus::Queued
                );
                assert_eq!(state(&h, c).await.inbox.len(), 1);

                let before = state(&h, c).await;
                let seq = h.inspect().await.unwrap().last_commit_seq;
                let mut draft = EntryDraft::new("too-deep-to-queue");
                draft.data = Some(json!([boundary]));
                assert!(
                    a.write(
                        &h,
                        c,
                        draft,
                        WriteOptions {
                            request_id: Some("bad".into()),
                            ..Default::default()
                        }
                    )
                    .await
                    .is_err()
                );
                assert_eq!(state(&h, c).await, before);
                assert_eq!(h.inspect().await.unwrap().last_commit_seq, seq);
                h.commit(move |tx| {
                    Box::pin(async move {
                        assert!(tx.submission_by_request(c, "bad").await?.is_none());
                        assert_eq!(
                            tx.scan_submissions(
                                SubmissionQuery {
                                    conversation_id: Some(c),
                                    ..Default::default()
                                },
                                128,
                                None
                            )
                            .await?
                            .items
                            .len(),
                            2
                        );
                        assert_eq!(
                            tx.scan_tasks(TaskQuery::default(), 128, None)
                                .await?
                                .items
                                .len(),
                            1
                        );
                        Ok(())
                    })
                })
                .await
                .unwrap();
                assert_eq!(log(&h, c).await.len(), 1);
                assert!(!h.inspect().await.unwrap().progress_enabled);
                h.close().await.unwrap();
            },
            driver,
        )
        .await;
    }));
}
