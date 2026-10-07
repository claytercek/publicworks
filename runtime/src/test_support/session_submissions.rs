//! Session-level submission contracts, run against both adapters without a Harness.
use crate::*;
use serde_json::{Value, json};

fn missing() -> Id {
    Id::new(MAX_NUMBER).unwrap()
}
fn unanswered() -> SubmissionSettlement {
    SubmissionSettlement::Unanswered {
        reason: "test".into(),
        detail: None,
    }
}
fn definition() -> TaskDefinition {
    TaskDefinition::new(
        "submission-test",
        1,
        |_| Ok(Value::Null),
        Default::default(),
    )
}
fn options(conversation: Id) -> TaskOptions {
    TaskOptions {
        ownership: TaskOwnership::Conversation,
        conversation_id: Some(conversation),
        background: false,
    }
}
fn item(submission_id: Id) -> InboxItem {
    InboxItem {
        submission_id,
        payload: json!({"opaque": true}),
    }
}
async fn seed(session: &Session) -> (Id, Id, Id, Id) {
    session
        .commit(|tx| {
            Box::pin(async move {
                let c = tx.create_conversation().await?.id;
                let other = tx.create_conversation().await?.id;
                let entry = tx.append_entry(c, EntryDraft::new("input")).await?.id;
                let task = tx
                    .create_task(definition(), Value::Null, options(c))
                    .await?
                    .id;
                Ok((c, other, entry, task))
            })
        })
        .await
        .unwrap()
        .value
}

pub async fn submission_tx_reads(session: &Session) {
    let (c, other, _, _) = seed(session).await;
    let original = session
        .commit(move |tx| {
            Box::pin(async move {
                assert!(tx.submission(missing()).await?.is_none());
                assert!(tx.submission_by_request(c, "retry").await?.is_none());
                assert!(tx.conversation_state(c).await?.is_none());
                let record = tx
                    .create_submission(c, SubmissionType::Input, Some("retry".into()))
                    .await?;
                tx.create_submission(other, SubmissionType::Write, Some("retry".into()))
                    .await?;
                tx.set_conversation_state(
                    c,
                    ConversationStateDraft {
                        inbox: vec![item(record.id)],
                        ..Default::default()
                    },
                )
                .await?;
                assert_eq!(
                    tx.submission(record.id).await.unwrap_err(),
                    SessionError::ReadAfterWrite
                );
                assert_eq!(
                    tx.submission_by_request(c, "retry").await.unwrap_err(),
                    SessionError::ReadAfterWrite
                );
                assert_eq!(
                    tx.scan_submissions(SubmissionQuery::default(), 10, None)
                        .await
                        .unwrap_err(),
                    SessionError::ReadAfterWrite
                );
                assert_eq!(
                    tx.conversation_state(c).await.unwrap_err(),
                    SessionError::ReadAfterWrite
                );
                Ok(record)
            })
        })
        .await
        .unwrap();
    let record = original.value;
    let id = record.id;
    let read = session
        .commit(move |tx| {
            Box::pin(async move {
                assert_eq!(
                    tx.submission_by_request(c, "retry").await?,
                    Some(record.clone())
                );
                assert_eq!(tx.submission(id).await?, Some(record.clone()));
                let page = tx
                    .scan_submissions(
                        SubmissionQuery {
                            conversation_id: Some(c),
                            status: Some(SubmissionStatus::Queued),
                        },
                        1,
                        None,
                    )
                    .await?;
                assert_eq!(page.items, vec![record]);
                let page = tx
                    .scan_submissions(SubmissionQuery::default(), 1, None)
                    .await?;
                assert_eq!(page.items[0].id, id);
                assert_eq!(
                    tx.scan_submissions(SubmissionQuery::default(), 1, page.next)
                        .await?
                        .items[0]
                        .conversation_id,
                    other
                );
                let mut detached = tx.conversation_state(c).await?.unwrap();
                detached.inbox[0].payload = json!("changed");
                assert_eq!(tx.conversation_state(c).await?.unwrap().inbox[0], item(id));
                Ok(id)
            })
        })
        .await
        .unwrap();
    // Request admission can return the committed ID without consuming a Seq.
    assert_eq!(read.value, id);
    assert!(read.seq.is_none());
    for kind in [SubmissionType::Input, SubmissionType::Write] {
        let result = session
            .commit(move |tx| {
                Box::pin(async move {
                    tx.append_entry(c, EntryDraft::new("must roll back"))
                        .await?;
                    tx.create_submission(c, kind, Some("retry".into())).await
                })
            })
            .await;
        assert!(matches!(result, Err(SessionError::Invalid(_))));
    }
    for kind in [SubmissionType::Input, SubmissionType::Write] {
        let result = session
            .commit(move |tx| {
                Box::pin(async move {
                    tx.create_submission(c, SubmissionType::Input, Some("new".into()))
                        .await?;
                    tx.create_submission(c, kind, Some("new".into())).await
                })
            })
            .await;
        assert!(matches!(result, Err(SessionError::Invalid(_))));
    }
    session
        .commit(move |tx| {
            Box::pin(async move {
                assert_eq!(
                    tx.scan_entries(EntryQuery::new(c), 100, None)
                        .await?
                        .items
                        .len(),
                    1
                );
                assert!(tx.submission_by_request(c, "new").await?.is_none());
                // Even a caught failed mutation latches all reads.
                assert!(tx.settle_submission(missing(), unanswered()).await.is_err());
                assert_eq!(
                    tx.conversation_state(c).await.unwrap_err(),
                    SessionError::ReadAfterWrite
                );
                Ok(())
            })
        })
        .await
        .unwrap();
}

pub async fn submission_tx_transitions(session: &Session) {
    let (c, _, entry, _) = seed(session).await;
    // Every legal edge, including coalescing creation, placement, and settlement.
    for kind in [SubmissionType::Input, SubmissionType::Write] {
        for placed in [false, true] {
            let record = session
                .commit(move |tx| {
                    Box::pin(async move {
                        let record = tx.create_submission(c, kind, None).await?;
                        assert!(
                            tx.settle_submission(
                                record.id,
                                SubmissionSettlement::Done { answer: entry }
                            )
                            .await
                            .is_err()
                        );
                        if placed {
                            tx.place_submission(record.id, entry).await?;
                            assert!(tx.place_submission(record.id, entry).await.is_err());
                        }
                        let final_record = if placed && kind == SubmissionType::Write {
                            assert!(tx.settle_submission(record.id, unanswered()).await.is_err());
                            SubmissionRecord {
                                state: SubmissionState::WriteDone { entry },
                                ..record
                            }
                        } else {
                            tx.settle_submission(record.id, unanswered()).await?
                        };
                        assert!(
                            tx.settle_submission(final_record.id, unanswered())
                                .await
                                .is_err()
                        );
                        assert!(tx.place_submission(final_record.id, entry).await.is_err());
                        Ok(final_record)
                    })
                })
                .await
                .unwrap()
                .value;
            let id = record.id;
            session
                .commit(move |tx| {
                    Box::pin(async move {
                        assert_eq!(tx.submission(id).await?, Some(record));
                        assert!(tx.settle_submission(id, unanswered()).await.is_err());
                        assert!(tx.place_submission(id, entry).await.is_err());
                        Ok(())
                    })
                })
                .await
                .unwrap();
        }
    }
    // A committed queued baseline may traverse two legal edges in one commit.
    for done in [false, true] {
        let id = session
            .commit(move |tx| {
                Box::pin(async move {
                    Ok(tx
                        .create_submission(c, SubmissionType::Input, None)
                        .await?
                        .id)
                })
            })
            .await
            .unwrap()
            .value;
        let result = session
            .commit(move |tx| {
                Box::pin(async move {
                    tx.place_submission(id, entry).await?;
                    tx.settle_submission(
                        id,
                        if done {
                            SubmissionSettlement::Done { answer: entry }
                        } else {
                            unanswered()
                        },
                    )
                    .await
                })
            })
            .await
            .unwrap()
            .value;
        assert_eq!(
            result.state,
            if done {
                SubmissionState::InputDone {
                    entry,
                    answer: entry,
                }
            } else {
                SubmissionState::InputUnanswered {
                    entry: Some(entry),
                    reason: "test".into(),
                    detail: None,
                }
            }
        );
    }
    let input = session
        .commit(move |tx| {
            Box::pin(async move {
                let s = tx.create_submission(c, SubmissionType::Input, None).await?;
                tx.place_submission(s.id, entry).await
            })
        })
        .await
        .unwrap()
        .value;
    let id = input.id;
    // Both jobs are admitted before either observer is polled. FIFO wins.
    let winner = session.commit(move |tx| {
        Box::pin(async move {
            tx.settle_submission(id, SubmissionSettlement::Done { answer: entry })
                .await
        })
    });
    let loser = session
        .commit(move |tx| Box::pin(async move { tx.settle_submission(id, unanswered()).await }));
    assert!(matches!(loser.await, Err(SessionError::Invalid(_))));
    assert_eq!(
        winner.await.unwrap().value.state,
        SubmissionState::InputDone {
            entry,
            answer: entry
        }
    );
    // Invalid references and unknown records cannot leak a partial receipt.
    for input in [false, true] {
        assert!(matches!(
            session
                .commit(move |tx| Box::pin(async move {
                    let s = tx
                        .create_submission(
                            c,
                            if input {
                                SubmissionType::Input
                            } else {
                                SubmissionType::Write
                            },
                            Some("bad-entry".into()),
                        )
                        .await?;
                    tx.place_submission(s.id, missing()).await
                }))
                .await,
            Err(SessionError::Invalid(_))
        ));
    }
    assert!(
        session
            .commit(move |tx| Box::pin(async move { tx.place_submission(missing(), entry).await }))
            .await
            .is_err()
    );
    assert!(
        session
            .commit(move |tx| Box::pin(async move {
                tx.create_submission(missing(), SubmissionType::Input, None)
                    .await
            }))
            .await
            .is_err()
    );
    session
        .commit(move |tx| {
            Box::pin(async move {
                assert!(tx.submission_by_request(c, "bad-entry").await?.is_none());
                assert_eq!(
                    tx.submission(id).await?.unwrap().state,
                    SubmissionState::InputDone {
                        entry,
                        answer: entry
                    }
                );
                Ok(())
            })
        })
        .await
        .unwrap();
}

pub async fn submission_tx_final_candidates(session: &Session) {
    let (c, other, entry, task) = seed(session).await;
    // An inbox can be cleared before OR after its submission is placed; a run
    // can be installed before OR after that placement, using the final view.
    for state_first in [false, true] {
        let id = session
            .commit(move |tx| {
                Box::pin(async move {
                    let s = tx.create_submission(c, SubmissionType::Input, None).await?;
                    tx.set_conversation_state(
                        c,
                        ConversationStateDraft {
                            inbox: vec![item(s.id)],
                            ..Default::default()
                        },
                    )
                    .await?;
                    Ok(s.id)
                })
            })
            .await
            .unwrap()
            .value;
        // An invalid final inbox rejects regardless of staging order.
        assert!(
            session
                .commit(move |tx| Box::pin(async move {
                    let draft = ConversationStateDraft {
                        inbox: vec![item(id)],
                        ..Default::default()
                    };
                    if state_first {
                        tx.set_conversation_state(c, draft.clone()).await?;
                    }
                    tx.place_submission(id, entry).await?;
                    if !state_first {
                        tx.set_conversation_state(c, draft).await?;
                    }
                    Ok(())
                }))
                .await
                .is_err()
        );
        session
            .commit(move |tx| {
                Box::pin(async move {
                    assert_eq!(
                        tx.submission(id).await?.unwrap().status(),
                        SubmissionStatus::Queued
                    );
                    let draft = ConversationStateDraft {
                        run: Some(ConversationRun {
                            task_id: task,
                            input_submission_ids: vec![id],
                        }),
                        ..Default::default()
                    };
                    if state_first {
                        tx.set_conversation_state(c, draft.clone()).await?;
                    }
                    tx.place_submission(id, entry).await?;
                    if !state_first {
                        tx.set_conversation_state(c, draft).await?;
                    }
                    Ok(())
                })
            })
            .await
            .unwrap();
        // A still-referenced input cannot settle without clearing its run.
        assert!(
            session
                .commit(move |tx| Box::pin(
                    async move { tx.settle_submission(id, unanswered()).await }
                ))
                .await
                .is_err()
        );
        session
            .commit(move |tx| {
                Box::pin(async move {
                    if state_first {
                        tx.set_conversation_state(c, Default::default()).await?;
                    }
                    tx.settle_submission(id, unanswered()).await?;
                    if !state_first {
                        tx.set_conversation_state(c, Default::default()).await?;
                    }
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
    let (queued, placed, write, foreign, foreign_task) = session
        .commit(move |tx| {
            Box::pin(async move {
                let queued = tx
                    .create_submission(c, SubmissionType::Input, None)
                    .await?
                    .id;
                let placed = tx
                    .create_submission(c, SubmissionType::Input, None)
                    .await?
                    .id;
                tx.place_submission(placed, entry).await?;
                let write = tx
                    .create_submission(c, SubmissionType::Write, None)
                    .await?
                    .id;
                let foreign = tx
                    .create_submission(other, SubmissionType::Input, None)
                    .await?
                    .id;
                let foreign_task = tx
                    .create_task(definition(), Value::Null, options(other))
                    .await?
                    .id;
                Ok((queued, placed, write, foreign, foreign_task))
            })
        })
        .await
        .unwrap()
        .value;
    let invalid_drafts = vec![
        ConversationStateDraft {
            inbox: vec![item(queued), item(queued)],
            ..Default::default()
        },
        ConversationStateDraft {
            inbox: vec![item(missing())],
            ..Default::default()
        },
        ConversationStateDraft {
            inbox: vec![item(foreign)],
            ..Default::default()
        },
        ConversationStateDraft {
            inbox: vec![item(placed)],
            ..Default::default()
        },
        ConversationStateDraft {
            run: Some(ConversationRun {
                task_id: missing(),
                input_submission_ids: vec![],
            }),
            ..Default::default()
        },
        ConversationStateDraft {
            run: Some(ConversationRun {
                task_id: foreign_task,
                input_submission_ids: vec![],
            }),
            ..Default::default()
        },
        ConversationStateDraft {
            run: Some(ConversationRun {
                task_id: task,
                input_submission_ids: vec![queued],
            }),
            ..Default::default()
        },
        ConversationStateDraft {
            run: Some(ConversationRun {
                task_id: task,
                input_submission_ids: vec![write],
            }),
            ..Default::default()
        },
        ConversationStateDraft {
            run: Some(ConversationRun {
                task_id: task,
                input_submission_ids: vec![foreign],
            }),
            ..Default::default()
        },
        ConversationStateDraft {
            run: Some(ConversationRun {
                task_id: task,
                input_submission_ids: vec![missing()],
            }),
            ..Default::default()
        },
        ConversationStateDraft {
            run: Some(ConversationRun {
                task_id: task,
                input_submission_ids: vec![placed, placed],
            }),
            ..Default::default()
        },
    ];
    for draft in invalid_drafts {
        assert!(matches!(
            session
                .commit(move |tx| Box::pin(async move {
                    tx.append_entry(c, EntryDraft::new("rollback")).await?;
                    tx.set_conversation_state(c, draft).await
                }))
                .await,
            Err(SessionError::Invalid(_))
        ));
    }
    assert!(
        session
            .commit(move |tx| Box::pin(async move {
                tx.set_conversation_state(missing(), Default::default())
                    .await
            }))
            .await
            .is_err()
    );
    session
        .commit(move |tx| {
            Box::pin(async move {
                let state = tx.conversation_state(c).await?.unwrap();
                assert!(state.inbox.is_empty() && state.run.is_none());
                assert_eq!(
                    tx.scan_entries(EntryQuery::new(c), 100, None)
                        .await?
                        .items
                        .len(),
                    1
                );
                // Public update cannot move a state or steal another record's ID;
                // a caught error preserves the previous candidate.
                tx.update_conversation_state(c, |s| s.agent_config = Some(Value::Null))
                    .await?;
                assert!(
                    tx.update_conversation_state(c, move |s| s.id = task)
                        .await
                        .is_err()
                );
                assert!(
                    tx.update_conversation_state(c, move |s| s.conversation_id = other)
                        .await
                        .is_err()
                );
                let final_state = tx.update_conversation_state(c, |_| {}).await?;
                assert_eq!(final_state.id, state.id);
                assert_eq!(final_state.agent_config, Some(Value::Null));
                Ok(())
            })
        })
        .await
        .unwrap();
    // Entry references include a new fork's committed ancestry, but exclude
    // another conversation's entries and missing answers.
    session
        .commit(move |tx| {
            Box::pin(async move {
                let child = tx.fork_conversation(c, entry).await?.id;
                let s = tx
                    .create_submission(child, SubmissionType::Input, None)
                    .await?
                    .id;
                tx.place_submission(s, entry).await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    for bad_answer in [false, true] {
        assert!(matches!(
            session
                .commit(move |tx| Box::pin(async move {
                    let s = tx
                        .create_submission(
                            if bad_answer { c } else { other },
                            SubmissionType::Input,
                            None,
                        )
                        .await?
                        .id;
                    tx.place_submission(s, entry).await?;
                    if bad_answer {
                        tx.settle_submission(s, SubmissionSettlement::Done { answer: missing() })
                            .await?;
                    }
                    Ok(())
                }))
                .await,
            Err(SessionError::Invalid(_))
        ));
    }
    // Final references can point to newly staged tasks, submissions and entries.
    session
        .commit(|tx| {
            Box::pin(async move {
                let c = tx.create_conversation().await?.id;
                let task = tx
                    .create_task(definition(), Value::Null, options(c))
                    .await?
                    .id;
                let s = tx
                    .create_submission(c, SubmissionType::Input, None)
                    .await?
                    .id;
                tx.set_conversation_state(
                    c,
                    ConversationStateDraft {
                        run: Some(ConversationRun {
                            task_id: task,
                            input_submission_ids: vec![s],
                        }),
                        ..Default::default()
                    },
                )
                .await?;
                let entry = tx.append_entry(c, EntryDraft::new("new")).await?.id;
                tx.place_submission(s, entry).await?;
                Ok(())
            })
        })
        .await
        .unwrap();
}

pub async fn submission_tx_withdrawal(session: &Session) {
    let (c, other, entry, _) = seed(session).await;
    let ids = session
        .commit(move |tx| {
            Box::pin(async move {
                let mut ids = Vec::new();
                for kind in [
                    SubmissionType::Input,
                    SubmissionType::Write,
                    SubmissionType::Input,
                ] {
                    ids.push(tx.create_submission(c, kind, None).await?.id);
                }
                tx.set_conversation_state(
                    c,
                    ConversationStateDraft {
                        inbox: ids.iter().copied().map(item).collect(),
                        ..Default::default()
                    },
                )
                .await?;
                Ok(ids)
            })
        })
        .await
        .unwrap()
        .value;
    let middle = ids[1];
    let noop = session
        .commit(move |tx| {
            Box::pin(async move {
                assert_eq!(
                    tx.withdraw_submission(missing(), None).await?,
                    WithdrawalResult::NotFound
                );
                assert_eq!(
                    tx.withdraw_submission(middle, Some(other)).await?,
                    WithdrawalResult::NotFound
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    assert!(noop.seq.is_none());
    let result: Result<CommitReceipt<()>, _> = session
        .commit(move |tx| {
            Box::pin(async move {
                assert_eq!(
                    tx.withdraw_submission(middle, None).await?,
                    WithdrawalResult::Aborted
                );
                Err(SessionError::Invalid("callback rollback".into()))
            })
        })
        .await;
    assert!(result.is_err());
    session
        .commit(move |tx| {
            Box::pin(async move {
                assert_eq!(tx.conversation_state(c).await?.unwrap().inbox.len(), 3);
                assert_eq!(
                    tx.submission(middle).await?.unwrap().status(),
                    SubmissionStatus::Queued
                );
                assert_eq!(
                    tx.withdraw_submission(middle, Some(c)).await?,
                    WithdrawalResult::Aborted
                );
                assert_eq!(
                    tx.withdraw_submission(middle, None).await?,
                    WithdrawalResult::Settled
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    session
        .commit(move |tx| {
            Box::pin(async move {
                let state = tx.conversation_state(c).await?.unwrap();
                assert_eq!(state.inbox, vec![item(ids[0]), item(ids[2])]);
                assert_eq!(
                    tx.submission(middle).await?.unwrap().state,
                    SubmissionState::WriteUnanswered {
                        reason: "aborted".into(),
                        detail: None
                    }
                );
                // Leaving a committed inbox reference behind rejects at assembly.
                tx.place_submission(ids[0], entry).await
            })
        })
        .await
        .unwrap_err();
    session
        .commit(move |tx| {
            Box::pin(async move {
                let s = tx
                    .create_submission(c, SubmissionType::Input, None)
                    .await?
                    .id;
                tx.place_submission(s, entry).await?;
                assert_eq!(
                    tx.withdraw_submission(s, None).await?,
                    WithdrawalResult::AlreadyPlaced
                );
                tx.settle_submission(s, unanswered()).await?;
                assert_eq!(
                    tx.withdraw_submission(s, None).await?,
                    WithdrawalResult::Settled
                );
                // Withdrawal composes with a newly created inbox as well.
                let c = tx.create_conversation().await?.id;
                let s = tx
                    .create_submission(c, SubmissionType::Input, None)
                    .await?
                    .id;
                tx.set_conversation_state(
                    c,
                    ConversationStateDraft {
                        inbox: vec![item(s)],
                        ..Default::default()
                    },
                )
                .await?;
                assert_eq!(
                    tx.withdraw_submission(s, None).await?,
                    WithdrawalResult::Aborted
                );
                assert!(
                    tx.update_conversation_state(c, |_| {})
                        .await?
                        .inbox
                        .is_empty()
                );
                // A queued receipt without an inbox is also withdrawable.
                let s = tx
                    .create_submission(c, SubmissionType::Write, None)
                    .await?
                    .id;
                assert_eq!(
                    tx.withdraw_submission(s, None).await?,
                    WithdrawalResult::Aborted
                );
                Ok(())
            })
        })
        .await
        .unwrap();
}

fn nested(depth: usize) -> Value {
    (0..depth).fold(Value::Null, |v, _| Value::Array(vec![v]))
}
pub async fn submission_tx_json(session: &Session) {
    let (c, _, _, _) = seed(session).await;
    let payload = json!({"$serde_json::private::Number": "ordinary text", "numbers": [i64::MIN, u64::MAX, 1.2345678901234567_f64]});
    let expected = payload.clone();
    let (id, state) = session
        .commit(move |tx| {
            Box::pin(async move {
                let s = tx
                    .create_submission(c, SubmissionType::Input, None)
                    .await?
                    .id;
                let state = tx
                    .set_conversation_state(
                        c,
                        ConversationStateDraft {
                            inbox: vec![InboxItem {
                                submission_id: s,
                                payload: payload.clone(),
                            }],
                            agent_config: Some(payload.clone()),
                            run: None,
                        },
                    )
                    .await?;
                // Failed payload updates do not replace a good candidate.
                assert!(
                    tx.update_conversation_state(c, |s| s.inbox[0].payload =
                        nested(MAX_JSON_DEPTH - 1))
                        .await
                        .is_err()
                );
                assert!(
                    tx.update_conversation_state(c, |s| s.agent_config =
                        Some(nested(MAX_JSON_DEPTH + 1)))
                        .await
                        .is_err()
                );
                let detail = tx
                    .create_submission(c, SubmissionType::Write, None)
                    .await?
                    .id;
                assert!(
                    tx.settle_submission(
                        detail,
                        SubmissionSettlement::Unanswered {
                            reason: "bad".into(),
                            detail: Some(nested(MAX_JSON_DEPTH + 1))
                        }
                    )
                    .await
                    .is_err()
                );
                tx.settle_submission(
                    detail,
                    SubmissionSettlement::Unanswered {
                        reason: "ok".into(),
                        detail: Some(payload),
                    },
                )
                .await?;
                // Canonical-native checks also hold under feature unification.
                let extended: Value = serde_json::from_str("18446744073709551616").unwrap();
                let spelling = extended.to_string();
                if spelling == "18446744073709551616" {
                    assert!(
                        tx.update_conversation_state(c, move |s| s.agent_config =
                            Some(extended.clone()))
                            .await
                            .is_err()
                    );
                }
                Ok((detail, state))
            })
        })
        .await
        .unwrap()
        .value;
    let ids = session
        .commit(move |tx| {
            Box::pin(async move {
                assert_eq!(tx.conversation_state(c).await?, Some(state));
                let mut record = tx.submission(id).await?.unwrap();
                assert_eq!(
                    record.state,
                    SubmissionState::WriteUnanswered {
                        reason: "ok".into(),
                        detail: Some(expected)
                    }
                );
                if let SubmissionState::WriteUnanswered { detail, .. } = &mut record.state {
                    *detail = None;
                }
                assert_ne!(tx.submission(id).await?, Some(record));
                tx.update_conversation_state(c, |s| {
                    s.inbox[0].payload = nested(MAX_JSON_DEPTH - 2);
                    s.agent_config = Some(nested(MAX_JSON_DEPTH));
                })
                .await?;
                let mut ids = Vec::new();
                for detail in [None, Some(Value::Null), Some(nested(MAX_JSON_DEPTH))] {
                    let id = tx
                        .create_submission(c, SubmissionType::Write, None)
                        .await?
                        .id;
                    tx.settle_submission(
                        id,
                        SubmissionSettlement::Unanswered {
                            reason: "boundary".into(),
                            detail,
                        },
                    )
                    .await?;
                    ids.push(id);
                }
                Ok(ids)
            })
        })
        .await
        .unwrap()
        .value;
    session
        .commit(move |tx| {
            Box::pin(async move {
                for (id, detail) in
                    ids.into_iter()
                        .zip([None, Some(Value::Null), Some(nested(MAX_JSON_DEPTH))])
                {
                    assert_eq!(
                        tx.submission(id).await?.unwrap().state,
                        SubmissionState::WriteUnanswered {
                            reason: "boundary".into(),
                            detail
                        }
                    );
                }
                assert_eq!(
                    tx.conversation_state(c).await?.unwrap().inbox[0].payload,
                    nested(MAX_JSON_DEPTH - 2)
                );
                Ok(())
            })
        })
        .await
        .unwrap();
}
