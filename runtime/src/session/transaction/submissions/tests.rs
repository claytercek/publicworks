use super::*;
use futures_lite::future::{block_on, zip};

fn id(n: u64) -> Id {
    Id::new(n).unwrap()
}
fn record(state: SubmissionState) -> SubmissionRecord {
    SubmissionRecord {
        id: id(3),
        conversation_id: id(2),
        request_id: Some("request".into()),
        state,
    }
}

#[test]
fn transition_matrix_and_immutable_replacements() {
    use SubmissionState::*;
    let states = [
        InputQueued,
        InputPlaced { entry: id(4) },
        InputDone {
            entry: id(4),
            answer: id(5),
        },
        InputUnanswered {
            entry: None,
            reason: "x".into(),
            detail: None,
        },
        InputUnanswered {
            entry: Some(id(4)),
            reason: "x".into(),
            detail: None,
        },
        WriteQueued,
        WriteDone { entry: id(4) },
        WriteUnanswered {
            reason: "x".into(),
            detail: None,
        },
    ];
    for (from, previous) in states.iter().enumerate() {
        for (to, next) in states.iter().enumerate() {
            let legal = matches!((from, to), (0, 1 | 3) | (1, 2 | 4) | (5, 6 | 7));
            assert_eq!(
                transition(&record(previous.clone()), &record(next.clone())).is_ok(),
                legal,
                "{previous:?} -> {next:?}"
            );
        }
    }
    let previous = record(InputQueued);
    for field in 0..4 {
        let mut next = record(InputPlaced { entry: id(4) });
        match field {
            0 => next.id = id(8),
            1 => next.conversation_id = id(8),
            2 => next.request_id = None,
            _ => next.state = WriteDone { entry: id(4) },
        }
        let state = Rc::new(RefCell::new(State::default()));
        assert!(stage_submission(&state, &previous, next).is_err());
        assert!(state.borrow().submissions.is_empty());
    }
    assert!(
        transition(
            &record(InputPlaced { entry: id(4) }),
            &record(InputDone {
                entry: id(8),
                answer: id(5)
            })
        )
        .is_err()
    );
    assert!(
        transition(
            &record(InputPlaced { entry: id(4) }),
            &record(InputUnanswered {
                entry: Some(id(8)),
                reason: "x".into(),
                detail: None
            })
        )
        .is_err()
    );
}

#[test]
fn final_assembly_rejects_conflicting_ids_and_state_identity_changes() {
    block_on(async {
        let mut storage = MemoryStorage::new();
        storage
            .commit(vec![StorageWrite::Conversation(ConversationRecord {
                id: id(2),
                parent: None,
                owner: None,
            })])
            .await
            .unwrap();
        let state = ConversationStateRecord {
            id: id(3),
            conversation_id: id(2),
            run: None,
            inbox: vec![],
            agent_config: None,
        };
        storage
            .commit(vec![StorageWrite::ConversationState(state.clone())])
            .await
            .unwrap();
        let mut changed = state.clone();
        changed.id = id(4);
        assert!(
            assemble(
                &mut storage,
                vec![],
                BTreeMap::new(),
                BTreeMap::from([(id(2), changed)])
            )
            .await
            .is_err()
        );
        let mut changed = state.clone();
        changed.conversation_id = id(4);
        assert!(
            assemble(
                &mut storage,
                vec![],
                BTreeMap::new(),
                BTreeMap::from([(id(2), changed)])
            )
            .await
            .is_err()
        );
        // Different candidate kinds cannot share a globally allocated ID.
        assert!(
            assemble(
                &mut storage,
                vec![StorageWrite::Entry(EntryRecord::new(
                    id(3),
                    id(2),
                    "collision"
                ))],
                BTreeMap::new(),
                BTreeMap::from([(id(2), state)])
            )
            .await
            .is_err()
        );
        // Two different states also cannot share one identity.
        let states = [2, 4].map(|c| {
            (
                id(c),
                ConversationStateRecord {
                    id: id(3),
                    conversation_id: id(c),
                    run: None,
                    inbox: vec![],
                    agent_config: None,
                },
            )
        });
        assert!(
            assemble(
                &mut storage,
                vec![StorageWrite::Conversation(ConversationRecord {
                    id: id(4),
                    parent: None,
                    owner: None
                })],
                BTreeMap::new(),
                BTreeMap::from(states)
            )
            .await
            .is_err()
        );
    });
}

#[test]
fn final_assembly_rechecks_submission_identity_and_creation_class() {
    block_on(async {
        let mut storage = MemoryStorage::new();
        let original = record(SubmissionState::InputQueued);
        storage
            .commit(vec![
                StorageWrite::Conversation(ConversationRecord {
                    id: id(2),
                    parent: None,
                    owner: None,
                }),
                StorageWrite::Conversation(ConversationRecord {
                    id: id(8),
                    parent: None,
                    owner: None,
                }),
                StorageWrite::Submission(original.clone()),
            ])
            .await
            .unwrap();
        for field in 0..5 {
            let mut next = original.clone();
            next.state = SubmissionState::InputUnanswered {
                entry: None,
                reason: "x".into(),
                detail: None,
            };
            match field {
                0 => next.id = id(8),
                1 => next.conversation_id = id(8),
                2 => next.request_id = None,
                3 => {
                    next.state = SubmissionState::WriteUnanswered {
                        reason: "x".into(),
                        detail: None,
                    }
                }
                _ => {}
            }
            let candidate = SubmissionCandidate {
                original: if field == 4 {
                    None
                } else {
                    Some(original.clone())
                },
                record: next,
            };
            assert!(
                assemble(
                    &mut storage,
                    vec![],
                    BTreeMap::from([(id(3), candidate)]),
                    BTreeMap::new()
                )
                .await
                .is_err()
            );
        }
    });
}

#[test]
fn private_replacements_emit_one_write_per_record() {
    block_on(async {
        let (session, driver) = Session::new(MemoryStorage::new());
        zip(
            async {
                session
                    .commit(|tx| {
                        Box::pin(async move {
                            let c = tx.create_conversation().await?.id;
                            let e = tx.append_entry(c, EntryDraft::new("input")).await?.id;
                            let s = tx
                                .create_submission(c, SubmissionType::Input, None)
                                .await?
                                .id;
                            tx.place_submission(s, e).await?;
                            tx.settle_submission(s, SubmissionSettlement::Done { answer: e })
                                .await?;
                            let first = tx.set_conversation_state(c, Default::default()).await?;
                            tx.update_conversation_state(c, |s| s.agent_config = Some(Value::Null))
                                .await?;
                            let state = tx.state.borrow();
                            assert_eq!(state.submissions.len(), 1);
                            assert_eq!(state.conversation_states.len(), 1);
                            assert_eq!(state.conversation_states[&c].id, first.id);
                            Ok(())
                        })
                    })
                    .await
                    .unwrap(); // both adapters reject duplicate IDs in a batch
                session.close().await.unwrap();
            },
            driver,
        )
        .await;
    });
}
