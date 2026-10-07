use super::*;
use serde_json::{Value, json};

pub fn submission(n: u64, conversation: u64, state: SubmissionState) -> SubmissionRecord {
    SubmissionRecord {
        id: id(n),
        conversation_id: id(conversation),
        request_id: None,
        state,
    }
}

fn states(value: Value) -> Vec<SubmissionState> {
    vec![
        SubmissionState::InputQueued,
        SubmissionState::InputPlaced { entry: id(40) },
        SubmissionState::InputDone {
            entry: id(40),
            answer: id(41),
        },
        SubmissionState::InputUnanswered {
            entry: Some(id(40)),
            reason: "model_error".into(),
            detail: Some(value.clone()),
        },
        SubmissionState::InputUnanswered {
            entry: None,
            reason: "reset".into(),
            detail: None,
        },
        SubmissionState::WriteQueued,
        SubmissionState::WriteDone { entry: id(42) },
        SubmissionState::WriteUnanswered {
            reason: "stale".into(),
            detail: Some(value),
        },
        SubmissionState::WriteUnanswered {
            reason: "aborted".into(),
            detail: None,
        },
    ]
}

/// Whole-record replacements, shared IDs, filtered cursors, request scope, and
/// detached return values.
pub async fn submission_storage(store: &mut dyn Storage) {
    assert!(store.submission(id(2)).await.unwrap().is_none());
    assert!(
        store
            .submission_by_request(id(1), "same")
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.conversation_state(id(1)).await.unwrap().is_none());

    let mut first = submission(2, 1, SubmissionState::InputQueued);
    first.request_id = Some("same".into());
    let mut second = submission(3, 1, SubmissionState::WriteQueued);
    second.request_id = Some("write".into());
    let mut other_conversation = submission(4, 9, SubmissionState::InputQueued);
    other_conversation.request_id = Some("same".into());
    store
        .commit(vec![
            StorageWrite::Submission(other_conversation.clone()),
            StorageWrite::Submission(second.clone()),
            StorageWrite::Submission(first.clone()),
        ])
        .await
        .unwrap();

    assert_eq!(
        store.submission_by_request(id(1), "same").await.unwrap(),
        Some(first.clone())
    );
    assert_eq!(
        store.submission_by_request(id(9), "same").await.unwrap(),
        Some(other_conversation.clone())
    );
    assert!(
        store
            .submission_by_request(id(1), "missing")
            .await
            .unwrap()
            .is_none()
    );

    let page = store
        .scan_submissions(SubmissionQuery::default(), 2, None)
        .await
        .unwrap();
    assert_eq!(page.items, vec![first.clone(), second.clone()]);
    let cursor: Cursor =
        serde_json::from_value(serde_json::to_value(page.next.unwrap()).unwrap()).unwrap();
    let page = store
        .scan_submissions(SubmissionQuery::default(), 2, Some(cursor))
        .await
        .unwrap();
    assert_eq!(page.items, vec![other_conversation]);
    assert!(page.next.is_none());
    assert_eq!(
        store
            .scan_submissions(
                SubmissionQuery {
                    conversation_id: Some(id(1)),
                    status: Some(SubmissionStatus::Queued),
                },
                10,
                None,
            )
            .await
            .unwrap()
            .items,
        vec![first.clone(), second]
    );
    assert!(
        store
            .scan_submissions(
                SubmissionQuery {
                    conversation_id: Some(id(1)),
                    status: Some(SubmissionStatus::Done),
                },
                10,
                None,
            )
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert!(
        store
            .scan_submissions(SubmissionQuery::default(), 0, None)
            .await
            .is_err()
    );

    let replacement = submission(
        2,
        1,
        SubmissionState::InputUnanswered {
            entry: None,
            reason: "aborted".into(),
            detail: Some(json!({"detached": [1]})),
        },
    );
    store
        .commit(vec![StorageWrite::Submission(replacement.clone())])
        .await
        .unwrap();
    let mut detached = store.submission(id(2)).await.unwrap().unwrap();
    if let SubmissionState::InputUnanswered { detail, .. } = &mut detached.state {
        detail.as_mut().unwrap()["detached"][0] = json!(99);
    } else {
        panic!("unexpected state");
    }
    assert_eq!(store.submission(id(2)).await.unwrap(), Some(replacement));

    for collision in [
        StorageWrite::Conversation(conversation(2)),
        StorageWrite::Entry(entry(2, 1)),
        StorageWrite::Task(task(2)),
        StorageWrite::ConversationState(ConversationStateRecord {
            id: id(2),
            conversation_id: id(1),
            run: None,
            inbox: Vec::new(),
            agent_config: None,
        }),
    ] {
        assert!(store.commit(vec![collision]).await.is_err());
    }
    assert_eq!(store.mint_id().await.unwrap(), id(5));
}

/// Every legal record shape plus strict impossible-state decoding and null/omit.
pub async fn submission_json(store: &mut dyn Storage) {
    let opaque = json!({
        "$serde_json::private::Number": "ordinary text",
        "nested": [null, {"x": u64::MAX}]
    });
    for state in states(opaque) {
        let mut record = submission(2, 1, state);
        record.request_id = Some("request".into());
        store
            .commit(vec![StorageWrite::Submission(record.clone())])
            .await
            .unwrap();
        assert_eq!(store.submission(id(2)).await.unwrap(), Some(record.clone()));
        let value = serde_json::to_value(&record).unwrap();
        assert_eq!(
            value.get("detail").is_some(),
            record.state.detail().is_some()
        );
        assert_eq!(
            serde_json::from_value::<SubmissionRecord>(value).unwrap(),
            record
        );
        let text = serde_json::to_string(&record).unwrap();
        for wrapper in [
            format!(r#"{{"type":"submission","value":{text}}}"#),
            format!(r#"{{"value":{text},"type":"submission"}}"#),
        ] {
            assert_eq!(
                serde_json::from_str::<StorageWrite>(&wrapper).unwrap(),
                StorageWrite::Submission(record.clone())
            );
        }
    }

    let explicit_null: SubmissionRecord = serde_json::from_str(
        r#"{"id":2,"conversationId":1,"type":"write","status":"unanswered","reason":"x","detail":null}"#,
    )
    .unwrap();
    assert!(matches!(
        explicit_null.state,
        SubmissionState::WriteUnanswered {
            detail: Some(Value::Null),
            ..
        }
    ));
    let omitted: SubmissionRecord = serde_json::from_str(
        r#"{"id":2,"conversationId":1,"type":"write","status":"unanswered","reason":"x"}"#,
    )
    .unwrap();
    assert!(matches!(
        omitted.state,
        SubmissionState::WriteUnanswered { detail: None, .. }
    ));

    for malformed in [
        r#"{"id":2,"conversationId":1,"type":"write","status":"placed","entry":3}"#,
        r#"{"id":2,"conversationId":1,"type":"write","status":"done","entry":3,"answer":4}"#,
        r#"{"id":2,"conversationId":1,"type":"input","status":"done","entry":3}"#,
        r#"{"id":2,"conversationId":1,"type":"input","status":"queued","entry":3}"#,
        r#"{"id":2,"conversationId":1,"type":"input","status":"unanswered"}"#,
        r#"{"id":2,"conversationId":1,"type":"input","status":"future"}"#,
    ] {
        assert!(
            serde_json::from_str::<SubmissionRecord>(malformed).is_err(),
            "{malformed}"
        );
    }

    for settlement in [
        SubmissionSettlement::Done { answer: id(8) },
        SubmissionSettlement::Unanswered {
            reason: "faulted".into(),
            detail: Some(Value::Null),
        },
        SubmissionSettlement::Unanswered {
            reason: "reset".into(),
            detail: None,
        },
    ] {
        let value = serde_json::to_value(&settlement).unwrap();
        assert_eq!(
            serde_json::from_value::<SubmissionSettlement>(value).unwrap(),
            settlement
        );
    }
}

/// Dedicated state is replaceable, ordered, detached, and in the shared ID namespace.
pub async fn conversation_state_storage(store: &mut dyn Storage) {
    let mut state = ConversationStateRecord {
        id: id(2),
        conversation_id: id(10),
        run: Some(ConversationRun {
            task_id: id(30),
            input_submission_ids: vec![id(3), id(4)],
        }),
        inbox: vec![
            InboxItem {
                submission_id: id(5),
                payload: json!({"mode":"followUp", "content": null}),
            },
            InboxItem {
                submission_id: id(6),
                payload: json!({"mode":"write", "draft":{"kind":"note"}}),
            },
        ],
        agent_config: Some(Value::Null),
    };
    store
        .commit(vec![StorageWrite::ConversationState(state.clone())])
        .await
        .unwrap();
    assert_eq!(
        store.conversation_state(id(10)).await.unwrap(),
        Some(state.clone())
    );
    assert!(store.conversation_state(id(11)).await.unwrap().is_none());

    let mut detached = store.conversation_state(id(10)).await.unwrap().unwrap();
    detached.inbox.reverse();
    detached.agent_config = Some(json!({"changed":true}));
    assert_eq!(
        store.conversation_state(id(10)).await.unwrap(),
        Some(state.clone())
    );

    state.run.as_mut().unwrap().input_submission_ids.push(id(7));
    state.inbox.remove(0);
    state.agent_config = None;
    store
        .commit(vec![StorageWrite::ConversationState(state.clone())])
        .await
        .unwrap();
    assert_eq!(
        store.conversation_state(id(10)).await.unwrap(),
        Some(state.clone())
    );
    let value = serde_json::to_value(&state).unwrap();
    assert!(value.get("agentConfig").is_none());
    assert_eq!(
        serde_json::from_value::<ConversationStateRecord>(value).unwrap(),
        state
    );

    for collision in [
        StorageWrite::Conversation(conversation(2)),
        StorageWrite::Entry(entry(2, 1)),
        StorageWrite::Task(task(2)),
        StorageWrite::Submission(submission(2, 1, SubmissionState::InputQueued)),
    ] {
        assert!(store.commit(vec![collision]).await.is_err());
    }
}

/// Opaque submission and conversation-state values use the native numeric domain
/// and the shared depth budget, including inbox structural wrappers.
pub async fn submission_payload_limits(store: &mut dyn Storage) {
    let accepted = submission(
        2,
        1,
        SubmissionState::InputUnanswered {
            entry: None,
            reason: "x".into(),
            detail: Some(nested(64)),
        },
    );
    store
        .commit(vec![StorageWrite::Submission(accepted.clone())])
        .await
        .unwrap();
    assert_eq!(
        store.submission(id(2)).await.unwrap(),
        Some(accepted.clone())
    );
    assert_eq!(
        serde_json::from_str::<SubmissionRecord>(&serde_json::to_string(&accepted).unwrap())
            .unwrap(),
        accepted
    );

    let accepted_state = ConversationStateRecord {
        id: id(3),
        conversation_id: id(1),
        run: None,
        inbox: vec![InboxItem {
            submission_id: id(2),
            payload: nested(62),
        }],
        agent_config: Some(nested(64)),
    };
    store
        .commit(vec![StorageWrite::ConversationState(
            accepted_state.clone(),
        )])
        .await
        .unwrap();
    assert_eq!(
        store.conversation_state(id(1)).await.unwrap(),
        Some(accepted_state.clone())
    );
    assert_eq!(
        serde_json::from_str::<ConversationStateRecord>(
            &serde_json::to_string(&accepted_state).unwrap()
        )
        .unwrap(),
        accepted_state
    );

    let invalid_submission = submission(
        4,
        1,
        SubmissionState::WriteUnanswered {
            reason: "x".into(),
            detail: Some(nested(65)),
        },
    );
    let invalid_state_payload = ConversationStateRecord {
        id: id(5),
        conversation_id: id(5),
        run: None,
        inbox: vec![InboxItem {
            submission_id: id(4),
            payload: nested(63),
        }],
        agent_config: None,
    };
    let invalid_state_config = ConversationStateRecord {
        id: id(6),
        conversation_id: id(6),
        run: None,
        inbox: Vec::new(),
        agent_config: Some(nested(65)),
    };
    assert!(
        serde_json::from_str::<SubmissionRecord>(
            &serde_json::to_string(&invalid_submission).unwrap()
        )
        .is_err()
    );
    for invalid_state in [&invalid_state_payload, &invalid_state_config] {
        assert!(
            serde_json::from_str::<ConversationStateRecord>(
                &serde_json::to_string(invalid_state).unwrap()
            )
            .is_err()
        );
    }
    for invalid in [
        StorageWrite::Submission(invalid_submission),
        StorageWrite::ConversationState(invalid_state_payload),
        StorageWrite::ConversationState(invalid_state_config),
    ] {
        assert!(
            store
                .commit(vec![
                    StorageWrite::Submission(submission(99, 1, SubmissionState::InputQueued)),
                    invalid,
                ])
                .await
                .is_err()
        );
        assert!(store.submission(id(99)).await.unwrap().is_none());
    }
}

#[cfg(test)]
mod tests {
    macro_rules! check {
        ($name:ident) => {
            #[test]
            fn $name() {
                futures_lite::future::block_on(super::$name(&mut crate::MemoryStorage::new()));
            }
        };
    }
    check!(submission_storage);
    check!(submission_json);
    check!(conversation_state_storage);
    check!(submission_payload_limits);
}
