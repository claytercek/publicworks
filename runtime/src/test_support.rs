//! Shared black-box storage checks. Each check requires a fresh, empty adapter.
use crate::*;

pub fn id(n: u64) -> Id {
    Id::new(n).unwrap()
}
pub fn conversation(n: u64) -> ConversationRecord {
    ConversationRecord {
        id: id(n),
        parent: None,
        owner: None,
    }
}
pub fn entry(n: u64, conversation: u64) -> EntryRecord {
    EntryRecord::new(id(n), id(conversation), "test")
}

fn native_json_fixture(number_text: &str) -> serde_json::Value {
    serde_json::json!({
        "$serde_json::private::Number": number_text,
        "$serde_json::private::RawValue": "ordinary text",
        "nested": [{"$serde_json::private::Number": "123"}],
        "numbers": [
            i64::MIN,
            u64::MAX,
            f64::MIN,
            f64::MAX,
            f64::MIN_POSITIVE,
            f64::from_bits(1),
            0.12345678901234568_f64,
            -0.0_f64
        ]
    })
}

fn assert_native_json_fixture(value: &serde_json::Value, number_text: &str) {
    assert!(value.is_object());
    assert_eq!(value["$serde_json::private::Number"], number_text);
    assert_eq!(value["$serde_json::private::RawValue"], "ordinary text");
    assert!(value["nested"][0].is_object());
    assert_eq!(value["nested"][0]["$serde_json::private::Number"], "123");

    let numbers = value["numbers"].as_array().unwrap();
    assert_eq!(numbers.len(), 8);
    assert_eq!(numbers[0].as_i64(), Some(i64::MIN));
    assert_eq!(numbers[1].as_u64(), Some(u64::MAX));
    for (actual, expected) in numbers[2..].iter().zip([
        f64::MIN,
        f64::MAX,
        f64::MIN_POSITIVE,
        f64::from_bits(1),
        0.12345678901234568_f64,
        -0.0_f64,
    ]) {
        assert_eq!(actual.as_f64().unwrap().to_bits(), expected.to_bits());
    }
}

pub async fn batches(store: &mut dyn Storage) {
    assert!(
        store
            .conversation(ROOT_CONVERSATION)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .scan_conversations(ConversationQuery::default(), 10, None)
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert_eq!(store.mint_id().await.unwrap(), id(2));
    assert_eq!(store.commit(vec![]).await.unwrap(), Seq::new(1).unwrap());
    let seq = store
        .commit(vec![
            StorageWrite::Conversation(conversation(1)),
            StorageWrite::Entry(entry(3, 1)),
            StorageWrite::Entry(entry(4, 1)),
        ])
        .await
        .unwrap();
    assert_eq!(seq, Seq::new(2).unwrap());
    assert_eq!(store.entry(id(3)).await.unwrap().unwrap().commit_seq, seq);
    assert_eq!(store.entry(id(4)).await.unwrap().unwrap().commit_seq, seq);
    assert_eq!(store.mint_id().await.unwrap(), id(5));
    assert_eq!(
        store.conversation(id(1)).await.unwrap(),
        Some(conversation(1))
    );
}

#[cfg(test)]
mod tests {
    #[test]
    fn memory_batches() {
        futures_lite::future::block_on(super::batches(&mut crate::MemoryStorage::new()));
    }
}

pub async fn atomic_failures(store: &mut dyn Storage) {
    store
        .commit(vec![
            StorageWrite::Conversation(conversation(1)),
            StorageWrite::Entry(entry(2, 1)),
        ])
        .await
        .unwrap();
    // The third write fails, after two different record kinds have been staged.
    let error = store
        .commit(vec![
            StorageWrite::Conversation(conversation(10)),
            StorageWrite::Entry(entry(11, 10)),
            StorageWrite::Entry(entry(2, 1)),
        ])
        .await
        .unwrap_err();
    assert!(matches!(error, StorageError::Other(_)));
    assert!(store.conversation(id(10)).await.unwrap().is_none());
    assert!(store.entry(id(11)).await.unwrap().is_none());
    assert_eq!(
        store.entry(id(2)).await.unwrap().unwrap().entry,
        entry(2, 1)
    );
    assert_eq!(store.mint_id().await.unwrap(), id(3));
    assert_eq!(store.commit(vec![]).await.unwrap(), Seq::new(2).unwrap());
    for writes in [
        vec![StorageWrite::Conversation(conversation(1))],
        vec![StorageWrite::Entry(entry(1, 1))],
        vec![StorageWrite::Conversation(conversation(2))],
        vec![
            StorageWrite::Entry(entry(20, 1)),
            StorageWrite::Entry(entry(20, 1)),
        ],
        vec![
            StorageWrite::Conversation(conversation(21)),
            StorageWrite::Entry(entry(21, 1)),
        ],
    ] {
        assert!(matches!(
            store.commit(writes).await,
            Err(StorageError::Other(_))
        ));
    }
    assert!(store.entry(id(20)).await.unwrap().is_none());
    assert!(store.conversation(id(21)).await.unwrap().is_none());
    assert_eq!(store.commit(vec![]).await.unwrap(), Seq::new(3).unwrap());
}

pub async fn detached_json(store: &mut dyn Storage) {
    let mut original = entry(2, 1);
    original.model = Some(vec![
        serde_json::json!({"role":"user","content":[{"type":"text","text":"hi"}]}),
    ]);
    original.data = Some(serde_json::json!({"nested": [1, null, {"x": true}],
        "unsigned": u64::MAX, "signed": i64::MIN, "float": 0.12345678901234568_f64}));
    original.edits = Some(vec![ContextEdit::Replace {
        target: id(2),
        messages: vec![serde_json::json!({"extra": "opaque"})],
    }]);
    original.by_task_id = Some(id(99));
    let mut explicit_null = entry(3, 1);
    explicit_null.data = Some(serde_json::Value::Null);
    store
        .commit(vec![
            StorageWrite::Conversation(conversation(1)),
            StorageWrite::Entry(original.clone()),
            StorageWrite::Entry(explicit_null.clone()),
            StorageWrite::Entry(entry(4, 1)),
        ])
        .await
        .unwrap();
    let mut returned = store.entry(id(2)).await.unwrap().unwrap();
    returned.entry.data.as_mut().unwrap()["nested"][0] = serde_json::json!(999);
    returned.entry.model.as_mut().unwrap().clear();
    assert_eq!(store.entry(id(2)).await.unwrap().unwrap().entry, original);
    let mut scan = store
        .scan_entries(EntryQuery::new(id(1)), 10, None)
        .await
        .unwrap();
    scan.items[0].kind = "changed".into();
    assert_eq!(
        store.entry(id(4)).await.unwrap().unwrap().entry.kind,
        "test"
    );
    assert_eq!(
        store.entry(id(3)).await.unwrap().unwrap().entry,
        explicit_null
    );
    assert_eq!(store.entry(id(4)).await.unwrap().unwrap().entry.data, None);
    for record in [explicit_null, entry(4, 1)] {
        let json = serde_json::to_value(&record).unwrap();
        assert_eq!(json.get("data").is_some(), record.data.is_some());
        assert_eq!(serde_json::from_value::<EntryRecord>(json).unwrap(), record);
    }
    let mut returned = store.conversation(id(1)).await.unwrap().unwrap();
    returned.parent = Some(ParentLink {
        conversation_id: id(90),
        at: id(91),
    });
    assert!(returned.parent.is_some());
    assert_eq!(
        store.conversation(id(1)).await.unwrap(),
        Some(conversation(1))
    );
}

pub async fn scans_and_forks(store: &mut dyn Storage) {
    let mut child = conversation(6);
    child.parent = Some(ParentLink {
        conversation_id: id(1),
        at: id(3),
    });
    child.owner = Some(OwnerLink {
        conversation_id: id(1),
        task_id: id(90),
    });
    let mut grandchild = conversation(9);
    grandchild.parent = Some(ParentLink {
        conversation_id: id(6),
        at: id(7),
    });
    grandchild.owner = Some(OwnerLink {
        conversation_id: id(6),
        task_id: id(90),
    });
    let mut marker = entry(3, 1);
    marker.head = Some(id(2));
    let mut child_marker = entry(8, 6);
    child_marker.head = Some(id(7));
    // Deliberately unsorted batch: order is by ID, not write position or commit Seq.
    store
        .commit(vec![
            StorageWrite::Conversation(grandchild),
            StorageWrite::Entry(entry(10, 9)),
            StorageWrite::Conversation(child),
            StorageWrite::Entry(child_marker),
            StorageWrite::Entry(entry(7, 6)),
            StorageWrite::Conversation(conversation(1)),
            StorageWrite::Entry(entry(4, 1)),
            StorageWrite::Entry(entry(2, 1)),
            StorageWrite::Entry(marker),
        ])
        .await
        .unwrap();
    let ids = |page: &Page<EntryRecord>| page.items.iter().map(|r| r.id.get()).collect::<Vec<_>>();
    let query = EntryQuery::new(id(9));
    let first = store.scan_entries(query.clone(), 2, None).await.unwrap();
    assert_eq!(ids(&first), [10, 7]);
    let cursor: Cursor =
        serde_json::from_value(serde_json::to_value(first.next.unwrap()).unwrap()).unwrap();
    let second = store
        .scan_entries(query.clone(), 2, Some(cursor))
        .await
        .unwrap();
    assert_eq!(ids(&second), [3, 2]);
    assert!(second.next.is_none());
    let bounded = EntryQuery {
        min_entry_id: Some(id(3)),
        max_entry_id: Some(id(7)),
        ..query.clone()
    };
    assert_eq!(
        ids(&store.scan_entries(bounded, 10, None).await.unwrap()),
        [7, 3]
    );
    let inverted = EntryQuery {
        min_entry_id: Some(id(7)),
        max_entry_id: Some(id(3)),
        ..query
    };
    assert!(
        store
            .scan_entries(inverted, 10, None)
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert!(store.visible_entry(id(9), id(4)).await.unwrap().is_none());
    assert!(store.visible_entry(id(9), id(8)).await.unwrap().is_none());
    assert_eq!(
        store
            .visible_entry(id(9), id(3))
            .await
            .unwrap()
            .unwrap()
            .entry
            .id,
        id(3)
    );
    assert!(store.entry(id(4)).await.unwrap().is_some());
    assert_eq!(
        store
            .find_latest_head_marker(id(6), None)
            .await
            .unwrap()
            .unwrap()
            .id,
        id(8)
    );
    assert_eq!(
        store
            .find_latest_head_marker(id(9), None)
            .await
            .unwrap()
            .unwrap()
            .id,
        id(3)
    );
    assert_eq!(
        store
            .find_latest_head_marker(id(6), Some(id(7)))
            .await
            .unwrap()
            .unwrap()
            .head,
        Some(id(2))
    );
    assert!(
        store
            .find_latest_head_marker(id(1), Some(id(2)))
            .await
            .unwrap()
            .is_none()
    );
    let first = store
        .scan_conversations(ConversationQuery::default(), 2, None)
        .await
        .unwrap();
    assert_eq!(
        first.items.iter().map(|r| r.id.get()).collect::<Vec<_>>(),
        [1, 6]
    );
    let last = store
        .scan_conversations(ConversationQuery::default(), 2, first.next)
        .await
        .unwrap();
    assert_eq!(
        last.items.iter().map(|r| r.id.get()).collect::<Vec<_>>(),
        [9]
    );
    assert!(last.next.is_none());
    let filtered = store
        .scan_conversations(
            ConversationQuery {
                owner_conversation_id: Some(id(1)),
                owner_task_id: Some(id(90)),
            },
            5,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        filtered
            .items
            .iter()
            .map(|r| r.id.get())
            .collect::<Vec<_>>(),
        [6]
    );
    assert!(
        store
            .scan_entries(EntryQuery::new(id(1)), 0, None)
            .await
            .is_err()
    );
    assert!(
        store
            .scan_conversations(ConversationQuery::default(), 0, None)
            .await
            .is_err()
    );
    assert!(
        store
            .scan_entries(EntryQuery::new(id(50)), 10, None)
            .await
            .is_err()
    );
    assert!(store.visible_entry(id(50), id(2)).await.is_err());
    assert!(store.find_latest_head_marker(id(50), None).await.is_err());
    assert!(store.entry(id(50)).await.unwrap().is_none());
    assert!(store.conversation(id(50)).await.unwrap().is_none());
}

pub async fn closed(store: &mut dyn Storage) {
    // Unpolled built-in operations cannot change the store.
    drop(store.commit(vec![StorageWrite::Conversation(conversation(1))]));
    assert!(store.conversation(id(1)).await.unwrap().is_none());
    store.close().await.unwrap();
    assert!(matches!(
        store.commit(vec![]).await,
        Err(StorageError::Rejected(_))
    ));
    assert!(matches!(
        store.mint_id().await,
        Err(StorageError::Rejected(_))
    ));
    assert!(store.conversation(id(1)).await.is_err());
    assert!(store.submission(id(2)).await.is_err());
    assert!(
        store
            .scan_submissions(SubmissionQuery::default(), 1, None)
            .await
            .is_err()
    );
    assert!(store.submission_by_request(id(1), "request").await.is_err());
    assert!(store.conversation_state(id(1)).await.is_err());
    assert!(store.entry(id(2)).await.is_err());
    assert!(store.visible_entry(id(1), id(2)).await.is_err());
    assert!(
        store
            .scan_conversations(ConversationQuery::default(), 1, None)
            .await
            .is_err()
    );
    assert!(
        store
            .scan_entries(EntryQuery::new(id(1)), 1, None)
            .await
            .is_err()
    );
    assert!(store.find_latest_head_marker(id(1), None).await.is_err());
    assert!(store.close().await.is_err());
}

#[cfg(test)]
mod more_tests {
    macro_rules! check {
        ($name:ident) => {
            #[test]
            fn $name() {
                futures_lite::future::block_on(super::$name(&mut crate::MemoryStorage::new()));
            }
        };
    }
    check!(atomic_failures);
    check!(detached_json);
    check!(scans_and_forks);
    check!(closed);
    check!(id_limits);
    check!(opaque_objects);
    check!(native_numeric_domain);
    check!(nesting_limits);
}

pub async fn id_limits(store: &mut dyn Storage) {
    assert!(Id::new(0).is_err());
    assert!(Id::new(MAX_NUMBER + 1).is_err());
    assert!(Seq::new(0).is_err());
    assert!(serde_json::from_str::<Id>("9007199254740992").is_err());
    let last = conversation(MAX_NUMBER);
    store
        .commit(vec![StorageWrite::Conversation(last.clone())])
        .await
        .unwrap();
    assert_eq!(
        store.conversation(id(MAX_NUMBER)).await.unwrap(),
        Some(last)
    );
    assert!(matches!(store.mint_id().await, Err(StorageError::Other(_))));
    // ID exhaustion does not prevent empty commits or reads.
    assert_eq!(store.commit(vec![]).await.unwrap(), Seq::new(2).unwrap());
}

pub async fn opaque_objects(store: &mut dyn Storage) {
    assert!(
        serde_json::from_str::<EntryRecord>(
            r#"{"id":2,"conversationId":1,"kind":"test","data":1e400}"#
        )
        .is_err()
    );
    assert!(serde_json::Number::from_f64(f64::INFINITY).is_none());
    store
        .commit(vec![StorageWrite::Conversation(conversation(1))])
        .await
        .unwrap();
    for (index, text) in ["123", "ordinary text"].into_iter().enumerate() {
        let payload = native_json_fixture(text);
        let mut record = entry(index as u64 + 2, 1);
        record.data = Some(payload.clone());
        record.model = Some(vec![
            payload.clone(),
            serde_json::json!({"nested": [payload.clone()]}),
        ]);
        record.edits = Some(vec![ContextEdit::Replace {
            target: id(2),
            messages: vec![serde_json::json!({"nested": [payload.clone()]}), payload],
        }]);
        store
            .commit(vec![StorageWrite::Entry(record.clone())])
            .await
            .unwrap();
        let stored = store.entry(record.id).await.unwrap().unwrap().entry;
        assert_native_json_fixture(stored.data.as_ref().unwrap(), text);
        assert_eq!(stored, record);

        let decoded =
            serde_json::from_str::<EntryRecord>(&serde_json::to_string(&record).unwrap()).unwrap();
        assert_native_json_fixture(decoded.data.as_ref().unwrap(), text);
        assert_eq!(decoded, record);
        assert_eq!(
            serde_json::from_value::<EntryRecord>(serde_json::to_value(&record).unwrap()).unwrap(),
            record
        );
    }
    assert_eq!(
        store
            .scan_entries(EntryQuery::new(id(1)), 10, None)
            .await
            .unwrap()
            .items
            .len(),
        2
    );
}

fn nested(depth: usize) -> serde_json::Value {
    let mut value = serde_json::Value::Null;
    for level in 0..depth {
        value = if level % 2 == 0 {
            serde_json::json!([value])
        } else {
            serde_json::json!({"nested": value})
        };
    }
    value
}

pub async fn nesting_limits(store: &mut dyn Storage) {
    assert_eq!(MAX_JSON_DEPTH, 64);
    store
        .commit(vec![StorageWrite::Conversation(conversation(1))])
        .await
        .unwrap();
    let mut accepted = entry(2, 1);
    accepted.data = Some(nested(64));
    accepted.model = Some(vec![nested(63)]); // Model array consumes one level.
    accepted.edits = Some(vec![ContextEdit::Replace {
        target: id(2),
        messages: vec![nested(61)],
    }]); // edits + edit + messages = three levels.
    store
        .commit(vec![StorageWrite::Entry(accepted.clone())])
        .await
        .unwrap();
    assert_eq!(store.entry(id(2)).await.unwrap().unwrap().entry, accepted);
    for field in 0..4 {
        let mut rejected = entry(4, 1);
        match field {
            0 => rejected.data = Some(nested(65)),
            1 => rejected.model = Some(vec![nested(64)]),
            2 => {
                rejected.edits = Some(vec![ContextEdit::Replace {
                    target: id(2),
                    messages: vec![nested(62)],
                }])
            }
            _ => rejected.data = Some(nested(130)),
        }
        assert!(matches!(
            store
                .commit(vec![
                    StorageWrite::Entry(entry(3, 1)),
                    StorageWrite::Entry(rejected)
                ])
                .await,
            Err(StorageError::Other(_))
        ));
        assert!(store.entry(id(3)).await.unwrap().is_none());
        assert!(store.entry(id(4)).await.unwrap().is_none());
        assert_eq!(store.entry(id(2)).await.unwrap().unwrap().entry, accepted);
    }
    assert_eq!(store.commit(vec![]).await.unwrap(), Seq::new(3).unwrap());
    assert_eq!(store.mint_id().await.unwrap(), id(3));
}

pub async fn native_numeric_domain(store: &mut dyn Storage) {
    store
        .commit(vec![StorageWrite::Conversation(conversation(1))])
        .await
        .unwrap();
    for literal in [
        "1e400",
        "18446744073709551616",
        "1.000",
        "0.123456789012345678901",
    ] {
        // With default serde_json these either fail parsing or are already native
        // rounded/canonical values. Feature-unified arbitrary_precision retains
        // the unsupported token, which Storage must reject before persisting.
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(literal) {
            let retained_token = value.to_string();
            if retained_token != literal {
                continue;
            }
            let mut record = entry(3, 1);
            record.data = Some(value.clone());
            record.model = Some(vec![value.clone()]);
            record.edits = Some(vec![ContextEdit::Replace {
                target: id(2),
                messages: vec![value],
            }]);
            assert!(matches!(
                store
                    .commit(vec![
                        StorageWrite::Entry(entry(2, 1)),
                        StorageWrite::Entry(record)
                    ])
                    .await,
                Err(StorageError::Other(_))
            ));
            assert!(store.entry(id(2)).await.unwrap().is_none());
            assert!(store.entry(id(3)).await.unwrap().is_none());
        }
    }
    assert_eq!(store.commit(vec![]).await.unwrap(), Seq::new(2).unwrap());
    assert_eq!(store.mint_id().await.unwrap(), id(2));
}

#[cfg(test)]
mod write_json_tests {
    use super::*;

    #[test]
    fn write_member_order_preserves_opaque_payloads() {
        let entry_json = r#"{"id":2,"conversationId":1,"kind":"x","data":null,"model":[{"$serde_json::private::Number":"123"}],"edits":[{"messages":[{"nested":{"$serde_json::private::Number":"ordinary text"}}],"target":2,"action":"replace"}]}"#;
        let mut record = entry(2, 1);
        record.kind = "x".into();
        record.data = Some(serde_json::Value::Null);
        record.model = Some(vec![
            serde_json::json!({"$serde_json::private::Number":"123"}),
        ]);
        record.edits = Some(vec![ContextEdit::Replace {
            target: id(2),
            messages: vec![
                serde_json::json!({"nested":{"$serde_json::private::Number":"ordinary text"}}),
            ],
        }]);
        for (kind, json, expected) in [
            ("entry", entry_json, StorageWrite::Entry(record)),
            (
                "conversation",
                r#"{"id":1}"#,
                StorageWrite::Conversation(conversation(1)),
            ),
        ] {
            for input in [
                format!(r#"{{"type":"{kind}","value":{json}}}"#),
                format!(r#"{{"value":{json},"type":"{kind}"}}"#),
            ] {
                assert_eq!(
                    serde_json::from_str::<StorageWrite>(&input).unwrap(),
                    expected
                );
            }
            assert_eq!(
                serde_json::from_value::<StorageWrite>(serde_json::to_value(&expected).unwrap())
                    .unwrap(),
                expected
            );
            assert_eq!(serde_json::to_value(&expected).unwrap()["type"], kind);
        }
    }

    #[test]
    fn malformed_write_wrappers_reject() {
        for input in [
            r#"{"type":"unknown","value":{"id":1}}"#,
            r#"{"type":7,"value":{"id":1}}"#,
            r#"{"type":"conversation","type":"entry","value":{"id":1}}"#,
            r#"{"type":"conversation","value":{"id":1},"value":{"id":2}}"#,
            r#"{"value":{"id":1}}"#,
        ] {
            assert!(
                serde_json::from_str::<StorageWrite>(input).is_err(),
                "{input}"
            );
        }
    }
}

mod tasks;
pub use tasks::{task, task_json, task_payload_limits, task_storage};
mod submissions;
pub use submissions::{
    conversation_state_storage, submission, submission_json, submission_payload_limits,
    submission_storage,
};

mod session_submissions;
pub use session_submissions::*;
