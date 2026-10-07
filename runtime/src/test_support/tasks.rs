use super::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub fn task(n: u64) -> TaskRecord {
    TaskRecord {
        id: id(n),
        conversation_id: id(1),
        kind: "unknown.task".into(),
        version: 7,
        input: Value::Null,
        owner: None,
        background: false,
        abort_requested: false,
        state: TaskState::Pending {
            checkpoint: Value::Null,
        },
        memos: None,
    }
}
/// Full replacements, collision rollback, AND filters, pagination and detached values.
pub async fn task_storage(store: &mut dyn Storage) {
    assert!(store.task(id(2)).await.unwrap().is_none());
    let mut first = task(2);
    first.memos = Some(BTreeMap::from([("memo".into(), json!({"x":1}))]));
    let mut second = task(3);
    second.kind = "other".into();
    second.owner = Some(id(1));
    second.background = true;
    second.abort_requested = true;
    second.state = TaskState::Running {
        checkpoint: json!(42),
    };
    store
        .commit(vec![
            StorageWrite::Task(second.clone()),
            StorageWrite::Task(first.clone()),
        ])
        .await
        .unwrap();
    let mut changed = first.clone();
    changed.input = json!({"changed": true});
    changed.memos = None;
    assert_eq!(
        store
            .commit(vec![
                StorageWrite::Task(first),
                StorageWrite::Task(changed.clone())
            ])
            .await
            .unwrap(),
        Seq::new(2).unwrap()
    );
    for writes in [
        vec![
            StorageWrite::Task(task(20)),
            StorageWrite::Conversation(conversation(2)),
        ],
        vec![
            StorageWrite::Entry(entry(20, 1)),
            StorageWrite::Task(task(20)),
        ],
        vec![
            StorageWrite::Task(task(20)),
            StorageWrite::Entry(entry(20, 1)),
        ],
        vec![
            StorageWrite::Conversation(conversation(20)),
            StorageWrite::Task(task(20)),
        ],
    ] {
        assert!(matches!(
            store.commit(writes).await,
            Err(StorageError::Other(_))
        ));
        assert!(store.task(id(20)).await.unwrap().is_none());
        assert!(store.entry(id(20)).await.unwrap().is_none());
        assert!(store.conversation(id(20)).await.unwrap().is_none());
    }
    assert_eq!(store.mint_id().await.unwrap(), id(4));
    assert_eq!(store.commit(vec![]).await.unwrap(), Seq::new(3).unwrap());
    let mut detached = store.task(id(2)).await.unwrap().unwrap();
    detached.input = json!("changed outside");
    assert_eq!(store.task(id(2)).await.unwrap(), Some(changed.clone()));
    let page = store
        .scan_tasks(TaskQuery::default(), 1, None)
        .await
        .unwrap();
    assert_eq!(page.items, vec![changed]);
    let cursor =
        serde_json::from_str(&serde_json::to_string(&page.next.unwrap()).unwrap()).unwrap();
    let mut page = store
        .scan_tasks(TaskQuery::default(), 1, Some(cursor))
        .await
        .unwrap();
    assert_eq!(page.items, vec![second.clone()]);
    assert!(page.next.is_none());
    page.items[0].kind = "mutated".into();
    assert_eq!(store.task(id(3)).await.unwrap(), Some(second.clone()));
    let query = TaskQuery {
        conversation_id: Some(id(1)),
        owner: Some(id(1)),
        kind: Some("other".into()),
        status: Some(TaskStatus::Running),
        abort_requested: Some(true),
        background: Some(true),
    };
    assert_eq!(
        store
            .scan_tasks(query.clone(), 10, None)
            .await
            .unwrap()
            .items,
        vec![second]
    );
    for field in 0..6 {
        let mut q = query.clone();
        match field {
            0 => q.conversation_id = Some(id(9)),
            1 => q.owner = Some(id(9)),
            2 => q.kind = Some("unknown.task".into()),
            3 => q.status = Some(TaskStatus::Pending),
            4 => q.abort_requested = Some(false),
            _ => q.background = Some(false),
        }
        assert!(
            store
                .scan_tasks(q, 10, None)
                .await
                .unwrap()
                .items
                .is_empty()
        );
    }
    assert!(
        store
            .scan_tasks(TaskQuery::default(), 0, None)
            .await
            .is_err()
    );
    store.close().await.unwrap();
    assert!(matches!(
        store.task(id(2)).await,
        Err(StorageError::Rejected(_))
    ));
    assert!(matches!(
        store.scan_tasks(TaskQuery::default(), 1, None).await,
        Err(StorageError::Rejected(_))
    ));
}
fn states(value: Value) -> Vec<TaskState> {
    let error = TaskOutcomeError {
        message: "oops".into(),
        detail: Some(value.clone()),
    };
    let mut states = vec![
        TaskState::Pending {
            checkpoint: value.clone(),
        },
        TaskState::Running {
            checkpoint: value.clone(),
        },
    ];
    for policy in [JoinPolicy::FailFast, JoinPolicy::AllSettled] {
        states.push(TaskState::Waiting {
            checkpoint: value.clone(),
            on: vec![id(20), id(21)],
            policy,
        });
    }
    for outcome in [
        TaskOutcome::Completed {
            result: value.clone(),
        },
        TaskOutcome::Failed {
            error: error.clone(),
            result: Some(value.clone()),
        },
        TaskOutcome::Failed {
            error: TaskOutcomeError {
                message: "absent".into(),
                detail: None,
            },
            result: None,
        },
        TaskOutcome::Aborted {
            reason: Some("stop".into()),
            result: Some(value),
        },
        TaskOutcome::Aborted {
            reason: None,
            result: None,
        },
        TaskOutcome::Orphaned {
            reason: "stored only".into(),
        },
        TaskOutcome::Faulted { error },
    ] {
        states.push(TaskState::Completing {
            outcome: outcome.clone(),
        });
        states.push(TaskState::Terminal { outcome });
    }
    states
}
/// Every state/outcome and opaque field, explicit null versus omission, wrapper member order.
pub async fn task_json(store: &mut dyn Storage) {
    let opaque = json!({"nested":[{"$serde_json::private::Number":"123"},{"$serde_json::private::Number":"ordinary"}], "numbers":[u64::MAX,i64::MIN,f64::MAX,f64::from_bits(1),-0.0_f64]});
    for value in [opaque, Value::Null] {
        for state in states(value.clone()) {
            let mut record = task(2);
            record.input = value.clone();
            record.state = state;
            if !matches!(
                record.status(),
                TaskStatus::Completing | TaskStatus::Terminal
            ) {
                record.memos = Some(BTreeMap::from([("memo".into(), value.clone())]));
            }
            store
                .commit(vec![StorageWrite::Task(record.clone())])
                .await
                .unwrap();
            assert_eq!(store.task(id(2)).await.unwrap(), Some(record.clone()));
            let text = serde_json::to_string(&record).unwrap();
            assert_eq!(serde_json::from_str::<TaskRecord>(&text).unwrap(), record);
            assert_eq!(
                serde_json::from_value::<TaskRecord>(serde_json::to_value(&record).unwrap())
                    .unwrap(),
                record
            );
            for wrapper in [
                format!(r#"{{"type":"task","value":{text}}}"#),
                format!(r#"{{"value":{text},"type":"task"}}"#),
            ] {
                assert_eq!(
                    serde_json::from_str::<StorageWrite>(&wrapper).unwrap(),
                    StorageWrite::Task(record.clone())
                );
            }
        }
    }
    let terminal_memos_null = r#"{"id":2,"conversationId":1,"kind":"x","version":1,"input":null,"background":false,"abortRequested":false,"state":{"status":"terminal","outcome":{"status":"completed","result":null}},"memos":null}"#;
    assert!(serde_json::from_str::<TaskRecord>(terminal_memos_null).is_err());
    let checkpoint_first =
        r#"{"checkpoint":{"$serde_json::private::Number":"ordinary"},"status":"running"}"#;
    assert_eq!(
        serde_json::from_str::<TaskState>(checkpoint_first).unwrap(),
        TaskState::Running {
            checkpoint: json!({"$serde_json::private::Number":"ordinary"})
        }
    );
    assert!(serde_json::from_str::<TaskState>(r#"{"status":"pending"}"#).is_err());
    assert!(serde_json::from_str::<TaskOutcome>(r#"{"status":"completed"}"#).is_err());
    assert_eq!(
        serde_json::from_str::<TaskOutcome>(r#"{"result":null,"status":"aborted"}"#).unwrap(),
        TaskOutcome::Aborted {
            reason: None,
            result: Some(Value::Null)
        }
    );
}
/// Native numeric and depth bounds include the state/outcome/error and memo wrappers.
pub async fn task_payload_limits(store: &mut dyn Storage) {
    for (field, allowed) in [(0, 64), (1, 63), (2, 62), (3, 61), (4, 63)] {
        let build = |value: Value| {
            let mut r = task(2);
            match field {
                0 => r.input = value,
                1 => r.state = TaskState::Running { checkpoint: value },
                2 => {
                    r.state = TaskState::Terminal {
                        outcome: TaskOutcome::Completed { result: value },
                    }
                }
                3 => {
                    r.state = TaskState::Completing {
                        outcome: TaskOutcome::Faulted {
                            error: TaskOutcomeError {
                                message: "x".into(),
                                detail: Some(value),
                            },
                        },
                    }
                }
                _ => r.memos = Some(BTreeMap::from([("key".into(), value)])),
            }
            r
        };
        let accepted = build(nested(allowed));
        store
            .commit(vec![StorageWrite::Task(accepted.clone())])
            .await
            .unwrap();
        assert_eq!(store.task(id(2)).await.unwrap(), Some(accepted.clone()));
        assert_eq!(
            serde_json::from_str::<TaskRecord>(&serde_json::to_string(&accepted).unwrap()).unwrap(),
            accepted
        );
        let mut invalids = vec![build(nested(allowed + 1))];
        for token in [
            "1e400",
            "18446744073709551616",
            "1.000",
            "0.123456789012345678901",
        ] {
            if let Ok(value) = serde_json::from_str::<Value>(token) {
                // Compare the retained numeric token, not Value's string equality.
                let retained_token = value.to_string();
                if retained_token == token {
                    invalids.push(build(value));
                }
            }
        }
        for invalid in invalids {
            assert!(
                serde_json::from_str::<TaskRecord>(&serde_json::to_string(&invalid).unwrap())
                    .is_err()
            );
            assert!(matches!(
                store
                    .commit(vec![
                        StorageWrite::Task(task(99)),
                        StorageWrite::Task(invalid)
                    ])
                    .await,
                Err(StorageError::Other(_))
            ));
            assert!(store.task(id(99)).await.unwrap().is_none());
            assert_eq!(store.task(id(2)).await.unwrap(), Some(accepted.clone()));
        }
    }
    let mut invalid = task(99);
    invalid.state = TaskState::Terminal {
        outcome: TaskOutcome::Completed {
            result: Value::Null,
        },
    };
    invalid.memos = Some(BTreeMap::new());
    assert!(serde_json::from_str::<TaskRecord>(&serde_json::to_string(&invalid).unwrap()).is_err());
    assert!(
        store
            .commit(vec![StorageWrite::Task(invalid)])
            .await
            .is_err()
    );
    assert_eq!(store.mint_id().await.unwrap(), id(3));
    assert_eq!(store.commit(vec![]).await.unwrap(), Seq::new(6).unwrap());
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
    check!(task_storage);
    check!(task_json);
    check!(task_payload_limits);
}
