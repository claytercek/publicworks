use publicworks_storage_sqlite::SqliteStorage;

#[test]
fn sqlite_batches() {
    let mut store = SqliteStorage::open(":memory:").unwrap();
    futures_lite::future::block_on(publicworks_runtime::test_support::batches(&mut store));
}

macro_rules! check {
    ($name:ident) => {
        #[test]
        fn $name() {
            let mut store = SqliteStorage::open(":memory:").unwrap();
            futures_lite::future::block_on(publicworks_runtime::test_support::$name(&mut store));
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
check!(task_storage);
check!(task_json);
check!(task_payload_limits);

use publicworks_runtime::{
    test_support::{conversation, entry, id},
    *,
};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

struct Database {
    dir: PathBuf,
    path: PathBuf,
}
impl Database {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "publicworks-sqlite-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("store.db");
        Self { dir, path }
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[test]
fn reopen_preserves_records_allocators_and_rollback() {
    let db = Database::new();
    futures_lite::future::block_on(async {
        let mut store = SqliteStorage::open(&db.path).unwrap();
        assert_eq!(store.mint_id().await.unwrap(), id(2));
        let mut record = entry(3, 1);
        record.data = Some(serde_json::Value::Null);
        store
            .commit(vec![
                StorageWrite::Conversation(conversation(1)),
                StorageWrite::Entry(record.clone()),
            ])
            .await
            .unwrap();
        assert!(
            store
                .commit(vec![
                    StorageWrite::Conversation(conversation(40)),
                    StorageWrite::Entry(entry(41, 40)),
                    StorageWrite::Entry(entry(3, 1))
                ])
                .await
                .is_err()
        );
        assert_eq!(store.mint_id().await.unwrap(), id(4));
        store.close().await.unwrap();
        let mut reopened = SqliteStorage::open(&db.path).unwrap();
        assert_eq!(reopened.mint_id().await.unwrap(), id(5));
        assert_eq!(
            reopened.conversation(id(1)).await.unwrap(),
            Some(conversation(1))
        );
        assert_eq!(
            reopened.entry(id(3)).await.unwrap().unwrap(),
            StoredEntry {
                entry: record,
                commit_seq: Seq::new(1).unwrap()
            }
        );
        assert!(reopened.conversation(id(40)).await.unwrap().is_none());
        assert!(reopened.entry(id(41)).await.unwrap().is_none());
        assert_eq!(reopened.commit(vec![]).await.unwrap(), Seq::new(2).unwrap());
        drop(reopened); // Also exercise ordinary connection drop rather than explicit close.
        let mut again = SqliteStorage::open(&db.path).unwrap();
        assert_eq!(again.commit(vec![]).await.unwrap(), Seq::new(3).unwrap());
        assert_eq!(again.mint_id().await.unwrap(), id(6));
    });
}

#[test]
fn unknown_or_invalid_schemas_reject_repeatedly() {
    for fixture in [
        "CREATE TABLE publicworks_schema(singleton INTEGER,version INTEGER); INSERT INTO publicworks_schema VALUES(1,2);",
        "CREATE TABLE publicworks_schema(singleton INTEGER,version INTEGER); INSERT INTO publicworks_schema VALUES(1,1);",
        "CREATE TABLE unrelated(x INTEGER);",
    ] {
        let db = Database::new();
        let setup = rusqlite::Connection::open(&db.path).unwrap();
        setup.execute_batch(fixture).unwrap();
        drop(setup);
        assert!(matches!(
            SqliteStorage::open(&db.path),
            Err(StorageError::Other(_))
        ));
        assert!(SqliteStorage::open(&db.path).is_err());
    }
}

#[test]
fn corrupt_record_reads_do_not_reinitialize_schema() {
    let db = Database::new();
    drop(SqliteStorage::open(&db.path).unwrap());
    let setup = rusqlite::Connection::open(&db.path).unwrap();
    setup
        .execute(
            "INSERT INTO publicworks_records VALUES (1,'conversation','not-json',1)",
            [],
        )
        .unwrap();
    drop(setup);
    for _ in 0..2 {
        let mut store = SqliteStorage::open(&db.path).unwrap();
        assert!(futures_lite::future::block_on(store.conversation(id(1))).is_err());
    }
}

#[test]
fn same_columns_without_constraints_are_not_our_schema() {
    let db = Database::new();
    drop(SqliteStorage::open(&db.path).unwrap());
    let setup = rusqlite::Connection::open(&db.path).unwrap();
    setup
        .execute_batch(
            "DROP TABLE publicworks_records;
        CREATE TABLE publicworks_records(id INTEGER, kind TEXT, record TEXT, commit_seq INTEGER);",
        )
        .unwrap();
    drop(setup);
    for _ in 0..2 {
        assert!(matches!(
            SqliteStorage::open(&db.path),
            Err(StorageError::Other(_))
        ));
    }
}

#[test]
fn duplicate_external_ids_fail_reads_instead_of_overwriting() {
    let db = Database::new();
    let mut store = SqliteStorage::open(&db.path).unwrap();
    futures_lite::future::block_on(store.commit(vec![StorageWrite::Conversation(conversation(1))]))
        .unwrap();
    // Deliberate corruption after opening bypasses initialization-time schema checks.
    let setup = rusqlite::Connection::open(&db.path).unwrap();
    setup
        .execute_batch(
            "ALTER TABLE publicworks_records RENAME TO original;
        CREATE TABLE publicworks_records(id INTEGER, kind TEXT, record TEXT, commit_seq INTEGER);
        INSERT INTO publicworks_records SELECT * FROM original;
        INSERT INTO publicworks_records SELECT * FROM original;",
        )
        .unwrap();
    drop(setup);
    assert!(matches!(
        futures_lite::future::block_on(store.conversation(id(1))),
        Err(StorageError::Other(_))
    ));
}

#[test]
fn excessive_external_nesting_fails_reads_safely() {
    for depth in [65, 130] {
        let db = Database::new();
        drop(SqliteStorage::open(&db.path).unwrap());
        let payload = format!("{}null{}", "[".repeat(depth), "]".repeat(depth));
        let json = format!(r#"{{"id":2,"conversationId":1,"kind":"external","data":{payload}}}"#);
        let setup = rusqlite::Connection::open(&db.path).unwrap();
        setup
            .execute(
                "INSERT INTO publicworks_records VALUES (2,'entry',?1,1)",
                [json],
            )
            .unwrap();
        drop(setup);
        let mut store = SqliteStorage::open(&db.path).unwrap();
        assert!(matches!(
            futures_lite::future::block_on(store.entry(id(2))),
            Err(StorageError::Other(_))
        ));
    }
}

#[test]
fn old_and_future_complete_schemas_are_not_reinterpreted() {
    for version in [1, 2, 4] {
        let db = Database::new();
        drop(SqliteStorage::open(&db.path).unwrap());
        let setup = rusqlite::Connection::open(&db.path).unwrap();
        setup
            .execute("UPDATE publicworks_schema SET version=?1", [version])
            .unwrap();
        drop(setup);
        for _ in 0..2 {
            assert!(matches!(
                SqliteStorage::open(&db.path),
                Err(StorageError::Other(_))
            ));
        }
    }
}

#[test]
fn baseline_json_fixture_survives_feature_unification() {
    let db = Database::new();
    drop(SqliteStorage::open(&db.path).unwrap());
    // Ordinary record JSON, including native numbers and literal object keys.
    // It must decode identically whether an embedding host enables arbitrary_precision or not.
    let baseline = r#"{"id":2,"conversationId":1,"kind":"fixture","data":{"$serde_json::private::Number":"ordinary text"},"model":[{"nested":[{"$serde_json::private::Number":"123"}]},18446744073709551615,-9223372036854775808,1.7976931348623157e+308,5e-324],"edits":[{"action":"replace","target":2,"messages":[{"$serde_json::private::Number":"ordinary text"}]}]}"#;
    let setup = rusqlite::Connection::open(&db.path).unwrap();
    setup
        .execute(
            "INSERT INTO publicworks_records VALUES (2,'entry',?1,1)",
            [baseline],
        )
        .unwrap();
    drop(setup);
    let mut store = SqliteStorage::open(&db.path).unwrap();
    let record = futures_lite::future::block_on(store.entry(id(2)))
        .unwrap()
        .unwrap()
        .entry;
    assert_eq!(
        record.data,
        Some(serde_json::json!({"$serde_json::private::Number":"ordinary text"}))
    );
    assert_eq!(
        record.model,
        Some(vec![
            serde_json::json!({"nested":[{"$serde_json::private::Number":"123"}]}),
            serde_json::json!(u64::MAX),
            serde_json::json!(i64::MIN),
            serde_json::json!(f64::MAX),
            serde_json::json!(f64::from_bits(1))
        ])
    );
    assert_eq!(
        record.edits,
        Some(vec![ContextEdit::Replace {
            target: id(2),
            messages: vec![serde_json::json!({"$serde_json::private::Number":"ordinary text"})]
        }])
    );
}

#[test]
fn session_transactions_survive_sqlite_reopen() {
    use futures_lite::future::{block_on, zip};
    let db = Database::new();
    let (parent, cutoff, last, child) = block_on(async {
        let (session, driver) = Session::new(SqliteStorage::open(&db.path).unwrap());
        let (records, ()) = zip(async {
            let receipt = session.commit(|tx| Box::pin(async move {
                let parent = tx.create_conversation().await?;
                let mut draft = EntryDraft::new("first");
                draft.data = Some(serde_json::json!({"$serde_json::private::Number":"object", "data":null}));
                draft.head = Some(Head::SelfEntry);
                let first = tx.append_entry(parent.id, draft).await?;
                let last = tx.append_entry(parent.id, EntryDraft::new("last")).await?;
                Ok((parent.id, first, last.id))
            })).await.unwrap();
            assert_eq!(receipt.seq.unwrap().get(), 1);
            let (parent, first, last) = receipt.value;
            let cutoff = first.id;
            let child = session.commit(move |tx| Box::pin(async move {
                tx.fork_conversation(parent, cutoff).await
            })).await.unwrap();
            assert_eq!(child.seq.unwrap().get(), 2);
            // A failed callback must not leave its created conversation on disk.
            let failed = session.commit(|tx| Box::pin(async move {
                tx.create_conversation().await?;
                Err::<(), _>(SessionError::Invalid("discard".into()))
            })).await;
            assert!(failed.is_err());
            session.close().await.unwrap();
            (parent, first, last, child.value.id)
        }, driver).await;
        records
    });
    block_on(async {
        let (session, driver) = Session::new(SqliteStorage::open(&db.path).unwrap());
        zip(
            async {
                let expected_cutoff = cutoff.clone();
                let read = session
                    .commit(move |tx| {
                        Box::pin(async move {
                            assert_eq!(
                                tx.scan_conversations(ConversationQuery::default(), 10, None)
                                    .await?
                                    .items
                                    .len(),
                                2
                            );
                            assert_eq!(
                                tx.conversation(child)
                                    .await?
                                    .unwrap()
                                    .parent
                                    .unwrap()
                                    .conversation_id,
                                parent
                            );
                            assert!(tx.visible_entry(child, last).await?.is_none());
                            assert_eq!(
                                tx.visible_entry(child, expected_cutoff.id)
                                    .await?
                                    .unwrap()
                                    .entry,
                                expected_cutoff
                            );
                            tx.scan_entries(EntryQuery::new(parent), 10, None).await
                        })
                    })
                    .await
                    .unwrap();
                assert!(read.seq.is_none());
                assert_eq!(
                    read.value.items.iter().map(|e| e.id).collect::<Vec<_>>(),
                    vec![last, cutoff.id]
                );
                let append = session
                    .commit(move |tx| {
                        Box::pin(
                            async move { tx.append_entry(child, EntryDraft::new("child")).await },
                        )
                    })
                    .await
                    .unwrap();
                assert_eq!(append.seq.unwrap().get(), 3);
                assert!(append.value.id > child);
                session.close().await.unwrap();
            },
            driver,
        )
        .await;
    });
}

fn durable_task(n: u64) -> TaskRecord {
    let opaque = serde_json::json!({
        "$serde_json::private::Number": "ordinary text",
        "nested": [null, {"$serde_json::private::Number": "123"}],
        "numbers": [u64::MAX, i64::MIN, f64::MAX, f64::from_bits(1)],
    });
    TaskRecord {
        id: id(n),
        conversation_id: id(1),
        kind: "unknown-host-kind".into(),
        version: 42,
        input: opaque.clone(),
        owner: Some(id(99)),
        background: true,
        abort_requested: true,
        state: TaskState::Running {
            checkpoint: opaque.clone(),
        },
        memos: Some(std::collections::BTreeMap::from([
            ("saved".into(), opaque),
            ("explicit-null".into(), serde_json::Value::Null),
        ])),
    }
}

#[test]
fn task_states_and_opaque_payloads_survive_sqlite_reopen() {
    let db = Database::new();
    let payload = durable_task(2).input;
    let error = TaskOutcomeError {
        message: "host failure".into(),
        detail: Some(payload.clone()),
    };
    let outcomes = vec![
        TaskOutcome::Completed {
            result: payload.clone(),
        },
        TaskOutcome::Failed {
            error: error.clone(),
            result: Some(serde_json::Value::Null),
        },
        TaskOutcome::Aborted {
            reason: Some("requested".into()),
            result: Some(payload.clone()),
        },
        TaskOutcome::Orphaned {
            reason: "missing host".into(),
        },
        TaskOutcome::Faulted { error },
    ];
    let mut states = vec![
        TaskState::Pending {
            checkpoint: serde_json::Value::Null,
        },
        TaskState::Running {
            checkpoint: payload.clone(),
        },
        TaskState::Waiting {
            checkpoint: payload,
            on: vec![id(90), id(91)],
            policy: JoinPolicy::AllSettled,
        },
    ];
    for outcome in outcomes {
        states.push(TaskState::Completing {
            outcome: outcome.clone(),
        });
        states.push(TaskState::Terminal { outcome });
    }
    let records: Vec<_> = states
        .into_iter()
        .enumerate()
        .map(|(i, state)| {
            let mut record = durable_task(i as u64 + 2);
            record.state = state;
            if matches!(
                record.status(),
                TaskStatus::Completing | TaskStatus::Terminal
            ) {
                record.memos = None;
            }
            record
        })
        .collect();
    futures_lite::future::block_on(async {
        let mut store = SqliteStorage::open(&db.path).unwrap();
        store
            .commit(records.iter().cloned().map(StorageWrite::Task).collect())
            .await
            .unwrap();
        store.close().await.unwrap();
        let mut reopened = SqliteStorage::open(&db.path).unwrap();
        for record in &records {
            assert_eq!(
                reopened.task(record.id).await.unwrap(),
                Some(record.clone())
            );
        }
        assert_eq!(
            reopened
                .scan_tasks(TaskQuery::default(), 100, None)
                .await
                .unwrap()
                .items,
            records
        );
        reopened.close().await.unwrap();
    });
}

#[test]
fn task_full_replacements_and_cross_kind_failures_are_atomic() {
    futures_lite::future::block_on(async {
        let mut store = SqliteStorage::open(":memory:").unwrap();
        let original = durable_task(3);
        store
            .commit(vec![
                StorageWrite::Conversation(conversation(1)),
                StorageWrite::Entry(entry(2, 1)),
                StorageWrite::Task(original.clone()),
            ])
            .await
            .unwrap();
        let mut terminal = original.clone();
        terminal.state = TaskState::Terminal {
            outcome: TaskOutcome::Completed {
                result: serde_json::Value::Null,
            },
        };
        terminal.memos = None;
        let mut replacement = original.clone();
        // These are Session constraints, deliberately not imposed by raw Storage.
        replacement.conversation_id = id(88);
        replacement.kind = "replacement-kind".into();
        replacement.version = 9;
        replacement.input = serde_json::Value::Null;
        replacement.owner = None;
        replacement.background = false;
        replacement.abort_requested = false;
        assert_eq!(
            store
                .commit(vec![
                    StorageWrite::Task(terminal),
                    StorageWrite::Task(replacement.clone()),
                ])
                .await
                .unwrap(),
            Seq::new(2).unwrap()
        );
        assert_eq!(store.task(id(3)).await.unwrap(), Some(replacement.clone()));
        for collision in [
            StorageWrite::Task(durable_task(1)),
            StorageWrite::Task(durable_task(2)),
            StorageWrite::Conversation(conversation(3)),
            StorageWrite::Entry(entry(3, 1)),
        ] {
            assert!(matches!(
                store
                    .commit(vec![
                        StorageWrite::Task(original.clone()),
                        StorageWrite::Task(durable_task(50)),
                        StorageWrite::Conversation(conversation(51)),
                        StorageWrite::Entry(entry(52, 51)),
                        collision,
                    ])
                    .await,
                Err(StorageError::Other(_))
            ));
            assert_eq!(store.task(id(3)).await.unwrap(), Some(replacement.clone()));
            assert!(store.task(id(50)).await.unwrap().is_none());
            assert!(store.conversation(id(51)).await.unwrap().is_none());
            assert!(store.entry(id(52)).await.unwrap().is_none());
        }
        assert_eq!(
            store.conversation(id(1)).await.unwrap(),
            Some(conversation(1))
        );
        assert_eq!(
            store.entry(id(2)).await.unwrap().unwrap().entry,
            entry(2, 1)
        );
        assert_eq!(store.mint_id().await.unwrap(), id(4));
        assert_eq!(store.commit(vec![]).await.unwrap(), Seq::new(3).unwrap());
        // Conflicts involving new IDs in the same batch must roll back too.
        for first in [
            StorageWrite::Conversation(conversation(60)),
            StorageWrite::Entry(entry(60, 1)),
        ] {
            assert!(
                store
                    .commit(vec![first, StorageWrite::Task(durable_task(60))])
                    .await
                    .is_err()
            );
        }
        assert!(store.task(id(60)).await.unwrap().is_none());
        assert_eq!(store.mint_id().await.unwrap(), id(5));
        assert_eq!(store.commit(vec![]).await.unwrap(), Seq::new(4).unwrap());
    });
}

#[test]
fn malformed_external_task_payloads_fail_snapshot_reads() {
    for corruption in [
        "id-mismatch",
        "unknown-state",
        "deep-checkpoint",
        "terminal-memos",
    ] {
        let db = Database::new();
        drop(SqliteStorage::open(&db.path).unwrap());
        let mut record = durable_task(2);
        match corruption {
            "id-mismatch" => record.id = id(3),
            "deep-checkpoint" => {
                let mut checkpoint = serde_json::Value::Null;
                for _ in 0..65 {
                    checkpoint = serde_json::json!([checkpoint]);
                }
                record.state = TaskState::Running { checkpoint };
            }
            "terminal-memos" => {
                record.state = TaskState::Terminal {
                    outcome: TaskOutcome::Completed {
                        result: serde_json::Value::Null,
                    },
                }
            }
            _ => {}
        }
        let mut json = serde_json::to_string(&record).unwrap();
        if corruption == "unknown-state" {
            json = json.replace("\"running\"", "\"unknown\"");
        }
        rusqlite::Connection::open(&db.path)
            .unwrap()
            .execute(
                "INSERT INTO publicworks_records VALUES (2,'task',?1,1)",
                [json],
            )
            .unwrap();
        let mut store = SqliteStorage::open(&db.path).unwrap();
        assert!(matches!(
            futures_lite::future::block_on(store.task(id(2))),
            Err(StorageError::Other(_))
        ));
        // Every snapshot read validates task rows, not only task-targeted reads.
        assert!(matches!(
            futures_lite::future::block_on(store.entry(id(99))),
            Err(StorageError::Other(_))
        ));
    }
}

#[test]
fn restart_recovers_running_checkpoints_once_before_session_use() {
    use futures_lite::future::{block_on, zip};
    let db = Database::new();
    let mut running = durable_task(2);
    running.owner = None;
    let mut child = durable_task(3);
    child.owner = Some(running.id);
    child.background = false;
    let mut records = vec![running, child];
    for state in [
        TaskState::Pending {
            checkpoint: serde_json::json!({"already":"pending"}),
        },
        TaskState::Waiting {
            checkpoint: serde_json::json!({"still":"waiting"}),
            on: vec![id(2)],
            policy: JoinPolicy::FailFast,
        },
        TaskState::Completing {
            outcome: TaskOutcome::Completed {
                result: serde_json::json!({"complete":true}),
            },
        },
        TaskState::Terminal {
            outcome: TaskOutcome::Aborted {
                reason: None,
                result: Some(serde_json::Value::Null),
            },
        },
    ] {
        let mut record = durable_task(records.len() as u64 + 2);
        record.owner = None;
        record.state = state;
        if matches!(
            record.status(),
            TaskStatus::Completing | TaskStatus::Terminal
        ) {
            record.memos = None;
        }
        records.push(record);
    }
    block_on(async {
        let mut storage = SqliteStorage::open(&db.path).unwrap();
        let mut writes = vec![StorageWrite::Conversation(conversation(1))];
        writes.extend(records.iter().cloned().map(StorageWrite::Task));
        assert_eq!(storage.commit(writes).await.unwrap(), Seq::new(1).unwrap());
        storage.close().await.unwrap();
    });
    let expected: Vec<_> = records
        .into_iter()
        .map(|mut task| {
            if let TaskState::Running { checkpoint } = task.state {
                task.state = TaskState::Pending { checkpoint };
            }
            task
        })
        .collect();
    // No executor or task definitions are involved: host code polls the driver,
    // awaits recovery admission, and only then receives a usable Session handle.
    for first_recovery in [true, false] {
        block_on(async {
            let (recovery, driver) =
                Session::open_recovered(SqliteStorage::open(&db.path).unwrap());
            zip(
                async {
                    let receipt = recovery.await.unwrap();
                    assert_eq!(receipt.seq, first_recovery.then(|| Seq::new(2).unwrap()));
                    let session = receipt.value;
                    let expected = expected.clone();
                    let read = session
                        .commit(move |tx| {
                            Box::pin(async move {
                                for task in &expected {
                                    assert_eq!(tx.task(task.id).await?, Some(task.clone()));
                                }
                                assert_eq!(
                                    tx.scan_tasks(TaskQuery::default(), 100, None).await?.items,
                                    expected
                                );
                                Ok(())
                            })
                        })
                        .await
                        .unwrap();
                    assert!(read.seq.is_none());
                    session.close().await.unwrap();
                },
                driver,
            )
            .await;
        });
    }
    block_on(async {
        let mut storage = SqliteStorage::open(&db.path).unwrap();
        assert_eq!(
            storage
                .scan_tasks(TaskQuery::default(), 100, None)
                .await
                .unwrap()
                .items,
            expected
        );
        // Both Running updates were one atomic commit; idempotent recovery and
        // reads consumed no sequence and minted no IDs.
        assert_eq!(storage.commit(vec![]).await.unwrap(), Seq::new(3).unwrap());
        assert_eq!(storage.mint_id().await.unwrap(), id(8));
        storage.close().await.unwrap();
    });
}
