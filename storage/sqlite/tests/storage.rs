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
check!(submission_storage);
check!(submission_json);
check!(conversation_state_storage);
check!(submission_payload_limits);

use publicworks_runtime::{
    test_support::{conversation, entry, id, submission},
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
        // The uncertain-classified failed commit abandons the remaining local
        // lease. Reopen also abandons unused IDs; allocator gaps are intentional.
        assert_eq!(store.mint_id().await.unwrap(), id(66));
        store.close().await.unwrap();
        let mut reopened = SqliteStorage::open(&db.path).unwrap();
        assert_eq!(reopened.mint_id().await.unwrap(), id(130));
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
        assert_eq!(again.mint_id().await.unwrap(), id(194));
    });
}

#[test]
fn id_leases_are_consecutive_locally_and_skip_explicit_or_abandoned_ids() {
    let db = Database::new();
    futures_lite::future::block_on(async {
        let mut store = SqliteStorage::open(&db.path).unwrap();
        assert_eq!(store.mint_id().await.unwrap(), id(2));
        assert_eq!(store.mint_id().await.unwrap(), id(3));
        // A raw explicit write may claim an unminted ID inside the active lease.
        store
            .commit(vec![StorageWrite::Conversation(conversation(4))])
            .await
            .unwrap();
        assert_eq!(store.mint_id().await.unwrap(), id(5));
        // A write beyond the lease advances durable metadata and discards it.
        store
            .commit(vec![StorageWrite::Conversation(conversation(100))])
            .await
            .unwrap();
        assert_eq!(store.mint_id().await.unwrap(), id(101));
        store.close().await.unwrap();

        let mut reopened = SqliteStorage::open(&db.path).unwrap();
        assert_eq!(reopened.mint_id().await.unwrap(), id(165));
    });
}

#[test]
fn reopen_preserves_submissions_request_lookup_and_conversation_state() {
    let db = Database::new();
    futures_lite::future::block_on(async {
        let mut input = submission(
            2,
            1,
            SubmissionState::InputUnanswered {
                entry: Some(id(8)),
                reason: "model_error".into(),
                detail: Some(serde_json::Value::Null),
            },
        );
        input.request_id = Some("retry-key".into());
        let write = submission(3, 1, SubmissionState::WriteQueued);
        let state = ConversationStateRecord {
            id: id(4),
            conversation_id: id(1),
            run: Some(ConversationRun {
                task_id: id(9),
                input_submission_ids: vec![id(2)],
            }),
            inbox: vec![InboxItem {
                submission_id: id(3),
                payload: serde_json::json!({"mode":"write", "draft":null}),
            }],
            agent_config: Some(serde_json::json!({"followUp":"all"})),
        };
        let mut store = SqliteStorage::open(&db.path).unwrap();
        store
            .commit(vec![
                StorageWrite::Submission(input.clone()),
                StorageWrite::Submission(write.clone()),
                StorageWrite::ConversationState(state.clone()),
            ])
            .await
            .unwrap();
        store.close().await.unwrap();

        let mut reopened = SqliteStorage::open(&db.path).unwrap();
        assert_eq!(
            reopened.submission(id(2)).await.unwrap(),
            Some(input.clone())
        );
        assert_eq!(
            reopened
                .submission_by_request(id(1), "retry-key")
                .await
                .unwrap(),
            Some(input.clone())
        );
        assert_eq!(
            reopened
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
            vec![write]
        );
        assert_eq!(
            reopened.conversation_state(id(1)).await.unwrap(),
            Some(state)
        );
        assert!(matches!(
            input.state,
            SubmissionState::InputUnanswered {
                detail: Some(serde_json::Value::Null),
                ..
            }
        ));
        reopened.close().await.unwrap();
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
fn corrupt_record_open_does_not_reinitialize_schema() {
    let db = Database::new();
    let mut store = SqliteStorage::open(&db.path).unwrap();
    futures_lite::future::block_on(store.commit(vec![StorageWrite::Entry(entry(2, 1))])).unwrap();
    drop(store);
    let setup = rusqlite::Connection::open(&db.path).unwrap();
    setup
        .execute("UPDATE publicworks_entries SET data='not-json'", [])
        .unwrap();
    drop(setup);
    for _ in 0..2 {
        assert!(matches!(
            SqliteStorage::open(&db.path),
            Err(StorageError::Other(_))
        ));
    }
    let setup = rusqlite::Connection::open(&db.path).unwrap();
    assert_eq!(
        setup
            .query_row("SELECT data FROM publicworks_entries", [], |r| r
                .get::<_, String>(0))
            .unwrap(),
        "not-json"
    );
}

#[test]
fn same_columns_without_constraints_are_not_our_schema() {
    let db = Database::new();
    drop(SqliteStorage::open(&db.path).unwrap());
    let setup = rusqlite::Connection::open(&db.path).unwrap();
    setup
        .execute_batch(
            "DROP TABLE publicworks_ids;
        CREATE TABLE publicworks_ids(id INTEGER, kind TEXT, commit_seq INTEGER);",
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
fn duplicate_external_ids_fail_open_instead_of_overwriting() {
    let db = Database::new();
    let mut store = SqliteStorage::open(&db.path).unwrap();
    futures_lite::future::block_on(store.commit(vec![StorageWrite::Conversation(conversation(1))]))
        .unwrap();
    drop(store);
    let setup = rusqlite::Connection::open(&db.path).unwrap();
    setup
        .execute_batch(
            "ALTER TABLE publicworks_ids RENAME TO original;
        CREATE TABLE publicworks_ids(id INTEGER, kind TEXT, commit_seq INTEGER);
        INSERT INTO publicworks_ids SELECT * FROM original;
        INSERT INTO publicworks_ids SELECT * FROM original;",
        )
        .unwrap();
    drop(setup);
    assert!(matches!(
        SqliteStorage::open(&db.path),
        Err(StorageError::Other(_))
    ));
}

#[test]
fn excessive_external_nesting_fails_open_safely() {
    for depth in [65, 130] {
        let db = Database::new();
        let mut store = SqliteStorage::open(&db.path).unwrap();
        futures_lite::future::block_on(store.commit(vec![StorageWrite::Entry(entry(2, 1))]))
            .unwrap();
        drop(store);
        let payload = format!("{}null{}", "[".repeat(depth), "]".repeat(depth));
        rusqlite::Connection::open(&db.path)
            .unwrap()
            .execute("UPDATE publicworks_entries SET data=?1", [payload])
            .unwrap();
        assert!(matches!(
            SqliteStorage::open(&db.path),
            Err(StorageError::Other(_))
        ));
    }
}

#[test]
fn old_and_future_complete_schemas_are_not_reinterpreted() {
    for version in [1, 2, 3, 4, 6] {
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
    let setup = rusqlite::Connection::open(&db.path).unwrap();
    setup
        .execute_batch(
            "INSERT INTO publicworks_ids VALUES(2,'entry',1);
        UPDATE publicworks_metadata SET next_id=3,next_seq=2;",
        )
        .unwrap();
    setup.execute(
        "INSERT INTO publicworks_entries(id,conversation_id,kind,data,model,edits) VALUES(2,1,'fixture',?1,?2,?3)",
        [r#"{"$serde_json::private::Number":"ordinary text"}"#,
         r#"[{"nested":[{"$serde_json::private::Number":"123"}]},18446744073709551615,-9223372036854775808,1.7976931348623157e+308,5e-324]"#,
         r#"[{"action":"replace","target":2,"messages":[{"$serde_json::private::Number":"ordinary text"}]}]"#],
    ).unwrap();
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
        assert_eq!(store.mint_id().await.unwrap(), id(68));
        assert_eq!(store.commit(vec![]).await.unwrap(), Seq::new(4).unwrap());
    });
}

#[test]
fn malformed_external_task_payloads_fail_open() {
    for corruption in [
        "registry-mismatch",
        "unknown-state",
        "deep-checkpoint",
        "terminal-memos",
        "status-mismatch",
    ] {
        let db = Database::new();
        let mut store = SqliteStorage::open(&db.path).unwrap();
        let mut record = durable_task(2);
        futures_lite::future::block_on(store.commit(vec![StorageWrite::Task(record.clone())]))
            .unwrap();
        drop(store);
        let setup = rusqlite::Connection::open(&db.path).unwrap();
        match corruption {
            "registry-mismatch" => {
                setup
                    .execute("UPDATE publicworks_ids SET kind='entry'", [])
                    .unwrap();
            }
            "unknown-state" => {
                setup
                    .execute(
                        "UPDATE publicworks_tasks SET state='{\"status\":\"unknown\"}'",
                        [],
                    )
                    .unwrap();
            }
            "deep-checkpoint" => {
                let mut checkpoint = serde_json::Value::Null;
                for _ in 0..65 {
                    checkpoint = serde_json::json!([checkpoint]);
                }
                record.state = TaskState::Running { checkpoint };
                setup
                    .execute(
                        "UPDATE publicworks_tasks SET state=?1",
                        [serde_json::to_string(&record.state).unwrap()],
                    )
                    .unwrap();
            }
            "terminal-memos" => {
                record.state = TaskState::Terminal {
                    outcome: TaskOutcome::Completed {
                        result: serde_json::Value::Null,
                    },
                };
                setup
                    .execute(
                        "UPDATE publicworks_tasks SET status='terminal',state=?1",
                        [serde_json::to_string(&record.state).unwrap()],
                    )
                    .unwrap();
            }
            "status-mismatch" => {
                setup
                    .execute("UPDATE publicworks_tasks SET status='pending'", [])
                    .unwrap();
            }
            _ => unreachable!(),
        }
        drop(setup);
        // Open audits unrelated records too. Reads no longer rescan all tables.
        for _ in 0..2 {
            assert!(matches!(
                SqliteStorage::open(&db.path),
                Err(StorageError::Other(_))
            ));
        }
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

#[test]
fn full_u64_task_versions_and_nullable_replacements_round_trip() {
    let db = Database::new();
    futures_lite::future::block_on(async {
        let mut store = SqliteStorage::open(&db.path).unwrap();
        let versions = [0, 1, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX];
        for (index, version) in versions.into_iter().enumerate() {
            let mut task = durable_task(index as u64 + 2);
            task.version = version;
            store.commit(vec![StorageWrite::Task(task)]).await.unwrap();
        }
        store.close().await.unwrap();
        let mut store = SqliteStorage::open(&db.path).unwrap();
        for (index, version) in versions.into_iter().enumerate() {
            let task = store.task(id(index as u64 + 2)).await.unwrap().unwrap();
            assert_eq!(task.version, version);
        }
        let mut replacement = durable_task(2);
        replacement.version = u64::MAX;
        replacement.input = serde_json::Value::Null;
        replacement.owner = None;
        replacement.memos = None;
        replacement.background = false;
        replacement.abort_requested = false;
        store
            .commit(vec![StorageWrite::Task(replacement.clone())])
            .await
            .unwrap();
        store.close().await.unwrap();
        let mut store = SqliteStorage::open(&db.path).unwrap();
        assert_eq!(store.task(id(2)).await.unwrap(), Some(replacement));
    });
}

#[test]
fn duplicate_request_and_state_lookups_choose_lowest_id_after_replacements() {
    let db = Database::new();
    futures_lite::future::block_on(async {
        let mut store = SqliteStorage::open(&db.path).unwrap();
        let mut low = submission(3, 1, SubmissionState::WriteQueued);
        low.request_id = Some("' OR 1=1 -- \0 🦀".into());
        let mut high = low.clone();
        high.id = id(20);
        let state = ConversationStateRecord {
            id: id(5),
            conversation_id: id(1),
            run: None,
            inbox: vec![],
            agent_config: Some(serde_json::Value::Null),
        };
        let mut high_state = state.clone();
        high_state.id = id(30);
        store
            .commit(vec![
                StorageWrite::Submission(high.clone()),
                StorageWrite::Submission(low.clone()),
                StorageWrite::ConversationState(high_state.clone()),
                StorageWrite::ConversationState(state.clone()),
            ])
            .await
            .unwrap();
        assert_eq!(
            store
                .submission_by_request(id(1), low.request_id.as_ref().unwrap())
                .await
                .unwrap(),
            Some(low.clone())
        );
        assert_eq!(
            store.conversation_state(id(1)).await.unwrap(),
            Some(state.clone())
        );
        low.request_id = None;
        low.conversation_id = id(99);
        low.state = SubmissionState::InputUnanswered {
            entry: Some(id(90)),
            reason: "test".into(),
            detail: Some(serde_json::Value::Null),
        };
        let mut moved_state = state;
        moved_state.conversation_id = id(99);
        moved_state.agent_config = None;
        store
            .commit(vec![
                StorageWrite::Submission(low.clone()),
                StorageWrite::ConversationState(moved_state.clone()),
            ])
            .await
            .unwrap();
        store.close().await.unwrap();
        let mut store = SqliteStorage::open(&db.path).unwrap();
        assert_eq!(
            store
                .submission_by_request(id(1), high.request_id.as_ref().unwrap())
                .await
                .unwrap(),
            Some(high)
        );
        assert_eq!(
            store.conversation_state(id(1)).await.unwrap(),
            Some(high_state)
        );
        assert_eq!(
            store.conversation_state(id(99)).await.unwrap(),
            Some(moved_state)
        );
        assert_eq!(store.submission(id(3)).await.unwrap(), Some(low.clone()));
        low.state = SubmissionState::WriteQueued;
        store
            .commit(vec![StorageWrite::Submission(low.clone())])
            .await
            .unwrap();
        assert_eq!(store.submission(id(3)).await.unwrap(), Some(low));
    });
}

#[test]
fn every_cross_kind_collision_is_atomic_including_same_batch() {
    fn writes(n: u64) -> Vec<StorageWrite> {
        vec![
            StorageWrite::Conversation(conversation(n)),
            StorageWrite::Entry(entry(n, 1)),
            StorageWrite::Task(durable_task(n)),
            StorageWrite::Submission(submission(n, 1, SubmissionState::WriteQueued)),
            StorageWrite::ConversationState(ConversationStateRecord {
                id: id(n),
                conversation_id: id(1),
                run: None,
                inbox: vec![],
                agent_config: None,
            }),
        ]
    }
    futures_lite::future::block_on(async {
        for (i, first) in writes(2).into_iter().enumerate() {
            for (j, second) in writes(2).into_iter().enumerate() {
                if i == j {
                    continue;
                }
                for same_batch in [false, true] {
                    let mut store = SqliteStorage::open(":memory:").unwrap();
                    let mut batch = vec![StorageWrite::Entry(entry(50, 1))];
                    if same_batch {
                        batch.push(first.clone());
                    } else {
                        store.commit(vec![first.clone()]).await.unwrap();
                    }
                    batch.push(second.clone());
                    assert!(matches!(
                        store.commit(batch).await,
                        Err(StorageError::Other(_))
                    ));
                    assert!(store.entry(id(50)).await.unwrap().is_none());
                    assert_eq!(
                        store.mint_id().await.unwrap(),
                        id(if same_batch { 2 } else { 3 })
                    );
                    assert_eq!(
                        store.commit(vec![]).await.unwrap().get(),
                        if same_batch { 1 } else { 2 }
                    );
                }
            }
        }
    });
}

#[test]
fn filtered_scans_match_memory_for_all_filter_combinations() {
    futures_lite::future::block_on(async {
        let mut sqlite = SqliteStorage::open(":memory:").unwrap();
        let mut memory = MemoryStorage::new();
        let mut writes = Vec::new();
        for n in 2..=65 {
            let mut task = durable_task(n);
            task.conversation_id = id(100 + n % 2);
            task.kind = if n % 3 == 0 { "a" } else { "b" }.into();
            task.background = n % 4 == 0;
            task.abort_requested = n % 5 == 0;
            if n % 2 == 0 {
                task.state = TaskState::Pending {
                    checkpoint: serde_json::Value::Null,
                };
            }
            writes.push(StorageWrite::Task(task));
            let mut conv = conversation(n + 100);
            if n % 4 != 0 {
                conv.owner = Some(OwnerLink {
                    conversation_id: id(100 + n % 2),
                    task_id: id(200 + n % 3),
                });
            }
            writes.push(StorageWrite::Conversation(conv));
            writes.push(StorageWrite::Submission(submission(
                n + 300,
                100 + n % 2,
                if n % 3 == 0 {
                    SubmissionState::WriteQueued
                } else {
                    SubmissionState::WriteDone { entry: id(n) }
                },
            )));
        }
        sqlite.commit(writes.clone()).await.unwrap();
        memory.commit(writes).await.unwrap();
        for conversation_id in [None, Some(id(100)), Some(id(999))] {
            for kind in [None, Some("a".to_owned()), Some("missing".to_owned())] {
                for status in [None, Some(TaskStatus::Pending), Some(TaskStatus::Running)] {
                    for background in [None, Some(false), Some(true)] {
                        for abort_requested in [None, Some(false), Some(true)] {
                            let query = TaskQuery {
                                conversation_id,
                                kind: kind.clone(),
                                status,
                                background,
                                abort_requested,
                            };
                            let mut cursor = None;
                            loop {
                                let expected = memory
                                    .scan_tasks(query.clone(), 3, cursor.clone())
                                    .await
                                    .unwrap();
                                assert_eq!(
                                    sqlite.scan_tasks(query.clone(), 3, cursor).await.unwrap(),
                                    expected
                                );
                                cursor = expected.next;
                                if cursor.is_none() {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            for owner_task_id in [None, Some(id(200)), Some(id(999))] {
                let query = ConversationQuery {
                    owner_conversation_id: conversation_id,
                    owner_task_id,
                };
                let mut cursor = None;
                loop {
                    let expected = memory
                        .scan_conversations(query.clone(), 2, cursor.clone())
                        .await
                        .unwrap();
                    assert_eq!(
                        sqlite
                            .scan_conversations(query.clone(), 2, cursor)
                            .await
                            .unwrap(),
                        expected
                    );
                    cursor = expected.next;
                    if cursor.is_none() {
                        break;
                    }
                }
            }
            for status in [
                None,
                Some(SubmissionStatus::Queued),
                Some(SubmissionStatus::Done),
                Some(SubmissionStatus::Placed),
            ] {
                let query = SubmissionQuery {
                    conversation_id,
                    status,
                };
                let mut cursor = None;
                loop {
                    let expected = memory
                        .scan_submissions(query.clone(), 2, cursor.clone())
                        .await
                        .unwrap();
                    assert_eq!(
                        sqlite
                            .scan_submissions(query.clone(), 2, cursor)
                            .await
                            .unwrap(),
                        expected
                    );
                    cursor = expected.next;
                    if cursor.is_none() {
                        break;
                    }
                }
            }
        }
        for limit in [0, usize::MAX] {
            assert_eq!(
                sqlite.scan_tasks(TaskQuery::default(), limit, None).await,
                memory.scan_tasks(TaskQuery::default(), limit, None).await
            );
        }
    });
}

#[test]
fn raw_fork_segments_and_cursors_match_memory_without_global_sorting() {
    futures_lite::future::block_on(async {
        let mut sqlite = SqliteStorage::open(":memory:").unwrap();
        let mut memory = MemoryStorage::new();
        let mut child = conversation(200);
        child.parent = Some(ParentLink {
            conversation_id: id(100),
            at: id(90),
        });
        let mut grandchild = conversation(300);
        grandchild.parent = Some(ParentLink {
            conversation_id: id(200),
            at: id(80),
        });
        let mut cycle = conversation(400);
        cycle.parent = Some(ParentLink {
            conversation_id: id(400),
            at: id(50),
        });
        let mut missing = conversation(500);
        missing.parent = Some(ParentLink {
            conversation_id: id(999),
            at: id(50),
        });
        let mut writes = vec![conversation(100), child, grandchild, cycle, missing]
            .into_iter()
            .map(StorageWrite::Conversation)
            .collect::<Vec<_>>();
        // Legal raw data but not Session chronology: child entries overlap the
        // ancestor's ID range. An ORDER BY across a UNION would change behavior.
        for (n, c) in [
            (1, 300),
            (5, 200),
            (7, 300),
            (20, 100),
            (40, 200),
            (70, 100),
            (85, 200),
            (90, 100),
            (95, 100),
        ] {
            let mut e = entry(n, c);
            if n % 2 == 0 || n == 7 {
                e.head = Some(id(999));
            }
            writes.push(StorageWrite::Entry(e));
        }
        sqlite.commit(writes.clone()).await.unwrap();
        memory.commit(writes).await.unwrap();
        for conversation_id in [id(100), id(200), id(300), id(400), id(500), id(999)] {
            for min_entry_id in [None, Some(id(1)), Some(id(21)), Some(id(91))] {
                for max_entry_id in [None, Some(id(1)), Some(id(80)), Some(id(90))] {
                    let query = EntryQuery {
                        conversation_id,
                        min_entry_id,
                        max_entry_id,
                    };
                    for limit in [0, 1, 2, 4, usize::MAX] {
                        let mut cursor = None;
                        for _ in 0..30 {
                            let expected = memory
                                .scan_entries(query.clone(), limit, cursor.clone())
                                .await;
                            let actual = sqlite.scan_entries(query.clone(), limit, cursor).await;
                            assert_eq!(actual, expected);
                            cursor = expected.ok().and_then(|p| p.next);
                            if cursor.is_none() {
                                break;
                            }
                        }
                    }
                    assert_eq!(
                        sqlite
                            .find_latest_head_marker(conversation_id, max_entry_id)
                            .await,
                        memory
                            .find_latest_head_marker(conversation_id, max_entry_id)
                            .await
                    );
                }
            }
            for n in [1, 5, 7, 20, 40, 70, 85, 90, 95, 999] {
                assert_eq!(
                    sqlite.visible_entry(conversation_id, id(n)).await,
                    memory.visible_entry(conversation_id, id(n)).await
                );
            }
            let cursor = Some(Cursor::from_after(id(1)));
            assert_eq!(
                sqlite
                    .scan_entries(EntryQuery::new(conversation_id), 2, cursor.clone())
                    .await,
                memory
                    .scan_entries(EntryQuery::new(conversation_id), 2, cursor)
                    .await
            );
        }
    });
}

#[test]
fn open_audits_registry_metadata_scalars_and_index_definitions() {
    for corruption in [
        "PRAGMA foreign_keys=OFF; DELETE FROM publicworks_ids WHERE id=2",
        "DELETE FROM publicworks_tasks WHERE id=2",
        "UPDATE publicworks_ids SET kind='entry' WHERE id=2",
        "UPDATE publicworks_metadata SET next_id=2",
        "UPDATE publicworks_metadata SET next_seq=1",
        "UPDATE publicworks_tasks SET conversation_id=0",
        "PRAGMA ignore_check_constraints=ON; UPDATE publicworks_tasks SET background=2",
        "PRAGMA ignore_check_constraints=ON; UPDATE publicworks_tasks SET version=x'01'",
        "DROP INDEX tasks_status",
        "DROP INDEX entries_head; CREATE INDEX entries_head ON publicworks_entries(conversation_id,id)",
        "CREATE INDEX unexpected ON publicworks_tasks(owner)",
        "CREATE TRIGGER unexpected AFTER INSERT ON publicworks_tasks BEGIN SELECT 1; END",
        "CREATE VIEW unexpected AS SELECT * FROM publicworks_tasks",
    ] {
        let db = Database::new();
        let mut store = SqliteStorage::open(&db.path).unwrap();
        futures_lite::future::block_on(store.commit(vec![StorageWrite::Task(durable_task(2))]))
            .unwrap();
        drop(store);
        let setup = rusqlite::Connection::open(&db.path).unwrap();
        setup.execute_batch(corruption).unwrap();
        drop(setup);
        for _ in 0..2 {
            assert!(
                SqliteStorage::open(&db.path).is_err(),
                "accepted {corruption}"
            );
        }
    }
}

#[test]
fn selected_rows_are_validated_but_unrelated_external_corruption_waits_until_open() {
    let db = Database::new();
    futures_lite::future::block_on(async {
        let mut store = SqliteStorage::open(&db.path).unwrap();
        store
            .commit(vec![
                StorageWrite::Conversation(conversation(1)),
                StorageWrite::Entry(entry(2, 1)),
                StorageWrite::Task(durable_task(3)),
            ])
            .await
            .unwrap();
        let external = rusqlite::Connection::open(&db.path).unwrap();
        external
            .execute("UPDATE publicworks_tasks SET input='broken-json'", [])
            .unwrap();
        assert_eq!(
            store.conversation(id(1)).await.unwrap(),
            Some(conversation(1))
        );
        assert!(store.entry(id(2)).await.unwrap().is_some());
        assert!(store.task(id(3)).await.is_err());
        assert!(
            store
                .scan_tasks(TaskQuery::default(), 1, None)
                .await
                .is_err()
        );
        assert!(
            store
                .scan_tasks(
                    TaskQuery {
                        kind: Some("unrelated".into()),
                        ..TaskQuery::default()
                    },
                    1,
                    None
                )
                .await
                .unwrap()
                .items
                .is_empty()
        );
        // Likewise an entry scan decodes only the bounded page and its lookahead.
        external
            .execute("UPDATE publicworks_entries SET data='broken-json'", [])
            .unwrap();
        assert!(store.entry(id(2)).await.is_err());
        assert!(
            store
                .scan_entries(EntryQuery::new(id(1)), 1, None)
                .await
                .is_err()
        );
        assert!(
            store
                .find_latest_head_marker(id(1), None)
                .await
                .unwrap()
                .is_none()
        );
        store.close().await.unwrap();
        assert!(SqliteStorage::open(&db.path).is_err());
    });
}

#[test]
fn sequence_exhaustion_rejects_atomically_and_maximum_sequence_reopens() {
    let db = Database::new();
    futures_lite::future::block_on(async {
        let mut store = SqliteStorage::open(&db.path).unwrap();
        rusqlite::Connection::open(&db.path)
            .unwrap()
            .execute("UPDATE publicworks_metadata SET next_seq=?1", [MAX_NUMBER])
            .unwrap();
        assert_eq!(
            store
                .commit(vec![StorageWrite::Task(durable_task(2))])
                .await
                .unwrap()
                .get(),
            MAX_NUMBER
        );
        assert!(
            store
                .commit(vec![StorageWrite::Conversation(conversation(50))])
                .await
                .is_err()
        );
        assert!(store.conversation(id(50)).await.unwrap().is_none());
        assert_eq!(store.mint_id().await.unwrap(), id(3));
        store.close().await.unwrap();
        let mut store = SqliteStorage::open(&db.path).unwrap();
        assert_eq!(store.task(id(2)).await.unwrap(), Some(durable_task(2)));
        assert!(store.commit(vec![]).await.is_err());
    });
}

#[test]
fn scans_decode_only_the_page_and_bounded_lookahead() {
    let db = Database::new();
    futures_lite::future::block_on(async {
        let mut store = SqliteStorage::open(&db.path).unwrap();
        store
            .commit(vec![
                StorageWrite::Conversation(conversation(1)),
                StorageWrite::Task(durable_task(2)),
                StorageWrite::Task(durable_task(3)),
                StorageWrite::Task(durable_task(4)),
                StorageWrite::Entry(entry(5, 1)),
                StorageWrite::Entry(entry(6, 1)),
                StorageWrite::Entry(entry(7, 1)),
            ])
            .await
            .unwrap();
        let external = rusqlite::Connection::open(&db.path).unwrap();
        external
            .execute(
                "UPDATE publicworks_tasks SET input='invalid' WHERE id=4",
                [],
            )
            .unwrap();
        external
            .execute(
                "UPDATE publicworks_entries SET data='invalid' WHERE id=5",
                [],
            )
            .unwrap();
        let tasks = store
            .scan_tasks(TaskQuery::default(), 1, None)
            .await
            .unwrap();
        assert_eq!(tasks.items[0].id, id(2));
        assert!(tasks.next.is_some());
        assert!(
            store
                .scan_tasks(TaskQuery::default(), 1, tasks.next)
                .await
                .is_err()
        );
        let entries = store
            .scan_entries(EntryQuery::new(id(1)), 1, None)
            .await
            .unwrap();
        assert_eq!(entries.items[0].id, id(7));
        assert!(entries.next.is_some());
        assert!(
            store
                .scan_entries(EntryQuery::new(id(1)), 1, entries.next)
                .await
                .is_err()
        );
        drop(external);
        store.close().await.unwrap();
        assert!(SqliteStorage::open(&db.path).is_err());
    });
}

#[test]
fn v4_is_rejected_without_modifying_its_records_or_journal_mode() {
    let db = Database::new();
    let setup = rusqlite::Connection::open(&db.path).unwrap();
    setup.execute_batch("CREATE TABLE publicworks_schema (singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL);
        INSERT INTO publicworks_schema VALUES (1,4);
        CREATE TABLE publicworks_metadata (singleton INTEGER PRIMARY KEY CHECK(singleton=1),next_id INTEGER NOT NULL,next_seq INTEGER NOT NULL);
        INSERT INTO publicworks_metadata VALUES(1,2,2);
        CREATE TABLE publicworks_records (id INTEGER PRIMARY KEY, kind TEXT NOT NULL CHECK(kind IN ('conversation','entry','task','submission','conversation_state')),record TEXT NOT NULL,commit_seq INTEGER NOT NULL);
        INSERT INTO publicworks_records VALUES (1,'conversation','{\"id\":1}',1);").unwrap();
    for _ in 0..2 {
        assert!(SqliteStorage::open(&db.path).is_err());
        assert_eq!(
            setup
                .query_row("SELECT record FROM publicworks_records", [], |r| r
                    .get::<_, String>(0))
                .unwrap(),
            r#"{"id":1}"#
        );
        assert_eq!(
            setup
                .query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "delete"
        );
    }
}
