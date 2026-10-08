use super::*;
use serde_json::{Value, json};

#[test]
fn all_submission_states_round_trip_and_replace_with_canonical_payloads() {
    let db = Database::new();
    futures_lite::future::block_on(async {
        let mut states = vec![
            SubmissionState::InputQueued,
            SubmissionState::InputPlaced { entry: id(8) },
            SubmissionState::InputDone {
                entry: id(8),
                answer: id(9),
            },
            SubmissionState::WriteQueued,
            SubmissionState::WriteDone { entry: id(8) },
        ];
        let mut deep = Value::Null;
        for _ in 0..MAX_JSON_DEPTH {
            deep = json!([deep]);
        }
        let opaque = json!({
            "numbers": [i64::MIN, u64::MAX, f64::MAX, f64::from_bits(1), -0.0],
            "$serde_json::private::Number": "literal object field",
            "$serde_json::private::RawValue": "also literal"
        });
        for detail in [None, Some(Value::Null), Some(opaque), Some(deep)] {
            states.push(SubmissionState::WriteUnanswered {
                reason: "test".into(),
                detail: detail.clone(),
            });
            for entry in [None, Some(id(8))] {
                states.push(SubmissionState::InputUnanswered {
                    entry,
                    reason: "test".into(),
                    detail: detail.clone(),
                });
            }
        }
        let records: Vec<_> = states
            .into_iter()
            .enumerate()
            .map(|(n, state)| {
                let mut record = submission(n as u64 + 2, 1, state);
                record.request_id = Some(format!("request-{n}"));
                record
            })
            .collect();
        let mut store = SqliteStorage::open(&db.path).unwrap();
        store
            .commit(
                records
                    .iter()
                    .cloned()
                    .map(StorageWrite::Submission)
                    .collect(),
            )
            .await
            .unwrap();
        store.close().await.unwrap();
        let mut store = SqliteStorage::open(&db.path).unwrap();
        for record in &records {
            assert_eq!(
                store.submission(record.id).await.unwrap().as_ref(),
                Some(record)
            );
            assert_eq!(
                store
                    .submission_by_request(id(1), record.request_id.as_ref().unwrap())
                    .await
                    .unwrap()
                    .as_ref(),
                Some(record)
            );
        }
        for status in [
            SubmissionStatus::Queued,
            SubmissionStatus::Placed,
            SubmissionStatus::Done,
            SubmissionStatus::Unanswered,
        ] {
            assert_eq!(
                store
                    .scan_submissions(
                        SubmissionQuery {
                            conversation_id: Some(id(1)),
                            status: Some(status)
                        },
                        usize::MAX,
                        None
                    )
                    .await
                    .unwrap()
                    .items,
                records
                    .iter()
                    .filter(|r| r.status() == status)
                    .cloned()
                    .collect::<Vec<_>>()
            );
        }
        // Replace one row through every variant. The indexed status must change
        // together with the canonical state; optional fields must not linger.
        for record in &records {
            let mut replacement = record.clone();
            replacement.id = id(2);
            store
                .commit(vec![StorageWrite::Submission(replacement.clone())])
                .await
                .unwrap();
            assert_eq!(store.submission(id(2)).await.unwrap(), Some(replacement));
        }
        store.close().await.unwrap();
        assert!(SqliteStorage::open(&db.path).is_ok());
    });
}

#[test]
fn malformed_submission_states_and_index_disagreement_reject_reads_and_open() {
    let too_deep = format!(
        r#"{{"type":"input","status":"unanswered","reason":"test","detail":{}null{}}}"#,
        "[".repeat(MAX_JSON_DEPTH + 1),
        "]".repeat(MAX_JSON_DEPTH + 1)
    );
    let cases = [
        ("queued", "broken JSON"),
        (
            "queued",
            r#"{"type":"input","status":"queued","unexpected":null}"#,
        ),
        (
            "queued",
            r#"{"type":"input","status":"queued","detail":null}"#,
        ),
        ("placed", r#"{"type":"write","status":"placed","entry":8}"#),
        ("done", r#"{"type":"input","status":"done","entry":8}"#),
        ("placed", r#"{"type":"input","status":"placed","entry":0}"#),
        (
            "unanswered",
            r#"{"type":"input","status":"unanswered","reason":"test","entry":null}"#,
        ),
        (
            "unanswered",
            r#"{"type":"input","status":"unanswered","reason":"test","detail":1e999}"#,
        ),
        ("unanswered", too_deep.as_str()),
        // Both statuses are legal individually, but the index must agree.
        ("done", r#"{"type":"input","status":"queued"}"#),
    ];
    for (status, state) in cases {
        let db = Database::new();
        let mut store = SqliteStorage::open(&db.path).unwrap();
        futures_lite::future::block_on(store.commit(vec![StorageWrite::Submission(submission(
            2,
            1,
            SubmissionState::InputQueued,
        ))]))
        .unwrap();
        let external = rusqlite::Connection::open(&db.path).unwrap();
        external
            .execute(
                "UPDATE publicworks_submissions SET status=?1,state=?2 WHERE id=2",
                [status, state],
            )
            .unwrap();
        futures_lite::future::block_on(async {
            assert!(matches!(
                store.submission(id(2)).await,
                Err(StorageError::Other(_))
            ));
            assert!(matches!(
                store
                    .scan_submissions(SubmissionQuery::default(), 1, None)
                    .await,
                Err(StorageError::Other(_))
            ));
        });
        drop(store);
        for _ in 0..2 {
            assert!(SqliteStorage::open(&db.path).is_err(), "accepted {state}");
            assert_eq!(
                external
                    .query_row(
                        "SELECT state FROM publicworks_submissions WHERE id=2",
                        [],
                        |row| row.get::<_, String>(0)
                    )
                    .unwrap(),
                state
            );
        }
    }
}

#[test]
fn v5_is_rejected_without_modifying_its_submission_or_journal_mode() {
    let db = Database::new();
    let setup = rusqlite::Connection::open(&db.path).unwrap();
    setup
        .execute_batch(include_str!("../fixtures/v5.sql"))
        .unwrap();
    setup.execute_batch("INSERT INTO publicworks_ids VALUES (2,'submission',1);
        INSERT INTO publicworks_submissions VALUES (2,1,'request','input','unanswered',NULL,NULL,'test','null');
        UPDATE publicworks_metadata SET next_id=3,next_seq=2;").unwrap();
    for _ in 0..2 {
        assert!(SqliteStorage::open(&db.path).is_err());
        assert_eq!(
            setup
                .query_row(
                    "SELECT submission_type,status,detail FROM publicworks_submissions",
                    [],
                    |row| Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?
                    ))
                )
                .unwrap(),
            ("input".into(), "unanswered".into(), "null".into())
        );
        assert_eq!(
            setup
                .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "delete"
        );
        assert_eq!(
            setup
                .query_row("SELECT version FROM publicworks_schema", [], |row| row
                    .get::<_, u64>(0))
                .unwrap(),
            5
        );
    }
}
