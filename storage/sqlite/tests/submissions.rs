use futures_lite::future::{block_on, zip};
use publicworks_runtime::*;

macro_rules! check {
    ($name:ident) => {
        #[test]
        fn $name() {
            block_on(async {
                let (session, driver) = Session::new(
                    publicworks_storage_sqlite::SqliteStorage::open(":memory:").unwrap(),
                );
                zip(
                    async {
                        test_support::$name(&session).await;
                        session.close().await.unwrap();
                    },
                    driver,
                )
                .await;
            });
        }
    };
}
check!(submission_tx_reads);
check!(submission_tx_transitions);
check!(submission_tx_final_candidates);
check!(submission_tx_withdrawal);
check!(submission_tx_json);

#[test]
fn submission_tx_reopen_preserves_request_state_and_withdrawal() {
    use publicworks_storage_sqlite::SqliteStorage;
    use serde_json::{Value, json};
    struct Database(std::path::PathBuf);
    impl Drop for Database {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let db = Database(std::env::temp_dir().join(format!(
        "publicworks-submission-tx-{}-{}.db", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )));
    block_on(async {
        let (session, driver) = Session::new(SqliteStorage::open(&db.0).unwrap());
        let ((c, id, state_id), ()) = zip(async {
            let ids = session.commit(|tx| Box::pin(async move {
                let c = tx.create_conversation().await?.id;
                let s = tx.create_submission(c, SubmissionType::Input, Some("retry".into())).await?;
                let state = tx.set_conversation_state(c, ConversationStateDraft {
                    inbox: vec![InboxItem { submission_id: s.id, payload: json!({"$serde_json::private::Number":"ordinary text", "max":u64::MAX}) }],
                    agent_config: Some(Value::Null),
                    run: None,
                }).await?;
                Ok((c, s.id, state.id))
            })).await.unwrap().value;
            session.close().await.unwrap();
            ids
        }, driver).await;
        let (session, driver) = Session::new(SqliteStorage::open(&db.0).unwrap());
        zip(async {
            let read = session.commit(move |tx| Box::pin(async move {
                let original = tx.submission_by_request(c, "retry").await?.unwrap();
                assert_eq!(original.id, id);
                assert_eq!(original.state, SubmissionState::InputQueued);
                let state = tx.conversation_state(c).await?.unwrap();
                assert_eq!(state.id, state_id);
                assert_eq!(state.agent_config, Some(Value::Null));
                assert_eq!(state.inbox[0].payload, json!({"$serde_json::private::Number":"ordinary text", "max":u64::MAX}));
                Ok(())
            })).await.unwrap();
            assert!(read.seq.is_none());
            session.commit(move |tx| Box::pin(async move {
                assert_eq!(tx.withdraw_submission(id, Some(c)).await?, WithdrawalResult::Aborted);
                Ok(())
            })).await.unwrap();
            session.close().await.unwrap();
        }, driver).await;
        let (session, driver) = Session::new(SqliteStorage::open(&db.0).unwrap());
        zip(
            async {
                session
                    .commit(move |tx| {
                        Box::pin(async move {
                            assert_eq!(
                                tx.submission_by_request(c, "retry").await?.unwrap().state,
                                SubmissionState::InputUnanswered {
                                    entry: None,
                                    reason: "aborted".into(),
                                    detail: None
                                }
                            );
                            let state = tx.conversation_state(c).await?.unwrap();
                            assert_eq!(state.id, state_id);
                            assert!(state.inbox.is_empty());
                            Ok(())
                        })
                    })
                    .await
                    .unwrap();
                session.close().await.unwrap();
            },
            driver,
        )
        .await;
    });
}
