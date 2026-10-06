use futures_lite::future::{block_on, zip};
use publicworks_runtime::*;
use serde_json::json;

#[test]
fn atomic_create_append_and_detached_read() {
    block_on(async {
        let (session, driver) = Session::new(MemoryStorage::new());
        let command = async {
            let receipt = session
                .commit(|tx| {
                    Box::pin(async move {
                        let conversation = tx.create_conversation().await?;
                        let mut draft = EntryDraft::new("message");
                        draft.data = Some(json!({"text": "original"}));
                        draft.head = Some(Head::SelfEntry);
                        let mut entry = tx.append_entry(conversation.id, draft).await?;
                        assert_eq!(entry.head, Some(entry.id));
                        entry.data = Some(json!("changed"));
                        Ok((conversation.id, entry.id))
                    })
                })
                .await
                .unwrap();
            assert_eq!(receipt.seq.unwrap().get(), 1);
            let (conversation, entry) = receipt.value;
            let read = session
                .commit(move |tx| {
                    Box::pin(
                        async move { Ok(tx.visible_entry(conversation, entry).await?.unwrap()) },
                    )
                })
                .await
                .unwrap();
            assert!(read.seq.is_none());
            assert_eq!(read.value.entry.data, Some(json!({"text":"original"})));
            session.close().await.unwrap();
        };
        zip(command, driver).await;
    });
}

async fn seed(session: &Session) -> (Id, Id, Id) {
    session
        .commit(|tx| {
            Box::pin(async move {
                let conversation = tx.create_conversation().await?;
                let first = tx
                    .append_entry(conversation.id, EntryDraft::new("first"))
                    .await?;
                let last = tx
                    .append_entry(conversation.id, EntryDraft::new("last"))
                    .await?;
                Ok((conversation.id, first.id, last.id))
            })
        })
        .await
        .unwrap()
        .value
}

#[test]
fn forks_require_committed_visible_cutoffs_and_retain_immediate_parent() {
    block_on(async {
        let (session, driver) = Session::new(MemoryStorage::new());
        zip(
            async {
                let (parent, first, last) = seed(&session).await;
                let child = session
                    .commit(move |tx| {
                        Box::pin(async move { tx.fork_conversation(parent, first).await })
                    })
                    .await
                    .unwrap()
                    .value;
                let nested = session
                    .commit(move |tx| {
                        Box::pin(async move { tx.fork_conversation(child.id, first).await })
                    })
                    .await
                    .unwrap()
                    .value;
                assert_eq!(nested.parent.unwrap().conversation_id, child.id);
                let invisible = session
                    .commit(move |tx| {
                        Box::pin(async move { tx.fork_conversation(child.id, last).await })
                    })
                    .await;
                assert!(matches!(invisible, Err(SessionError::Invalid(_))));
                let staged = session
                    .commit(move |tx| {
                        Box::pin(async move {
                            let entry = tx
                                .append_entry(parent, EntryDraft::new("not yet committed"))
                                .await?;
                            tx.fork_conversation(parent, entry.id).await
                        })
                    })
                    .await;
                assert!(matches!(staged, Err(SessionError::Invalid(_))));
                let read = session
                    .commit(move |tx| {
                        Box::pin(async move {
                            let parent_entries =
                                tx.scan_entries(EntryQuery::new(parent), 10, None).await?;
                            let child_entries =
                                tx.scan_entries(EntryQuery::new(child.id), 10, None).await?;
                            Ok((parent_entries, child_entries))
                        })
                    })
                    .await
                    .unwrap();
                assert_eq!(
                    read.value.0.items.iter().map(|e| e.id).collect::<Vec<_>>(),
                    vec![last, first]
                );
                assert_eq!(
                    read.value.1.items.iter().map(|e| e.id).collect::<Vec<_>>(),
                    vec![first]
                );
                session.close().await.unwrap();
            },
            driver,
        )
        .await;
    });
}

#[test]
fn read_after_write_latches_at_attempt_even_on_caught_failure_or_unpolled_waiter() {
    block_on(async {
        let (session, driver) = Session::new(MemoryStorage::new());
        zip(
            async {
                session
                    .commit(|tx| {
                        Box::pin(async move {
                            assert!(tx.conversation(ROOT_CONVERSATION).await?.is_none());
                            assert!(
                                tx.append_entry(ROOT_CONVERSATION, EntryDraft::new("missing"))
                                    .await
                                    .is_err()
                            );
                            assert_eq!(
                                tx.conversation(ROOT_CONVERSATION).await.unwrap_err(),
                                SessionError::ReadAfterWrite
                            );
                            assert_eq!(
                                tx.entry(ROOT_CONVERSATION).await.unwrap_err(),
                                SessionError::ReadAfterWrite
                            );
                            assert_eq!(
                                tx.visible_entry(ROOT_CONVERSATION, ROOT_CONVERSATION)
                                    .await
                                    .unwrap_err(),
                                SessionError::ReadAfterWrite
                            );
                            assert_eq!(
                                tx.scan_conversations(ConversationQuery::default(), 1, None)
                                    .await
                                    .unwrap_err(),
                                SessionError::ReadAfterWrite
                            );
                            assert_eq!(
                                tx.scan_entries(EntryQuery::new(ROOT_CONVERSATION), 1, None)
                                    .await
                                    .unwrap_err(),
                                SessionError::ReadAfterWrite
                            );
                            assert_eq!(
                                tx.find_latest_head_marker(ROOT_CONVERSATION, None)
                                    .await
                                    .unwrap_err(),
                                SessionError::ReadAfterWrite
                            );
                            Ok(())
                        })
                    })
                    .await
                    .unwrap();
                let pending = session
                    .commit(|tx| {
                        Box::pin(async move {
                            drop(tx.create_conversation());
                            assert_eq!(
                                tx.conversation(ROOT_CONVERSATION).await.unwrap_err(),
                                SessionError::ReadAfterWrite
                            );
                            Ok(())
                        })
                    })
                    .await;
                assert!(matches!(pending, Err(SessionError::PendingOperations)));
                assert_eq!(
                    session
                        .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
                        .await
                        .unwrap()
                        .seq
                        .unwrap()
                        .get(),
                    1
                );
                session.close().await.unwrap();
            },
            driver,
        )
        .await;
    });
}

#[test]
fn callback_and_payload_errors_discard_writes_and_empty_skips_sequence() {
    block_on(async {
        let (session, driver) = Session::new(MemoryStorage::new());
        zip(
            async {
                let failure = session
                    .commit(|tx| {
                        Box::pin(async move {
                            tx.create_conversation().await?;
                            Err::<(), _>(SessionError::Invalid("callback failed".into()))
                        })
                    })
                    .await;
                assert!(matches!(failure, Err(SessionError::Invalid(_))));
                let invalid = session
                    .commit(|tx| {
                        Box::pin(async move {
                            let conversation = tx.create_conversation().await?;
                            let mut draft = EntryDraft::new("too deep");
                            let mut value = json!(null);
                            for _ in 0..=MAX_JSON_DEPTH {
                                value = serde_json::Value::Array(vec![value]);
                            }
                            draft.data = Some(value);
                            tx.append_entry(conversation.id, draft).await
                        })
                    })
                    .await;
                assert!(matches!(
                    invalid,
                    Err(SessionError::Storage(StorageError::Other(_)))
                ));
                let empty = session
                    .commit(|tx| {
                        Box::pin(async move {
                            tx.scan_conversations(ConversationQuery::default(), 10, None)
                                .await
                        })
                    })
                    .await
                    .unwrap();
                assert!(empty.value.items.is_empty());
                assert!(empty.seq.is_none());
                let next = session
                    .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
                    .await
                    .unwrap();
                assert_eq!(next.seq.unwrap().get(), 1);
                assert!(next.value.id.get() > 2); // failed transactions may consume minted IDs
                session.close().await.unwrap();
            },
            driver,
        )
        .await;
    });
}

#[test]
fn append_keeps_opaque_json_and_does_not_validate_head_or_edit_targets() {
    block_on(async {
        let (session, driver) = Session::new(MemoryStorage::new());
        zip(async {
            let (conversation, entry) = session.commit(|tx| Box::pin(async move {
                let conversation = tx.create_conversation().await?;
                let mut draft = EntryDraft::new("opaque");
                draft.data = Some(serde_json::Value::Null);
                draft.model = Some(vec![json!({"$serde_json::private::Number":"ordinary", "nested": [1, true]})]);
                draft.head = Some(Head::Entry(ROOT_CONVERSATION));
                draft.edits = Some(vec![ContextEdit::Replace { target: ROOT_CONVERSATION, messages: vec![json!({"text":"replacement"})] }]);
                let entry = tx.append_entry(conversation.id, draft).await?;
                Ok((conversation.id, entry))
            })).await.unwrap().value;
            let id = entry.id;
            let mut read = session.commit(move |tx| Box::pin(async move { tx.find_latest_head_marker(conversation, None).await })).await.unwrap().value.unwrap();
            assert_eq!(read, entry);
            read.model.as_mut().unwrap().clear();
            read.edits.as_mut().unwrap().clear();
            let again = session.commit(move |tx| Box::pin(async move { tx.entry(id).await })).await.unwrap().value.unwrap();
            assert_eq!(again.entry, entry);
            session.close().await.unwrap();
        }, driver).await;
    });
}
