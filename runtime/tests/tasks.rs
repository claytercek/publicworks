use futures_lite::future::block_on;
use publicworks_runtime::*;
use serde_json::json;

#[test]
fn tasks_replace_and_roundtrip() {
    block_on(async {
        let mut store = MemoryStorage::new();
        let mut record = TaskRecord {
            id: Id::new(2).unwrap(),
            conversation_id: ROOT_CONVERSATION,
            kind: "unknown".into(),
            version: 42,
            input: json!(null),
            owner: None,
            background: false,
            abort_requested: false,
            state: TaskState::Running {
                checkpoint: json!({"$serde_json::private::Number":"opaque"}),
            },
            memos: None,
        };
        store
            .commit(vec![StorageWrite::Task(record.clone())])
            .await
            .unwrap();
        record.state = TaskState::Pending {
            checkpoint: json!(null),
        };
        store
            .commit(vec![
                StorageWrite::Task(record.clone()),
                StorageWrite::Task(record.clone()),
            ])
            .await
            .unwrap();
        assert_eq!(store.task(record.id).await.unwrap(), Some(record.clone()));
        let text = serde_json::to_string(&record).unwrap();
        assert_eq!(serde_json::from_str::<TaskRecord>(&text).unwrap(), record);
    });
}

fn initializer() -> TaskInitializer {
    TaskInitializer::new("creation.only", 11, |input| Ok(json!({"initial":input})))
}
fn options(conversation_id: Option<Id>, ownership: TaskOwnership) -> TaskOptions {
    TaskOptions {
        conversation_id,
        ownership,
        background: false,
    }
}
#[test]
fn session_creates_staged_parent_child_and_owned_conversations() {
    use futures_lite::future::zip;
    block_on(async {
        let (session, driver) = Session::new(MemoryStorage::new());
        zip(
            async {
                let receipt = session
                    .commit(|tx| {
                        Box::pin(async move {
                            let c = tx.create_conversation().await?;
                            let mut parent = tx
                                .create_task(
                                    initializer(),
                                    json!({"opaque":null}),
                                    options(Some(c.id), TaskOwnership::Conversation),
                                )
                                .await?;
                            let child = tx
                                .create_task(
                                    initializer(),
                                    json!(null),
                                    options(None, TaskOwnership::Task(parent.id)),
                                )
                                .await?;
                            let owned =
                                tx.create_conversation_owned(Owner::Task(parent.id)).await?;
                            assert_eq!(child.conversation_id, c.id);
                            assert_eq!(child.owner, Some(parent.id));
                            assert_eq!(
                                owned.owner,
                                Some(OwnerLink {
                                    conversation_id: c.id,
                                    task_id: parent.id
                                })
                            );
                            parent.input = json!("detached");
                            assert_eq!(
                                tx.task(parent.id).await.unwrap_err(),
                                SessionError::ReadAfterWrite
                            );
                            assert_eq!(
                                tx.scan_tasks(TaskQuery::default(), 10, None)
                                    .await
                                    .unwrap_err(),
                                SessionError::ReadAfterWrite
                            );
                            let entry = tx.append_entry(c.id, EntryDraft::new("cutoff")).await?;
                            Ok((parent.id, child, entry.id))
                        })
                    })
                    .await
                    .unwrap();
                assert_eq!(receipt.seq.unwrap().get(), 1);
                let (id, child, at) = receipt.value;
                let conversation = child.conversation_id;
                let fork = session
                    .commit(move |tx| {
                        Box::pin(async move {
                            tx.fork_conversation_owned(conversation, at, Owner::Task(id))
                                .await
                        })
                    })
                    .await
                    .unwrap()
                    .value;
                assert_eq!(fork.owner.unwrap().task_id, id);
                let read = session
                    .commit(move |tx| {
                        Box::pin(async move {
                            let parent = tx.task(id).await?.unwrap();
                            assert_eq!(parent.input, json!({"opaque":null}));
                            assert_eq!(parent.version, 11);
                            assert_eq!(tx.task(child.id).await?, Some(child));
                            tx.scan_tasks(TaskQuery::default(), 10, None).await
                        })
                    })
                    .await
                    .unwrap();
                assert!(read.seq.is_none());
                assert_eq!(read.value.items.len(), 2);
                session.close().await.unwrap();
            },
            driver,
        )
        .await;
    });
}
#[test]
fn creation_failures_discard_batch_and_latch_reads_without_poison() {
    use futures_lite::future::zip;
    block_on(async {
        let (session, driver) = Session::new(MemoryStorage::new());
        zip(
            async {
                for failure in 0..7 {
                    let result = session
                        .commit(move |tx| {
                            Box::pin(async move {
                                let c = tx.create_conversation().await?;
                                let parent = tx
                                    .create_task(
                                        initializer(),
                                        json!(null),
                                        options(Some(c.id), TaskOwnership::Conversation),
                                    )
                                    .await?;
                                match failure {
                                    0 => {
                                        tx.create_task(
                                            initializer(),
                                            json!(null),
                                            options(None, TaskOwnership::Conversation),
                                        )
                                        .await?;
                                    }
                                    1 => {
                                        tx.create_task(
                                            initializer(),
                                            json!(null),
                                            options(
                                                Some(ROOT_CONVERSATION),
                                                TaskOwnership::Task(parent.id),
                                            ),
                                        )
                                        .await?;
                                    }
                                    2 => {
                                        let mut opts =
                                            options(None, TaskOwnership::Task(parent.id));
                                        opts.background = true;
                                        tx.create_task(initializer(), json!(null), opts).await?;
                                    }
                                    3 => {
                                        tx.create_task(
                                            initializer(),
                                            json!(null),
                                            options(None, TaskOwnership::Task(ROOT_CONVERSATION)),
                                        )
                                        .await?;
                                    }
                                    4 => {
                                        tx.create_task(
                                            TaskInitializer::new("fails", 1, |_| {
                                                Err(SessionError::Invalid("initializer".into()))
                                            }),
                                            json!(null),
                                            options(Some(c.id), TaskOwnership::Conversation),
                                        )
                                        .await?;
                                    }
                                    5 => {
                                        let mut v = json!(null);
                                        for _ in 0..64 {
                                            v = json!([v]);
                                        }
                                        tx.create_task(
                                            TaskInitializer::new("deep", 1, move |_| Ok(v)),
                                            json!(null),
                                            options(Some(c.id), TaskOwnership::Conversation),
                                        )
                                        .await?;
                                    }
                                    _ => return Err(SessionError::Invalid("callback".into())),
                                }
                                Ok(())
                            })
                        })
                        .await;
                    assert!(result.is_err());
                }
                let read = session
                    .commit(|tx| {
                        Box::pin(async move {
                            assert!(
                                tx.scan_tasks(TaskQuery::default(), 10, None)
                                    .await?
                                    .items
                                    .is_empty()
                            );
                            tx.scan_conversations(ConversationQuery::default(), 10, None)
                                .await
                        })
                    })
                    .await
                    .unwrap();
                assert!(read.value.items.is_empty());
                assert!(read.seq.is_none());
                let caught = session
                    .commit(|tx| {
                        Box::pin(async move {
                            assert!(
                                tx.create_task(
                                    initializer(),
                                    json!(null),
                                    options(None, TaskOwnership::Conversation)
                                )
                                .await
                                .is_err()
                            );
                            assert_eq!(
                                tx.task(ROOT_CONVERSATION).await.unwrap_err(),
                                SessionError::ReadAfterWrite
                            );
                            Ok(())
                        })
                    })
                    .await
                    .unwrap();
                assert!(caught.seq.is_none());
                let unpolled = session
                    .commit(|tx| {
                        Box::pin(async move {
                            drop(tx.create_task(
                                initializer(),
                                json!(null),
                                options(None, TaskOwnership::Conversation),
                            ));
                            assert_eq!(
                                tx.scan_tasks(TaskQuery::default(), 1, None)
                                    .await
                                    .unwrap_err(),
                                SessionError::ReadAfterWrite
                            );
                            Ok(())
                        })
                    })
                    .await;
                assert!(matches!(unpolled, Err(SessionError::PendingOperations)));
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
fn recovery_normalizes_all_pages_and_preserves_unknown_definitions_and_other_states() {
    use futures_lite::future::zip;
    block_on(async {
        let mut storage = MemoryStorage::new();
        let mut records = Vec::new();
        for id in 2..270 {
            let state = match id {
                2 => TaskState::Waiting {
                    checkpoint: json!({"waiting":true}),
                    on: vec![Id::new(3).unwrap()],
                    policy: JoinPolicy::FailFast,
                },
                3 => TaskState::Completing {
                    outcome: TaskOutcome::Failed {
                        error: TaskOutcomeError {
                            message: "stored only".into(),
                            detail: Some(json!(null)),
                        },
                        result: None,
                    },
                },
                4 => TaskState::Terminal {
                    outcome: TaskOutcome::Orphaned {
                        reason: "retained".into(),
                    },
                },
                5 => TaskState::Pending {
                    checkpoint: json!(null),
                },
                _ => TaskState::Running {
                    checkpoint: json!({"checkpoint":id}),
                },
            };
            let memos = if id > 5 {
                Some(std::collections::BTreeMap::from([(
                    "opaque".into(),
                    json!({"$serde_json::private::Number":"123"}),
                )]))
            } else {
                None
            };
            records.push(TaskRecord {
                id: Id::new(id).unwrap(),
                conversation_id: ROOT_CONVERSATION,
                kind: "unregistered.future".into(),
                version: u64::MAX,
                input: json!({"original":id}),
                owner: Some(Id::new(999).unwrap()),
                background: id % 2 == 0,
                abort_requested: id % 3 == 0,
                state,
                memos,
            });
        }
        let mut writes = vec![StorageWrite::Conversation(ConversationRecord {
            id: ROOT_CONVERSATION,
            parent: None,
            owner: None,
        })];
        writes.extend(records.iter().cloned().map(StorageWrite::Task));
        storage.commit(writes).await.unwrap();
        let (opening, driver) = Session::open_recovered(storage);
        zip(
            async {
                let receipt = opening.await.unwrap();
                assert_eq!(receipt.seq.unwrap().get(), 2);
                let session = receipt.value;
                let actual = session
                    .commit(|tx| {
                        Box::pin(
                            async move { tx.scan_tasks(TaskQuery::default(), 1000, None).await },
                        )
                    })
                    .await
                    .unwrap();
                for record in &mut records {
                    if let TaskState::Running { checkpoint } = &record.state {
                        record.state = TaskState::Pending {
                            checkpoint: checkpoint.clone(),
                        };
                    }
                }
                assert_eq!(actual.value.items, records);
                assert!(actual.seq.is_none());
                assert_eq!(
                    session
                        .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
                        .await
                        .unwrap()
                        .seq
                        .unwrap()
                        .get(),
                    3
                );
                session.close().await.unwrap();
            },
            driver,
        )
        .await;
        let (opening, driver) = Session::open_recovered(MemoryStorage::new());
        zip(
            async {
                let opened = opening.await.unwrap();
                assert!(opened.seq.is_none());
                opened.value.close().await.unwrap();
            },
            driver,
        )
        .await;
    });
}

#[test]
fn normalization_preserves_storage_valid_dangling_references() {
    use futures_lite::future::zip;
    block_on(async {
        let mut storage = MemoryStorage::new();
        let mut record = TaskRecord {
            id: Id::new(2).unwrap(),
            conversation_id: Id::new(777).unwrap(),
            kind: "unknown".into(),
            version: 123,
            input: json!({"opaque":null}),
            owner: Some(Id::new(888).unwrap()),
            background: false,
            abort_requested: true,
            state: TaskState::Running {
                checkpoint: json!({"phase":"kept"}),
            },
            memos: Some(std::collections::BTreeMap::from([(
                "memo".into(),
                json!(null),
            )])),
        };
        storage
            .commit(vec![StorageWrite::Task(record.clone())])
            .await
            .unwrap();
        let (opening, driver) = Session::open_recovered(storage);
        zip(
            async {
                let receipt = match opening.await {
                    Ok(receipt) => receipt,
                    Err(error) => panic!("storage-valid replacement failed: {error}"),
                };
                assert_eq!(receipt.seq.unwrap().get(), 2);
                let session = receipt.value;
                record.state = TaskState::Pending {
                    checkpoint: json!({"phase":"kept"}),
                };
                let id = record.id;
                assert_eq!(
                    session
                        .commit(move |tx| Box::pin(async move { tx.task(id).await }))
                        .await
                        .unwrap()
                        .value,
                    Some(record)
                );
                session.close().await.unwrap();
            },
            driver,
        )
        .await;
    });
}
