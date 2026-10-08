use futures_lite::future::{block_on, zip};
use publicworks_runtime::{test_support::TempDatabase, *};
use publicworks_storage_sqlite::SqliteStorage;
use serde_json::json;
use std::{cell::Cell, collections::BTreeMap, rc::Rc};

struct TestDb(TempDatabase);
impl TestDb {
    fn new(label: &str) -> Self {
        Self(TempDatabase::new(
            &format!("publicworks-harness-release-{label}-"),
            "harness.db",
        ))
    }

    fn open(&self) -> SqliteStorage {
        SqliteStorage::open(self.0.path()).unwrap()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FaultPoint {
    Running,
    Terminal,
}
impl FaultPoint {
    fn matches(self, writes: &[StorageWrite]) -> bool {
        writes.iter().any(|write| {
            let StorageWrite::Task(task) = write else {
                return false;
            };
            matches!(
                (self, task.status()),
                (Self::Running, TaskStatus::Running) | (Self::Terminal, TaskStatus::Terminal)
            )
        })
    }
}

#[derive(Default)]
struct Probe {
    fault: Cell<Option<FaultPoint>>,
    hit: Cell<bool>,
}

struct UncertainAfterCommit {
    inner: SqliteStorage,
    probe: Rc<Probe>,
}
impl Storage for UncertainAfterCommit {
    publicworks_runtime::forward_storage_methods!(inner;
        mint_id,
        conversation,
        scan_conversations,
        task,
        scan_tasks,
        submission,
        scan_submissions,
        submission_by_request,
        conversation_state,
        entry,
        visible_entry,
        scan_entries,
        find_latest_head_marker,
        close,
    );
    fn commit(&mut self, writes: Vec<StorageWrite>) -> StorageFuture<'_, Seq> {
        Box::pin(async move {
            let fault = self.probe.fault.get();
            let inject = fault.is_some_and(|point| point.matches(&writes));
            let seq = self.inner.commit(writes).await?;
            if inject {
                self.probe.fault.set(None);
                self.probe.hit.set(true);
                Err(StorageError::Other(
                    "injected uncertainty after SQLite commit".into(),
                ))
            } else {
                Ok(seq)
            }
        })
    }
}

fn definition(calls: Rc<Cell<usize>>) -> TaskDefinition {
    let handler: PhaseHandler = Rc::new(move |_, runtime| {
        let calls = calls.clone();
        Box::pin(async move {
            calls.set(calls.get() + 1);
            runtime
                .commit(|_, _| Box::pin(async { Ok(Some(TaskUpdate::Complete(json!("done")))) }))
                .await
                .map_err(|error| TaskOutcomeError {
                    message: error.to_string(),
                    detail: None,
                })?;
            Ok(())
        })
    });
    TaskDefinition::new(
        "release-recovery",
        1,
        |_| Ok(json!({"phase":"run"})),
        BTreeMap::from([("run".into(), handler)]),
    )
}

fn assert_uncertain_close(result: Result<(), HarnessError>) {
    assert!(matches!(
        result,
        Err(HarnessError::Session(
            SessionError::Poisoned | SessionError::Storage(StorageError::Other(_))
        ))
    ));
}

#[test]
fn sqlite_recovery_respects_uncertain_reservation_and_terminal_persistence() {
    block_on(async {
        for point in [FaultPoint::Running, FaultPoint::Terminal] {
            let db = TestDb::new(match point {
                FaultPoint::Running => "running",
                FaultPoint::Terminal => "terminal",
            });
            let calls = Rc::new(Cell::new(0));
            let definition = definition(calls.clone());
            let probe = Rc::new(Probe::default());
            let (opening, driver) = Harness::open(
                UncertainAfterCommit {
                    inner: db.open(),
                    probe: probe.clone(),
                },
                TaskRegistry::new([definition.clone()]).unwrap(),
            );
            let first = async {
                let harness = opening.await.unwrap();
                let task = harness
                    .commit({
                        let definition = definition.clone();
                        move |tx| {
                            Box::pin(async move {
                                let conversation = tx.create_conversation().await?;
                                tx.create_task(
                                    definition,
                                    json!(null),
                                    TaskOptions {
                                        ownership: TaskOwnership::Conversation,
                                        conversation_id: Some(conversation.id),
                                        background: false,
                                    },
                                )
                                .await
                            })
                        }
                    })
                    .await
                    .unwrap()
                    .value;
                probe.fault.set(Some(point));
                let waiter = harness.wait_task(task.id);
                harness.resume().unwrap();
                assert_eq!(waiter.await, Err(HarnessError::Closed));
                assert_uncertain_close(harness.close().await);
                task.id
            };
            let (task_id, ()) = zip(first, driver).await;
            assert!(probe.hit.get());
            assert_eq!(calls.get(), usize::from(point == FaultPoint::Terminal));

            // Reopening is the only authority on whether the uncertain commit
            // reached durable SQLite state. Running is normalized and replayed;
            // terminal settlement is observed and never redispatched.
            let (opening, driver) =
                Harness::open(db.open(), TaskRegistry::new([definition.clone()]).unwrap());
            let second = async {
                let harness = opening.await.unwrap();
                let inspection = harness.inspect().await.unwrap();
                match point {
                    FaultPoint::Running => {
                        assert!(matches!(
                            inspection.tasks.as_slice(),
                            [TaskInspection {
                                task: TaskRecord {
                                    state: TaskState::Pending { .. },
                                    ..
                                },
                                ..
                            }]
                        ));
                    }
                    FaultPoint::Terminal => assert!(inspection.tasks.is_empty()),
                }
                harness.resume().unwrap();
                assert_eq!(
                    harness.wait_task(task_id).await.unwrap().status(),
                    TaskStatus::Terminal
                );
                harness.close().await.unwrap();
            };
            let ((), ()) = zip(second, driver).await;
            assert_eq!(calls.get(), 1);
        }
    });
}
