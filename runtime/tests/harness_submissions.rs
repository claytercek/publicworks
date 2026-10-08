use futures_lite::future::{block_on, poll_once};
use publicworks_runtime::*;
#[path = "../src/test_support/scaffold.rs"]
mod scaffold;
#[path = "../src/test_support/storage.rs"]
mod storage_scaffold;
use scaffold::Gate;
use serde_json::json;
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    future::Future,
    pin::pin,
    rc::Rc,
    task::Waker,
};

// Poll the host driver until it stops waking itself. External gates remain
// pending; no sleeps or test-only runtime hooks are needed.
async fn tick(driver: &mut HarnessDriver) -> Option<()> {
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll, Wake},
    };
    struct Signal(AtomicBool);
    impl Wake for Signal {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let signal = Arc::new(Signal(AtomicBool::new(false)));
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    loop {
        signal.0.store(false, Ordering::SeqCst);
        if let Poll::Ready(()) = Pin::new(&mut *driver).poll(&mut cx) {
            return Some(());
        }
        if !signal.0.load(Ordering::SeqCst) {
            return None;
        }
    }
}

#[derive(Clone, Copy, Default)]
enum Fault {
    #[default]
    None,
    Rejected,
    Before,
    After,
}
#[derive(Default)]
struct Probe {
    commit_gate: RefCell<Option<Gate>>,
    read_gate: RefCell<Option<Gate>>,
    fault: Cell<Fault>,
    commits: Cell<usize>,
}
struct Store {
    inner: MemoryStorage,
    probe: Rc<Probe>,
}
impl Storage for Store {
    forward_storage_methods!(inner;
        mint_id,
        conversation,
        scan_conversations,
        task,
        scan_tasks,
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
            self.probe.commits.set(self.probe.commits.get() + 1);
            let gate = self.probe.commit_gate.borrow().clone();
            if let Some(gate) = gate {
                gate.wait().await;
            }
            let fault = self.probe.fault.replace(Fault::None);
            match fault {
                Fault::Rejected => return Err(StorageError::Rejected("no effect".into())),
                Fault::Before => return Err(StorageError::Other("uncertain before".into())),
                _ => {}
            }
            let seq = self.inner.commit(writes).await?;
            if matches!(fault, Fault::After) {
                return Err(StorageError::Other("uncertain after".into()));
            }
            Ok(seq)
        })
    }
    fn submission(&mut self, id: Id) -> StorageFuture<'_, Option<SubmissionRecord>> {
        Box::pin(async move {
            let gate = self.probe.read_gate.borrow().clone();
            if let Some(gate) = gate {
                gate.wait().await;
            }
            self.inner.submission(id).await
        })
    }
}

async fn finish<F: Future>(driver: &mut HarnessDriver, future: F) -> F::Output {
    let mut future = pin!(future);
    for _ in 0..20 {
        if let Some(result) = poll_once(&mut future).await {
            return result;
        }
        tick(driver).await;
    }
    panic!("observation did not settle");
}
async fn open() -> (Harness, HarnessDriver, Rc<Probe>) {
    let probe = Rc::new(Probe::default());
    let (opening, mut driver) = Harness::open(
        Store {
            inner: MemoryStorage::new(),
            probe: probe.clone(),
        },
        TaskRegistry::default(),
    );
    let harness = finish(&mut driver, opening).await.unwrap();
    (harness, driver, probe)
}
async fn queued(
    harness: &Harness,
    driver: &mut HarnessDriver,
    kind: SubmissionType,
) -> SubmissionRecord {
    finish(
        driver,
        harness.commit(move |tx| {
            Box::pin(async move {
                let conversation = tx.create_conversation().await?;
                tx.create_submission(conversation.id, kind, Some("request".into()))
                    .await
            })
        }),
    )
    .await
    .unwrap()
    .value
}
fn unanswered() -> SubmissionSettlement {
    SubmissionSettlement::Unanswered {
        reason: "test".into(),
        detail: Some(json!(null)),
    }
}
async fn shutdown(harness: &Harness, driver: &mut HarnessDriver) {
    finish(driver, harness.close()).await.unwrap();
}

#[test]
fn reacquisition_status_and_abort_are_read_only_progress_paths() {
    block_on(async {
        let (harness, mut driver, probe) = open().await;
        let record = queued(&harness, &mut driver, SubmissionType::Input).await;
        let submission = finish(&mut driver, harness.submission(record.id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(submission.id(), record.id);
        let commits = probe.commits.get();
        assert_eq!(
            finish(&mut driver, submission.status()).await.unwrap(),
            record
        );
        assert_eq!(
            finish(&mut driver, submission.clone().read())
                .await
                .unwrap(),
            record
        );
        assert!(
            finish(&mut driver, harness.submission(Id::new(999).unwrap()))
                .await
                .unwrap()
                .is_none()
        );
        let conversation = finish(&mut driver, harness.conversation(record.conversation_id))
            .await
            .unwrap()
            .unwrap();
        assert!(
            finish(&mut driver, conversation.submission(record.id))
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(probe.commits.get(), commits);
        assert!(
            !finish(&mut driver, harness.inspect())
                .await
                .unwrap()
                .progress_enabled
        );
        assert_eq!(
            finish(&mut driver, submission.abort()).await.unwrap(),
            WithdrawalResult::Aborted
        );
        assert!(
            !finish(&mut driver, harness.inspect())
                .await
                .unwrap()
                .progress_enabled
        );
        assert_eq!(
            finish(&mut driver, submission.withdraw()).await.unwrap(),
            WithdrawalResult::Settled
        );
        shutdown(&harness, &mut driver).await;
    });
}

#[test]
fn terminal_fast_path_and_handle_reacquisition_after_reopen() {
    block_on(async {
        for state in [
            SubmissionState::InputUnanswered {
                entry: None,
                reason: "reset".into(),
                detail: None,
            },
            SubmissionState::WriteDone {
                entry: Id::new(3).unwrap(),
            },
        ] {
            let mut storage = MemoryStorage::new();
            let record = SubmissionRecord {
                id: Id::new(4).unwrap(),
                conversation_id: Id::new(2).unwrap(),
                request_id: None,
                state,
            };
            storage
                .commit(vec![
                    StorageWrite::Conversation(ConversationRecord {
                        id: record.conversation_id,
                        parent: None,
                        owner: None,
                    }),
                    StorageWrite::Entry(EntryRecord::new(
                        Id::new(3).unwrap(),
                        record.conversation_id,
                        "test",
                    )),
                    StorageWrite::Submission(record.clone()),
                ])
                .await
                .unwrap();
            let (opening, mut driver) = Harness::open(storage, TaskRegistry::default());
            let harness = finish(&mut driver, opening).await.unwrap();
            let submission = finish(&mut driver, harness.submission(record.id))
                .await
                .unwrap()
                .unwrap();
            assert!(
                !finish(&mut driver, harness.inspect())
                    .await
                    .unwrap()
                    .progress_enabled
            );
            assert_eq!(
                finish(&mut driver, submission.wait()).await.unwrap(),
                record
            );
            assert!(
                finish(&mut driver, harness.inspect())
                    .await
                    .unwrap()
                    .progress_enabled
            );
            assert_eq!(
                finish(&mut driver, submission.wait()).await.unwrap(),
                record
            );
            shutdown(&harness, &mut driver).await;
        }
    });
}

#[test]
fn multiple_waiters_drops_and_submission_only_publication_after_ack() {
    block_on(async {
        let (harness, mut driver, probe) = open().await;
        let record = queued(&harness, &mut driver, SubmissionType::Input).await;
        let submission = finish(&mut driver, harness.submission(record.id))
            .await
            .unwrap()
            .unwrap();
        // Admission is synchronous. This callback still runs, but must not register.
        drop(submission.wait());
        let mut first = submission.wait();
        let mut second = submission.clone().wait();
        let dropped = submission.wait();
        tick(&mut driver).await;
        drop(dropped);
        assert!(poll_once(&mut first).await.is_none());
        assert_eq!(
            finish(&mut driver, submission.read()).await.unwrap(),
            record
        );
        let gate = Gate::default();
        *probe.commit_gate.borrow_mut() = Some(gate.clone());
        let settle = harness.commit(move |tx| {
            Box::pin(async move { tx.settle_submission(record.id, unanswered()).await })
        });
        tick(&mut driver).await;
        assert!(poll_once(&mut first).await.is_none());
        assert!(poll_once(&mut second).await.is_none());
        gate.release();
        let receipt = finish(&mut driver, settle).await.unwrap();
        assert_eq!(finish(&mut driver, first).await.unwrap(), receipt.value);
        assert_eq!(finish(&mut driver, second).await.unwrap(), receipt.value);
        assert_eq!(
            finish(&mut driver, harness.inspect())
                .await
                .unwrap()
                .last_commit_seq,
            receipt.seq
        );
        shutdown(&harness, &mut driver).await;
    });
}

#[test]
fn final_assembled_placement_and_settlement_are_published_without_lost_wakeup() {
    block_on(async {
        for settle_first in [false, true] {
            let (harness, mut driver, _) = open().await;
            let record = queued(&harness, &mut driver, SubmissionType::Input).await;
            let submission = finish(&mut driver, harness.submission(record.id))
                .await
                .unwrap()
                .unwrap();
            let settle = || {
                harness.commit(move |tx| {
                    Box::pin(async move {
                        let entry = tx
                            .append_entry(record.conversation_id, EntryDraft::new("user"))
                            .await?;
                        let answer = tx
                            .append_entry(record.conversation_id, EntryDraft::new("answer"))
                            .await?;
                        tx.place_submission(record.id, entry.id).await?;
                        tx.settle_submission(
                            record.id,
                            SubmissionSettlement::Done { answer: answer.id },
                        )
                        .await
                    })
                })
            };
            let (wait, commit) = if settle_first {
                let commit = settle();
                (submission.wait(), commit)
            } else {
                let wait = submission.wait();
                (wait, settle())
            };
            let receipt = finish(&mut driver, commit).await.unwrap();
            assert!(matches!(
                receipt.value.state,
                SubmissionState::InputDone { .. }
            ));
            assert_eq!(finish(&mut driver, wait).await.unwrap(), receipt.value);
            shutdown(&harness, &mut driver).await;
        }
    });
}

#[test]
fn placement_does_not_notify_and_withdrawal_reports_all_outcomes() {
    block_on(async {
        let (harness, mut driver, probe) = open().await;
        let record = queued(&harness, &mut driver, SubmissionType::Input).await;
        let submission = finish(&mut driver, harness.submission(record.id))
            .await
            .unwrap()
            .unwrap();
        let mut wait = submission.wait();
        finish(
            &mut driver,
            harness.commit(move |tx| {
                Box::pin(async move {
                    let entry = tx
                        .append_entry(record.conversation_id, EntryDraft::new("user"))
                        .await?;
                    tx.place_submission(record.id, entry.id).await
                })
            }),
        )
        .await
        .unwrap();
        assert!(poll_once(&mut wait).await.is_none());
        let commits = probe.commits.get();
        assert_eq!(
            finish(&mut driver, submission.abort()).await.unwrap(),
            WithdrawalResult::AlreadyPlaced
        );
        assert_eq!(
            finish(
                &mut driver,
                harness.withdraw_submission(record.id, Some(Id::new(999).unwrap()))
            )
            .await
            .unwrap(),
            WithdrawalResult::NotFound
        );
        assert_eq!(
            finish(
                &mut driver,
                harness.withdraw_submission(Id::new(999).unwrap(), None)
            )
            .await
            .unwrap(),
            WithdrawalResult::NotFound
        );
        assert_eq!(probe.commits.get(), commits);
        finish(
            &mut driver,
            harness.commit(move |tx| {
                Box::pin(async move { tx.settle_submission(record.id, unanswered()).await })
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            finish(&mut driver, wait).await.unwrap().status(),
            SubmissionStatus::Unanswered
        );
        shutdown(&harness, &mut driver).await;
    });
}

#[test]
fn withdrawal_publishes_atomic_middle_inbox_removal() {
    block_on(async {
        let (harness, mut driver, _) = open().await;
        let (conversation, ids) = finish(
            &mut driver,
            harness.commit(|tx| {
                Box::pin(async move {
                    let conversation = tx.create_conversation().await?.id;
                    let mut ids = Vec::new();
                    for _ in 0..3 {
                        ids.push(
                            tx.create_submission(conversation, SubmissionType::Write, None)
                                .await?
                                .id,
                        );
                    }
                    tx.set_conversation_state(
                        conversation,
                        ConversationStateDraft {
                            inbox: ids
                                .iter()
                                .map(|id| InboxItem {
                                    submission_id: *id,
                                    payload: json!(id.get()),
                                })
                                .collect(),
                            ..Default::default()
                        },
                    )
                    .await?;
                    Ok((conversation, ids))
                })
            }),
        )
        .await
        .unwrap()
        .value;
        let submission = finish(&mut driver, harness.submission(ids[1]))
            .await
            .unwrap()
            .unwrap();
        let wait = submission.wait();
        let other = queued(&harness, &mut driver, SubmissionType::Write).await;
        let other = finish(&mut driver, harness.conversation(other.conversation_id))
            .await
            .unwrap()
            .unwrap();
        assert!(
            finish(&mut driver, other.submission(ids[1]))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            finish(&mut driver, other.withdraw_submission(ids[1]))
                .await
                .unwrap(),
            WithdrawalResult::NotFound
        );
        let scoped = finish(&mut driver, harness.conversation(conversation))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            finish(&mut driver, scoped.withdraw_submission(ids[1]))
                .await
                .unwrap(),
            WithdrawalResult::Aborted
        );
        assert!(
            matches!(finish(&mut driver, wait).await.unwrap().state, SubmissionState::WriteUnanswered { reason, .. } if reason == "aborted")
        );
        let state = finish(
            &mut driver,
            harness.commit(move |tx| {
                Box::pin(async move { tx.conversation_state(conversation).await })
            }),
        )
        .await
        .unwrap()
        .value
        .unwrap();
        assert_eq!(
            state
                .inbox
                .iter()
                .map(|item| item.submission_id)
                .collect::<Vec<_>>(),
            vec![ids[0], ids[2]]
        );
        shutdown(&harness, &mut driver).await;
    });
}

#[test]
fn wait_resumes_reopened_work_but_reads_do_not() {
    block_on(async {
        let conversation = Id::new(2).unwrap();
        let submission_id = Id::new(4).unwrap();
        let calls = Rc::new(Cell::new(0));
        let observed_calls = calls.clone();
        let handler: PhaseHandler = Rc::new(move |_, runtime| {
            let calls = calls.clone();
            Box::pin(async move {
                calls.set(calls.get() + 1);
                runtime
                    .commit(move |tx, _| {
                        Box::pin(async move {
                            tx.settle_submission(submission_id, unanswered()).await?;
                            Ok(Some(TaskUpdate::Complete(json!(null))))
                        })
                    })
                    .await
                    .map_err(|error| TaskOutcomeError {
                        message: error.to_string(),
                        detail: None,
                    })?;
                Ok(())
            })
        });
        let definition = TaskDefinition::new(
            "recover",
            1,
            |_| Ok(json!({"phase":"run"})),
            BTreeMap::from([("run".into(), handler)]),
        );
        let mut storage = MemoryStorage::new();
        storage
            .commit(vec![
                StorageWrite::Conversation(ConversationRecord {
                    id: conversation,
                    parent: None,
                    owner: None,
                }),
                StorageWrite::Entry(EntryRecord::new(Id::new(3).unwrap(), conversation, "user")),
                StorageWrite::Submission(SubmissionRecord {
                    id: submission_id,
                    conversation_id: conversation,
                    request_id: None,
                    state: SubmissionState::InputPlaced {
                        entry: Id::new(3).unwrap(),
                    },
                }),
                StorageWrite::Task(TaskRecord {
                    id: Id::new(5).unwrap(),
                    conversation_id: conversation,
                    kind: "recover".into(),
                    version: 1,
                    input: json!(null),
                    owner: None,
                    background: false,
                    abort_requested: false,
                    state: TaskState::Running {
                        checkpoint: json!({"phase":"run"}),
                    },
                    memos: None,
                }),
            ])
            .await
            .unwrap();
        let (opening, mut driver) =
            Harness::open(storage, TaskRegistry::new([definition]).unwrap());
        let harness = finish(&mut driver, opening).await.unwrap();
        let submission = finish(&mut driver, harness.submission(submission_id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            finish(&mut driver, submission.status())
                .await
                .unwrap()
                .status(),
            SubmissionStatus::Placed
        );
        assert_eq!(observed_calls.get(), 0);
        let inspection = finish(&mut driver, harness.inspect()).await.unwrap();
        assert!(!inspection.progress_enabled);
        assert_eq!(inspection.tasks[0].task.status(), TaskStatus::Pending);
        assert_eq!(
            finish(&mut driver, submission.wait())
                .await
                .unwrap()
                .status(),
            SubmissionStatus::Unanswered
        );
        assert_eq!(observed_calls.get(), 1);
        shutdown(&harness, &mut driver).await;
    });
}

#[test]
fn close_settles_registered_and_not_yet_registered_observers() {
    block_on(async {
        for registered in [false, true] {
            let (harness, mut driver, _) = open().await;
            let record = queued(&harness, &mut driver, SubmissionType::Input).await;
            let submission = finish(&mut driver, harness.submission(record.id))
                .await
                .unwrap()
                .unwrap();
            let first = submission.wait();
            let second = submission.wait();
            if registered {
                tick(&mut driver).await;
            }
            let close = harness.close();
            assert_eq!(finish(&mut driver, first).await, Err(HarnessError::Closed));
            assert_eq!(finish(&mut driver, second).await, Err(HarnessError::Closed));
            assert_eq!(submission.wait().await, Err(HarnessError::Closed));
            assert_eq!(submission.status().await, Err(HarnessError::Closed));
            assert_eq!(submission.abort().await, Err(HarnessError::Closed));
            assert!(matches!(
                harness.submission(record.id).await,
                Err(HarnessError::Closed)
            ));
            finish(&mut driver, close).await.unwrap();
        }
    });
}

#[test]
fn close_during_storage_read_rechecks_before_terminal_or_registration() {
    block_on(async {
        for terminal in [false, true] {
            let (harness, mut driver, probe) = open().await;
            let record = queued(&harness, &mut driver, SubmissionType::Input).await;
            let submission = finish(&mut driver, harness.submission(record.id))
                .await
                .unwrap()
                .unwrap();
            if terminal {
                finish(&mut driver, submission.abort()).await.unwrap();
            }
            let gate = Gate::default();
            *probe.read_gate.borrow_mut() = Some(gate.clone());
            let wait = submission.wait();
            tick(&mut driver).await;
            let close = harness.close();
            gate.release();
            assert_eq!(finish(&mut driver, wait).await, Err(HarnessError::Closed));
            finish(&mut driver, close).await.unwrap();
        }
    });
}

#[test]
fn driver_drop_settles_all_observers_and_handle_access() {
    block_on(async {
        for registered in [false, true] {
            let (harness, mut driver, _) = open().await;
            let record = queued(&harness, &mut driver, SubmissionType::Write).await;
            let submission = finish(&mut driver, harness.submission(record.id))
                .await
                .unwrap()
                .unwrap();
            let first = submission.wait();
            let second = submission.wait();
            if registered {
                tick(&mut driver).await;
            }
            drop(driver);
            assert_eq!(first.await, Err(HarnessError::DriverStopped));
            assert_eq!(second.await, Err(HarnessError::DriverStopped));
            assert_eq!(submission.wait().await, Err(HarnessError::DriverStopped));
            assert_eq!(submission.read().await, Err(HarnessError::DriverStopped));
            assert_eq!(submission.abort().await, Err(HarnessError::DriverStopped));
            assert_eq!(harness.close().await, Err(HarnessError::DriverStopped));
        }
    });
}

#[test]
fn submission_handles_retain_harness_but_waiters_do_not() {
    block_on(async {
        let (harness, mut driver, _) = open().await;
        let record = queued(&harness, &mut driver, SubmissionType::Input).await;
        let submission = finish(&mut driver, harness.submission(record.id))
            .await
            .unwrap()
            .unwrap();
        let wait = submission.wait();
        tick(&mut driver).await;
        drop(harness);
        let clone = submission.clone();
        drop(submission);
        assert_eq!(finish(&mut driver, clone.read()).await.unwrap(), record);
        drop(clone);
        assert_eq!(finish(&mut driver, wait).await, Err(HarnessError::Closed));
        assert_eq!(tick(&mut driver).await, Some(()));
    });
}

#[test]
fn rejection_and_uncertainty_never_publish_terminal_candidates() {
    block_on(async {
        for fault in [Fault::Rejected, Fault::Before, Fault::After] {
            let (harness, mut driver, probe) = open().await;
            let record = queued(&harness, &mut driver, SubmissionType::Input).await;
            let submission = finish(&mut driver, harness.submission(record.id))
                .await
                .unwrap()
                .unwrap();
            let mut wait = submission.wait();
            tick(&mut driver).await;
            let seq = finish(&mut driver, harness.inspect())
                .await
                .unwrap()
                .last_commit_seq;
            probe.fault.set(fault);
            assert!(finish(&mut driver, submission.abort()).await.is_err());
            if matches!(fault, Fault::Rejected) {
                assert!(poll_once(&mut wait).await.is_none());
                assert_eq!(
                    finish(&mut driver, submission.read()).await.unwrap(),
                    record
                );
                assert_eq!(
                    finish(&mut driver, harness.inspect())
                        .await
                        .unwrap()
                        .last_commit_seq,
                    seq
                );
                assert_eq!(
                    finish(&mut driver, submission.abort()).await.unwrap(),
                    WithdrawalResult::Aborted
                );
                assert_eq!(
                    finish(&mut driver, wait).await.unwrap().status(),
                    SubmissionStatus::Unanswered
                );
                shutdown(&harness, &mut driver).await;
            } else {
                assert_eq!(finish(&mut driver, wait).await, Err(HarnessError::Closed));
                assert!(matches!(
                    finish(&mut driver, harness.close()).await,
                    Err(HarnessError::Session(SessionError::Poisoned))
                ));
            }
        }
    });
}

#[test]
fn rejected_callback_never_publishes_staged_settlement() {
    block_on(async {
        let (harness, mut driver, probe) = open().await;
        let record = queued(&harness, &mut driver, SubmissionType::Input).await;
        let submission = finish(&mut driver, harness.submission(record.id))
            .await
            .unwrap()
            .unwrap();
        let mut wait = submission.wait();
        tick(&mut driver).await;
        let commits = probe.commits.get();
        let result = finish(
            &mut driver,
            harness.commit(move |tx| {
                Box::pin(async move {
                    tx.settle_submission(record.id, unanswered()).await?;
                    Err::<(), _>(SessionError::Invalid("reject callback".into()))
                })
            }),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(probe.commits.get(), commits);
        assert!(poll_once(&mut wait).await.is_none());
        assert_eq!(
            finish(&mut driver, submission.read()).await.unwrap(),
            record
        );
        shutdown(&harness, &mut driver).await;
        assert_eq!(wait.await, Err(HarnessError::Closed));
    });
}

#[test]
fn drop_during_read_does_not_register_late_or_cancel_survivors() {
    block_on(async {
        let (harness, mut driver, probe) = open().await;
        let record = queued(&harness, &mut driver, SubmissionType::Write).await;
        let submission = finish(&mut driver, harness.submission(record.id))
            .await
            .unwrap()
            .unwrap();
        let gate = Gate::default();
        *probe.read_gate.borrow_mut() = Some(gate.clone());
        let abandoned = submission.wait();
        tick(&mut driver).await;
        drop(abandoned);
        let survivor = submission.wait();
        gate.release();
        assert_eq!(
            finish(&mut driver, submission.abort()).await.unwrap(),
            WithdrawalResult::Aborted
        );
        assert_eq!(
            finish(&mut driver, survivor).await.unwrap().status(),
            SubmissionStatus::Unanswered
        );
        shutdown(&harness, &mut driver).await;
    });
}

// Wake may synchronously reenter the API on this local executor. Using a
// thread-local callback keeps the Waker itself Send + Sync without unsafe code.
thread_local! {
    static ON_WAKE: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
}
struct Reenter;
impl std::task::Wake for Reenter {
    fn wake(self: std::sync::Arc<Self>) {
        let callback = ON_WAKE.with(|slot| slot.borrow_mut().take());
        if let Some(callback) = callback {
            callback();
        }
    }
}

#[test]
fn terminal_close_and_driver_drop_wake_outside_control_borrows() {
    block_on(async {
        for mode in 0..3 {
            let (harness, mut driver, _) = open().await;
            let record = queued(&harness, &mut driver, SubmissionType::Input).await;
            let submission = finish(&mut driver, harness.submission(record.id))
                .await
                .unwrap()
                .unwrap();
            let mut wait = submission.wait();
            tick(&mut driver).await;
            let waker = Waker::from(std::sync::Arc::new(Reenter));
            assert!(
                std::pin::Pin::new(&mut wait)
                    .poll(&mut std::task::Context::from_waker(&waker))
                    .is_pending()
            );
            let reentrant = harness.clone();
            let woke = Rc::new(Cell::new(false));
            let observed = woke.clone();
            ON_WAKE.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move || {
                    let result = reentrant.resume();
                    match mode {
                        0 => assert_eq!(result, Ok(())),
                        1 => assert_eq!(result, Err(HarnessError::Closed)),
                        _ => assert_eq!(result, Err(HarnessError::DriverStopped)),
                    }
                    // Also exercises Session admission (or rejection) during waking.
                    drop(reentrant.submission(record.id));
                    observed.set(true);
                }))
            });
            match mode {
                0 => {
                    finish(&mut driver, submission.abort()).await.unwrap();
                    assert!(wait.await.is_ok());
                    shutdown(&harness, &mut driver).await;
                }
                1 => {
                    shutdown(&harness, &mut driver).await;
                    assert_eq!(wait.await, Err(HarnessError::Closed));
                }
                _ => {
                    drop(driver);
                    assert_eq!(wait.await, Err(HarnessError::DriverStopped));
                }
            }
            assert!(woke.get());
        }
    });
}

#[test]
fn dropping_abort_observation_does_not_cancel_withdrawal() {
    block_on(async {
        let (harness, mut driver, _) = open().await;
        let record = queued(&harness, &mut driver, SubmissionType::Write).await;
        let submission = finish(&mut driver, harness.submission(record.id))
            .await
            .unwrap()
            .unwrap();
        let wait = submission.wait();
        drop(submission.abort());
        assert!(matches!(
            finish(&mut driver, wait).await.unwrap().state,
            SubmissionState::WriteUnanswered { reason, .. } if reason == "aborted"
        ));
        shutdown(&harness, &mut driver).await;
    });
}

#[test]
fn close_rejects_observers_while_draining_an_admitted_settlement() {
    block_on(async {
        let (harness, mut driver, probe) = open().await;
        let record = queued(&harness, &mut driver, SubmissionType::Input).await;
        let submission = finish(&mut driver, harness.submission(record.id))
            .await
            .unwrap()
            .unwrap();
        let mut wait = submission.wait();
        tick(&mut driver).await;
        let gate = Gate::default();
        *probe.commit_gate.borrow_mut() = Some(gate.clone());
        let abort = submission.abort();
        tick(&mut driver).await;
        assert!(poll_once(&mut wait).await.is_none());
        let close = harness.close();
        assert_eq!(wait.await, Err(HarnessError::Closed));
        gate.release();
        assert_eq!(
            finish(&mut driver, abort).await.unwrap(),
            WithdrawalResult::Aborted
        );
        finish(&mut driver, close).await.unwrap();
    });
}
