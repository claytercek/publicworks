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
    pin::{Pin, pin},
    rc::Rc,
    task::{Context, Wake, Waker},
};

// Poll the host-owned driver until it stops waking itself. This keeps lifecycle
// assertions executor-neutral and lets tests distinguish quiescence from close.
async fn tick(driver: &mut HarnessDriver) -> Option<()> {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
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
        if Pin::new(&mut *driver).poll(&mut cx).is_ready() {
            return Some(());
        }
        if !signal.0.load(Ordering::SeqCst) {
            return None;
        }
    }
}

async fn finish<F: Future>(driver: &mut HarnessDriver, future: F) -> F::Output {
    let mut future = pin!(future);
    for _ in 0..100 {
        if let Some(result) = poll_once(&mut future).await {
            return result;
        }
        tick(driver).await;
    }
    panic!("observation did not settle");
}

async fn finish_driver(driver: &mut HarnessDriver) {
    for _ in 0..100 {
        if tick(driver).await.is_some() {
            return;
        }
    }
    panic!("driver did not settle");
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CommitFault {
    Rejected,
    UncertainBefore,
    UncertainAfter,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FaultPoint {
    Pending,
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
                (Self::Pending, TaskStatus::Pending)
                    | (Self::Running, TaskStatus::Running)
                    | (Self::Terminal, TaskStatus::Terminal)
            )
        })
    }
}

#[derive(Default)]
struct Probe {
    fault: RefCell<Option<(FaultPoint, CommitFault)>>,
    fault_hits: Cell<usize>,
    fault_persisted: Cell<bool>,
    closes: Cell<usize>,
}

struct ProbeStorage {
    inner: MemoryStorage,
    probe: Rc<Probe>,
}

impl ProbeStorage {
    fn new(inner: MemoryStorage, probe: Rc<Probe>) -> Self {
        Self { inner, probe }
    }
}

impl Storage for ProbeStorage {
    forward_storage_methods!(inner;
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
    );
    fn commit(&mut self, writes: Vec<StorageWrite>) -> StorageFuture<'_, Seq> {
        Box::pin(async move {
            let fault = {
                let mut configured = self.probe.fault.borrow_mut();
                if configured.is_some_and(|(point, _)| point.matches(&writes)) {
                    configured.take().map(|(_, fault)| fault)
                } else {
                    None
                }
            };
            if let Some(fault) = fault {
                self.probe.fault_hits.set(self.probe.fault_hits.get() + 1);
                match fault {
                    CommitFault::Rejected => {
                        return Err(StorageError::Rejected("release-gate rejection".into()));
                    }
                    CommitFault::UncertainBefore => {
                        return Err(StorageError::Other(
                            "release-gate uncertainty before persistence".into(),
                        ));
                    }
                    CommitFault::UncertainAfter => {}
                }
            }
            let seq = self.inner.commit(writes).await?;
            if matches!(fault, Some(CommitFault::UncertainAfter)) {
                self.probe.fault_persisted.set(true);
                return Err(StorageError::Other(
                    "release-gate uncertainty after persistence".into(),
                ));
            }
            Ok(seq)
        })
    }

    fn close(&mut self) -> StorageFuture<'_, ()> {
        Box::pin(async move {
            self.probe.closes.set(self.probe.closes.get() + 1);
            self.inner.close().await
        })
    }
}

fn task_record(id: u64, state: TaskState) -> TaskRecord {
    TaskRecord {
        id: Id::new(id).unwrap(),
        conversation_id: Id::new(2).unwrap(),
        kind: "release-gate".into(),
        version: 1,
        input: json!(id),
        owner: None,
        background: false,
        abort_requested: false,
        state,
        memos: None,
    }
}

fn completing_definition(calls: Rc<Cell<usize>>) -> TaskDefinition {
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
        "release-gate",
        1,
        |_| Ok(json!({"phase":"run"})),
        BTreeMap::from([("run".into(), handler)]),
    )
}

async fn create_task(
    harness: &Harness,
    driver: &mut HarnessDriver,
    definition: TaskDefinition,
) -> TaskRecord {
    finish(
        driver,
        harness.commit(move |tx| {
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
        }),
    )
    .await
    .unwrap()
    .value
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
fn open_is_paused_and_reconciliation_retries_only_after_a_new_wakeup() {
    block_on(async {
        let mut memory = MemoryStorage::new();
        let completing = task_record(
            3,
            TaskState::Completing {
                outcome: TaskOutcome::Completed { result: json!(3) },
            },
        );
        memory
            .commit(vec![
                StorageWrite::Conversation(ConversationRecord {
                    id: Id::new(2).unwrap(),
                    parent: None,
                    owner: None,
                }),
                StorageWrite::Task(completing.clone()),
            ])
            .await
            .unwrap();

        let probe = Rc::new(Probe::default());
        *probe.fault.borrow_mut() = Some((FaultPoint::Terminal, CommitFault::Rejected));
        let (opening, mut driver) = Harness::open(
            ProbeStorage::new(memory, probe.clone()),
            TaskRegistry::default(),
        );
        let harness = finish(&mut driver, opening).await.unwrap();

        let inspection = finish(&mut driver, harness.inspect()).await.unwrap();
        assert!(!inspection.progress_enabled);
        assert!(matches!(
            inspection.tasks.as_slice(),
            [TaskInspection {
                task: TaskRecord {
                    state: TaskState::Completing { .. },
                    ..
                },
                ..
            }]
        ));
        assert_eq!(probe.fault_hits.get(), 1);

        let waiter = harness.wait_task(completing.id);
        harness.resume().unwrap();
        let terminal = finish(&mut driver, waiter).await.unwrap();
        assert_eq!(terminal.status(), TaskStatus::Terminal);
        assert_eq!(probe.fault_hits.get(), 1);
        finish(&mut driver, harness.close()).await.unwrap();
        assert_eq!(probe.closes.get(), 1);
    });
}

#[test]
fn failed_open_closes_storage_and_withholds_the_harness() {
    block_on(async {
        let mut memory = MemoryStorage::new();
        memory
            .commit(vec![
                StorageWrite::Conversation(ConversationRecord {
                    id: Id::new(2).unwrap(),
                    parent: None,
                    owner: None,
                }),
                StorageWrite::Task(task_record(
                    3,
                    TaskState::Running {
                        checkpoint: json!({"phase":"run"}),
                    },
                )),
            ])
            .await
            .unwrap();
        let probe = Rc::new(Probe::default());
        *probe.fault.borrow_mut() = Some((FaultPoint::Pending, CommitFault::Rejected));
        let (opening, mut driver) = Harness::open(
            ProbeStorage::new(memory, probe.clone()),
            TaskRegistry::default(),
        );
        assert!(matches!(
            finish(&mut driver, opening).await,
            Err(HarnessError::Session(SessionError::Storage(
                StorageError::Rejected(_)
            )))
        ));
        // The open waiter observes failure only after the private close has settled.
        assert_eq!(probe.closes.get(), 1);
    });
}

#[test]
fn reservation_rejection_is_retryable_but_uncertainty_never_dispatches() {
    block_on(async {
        for fault in [
            CommitFault::Rejected,
            CommitFault::UncertainBefore,
            CommitFault::UncertainAfter,
        ] {
            let probe = Rc::new(Probe::default());
            let calls = Rc::new(Cell::new(0));
            let definition = completing_definition(calls.clone());
            let (opening, mut driver) = Harness::open(
                ProbeStorage::new(MemoryStorage::new(), probe.clone()),
                TaskRegistry::new([definition.clone()]).unwrap(),
            );
            let harness = finish(&mut driver, opening).await.unwrap();
            let task = create_task(&harness, &mut driver, definition).await;
            let waiter = harness.wait_task(task.id);
            tick(&mut driver).await;
            *probe.fault.borrow_mut() = Some((FaultPoint::Running, fault));

            harness.resume().unwrap();
            tick(&mut driver).await;
            assert_eq!(calls.get(), 0, "fault {fault:?} dispatched before ACK");
            assert_eq!(probe.fault_hits.get(), 1);

            if fault == CommitFault::Rejected {
                let inspection = finish(&mut driver, harness.inspect()).await.unwrap();
                assert!(matches!(
                    inspection.tasks[0].task.state,
                    TaskState::Pending { .. }
                ));
                assert!(!probe.fault_persisted.get());

                // A known-no-effect rejection does not spin. A later wake retries it.
                harness.resume().unwrap();
                assert_eq!(
                    finish(&mut driver, waiter).await.unwrap().status(),
                    TaskStatus::Terminal
                );
                assert_eq!(calls.get(), 1);
                finish(&mut driver, harness.close()).await.unwrap();
            } else {
                assert_eq!(finish(&mut driver, waiter).await, Err(HarnessError::Closed));
                assert_eq!(
                    probe.fault_persisted.get(),
                    fault == CommitFault::UncertainAfter
                );
                assert_uncertain_close(finish(&mut driver, harness.close()).await);
            }
            assert_eq!(probe.closes.get(), 1);
        }
    });
}

#[test]
fn close_waits_for_noncooperative_code_and_dropped_observers_cancel_nothing() {
    block_on(async {
        let entered = Gate::default();
        let release = Gate::default();
        let handler: PhaseHandler = Rc::new({
            let entered = entered.clone();
            let release = release.clone();
            move |_, _| {
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    entered.release();
                    // Deliberately ignore cancellation until the host-controlled gate.
                    release.wait().await;
                    Ok(())
                })
            }
        });
        let definition = TaskDefinition::new(
            "release-gate",
            1,
            |_| Ok(json!({"phase":"run"})),
            BTreeMap::from([("run".into(), handler)]),
        );
        let probe = Rc::new(Probe::default());
        let (opening, mut driver) = Harness::open(
            ProbeStorage::new(MemoryStorage::new(), probe.clone()),
            TaskRegistry::new([definition.clone()]).unwrap(),
        );
        let harness = finish(&mut driver, opening).await.unwrap();
        let task = create_task(&harness, &mut driver, definition).await;

        // One observer is abandoned before its state check, another after it has
        // registered. Neither owns task execution or shutdown.
        drop(harness.wait_task(task.id));
        tick(&mut driver).await;
        let registered_then_dropped = harness.wait_task(task.id);
        tick(&mut driver).await;
        drop(registered_then_dropped);
        let survivor = harness.wait_task(task.id);
        tick(&mut driver).await;

        harness.resume().unwrap();
        finish(&mut driver, entered.wait()).await;
        let abandoned_close = harness.close();
        drop(abandoned_close);
        let mut close = Box::pin(harness.close());
        tick(&mut driver).await;
        assert!(poll_once(&mut close).await.is_none());
        assert_eq!(probe.closes.get(), 0);
        assert_eq!(survivor.await, Err(HarnessError::Closed));

        release.release();
        finish(&mut driver, close).await.unwrap();
        assert_eq!(probe.closes.get(), 1);
    });
}

#[test]
fn abandoned_open_and_last_handle_drop_still_drive_orderly_close() {
    block_on(async {
        for abandon_open in [false, true] {
            let probe = Rc::new(Probe::default());
            let (opening, mut driver) = Harness::open(
                ProbeStorage::new(MemoryStorage::new(), probe.clone()),
                TaskRegistry::default(),
            );
            if abandon_open {
                drop(opening);
            } else {
                let harness = finish(&mut driver, opening).await.unwrap();
                let clone = harness.clone();
                drop(harness);
                assert_eq!(probe.closes.get(), 0);
                drop(clone);
            }
            finish_driver(&mut driver).await;
            assert_eq!(probe.closes.get(), 1);
        }
    });
}

thread_local! {
    static ON_WAKE: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
}

struct Reenter;
impl Wake for Reenter {
    fn wake(self: std::sync::Arc<Self>) {
        let callback = ON_WAKE.with(|slot| slot.borrow_mut().take());
        if let Some(callback) = callback {
            callback();
        }
    }
}

#[test]
fn terminal_and_close_wake_task_observers_outside_control_borrows() {
    block_on(async {
        for close in [false, true] {
            let probe = Rc::new(Probe::default());
            let definition = completing_definition(Rc::new(Cell::new(0)));
            let (opening, mut driver) = Harness::open(
                ProbeStorage::new(MemoryStorage::new(), probe.clone()),
                TaskRegistry::default(),
            );
            let harness = finish(&mut driver, opening).await.unwrap();
            let task = create_task(&harness, &mut driver, definition).await;
            let mut waiter = harness.wait_task(task.id);
            tick(&mut driver).await;

            let waker = Waker::from(std::sync::Arc::new(Reenter));
            assert!(
                Pin::new(&mut waiter)
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            let reentrant = harness.clone();
            let woke = Rc::new(Cell::new(false));
            let observed = woke.clone();
            ON_WAKE.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move || {
                    assert_eq!(
                        reentrant.resume(),
                        if close {
                            Err(HarnessError::Closed)
                        } else {
                            Ok(())
                        }
                    );
                    // Exercise synchronous Session admission from the wake too.
                    drop(reentrant.commit(|_| Box::pin(async { Ok(()) })));
                    observed.set(true);
                }));
            });

            if close {
                let shutdown = harness.close();
                assert!(woke.get());
                assert_eq!(waiter.await, Err(HarnessError::Closed));
                finish(&mut driver, shutdown).await.unwrap();
            } else {
                assert_eq!(
                    finish(&mut driver, harness.abort(task.id)).await.unwrap(),
                    AbortResult::Marked
                );
                tick(&mut driver).await;
                assert!(woke.get());
                assert_eq!(waiter.await.unwrap().status(), TaskStatus::Terminal);
                finish(&mut driver, harness.close()).await.unwrap();
            }
            assert_eq!(probe.closes.get(), 1);
        }
    });
}

#[test]
fn driver_drop_fences_noncooperative_code_and_wakes_observers_reentrantly() {
    block_on(async {
        let entered = Gate::default();
        let never = Gate::default();
        let escaped = Rc::new(RefCell::new(None::<TaskRuntime>));
        let handler: PhaseHandler = Rc::new({
            let entered = entered.clone();
            let never = never.clone();
            let escaped = escaped.clone();
            move |_, runtime| {
                let entered = entered.clone();
                let never = never.clone();
                *escaped.borrow_mut() = Some(runtime);
                Box::pin(async move {
                    entered.release();
                    never.wait().await;
                    Ok(())
                })
            }
        });
        let definition = TaskDefinition::new(
            "release-gate",
            1,
            |_| Ok(json!({"phase":"run"})),
            BTreeMap::from([("run".into(), handler)]),
        );
        let probe = Rc::new(Probe::default());
        let (opening, mut driver) = Harness::open(
            ProbeStorage::new(MemoryStorage::new(), probe.clone()),
            TaskRegistry::new([definition.clone()]).unwrap(),
        );
        let harness = finish(&mut driver, opening).await.unwrap();
        let task = create_task(&harness, &mut driver, definition).await;
        let mut waiter = harness.wait_task(task.id);
        tick(&mut driver).await;
        harness.resume().unwrap();
        finish(&mut driver, entered.wait()).await;

        let waker = Waker::from(std::sync::Arc::new(Reenter));
        assert!(
            Pin::new(&mut waiter)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let reentrant = harness.clone();
        let woke = Rc::new(Cell::new(false));
        let observed = woke.clone();
        ON_WAKE.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                assert_eq!(reentrant.resume(), Err(HarnessError::DriverStopped));
                drop(reentrant.commit(|_| Box::pin(async { Ok(()) })));
                observed.set(true);
            }));
        });

        drop(driver);
        assert!(woke.get());
        assert_eq!(waiter.await, Err(HarnessError::DriverStopped));
        assert_eq!(
            probe.closes.get(),
            0,
            "driver forfeiture cannot async-close"
        );
        let runtime = escaped.borrow().clone().unwrap();
        assert!(matches!(
            runtime.read(|_, _| Box::pin(async { Ok(()) })).await,
            Err(SessionError::DriverStopped)
        ));
        assert_eq!(harness.close().await, Err(HarnessError::DriverStopped));
    });
}
