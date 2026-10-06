use futures_lite::future::{block_on, poll_once};
use futures_util::FutureExt;
use publicworks_runtime::*;
use std::{cell::RefCell, panic::AssertUnwindSafe, rc::Rc, task::Waker};

// Poll the host driver until it stops waking itself. External gates remain
// pending; no sleeps or test-only runtime hooks are needed.
async fn tick(driver: &mut SessionDriver) -> Option<()> {
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

#[derive(Clone, Default)]
struct Gate(Rc<RefCell<(bool, Option<Waker>)>>);
impl Gate {
    async fn wait(&self) {
        std::future::poll_fn(|cx| {
            let mut state = self.0.borrow_mut();
            if state.0 {
                std::task::Poll::Ready(())
            } else {
                state.1 = Some(cx.waker().clone());
                std::task::Poll::Pending
            }
        })
        .await
    }
    fn release(&self) {
        let waker = {
            let mut state = self.0.borrow_mut();
            state.0 = true;
            state.1.take()
        };
        if let Some(waker) = waker {
            waker.wake();
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
    ConstructPanic,
    PollPanic,
}
#[derive(Default)]
struct Probe {
    events: RefCell<Vec<&'static str>>,
    commit_gate: Option<Gate>,
    mint_gate: Option<Gate>,
    read_gate: Option<Gate>,
    fault: RefCell<Fault>,
    close_error: bool,
}
struct Store {
    memory: MemoryStorage,
    probe: Rc<Probe>,
}
fn store(probe: Rc<Probe>) -> Store {
    Store {
        memory: MemoryStorage::new(),
        probe,
    }
}
impl Storage for Store {
    fn commit(&mut self, writes: Vec<StorageWrite>) -> StorageFuture<'_, Seq> {
        let fault = *self.probe.fault.borrow();
        if matches!(fault, Fault::ConstructPanic) {
            panic!("commit construction");
        }
        Box::pin(async move {
            self.probe.events.borrow_mut().push("commit entered");
            if let Some(gate) = &self.probe.commit_gate {
                gate.wait().await;
            }
            match fault {
                Fault::Rejected => return Err(StorageError::Rejected("no effect".into())),
                Fault::Before => return Err(StorageError::Other("before persistence".into())),
                Fault::PollPanic => panic!("commit poll"),
                _ => {}
            }
            let seq = self.memory.commit(writes).await?;
            self.probe.events.borrow_mut().push("persisted");
            if matches!(fault, Fault::After) {
                return Err(StorageError::Other("after persistence".into()));
            }
            Ok(seq)
        })
    }
    fn mint_id(&mut self) -> StorageFuture<'_, Id> {
        Box::pin(async move {
            self.probe.events.borrow_mut().push("mint entered");
            if let Some(gate) = &self.probe.mint_gate {
                gate.wait().await;
            }
            let id = self.memory.mint_id().await?;
            self.probe.events.borrow_mut().push("mint settled");
            Ok(id)
        })
    }
    fn conversation(&mut self, id: Id) -> StorageFuture<'_, Option<ConversationRecord>> {
        Box::pin(async move {
            self.probe.events.borrow_mut().push("read entered");
            if let Some(gate) = &self.probe.read_gate {
                gate.wait().await;
            }
            let result = self.memory.conversation(id).await;
            self.probe.events.borrow_mut().push("read settled");
            result
        })
    }
    fn scan_conversations(
        &mut self,
        query: ConversationQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<ConversationRecord>> {
        self.memory.scan_conversations(query, limit, cursor)
    }
    fn entry(&mut self, id: Id) -> StorageFuture<'_, Option<StoredEntry>> {
        self.memory.entry(id)
    }
    fn visible_entry(
        &mut self,
        conversation: Id,
        id: Id,
    ) -> StorageFuture<'_, Option<StoredEntry>> {
        self.memory.visible_entry(conversation, id)
    }
    fn scan_entries(
        &mut self,
        query: EntryQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<EntryRecord>> {
        self.memory.scan_entries(query, limit, cursor)
    }
    fn find_latest_head_marker(
        &mut self,
        conversation: Id,
        at: Option<Id>,
    ) -> StorageFuture<'_, Option<EntryRecord>> {
        self.memory.find_latest_head_marker(conversation, at)
    }
    fn close(&mut self) -> StorageFuture<'_, ()> {
        Box::pin(async move {
            self.probe.events.borrow_mut().push("close");
            self.memory.close().await?;
            if self.probe.close_error {
                Err(StorageError::Other("close failed".into()))
            } else {
                Ok(())
            }
        })
    }
}
fn create(session: &Session) -> CommitWaiter<ConversationRecord> {
    session.commit(|tx| Box::pin(async move { tx.create_conversation().await }))
}

#[test]
fn dropped_unpolled_queued_and_in_storage_waiters_do_not_cancel_fifo_work() {
    block_on(async {
        let gate = Gate::default();
        let probe = Rc::new(Probe {
            commit_gate: Some(gate.clone()),
            ..Probe::default()
        });
        let (session, mut driver) = Session::new(store(probe.clone()));
        drop(create(&session)); // admitted before its waiter is ever polled
        let queued = create(&session);
        let mut surviving = create(&session);
        assert!(tick(&mut driver).await.is_none());
        assert_eq!(
            &*probe.events.borrow(),
            &["mint entered", "mint settled", "commit entered"]
        );
        assert!(poll_once(&mut surviving).await.is_none()); // no premature receipt
        drop(queued);
        // The first waiter was dropped before its job entered storage. Drop a
        // fourth waiter while its actual commit is pending in a separate run below.
        gate.release();
        assert!(tick(&mut driver).await.is_none());
        assert_eq!(surviving.await.unwrap().seq.unwrap().get(), 3);
        let read = session.commit(|tx| {
            Box::pin(async move {
                tx.scan_conversations(ConversationQuery::default(), 10, None)
                    .await
            })
        });
        assert!(tick(&mut driver).await.is_none());
        assert_eq!(
            read.await
                .unwrap()
                .value
                .items
                .iter()
                .map(|c| c.id.get())
                .collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        let closed = session.close();
        driver.await;
        closed.await.unwrap();
        assert_eq!(probe.events.borrow().last(), Some(&"close"));
    });
    block_on(async {
        let gate = Gate::default();
        let probe = Rc::new(Probe {
            commit_gate: Some(gate.clone()),
            ..Probe::default()
        });
        let (session, mut driver) = Session::new(store(probe.clone()));
        let active = create(&session);
        assert!(tick(&mut driver).await.is_none());
        drop(active);
        let next = create(&session);
        gate.release();
        assert!(tick(&mut driver).await.is_none());
        assert_eq!(next.await.unwrap().seq.unwrap().get(), 2);
        drop(session);
        driver.await;
    });
}

#[test]
fn close_seals_now_drains_admitted_work_and_shares_one_result() {
    for close_error in [false, true] {
        block_on(async {
            let probe = Rc::new(Probe {
                close_error,
                ..Probe::default()
            });
            let (session, driver) = Session::new(store(probe.clone()));
            let admitted = create(&session);
            drop(session.close());
            assert_eq!(create(&session).await.unwrap_err(), SessionError::Closed);
            let close = session.close();
            driver.await;
            assert_eq!(admitted.await.unwrap().seq.unwrap().get(), 1);
            let expected = if close_error {
                Err(SessionError::Storage(StorageError::Other(
                    "close failed".into(),
                )))
            } else {
                Ok(())
            };
            assert_eq!(close.await, expected);
            assert_eq!(session.close().await, expected); // completed driver was dropped
            assert_eq!(
                probe
                    .events
                    .borrow()
                    .iter()
                    .filter(|e| **e == "close")
                    .count(),
                1
            );
        });
    }
}

#[test]
fn last_handle_drop_drains_even_with_queued_work() {
    block_on(async {
        let probe = Rc::new(Probe::default());
        let (session, driver) = Session::new(store(probe.clone()));
        let other = session.clone();
        let admitted = create(&session);
        drop(session);
        drop(other);
        driver.await;
        assert_eq!(admitted.await.unwrap().seq.unwrap().get(), 1);
        assert_eq!(probe.events.borrow().last(), Some(&"close"));
    });
}

#[test]
fn driver_drop_wakes_active_queued_and_close_waiters() {
    for stage in [0, 1, 2] {
        block_on(async {
            let gate = Gate::default();
            let probe = Rc::new(Probe {
                commit_gate: Some(gate.clone()),
                ..Probe::default()
            });
            let (session, mut driver) = Session::new(store(probe));
            let active = if stage == 1 {
                session.commit(move |tx| {
                    Box::pin(async move {
                        gate.wait().await;
                        tx.create_conversation().await
                    })
                })
            } else {
                create(&session)
            };
            let queued = create(&session);
            let close = session.close();
            if stage != 0 {
                assert!(tick(&mut driver).await.is_none());
            }
            drop(driver);
            assert_eq!(active.await.unwrap_err(), SessionError::DriverStopped);
            assert_eq!(queued.await.unwrap_err(), SessionError::DriverStopped);
            assert_eq!(close.await.unwrap_err(), SessionError::DriverStopped);
            assert_eq!(
                create(&session).await.unwrap_err(),
                SessionError::DriverStopped
            );
        });
    }
}

#[test]
fn only_explicit_commit_rejection_allows_continuation() {
    for fault in [Fault::Rejected, Fault::Before, Fault::After] {
        block_on(async {
            let probe = Rc::new(Probe {
                fault: RefCell::new(fault),
                ..Probe::default()
            });
            let (session, mut driver) = Session::new(store(probe.clone()));
            let failed = create(&session);
            let queued = create(&session);
            assert!(tick(&mut driver).await.is_none());
            assert!(matches!(failed.await, Err(SessionError::Storage(_))));
            if matches!(fault, Fault::Rejected) {
                assert!(matches!(
                    queued.await,
                    Err(SessionError::Storage(StorageError::Rejected(_)))
                ));
                *probe.fault.borrow_mut() = Fault::None;
                let next = create(&session);
                assert!(tick(&mut driver).await.is_none());
                assert_eq!(next.await.unwrap().seq.unwrap().get(), 1);
            } else {
                assert_eq!(queued.await.unwrap_err(), SessionError::Poisoned);
                assert_eq!(create(&session).await.unwrap_err(), SessionError::Poisoned);
                assert_eq!(
                    probe
                        .events
                        .borrow()
                        .iter()
                        .filter(|e| **e == "persisted")
                        .count(),
                    usize::from(matches!(fault, Fault::After))
                );
            }
            let close = session.close();
            driver.await;
            close.await.unwrap();
        });
    }
}

#[test]
fn callback_panics_unwind_waiter_after_cleanup_without_poisoning() {
    for construction in [false, true] {
        block_on(async {
            let (session, mut driver) = Session::new(MemoryStorage::new());
            let panic = session.commit(move |tx| -> TxFuture<'_, ()> {
                if construction {
                    panic!("callback construction");
                }
                Box::pin(async move {
                    tx.create_conversation().await?;
                    panic!("callback poll");
                })
            });
            let next = create(&session);
            assert!(tick(&mut driver).await.is_none());
            assert!(AssertUnwindSafe(panic).catch_unwind().await.is_err());
            assert_eq!(next.await.unwrap().seq.unwrap().get(), 1);
            drop(session);
            driver.await;
        });
    }
}

#[test]
fn storage_panics_at_construction_or_poll_poison_and_preserve_unwind() {
    for fault in [Fault::ConstructPanic, Fault::PollPanic] {
        block_on(async {
            let probe = Rc::new(Probe {
                fault: RefCell::new(fault),
                ..Probe::default()
            });
            let (session, mut driver) = Session::new(store(probe));
            let panic = create(&session);
            let queued = create(&session);
            assert!(tick(&mut driver).await.is_none());
            assert!(AssertUnwindSafe(panic).catch_unwind().await.is_err());
            assert_eq!(queued.await.unwrap_err(), SessionError::Poisoned);
            assert_eq!(create(&session).await.unwrap_err(), SessionError::Poisoned);
            let close = session.close();
            driver.await;
            close.await.unwrap();
        });
    }
}

#[test]
fn abandoned_mint_or_read_is_drained_before_next_callback_and_close() {
    for mint in [false, true] {
        block_on(async {
            let adapter_gate = Gate::default();
            let callback_gate = Gate::default();
            let probe = Rc::new(Probe {
                mint_gate: mint.then(|| adapter_gate.clone()),
                read_gate: (!mint).then(|| adapter_gate.clone()),
                ..Probe::default()
            });
            let (session, mut driver) = Session::new(store(probe.clone()));
            let release = callback_gate.clone();
            let mut abandoned = session.commit(move |tx| {
                Box::pin(async move {
                    if mint {
                        drop(tx.create_conversation());
                    } else {
                        drop(tx.conversation(ROOT_CONVERSATION));
                    }
                    callback_gate.wait().await;
                    Ok(())
                })
            });
            let next_probe = probe.clone();
            let next = session.commit(move |_| {
                Box::pin(async move {
                    next_probe.events.borrow_mut().push("next callback");
                    Ok(())
                })
            });
            assert!(tick(&mut driver).await.is_none());
            assert_eq!(probe.events.borrow().len(), 1);
            release.release(); // callback returns while adapter operation is pending
            assert!(tick(&mut driver).await.is_none());
            assert!(poll_once(&mut abandoned).await.is_none());
            assert_eq!(probe.events.borrow().len(), 1); // next job cannot start
            let closed = session.close();
            adapter_gate.release();
            driver.await;
            assert_eq!(
                abandoned.await.unwrap_err(),
                SessionError::PendingOperations
            );
            assert!(next.await.unwrap().seq.is_none());
            closed.await.unwrap();
            let expected = if mint { "mint settled" } else { "read settled" };
            assert_eq!(
                &probe.events.borrow()[1..],
                &[expected, "next callback", "close"]
            );
        });
    }
}

#[test]
fn pending_is_decided_at_callback_return_not_after_same_poll_drain() {
    block_on(async {
        let (session, mut driver) = Session::new(MemoryStorage::new());
        let pending = session.commit(|tx| {
            Box::pin(async move {
                drop(tx.create_conversation());
                Ok(())
            })
        });
        assert!(tick(&mut driver).await.is_none());
        assert_eq!(pending.await.unwrap_err(), SessionError::PendingOperations);
        let next = create(&session);
        assert!(tick(&mut driver).await.is_none());
        assert_eq!(next.await.unwrap().seq.unwrap().get(), 1);
        drop(session);
        driver.await;
    });
}

#[test]
fn already_settled_abandoned_operation_does_not_invalidate_transaction() {
    block_on(async {
        let gate = Gate::default();
        let release = gate.clone();
        let (session, mut driver) = Session::new(MemoryStorage::new());
        let commit = session.commit(move |tx| {
            Box::pin(async move {
                drop(tx.create_conversation());
                gate.wait().await;
                Ok(())
            })
        });
        assert!(tick(&mut driver).await.is_none());
        release.release();
        assert!(tick(&mut driver).await.is_none());
        assert_eq!(commit.await.unwrap().seq.unwrap().get(), 1);
        drop(session);
        driver.await;
    });
}

#[test]
fn callback_error_or_panic_still_drains_abandoned_adapter_work() {
    for panic in [false, true] {
        block_on(async {
            let adapter_gate = Gate::default();
            let callback_gate = Gate::default();
            let release = callback_gate.clone();
            let probe = Rc::new(Probe {
                mint_gate: Some(adapter_gate.clone()),
                ..Probe::default()
            });
            let (session, mut driver) = Session::new(store(probe.clone()));
            let mut failed = session.commit(move |tx| -> TxFuture<'_, ()> {
                Box::pin(async move {
                    drop(tx.create_conversation());
                    callback_gate.wait().await;
                    if panic {
                        panic!("callback failed while mint pending");
                    }
                    Err(SessionError::Invalid(
                        "callback failed while mint pending".into(),
                    ))
                })
            });
            let next = create(&session);
            assert!(tick(&mut driver).await.is_none());
            release.release();
            assert!(tick(&mut driver).await.is_none());
            assert!(poll_once(&mut failed).await.is_none());
            assert_eq!(&*probe.events.borrow(), &["mint entered"]);
            adapter_gate.release();
            assert!(tick(&mut driver).await.is_none());
            let result = AssertUnwindSafe(failed).catch_unwind().await;
            if panic {
                assert!(result.is_err());
            } else {
                assert!(matches!(result.unwrap(), Err(SessionError::Invalid(_))));
            }
            assert_eq!(next.await.unwrap().seq.unwrap().get(), 1);
            assert_eq!(
                probe
                    .events
                    .borrow()
                    .iter()
                    .filter(|e| **e == "persisted")
                    .count(),
                1
            );
            drop(session);
            driver.await;
        });
    }
}

#[test]
fn settled_but_unobserved_tx_error_is_not_sticky() {
    block_on(async {
        let gate = Gate::default();
        let release = gate.clone();
        let (session, mut driver) = Session::new(MemoryStorage::new());
        let commit = session.commit(move |tx| {
            Box::pin(async move {
                let conversation = tx.create_conversation().await?;
                drop(tx.append_entry(ROOT_CONVERSATION, EntryDraft::new("missing conversation")));
                gate.wait().await;
                Ok(conversation)
            })
        });
        assert!(tick(&mut driver).await.is_none());
        release.release();
        assert!(tick(&mut driver).await.is_none());
        assert_eq!(commit.await.unwrap().seq.unwrap().get(), 1);
        drop(session);
        driver.await;
    });
}

#[test]
fn dropping_driver_actually_wakes_registered_waiters() {
    block_on(async {
        use std::{
            future::Future,
            pin::Pin,
            sync::{
                Arc,
                atomic::{AtomicUsize, Ordering},
            },
            task::{Context, Wake},
        };
        struct Counter(AtomicUsize);
        impl Wake for Counter {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let gate = Gate::default();
        let (session, mut driver) = Session::new(store(Rc::new(Probe {
            commit_gate: Some(gate),
            ..Probe::default()
        })));
        let mut active = create(&session);
        let mut queued = create(&session);
        let mut close = session.close();
        assert!(tick(&mut driver).await.is_none());
        let counter = Arc::new(Counter(AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut active).poll(&mut cx).is_pending());
        assert!(Pin::new(&mut queued).poll(&mut cx).is_pending());
        assert!(Pin::new(&mut close).poll(&mut cx).is_pending());
        drop(driver);
        assert_eq!(counter.0.load(Ordering::SeqCst), 3);
        assert_eq!(active.await.unwrap_err(), SessionError::DriverStopped);
        assert_eq!(queued.await.unwrap_err(), SessionError::DriverStopped);
        assert_eq!(close.await.unwrap_err(), SessionError::DriverStopped);
    });
}
