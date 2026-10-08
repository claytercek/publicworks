use super::*;
use futures_lite::future::{block_on, zip};
use serde_json::json;

fn id(value: u64) -> Id {
    Id::new(value).unwrap()
}

fn task(value: u64, owner: Option<u64>) -> TaskRecord {
    TaskRecord {
        id: id(value),
        conversation_id: id(1),
        kind: "unregistered".into(),
        version: 1,
        input: Value::Null,
        owner: owner.map(id),
        background: false,
        abort_requested: false,
        state: TaskState::Pending {
            checkpoint: json!({"phase":"run"}),
        },
        memos: None,
    }
}

fn terminal() -> TaskState {
    TaskState::Terminal {
        outcome: TaskOutcome::Completed {
            result: Value::Null,
        },
    }
}

async fn storage(terminal_task: bool) -> MemoryStorage {
    let mut storage = MemoryStorage::new();
    let mut record = task(10, None);
    if terminal_task {
        record.state = terminal();
    }
    storage
        .commit(vec![
            StorageWrite::Conversation(ConversationRecord {
                id: id(1),
                owner: None,
                parent: None,
            }),
            StorageWrite::Task(record),
            StorageWrite::Submission(SubmissionRecord {
                id: id(11),
                conversation_id: id(1),
                request_id: None,
                state: SubmissionState::InputUnanswered {
                    entry: None,
                    reason: "test".into(),
                    detail: None,
                },
            }),
        ])
        .await
        .unwrap();
    storage
}

#[test]
fn task_and_idle_observers_cancel_queued_registration_and_prune_only_their_slot() {
    block_on(async {
        let (opening, driver) = Harness::open(storage(false).await, TaskRegistry::default());
        zip(
            async move {
                let harness = opening.await.unwrap();
                let (entered, started) = oneshot::channel();
                let (release, blocked) = oneshot::channel();
                let barrier = harness.commit(move |_| {
                    Box::pin(async move {
                        entered.send(()).unwrap();
                        blocked.await.unwrap();
                        Ok(())
                    })
                });
                started.await.unwrap();
                // Session owns both callbacks even though their observers are
                // dropped while the earlier callback is still suspended.
                drop(harness.wait_task(id(10)));
                drop(harness.wait_for_idle());
                let first_task = harness.wait_task(id(10));
                let second_task = harness.wait_task(id(10));
                let first_idle = harness.wait_for_idle();
                let second_idle = harness.wait_for_idle();
                release.send(()).unwrap();
                barrier.await.unwrap();
                harness
                    .commit(|_| Box::pin(async { Ok(()) }))
                    .await
                    .unwrap();
                {
                    let state = harness.control.0.borrow();
                    assert_eq!(state.task_waiters[&id(10)].len(), 2);
                    assert_eq!(state.idle_waiters[&None].len(), 2);
                }
                drop(first_task);
                drop(first_idle);
                {
                    let state = harness.control.0.borrow();
                    assert_eq!(state.task_waiters[&id(10)].len(), 1);
                    assert_eq!(state.idle_waiters[&None].len(), 1);
                }
                drop(second_task);
                drop(second_idle);
                {
                    let state = harness.control.0.borrow();
                    assert!(state.task_waiters.is_empty());
                    assert!(state.idle_waiters.is_empty());
                }
                let task = harness
                    .commit(|tx| Box::pin(async { tx.task(id(10)).await }))
                    .await
                    .unwrap()
                    .value
                    .unwrap();
                assert_eq!(task.status(), TaskStatus::Pending);
                assert!(!task.abort_requested);
                harness.close().await.unwrap();
            },
            driver,
        )
        .await;
    });
}

#[test]
fn queued_ready_observers_keep_their_existing_close_precedence() {
    block_on(async {
        let (opening, driver) = Harness::open(storage(true).await, TaskRegistry::default());
        zip(
            async move {
                let harness = opening.await.unwrap();
                let receipt = harness.submission(id(11)).await.unwrap().unwrap();
                let (entered, started) = oneshot::channel();
                let (release, blocked) = oneshot::channel();
                let barrier = harness.commit(move |_| {
                    Box::pin(async move {
                        entered.send(()).unwrap();
                        blocked.await.unwrap();
                        Ok(())
                    })
                });
                started.await.unwrap();
                let task = harness.wait_task(id(10));
                // Task observation alone does not enable scheduler progress.
                assert!(!harness.control.0.borrow().enabled);
                let idle = harness.wait_for_idle();
                assert!(harness.control.0.borrow().enabled);
                let submission = receipt.wait();
                let close = harness.close();
                release.send(()).unwrap();
                barrier.await.unwrap();
                assert_eq!(task.await.unwrap().status(), TaskStatus::Terminal);
                idle.await.unwrap();
                assert_eq!(submission.await, Err(HarnessError::Closed));
                close.await.unwrap();
                let state = harness.control.0.borrow();
                assert!(state.task_waiters.is_empty());
                assert!(state.idle_waiters.is_empty());
                assert!(state.submission_waiters.is_empty());
            },
            driver,
        )
        .await;
    });
}

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
fn registration_drop_releases_control_before_destroying_any_sender() {
    fn exercise<K: Ord + Clone, T>(
        control: &Rc<HarnessControl>,
        scope: K,
        table: fn(&mut HarnessState) -> &mut observers::Waiters<K, T>,
    ) {
        let registration = Registration::new(control, scope.clone(), table);
        let cancelled = registration.cancelled.clone();
        let (sender, mut receiver) = oneshot::channel();
        table(&mut control.0.borrow_mut())
            .entry(scope)
            .or_default()
            .insert(registration.key, sender);
        let waker = Waker::from(std::sync::Arc::new(Reenter));
        assert!(
            Pin::new(&mut receiver)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let owner = control.clone();
        let woke = Rc::new(Cell::new(false));
        let observed = woke.clone();
        ON_WAKE.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                // A mutable borrow makes an accidental wake inside the guard's
                // table-removal borrow fail immediately rather than hang.
                owner.0.borrow_mut().dirty = true;
                observed.set(true);
            }))
        });
        drop(registration);
        assert!(cancelled.get());
        assert!(woke.get());
        assert!(table(&mut control.0.borrow_mut()).is_empty());
        assert!(matches!(
            Pin::new(&mut receiver).poll(&mut Context::from_waker(&waker)),
            Poll::Ready(Err(_))
        ));
    }
    block_on(async {
        let (opening, driver) = Harness::open(storage(false).await, TaskRegistry::default());
        zip(
            async move {
                let harness = opening.await.unwrap();
                exercise(&harness.control, id(10), |state| &mut state.task_waiters);
                exercise(&harness.control, None, |state| &mut state.idle_waiters);
                exercise(&harness.control, id(11), |state| {
                    &mut state.submission_waiters
                });
                harness.close().await.unwrap();
            },
            driver,
        )
        .await;
    });
}

#[test]
fn actual_waiters_drop_registration_before_their_polled_receivers() {
    fn exercise<F: Future + Unpin, K: Ord + Clone + 'static, T: 'static>(
        mut waiter: F,
        control: &Rc<HarnessControl>,
        scope: K,
        table: fn(&mut HarnessState) -> &mut observers::Waiters<K, T>,
    ) {
        assert_eq!(table(&mut control.0.borrow_mut())[&scope].len(), 1);
        let waker = Waker::from(std::sync::Arc::new(Reenter));
        // Admission has settled, so this polls the actual observer receiver,
        // not the Session commit receipt that precedes it.
        assert!(
            Pin::new(&mut waiter)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let owner = control.clone();
        let woke = Rc::new(Cell::new(false));
        let observed = woke.clone();
        let removed_scope = scope.clone();
        ON_WAKE.with(|slot| {
            assert!(slot.borrow().is_none());
            *slot.borrow_mut() = Some(Box::new(move || {
                // Sender destruction must wake us after keyed removal and
                // borrow release, while the receiver still holds this waker.
                let mut state = owner.0.borrow_mut();
                assert!(!table(&mut state).contains_key(&removed_scope));
                observed.set(true);
            }));
        });
        drop(waiter);
        assert!(
            woke.get(),
            "dropping the future first destroys its receiver without this wake"
        );
        assert!(!table(&mut control.0.borrow_mut()).contains_key(&scope));
        ON_WAKE.with(|slot| assert!(slot.borrow().is_none()));
    }

    block_on(crate::test_support::bounded(async {
        let (opening, driver) = Harness::open(storage(false).await, TaskRegistry::default());
        zip(
            async move {
                let harness = opening.await.unwrap();
                let submission_id = harness
                    .commit(|tx| {
                        Box::pin(async {
                            Ok(tx
                                .create_submission(id(1), SubmissionType::Input, None)
                                .await?
                                .id)
                        })
                    })
                    .await
                    .unwrap()
                    .value;
                let submission = harness.submission(submission_id).await.unwrap().unwrap();
                let task = harness.wait_task(id(10));
                let idle = harness.wait_for_idle();
                let receipt = submission.wait();
                harness
                    .commit(|_| Box::pin(async { Ok(()) }))
                    .await
                    .unwrap();

                exercise(task, &harness.control, id(10), |state| {
                    &mut state.task_waiters
                });
                exercise(idle, &harness.control, None, |state| {
                    &mut state.idle_waiters
                });
                exercise(receipt, &harness.control, submission_id, |state| {
                    &mut state.submission_waiters
                });
                assert_eq!(
                    submission.status().await.unwrap().status(),
                    SubmissionStatus::Queued
                );
                harness.close().await.unwrap();
            },
            driver,
        )
        .await;
    }));
}

#[test]
fn reservation_roots_group_live_members_before_scope_expansion() {
    let mut tree = tree::Tree {
        tasks: BTreeMap::new(),
        conversations: BTreeMap::from([
            (
                id(1),
                ConversationRecord {
                    id: id(1),
                    parent: None,
                    owner: None,
                },
            ),
            (
                id(2),
                ConversationRecord {
                    id: id(2),
                    parent: None,
                    owner: Some(OwnerLink {
                        conversation_id: id(1),
                        task_id: id(10),
                    }),
                },
            ),
        ]),
    };
    for record in [task(10, None), task(20, None)] {
        tree.tasks.insert(record.id, record);
    }
    for value in 100..250 {
        tree.tasks.insert(id(value), task(value, Some(10)));
    }
    let mut owned = task(30, None);
    owned.conversation_id = id(2);
    tree.tasks.insert(owned.id, owned);
    let mut background = task(40, None);
    background.conversation_id = id(2);
    background.background = true;
    tree.tasks.insert(background.id, background);
    let mut terminal_child = task(50, Some(10));
    terminal_child.state = terminal();
    tree.tasks.insert(terminal_child.id, terminal_child);
    let mut terminal_root = task(60, None);
    terminal_root.state = terminal();
    tree.tasks.insert(terminal_root.id, terminal_root);
    let cyclic = task(70, Some(70));
    tree.tasks.insert(cyclic.id, cyclic);

    let roots = reservation_roots(&tree);
    assert_eq!(roots, BTreeSet::from([id(10), id(20), id(40)]));
    let scopes = roots
        .into_iter()
        .map(|root| (root, tree.scope(root).unwrap()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(scopes[&id(10)].len(), 153);
    assert!(scopes[&id(10)].contains(&id(30)));
    assert!(
        scopes[&id(10)].contains(&id(50)),
        "terminal descendants remain in a live root's scope"
    );
    assert_eq!(scopes[&id(20)], BTreeSet::from([id(20)]));
    assert_eq!(scopes[&id(40)], BTreeSet::from([id(40)]));
}
