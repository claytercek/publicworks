//! Abort admission shares the mutation line, never the run-request FIFO.
use super::*;

struct Acknowledged {
    result: AbortResult,
    normal: Option<Rc<Invocation>>,
    signal: Option<Rc<Invocation>>,
}
// Count settlement ownership, including commands discarded by SessionDriver drop.
struct PendingAbort(Rc<RunnerControl>);
impl Drop for PendingAbort {
    fn drop(&mut self) {
        let waker = {
            let mut state = self.0.0.borrow_mut();
            state.aborts -= 1;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}
impl TaskRunner {
    /// Persist intent and join the normal invocation observed on the Session line.
    /// This does not join cleanup. Never await this from that normal invocation.
    pub fn abort(&self, id: Id) -> AbortWaiter {
        {
            let mut state = self.control.0.borrow_mut();
            if state.closing || state.finished {
                let error = if state.dropped {
                    RunError::DriverStopped
                } else {
                    RunError::Closed
                };
                return AbortWaiter(Box::pin(async { Err(error) }));
            }
            state.aborts += 1;
        }
        let control = self.control.clone();
        let pending = PendingAbort(control.clone());
        let registry = self.registry.clone();
        let waiter = self.session.commit_options::<Acknowledged, _>(
            move |tx| {
                Box::pin(async move {
                    let record = tx
                        .task(id)
                        .await?
                        .ok_or_else(|| SessionError::Invalid("Task does not exist".into()))?;
                    if record.status() == TaskStatus::Terminal {
                        return Ok(Acknowledged {
                            result: AbortResult::Terminal,
                            normal: None,
                            signal: None,
                        });
                    }
                    let mut tree = tx.tree().await?;
                    let Some(scope) = tree.root(id).and_then(|root| tree.scope(root)) else {
                        return Ok(Acknowledged {
                            result: AbortResult::Blocked(BlockReason::UnsupportedScope),
                            normal: None,
                            signal: None,
                        });
                    };
                    let before = tree.tasks.clone();
                    let active = control
                        .0
                        .borrow()
                        .active
                        .upgrade()
                        .filter(|inv| !inv.abort_mode && !inv.ended.get());
                    let normal = active.as_ref().filter(|inv| inv.id == id).cloned();
                    if record.status() != TaskStatus::Completing
                        && !tree.owned_live(id)
                        && registry
                            .0
                            .get(&record.kind)
                            .is_none_or(|d| d.version != record.version)
                    {
                        let record = tree.tasks.get_mut(&id).unwrap();
                        record.state = TaskState::Terminal {
                            outcome: TaskOutcome::Orphaned {
                                reason: "Missing task definition or exact version for abort".into(),
                            },
                        };
                        record.memos = None;
                    } else {
                        tree.mark(id);
                    }
                    // Derive cascades and failFast on the Session line, not after
                    // a possibly cancellation-blocked child handler returns.
                    tree.reconcile(&scope);
                    let signal = active.filter(|inv| {
                        tree.tasks.get(&inv.id).is_some_and(|r| {
                            r.abort_requested || r.status() == TaskStatus::Terminal
                        })
                    });
                    tree.stage_changes(tx, &before).await?;
                    Ok(Acknowledged {
                        result: AbortResult::Marked,
                        normal,
                        signal,
                    })
                })
            },
            false,
            false,
            None,
            Some(Box::new(move |result| {
                // A failed or uncertain commit never acknowledges cancellation.
                if let Ok(Ok(receipt)) = result
                    && let Some(normal) = &receipt.value.signal
                {
                    normal.signal();
                }
                drop(pending);
            })),
        );
        AbortWaiter(Box::pin(async move {
            let receipt = AssertUnwindSafe(waiter)
                .catch_unwind()
                .await
                .map_err(|panic| RunError::Panicked(panic_message(&*panic)))??;
            if let Some(normal) = receipt.value.normal {
                normal.join().await;
            }
            Ok(receipt.value.result)
        }))
    }
}

/// Swap identities without releasing the drive-lifetime tree guard. Fence before waking
/// observers: their wakers may synchronously admit more Session work.
pub(super) fn handoff(_session: &Session, normal: &Rc<Invocation>) -> Rc<Invocation> {
    let abort = Rc::new(Invocation {
        id: normal.id,
        abort_mode: true,
        joined: RefCell::new(Vec::new()),
        ended: Cell::new(false),
        cancelled: Cell::new(false),
        waiters: RefCell::new(Vec::new()),
        runner: normal.runner.clone(),
    });
    normal.ended.set(true);
    if let Some(runner) = normal.runner.upgrade() {
        runner.0.borrow_mut().active = Rc::downgrade(&abort);
    }
    normal.end();
    abort
}
