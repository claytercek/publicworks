//! Serialized Session admission and host-owned settlement.
use crate::*;
use futures_channel::oneshot;
use futures_util::{FutureExt, future::Shared};
use std::{
    any::Any,
    cell::RefCell,
    collections::VecDeque,
    panic::{AssertUnwindSafe, resume_unwind},
    rc::Rc,
    task::{Context, Poll, Waker},
};

mod tasks;
mod transaction;
pub use tasks::{Owner, TaskInitializer, TaskOptions, TaskOwnership};
pub use transaction::{EntryDraft, Head, Tx};

/// Local, scoped callback future. The transaction cannot escape the callback.
pub type TxFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, SessionError>> + 'a>>;
type LocalFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;
type Panic = Box<dyn Any + Send>;
type Outcome<T> = Result<Result<T, SessionError>, Panic>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionError {
    Closed,
    DriverStopped,
    Poisoned,
    ReadAfterWrite,
    PendingOperations,
    TransactionSettled,
    Storage(StorageError),
    Invalid(String),
    /// Close has multiple observers, so an unwind is reported as a distinct
    /// failure rather than resuming one non-cloneable panic payload repeatedly.
    ClosePanicked,
}
impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("Session is closed"),
            Self::DriverStopped => f.write_str("Session driver stopped before settlement"),
            Self::Poisoned => f.write_str("Session is poisoned by an uncertain commit"),
            Self::ReadAfterWrite => f.write_str("Committed table read after mutation attempt"),
            Self::PendingOperations => {
                f.write_str("Session commit callback settled before its pending Tx operations")
            }
            Self::TransactionSettled => f.write_str("Transaction has settled"),
            Self::Storage(error) => error.fmt(f),
            Self::Invalid(message) => f.write_str(message),
            Self::ClosePanicked => f.write_str("Storage close panicked"),
        }
    }
}
impl std::error::Error for SessionError {}
impl From<StorageError> for SessionError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

#[derive(Debug)]
pub struct CommitReceipt<T> {
    pub value: T,
    /// Empty transactions do not call Storage or consume a sequence.
    pub seq: Option<Seq>,
}

/// Dropping this waiter never cancels admitted work. A genuine callback or
/// storage panic resumes unwinding here, after the driver has settled cleanup.
pub struct CommitWaiter<T>(LocalFuture<'static, Result<CommitReceipt<T>, SessionError>>);
impl<T> Future for CommitWaiter<T> {
    type Output = Result<CommitReceipt<T>, SessionError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}
/// Cloneable observation of the one storage-close result.
pub type CloseWaiter = Shared<LocalFuture<'static, Result<(), SessionError>>>;

type Job =
    Box<dyn for<'a> FnOnce(&'a mut dyn Storage, Rc<RefCell<Control>>) -> LocalFuture<'a, ()>>;
struct Control {
    jobs: VecDeque<Job>,
    closing: bool,
    stopped: bool,
    poisoned: bool,
    handles: usize,
    waker: Option<Waker>,
}
impl Control {
    fn admission(&self) -> Result<(), SessionError> {
        if self.stopped {
            Err(SessionError::DriverStopped)
        } else if self.closing {
            Err(SessionError::Closed)
        } else if self.poisoned {
            Err(SessionError::Poisoned)
        } else {
            Ok(())
        }
    }
}

/// Transaction authority. Keep the driver polled through shutdown; callbacks
/// must use `Tx` rather than awaiting `Session` methods.
pub struct Session {
    control: Rc<RefCell<Control>>,
    close: CloseWaiter,
}
impl Clone for Session {
    fn clone(&self) -> Self {
        self.control.borrow_mut().handles += 1;
        Self {
            control: self.control.clone(),
            close: self.close.clone(),
        }
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        let mut control = self.control.borrow_mut();
        control.handles -= 1;
        if control.handles == 0 {
            control.closing = true;
        }
        let waker = control.waker.take();
        drop(control);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// A host-polled `!Send` future. Dropping it seals admission and wakes waiters,
/// but does not asynchronously close storage or guarantee in-flight persistence.
pub struct SessionDriver {
    work: LocalFuture<'static, ()>,
    control: Rc<RefCell<Control>>,
}
impl Future for SessionDriver {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.work.as_mut().poll(cx)
    }
}
impl Drop for SessionDriver {
    fn drop(&mut self) {
        let (jobs, waker) = {
            let mut control = self.control.borrow_mut();
            control.stopped = true;
            control.closing = true;
            (std::mem::take(&mut control.jobs), control.waker.take())
        };
        // Dropping callbacks can drop Session handles; do it outside the borrow.
        drop(jobs);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}
impl Session {
    pub fn new(storage: impl Storage + 'static) -> (Self, SessionDriver) {
        let control = Rc::new(RefCell::new(Control {
            jobs: VecDeque::new(),
            closing: false,
            stopped: false,
            poisoned: false,
            handles: 1,
            waker: None,
        }));
        let (sender, receiver) = oneshot::channel();
        let close: CloseWaiter =
            (Box::pin(async move { receiver.await.unwrap_or(Err(SessionError::DriverStopped)) })
                as LocalFuture<'static, _>)
                .shared();
        let owner = control.clone();
        let work = Box::pin(async move {
            let mut storage = storage;
            loop {
                let job = std::future::poll_fn(|cx| {
                    let mut state = owner.borrow_mut();
                    if let Some(job) = state.jobs.pop_front() {
                        Poll::Ready(Some(job))
                    } else if state.closing {
                        Poll::Ready(None)
                    } else {
                        state.waker = Some(cx.waker().clone());
                        Poll::Pending
                    }
                })
                .await;
                match job {
                    Some(job) => job(&mut storage, owner.clone()).await,
                    None => break,
                }
            }
            let result = AssertUnwindSafe(async { storage.close().await })
                .catch_unwind()
                .await;
            let result = match result {
                Ok(result) => result.map_err(SessionError::Storage),
                Err(_) => Err(SessionError::ClosePanicked),
            };
            let _ = sender.send(result);
        });
        (
            Self {
                control: control.clone(),
                close,
            },
            SessionDriver { work, control },
        )
    }

    /// Synchronously checks admission and queues the owned callback. The returned
    /// future only observes completion; even an unpolled waiter may be dropped.
    pub fn commit<T: 'static, F>(&self, callback: F) -> CommitWaiter<T>
    where
        F: for<'a> FnOnce(&'a Tx) -> TxFuture<'a, T> + 'static,
    {
        self.commit_owned(callback, false)
    }

    fn commit_owned<T: 'static, F>(&self, callback: F, close_on_failure: bool) -> CommitWaiter<T>
    where
        F: for<'a> FnOnce(&'a Tx) -> TxFuture<'a, T> + 'static,
    {
        let mut control = self.control.borrow_mut();
        if let Err(error) = control.admission() {
            return CommitWaiter(Box::pin(async { Err(error) }));
        }
        let (sender, receiver) = oneshot::channel();
        control.jobs.push_back(Box::new(move |storage, owner| {
            Box::pin(async move {
                if owner.borrow().poisoned {
                    let _ = sender.send(Ok(Err(SessionError::Poisoned)));
                    return;
                }
                let result = transaction::prepare(storage, callback).await;
                let result = match result {
                    Ok(Ok((value, writes))) if writes.is_empty() => {
                        Ok(Ok(CommitReceipt { value, seq: None }))
                    }
                    Ok(Ok((value, writes))) => {
                        let settled = AssertUnwindSafe(async { storage.commit(writes).await })
                            .catch_unwind()
                            .await;
                        match settled {
                            Ok(Ok(seq)) => Ok(Ok(CommitReceipt {
                                value,
                                seq: Some(seq),
                            })),
                            Ok(Err(error)) => {
                                if !matches!(error, StorageError::Rejected(_)) {
                                    owner.borrow_mut().poisoned = true;
                                }
                                Ok(Err(SessionError::Storage(error)))
                            }
                            Err(panic) => {
                                owner.borrow_mut().poisoned = true;
                                Err(panic)
                            }
                        }
                    }
                    Ok(Err(error)) => Ok(Err(error)),
                    Err(panic) => Err(panic),
                };
                // Failed startup must close even if its observer remains unpolled.
                if close_on_failure && !matches!(&result, Ok(Ok(_))) {
                    owner.borrow_mut().closing = true;
                }
                // No cache to adopt and no stream to publish in this subset.
                let _ = sender.send(result);
            })
        }));
        let waker = control.waker.take();
        drop(control);
        if let Some(waker) = waker {
            waker.wake();
        }
        CommitWaiter(Box::pin(async move { receive(receiver).await }))
    }

    /// Seal admission now; drain already-admitted work and close Storage once.
    /// Repeated calls observe the same result, even when earlier waiters drop.
    pub fn close(&self) -> CloseWaiter {
        let mut control = self.control.borrow_mut();
        control.closing = true;
        let waker = control.waker.take();
        drop(control);
        if let Some(waker) = waker {
            waker.wake();
        }
        self.close.clone()
    }
}

async fn receive<T>(receiver: oneshot::Receiver<Outcome<T>>) -> Result<T, SessionError> {
    match receiver.await {
        Ok(Ok(result)) => result,
        Ok(Err(panic)) => resume_unwind(panic),
        Err(_) => Err(SessionError::DriverStopped),
    }
}

#[cfg(test)]
mod task_tests;
