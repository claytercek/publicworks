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
pub use tasks::{Owner, TaskOptions, TaskOwnership};
mod execution;
pub use execution::*;
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

type Settlement<T> = Box<dyn FnOnce(&Outcome<CommitReceipt<T>>)>;

type Job =
    Box<dyn for<'a> FnOnce(&'a mut dyn Storage, Rc<RefCell<Control>>) -> LocalFuture<'a, ()>>;
struct Control {
    jobs: VecDeque<Job>,
    closing: bool,
    drained: bool,
    finished: bool,
    stopped: bool,
    poisoned: bool,
    handles: usize,
    waker: Option<Waker>,
    runner: std::rc::Weak<execution::RunnerControl>,
    leaf: std::rc::Weak<execution::Invocation>,
}
type RunnerNotification = (Rc<execution::RunnerControl>, bool, bool, bool);
fn notify_runner(notification: Option<RunnerNotification>) {
    if let Some((runner, closing, stopped, poisoned)) = notification {
        runner.session_state(closing, stopped, poisoned);
    }
}
impl Control {
    fn runner_notification(&self) -> Option<RunnerNotification> {
        self.runner
            .upgrade()
            .map(|runner| (runner, self.closing, self.stopped, self.poisoned))
    }
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
        let notification = control.runner_notification();
        let waker = control.waker.take();
        drop(control);
        notify_runner(notification);
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
        let (jobs, waker, notification) = {
            let mut control = self.control.borrow_mut();
            control.stopped = !control.finished;
            control.closing = true;
            let notification = control.runner_notification();
            (
                std::mem::take(&mut control.jobs),
                control.waker.take(),
                notification,
            )
        };
        // Publish forfeiture before dropping jobs: their ownership guards can wake
        // a reentrant task driver, which must not report successful close. Both
        // notification and callback destruction happen outside the Session borrow.
        notify_runner(notification);
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
            drained: false,
            finished: false,
            stopped: false,
            poisoned: false,
            handles: 1,
            waker: None,
            runner: Default::default(),
            leaf: Default::default(),
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
                        state.drained = true;
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
            owner.borrow_mut().finished = true;
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
        self.commit_options(callback, close_on_failure, false, None, None)
    }

    // A runner settlement barrier may join the already-admitted mutation line
    // after close seals public admission, but never after Storage drain ends.
    fn commit_join<T: 'static, F>(
        &self,
        invocation: Rc<execution::Invocation>,
        callback: F,
    ) -> CommitWaiter<T>
    where
        F: for<'a> FnOnce(&'a Tx) -> TxFuture<'a, T> + 'static,
    {
        self.commit_options(callback, false, true, Some(invocation), None)
    }

    fn commit_reservation<T: 'static, F>(
        &self,
        invocation: Rc<execution::Invocation>,
        callback: F,
    ) -> CommitWaiter<T>
    where
        F: for<'a> FnOnce(&'a Tx) -> TxFuture<'a, T> + 'static,
    {
        self.commit_options(callback, false, false, Some(invocation), None)
    }

    fn commit_options<T: 'static, F>(
        &self,
        callback: F,
        close_on_failure: bool,
        join: bool,
        fence_on_failure: Option<Rc<execution::Invocation>>,
        settled: Option<Settlement<T>>,
    ) -> CommitWaiter<T>
    where
        F: for<'a> FnOnce(&'a Tx) -> TxFuture<'a, T> + 'static,
    {
        let mut control = self.control.borrow_mut();
        let admission = control.admission();
        if let Err(error) = admission
            && !(join && error == SessionError::Closed && !control.drained)
        {
            drop(control);
            if let Some(invocation) = &fence_on_failure {
                invocation.end();
            }
            if let Some(settled) = settled {
                settled(&Ok(Err(error.clone())));
            }
            return CommitWaiter(Box::pin(async { Err(error) }));
        }
        let (sender, receiver) = oneshot::channel();
        control.jobs.push_back(Box::new(move |storage, owner| {
            Box::pin(async move {
                if owner.borrow().poisoned {
                    if let Some(invocation) = &fence_on_failure {
                        invocation.end();
                    }
                    if let Some(settled) = settled {
                        settled(&Ok(Err(SessionError::Poisoned)));
                    }
                    let _ = sender.send(Ok(Err(SessionError::Poisoned)));
                    return;
                }
                let result = transaction::prepare(storage, callback, owner.clone()).await;
                // No await between the final invocation check and Storage.commit.
                // A started storage commit is still drained even after driver drop.
                let result = result.map(|result| {
                    result.and_then(|(value, writes, fence)| {
                        if let Some(fence) = fence {
                            fence.check()?;
                        }
                        Ok((value, writes))
                    })
                });
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
                if !matches!(&result, Ok(Ok(_)))
                    && let Some(invocation) = &fence_on_failure
                {
                    invocation.end();
                }
                // Failed startup must close even if its observer remains unpolled.
                if close_on_failure && !matches!(&result, Ok(Ok(_))) {
                    owner.borrow_mut().closing = true;
                }
                let notification = owner.borrow().runner_notification();
                notify_runner(notification);
                if let Some(settled) = settled {
                    settled(&result);
                }
                // Private settlement effects belong to the driver, not observers.
                let _ = sender.send(result);
            })
        }));
        let notification = control.runner_notification();
        let waker = control.waker.take();
        drop(control);
        notify_runner(notification);
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
        let notification = control.runner_notification();
        let waker = control.waker.take();
        drop(control);
        notify_runner(notification);
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
