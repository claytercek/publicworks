//! Explicit foreground tree execution, outside the Session mutation queue.
use super::*;
use std::{cell::Cell, collections::BTreeMap, rc::Weak};

pub type PhaseFuture = Pin<Box<dyn Future<Output = Result<(), TaskOutcomeError>>>>;
pub type PhaseHandler = Rc<dyn Fn(TaskRecord, TaskRuntime) -> PhaseFuture>;
type Initial = Rc<dyn Fn(&Value) -> Result<Value, SessionError>>;

/// Immutable executable definition. Clones share reusable callbacks.
#[derive(Clone)]
pub struct TaskDefinition {
    kind: String,
    version: u64,
    initial: Initial,
    phases: BTreeMap<String, PhaseHandler>,
    abort_handler: PhaseHandler,
}
impl TaskDefinition {
    pub fn new(
        kind: impl Into<String>,
        version: u64,
        initial: impl Fn(&Value) -> Result<Value, SessionError> + 'static,
        phases: BTreeMap<String, PhaseHandler>,
    ) -> Self {
        Self {
            kind: kind.into(),
            version,
            initial: Rc::new(initial),
            phases,
            abort_handler: Rc::new(|_, runtime| {
                Box::pin(async move {
                    runtime
                        .commit(|_, _| {
                            Box::pin(async {
                                Ok(Some(TaskUpdate::Abort {
                                    reason: None,
                                    result: None,
                                }))
                            })
                        })
                        .await
                        .map_err(|e| fault(e.to_string()))?;
                    Ok(())
                })
            }),
        }
    }
    pub fn with_abort_handler(mut self, handler: PhaseHandler) -> Self {
        self.abort_handler = handler;
        self
    }
    pub fn kind(&self) -> &str {
        &self.kind
    }
    pub fn version(&self) -> u64 {
        self.version
    }
    pub(super) fn initial(&self, input: &Value) -> Result<Value, SessionError> {
        (self.initial)(input)
    }
}

#[derive(Clone, Default)]
pub struct TaskRegistry(Rc<BTreeMap<String, TaskDefinition>>);
impl TaskRegistry {
    pub fn new(definitions: impl IntoIterator<Item = TaskDefinition>) -> Result<Self, RunError> {
        let mut entries = BTreeMap::new();
        for definition in definitions {
            let kind = definition.kind.clone();
            if entries.insert(kind.clone(), definition).is_some() {
                return Err(RunError::DuplicateKind(kind));
            }
        }
        Ok(Self(Rc::new(entries)))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockReason {
    MissingTask,
    MissingDefinition,
    VersionMismatch,
    UnsupportedScope,
    NotPending,
}
// Keep the detached receipt directly usable, like Session task reads.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
pub enum RunResult {
    Terminal(TaskRecord),
    Suspended(TaskRecord),
    Blocked(BlockReason),
    Interrupted,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunError {
    Session(SessionError),
    DriverStopped,
    Closed,
    AlreadyAttached,
    DuplicateKind(String),
    Panicked(String),
}
impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session(error) => error.fmt(f),
            Self::DriverStopped => f.write_str("Task driver stopped before settlement"),
            Self::Closed => f.write_str("Task runner is closed"),
            Self::AlreadyAttached => f.write_str("Session already has an attached task driver"),
            Self::DuplicateKind(kind) => write!(f, "Duplicate task kind: {kind}"),
            Self::Panicked(message) => {
                write!(f, "Task execution infrastructure panicked: {message}")
            }
        }
    }
}
impl std::error::Error for RunError {}
impl From<SessionError> for RunError {
    fn from(error: SessionError) -> Self {
        Self::Session(error)
    }
}

pub struct RunWaiter(LocalFuture<'static, Result<RunResult, RunError>>);
impl Future for RunWaiter {
    type Output = Result<RunResult, RunError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AbortResult {
    Marked,
    Terminal,
    Blocked(BlockReason),
}
pub struct AbortWaiter(LocalFuture<'static, Result<AbortResult, RunError>>);
impl Future for AbortWaiter {
    type Output = Result<AbortResult, RunError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}
pub type RunnerCloseWaiter = Shared<LocalFuture<'static, Result<(), RunError>>>;

struct Request {
    id: Id,
    sender: oneshot::Sender<Result<RunResult, RunError>>,
}
#[derive(Default)]
struct RunnerState {
    requests: VecDeque<Request>,
    aborts: usize,
    closing: bool,
    finished: bool,
    dropped: bool,
    session_error: Option<SessionError>,
    waker: Option<Waker>,
    active: Weak<Invocation>,
    drive: Weak<drive::Drive>,
}
pub(super) struct RunnerControl(RefCell<RunnerState>);
impl RunnerControl {
    pub(super) fn session_state(&self, closing: bool, stopped: bool, poisoned: bool) {
        if closing || stopped || poisoned {
            let mut state = self.0.borrow_mut();
            if stopped {
                state.session_error = Some(SessionError::DriverStopped);
            } else if poisoned {
                state.session_error = Some(SessionError::Poisoned);
            }
            state.closing = true;
            let active = state.active.upgrade();
            let requests = std::mem::take(&mut state.requests);
            let waker = state.waker.take();
            drop(state);
            for request in requests {
                let _ = request.sender.send(Ok(RunResult::Interrupted));
            }
            if let Some(active) = active {
                active.signal();
            }
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
    fn seal(&self) {
        self.session_state(true, false, false);
    }
}

pub(super) struct Invocation {
    id: Id,
    abort_mode: bool,
    joined: RefCell<Vec<Waker>>,
    ended: Cell<bool>,
    cancelled: Cell<bool>,
    waiters: RefCell<Vec<Waker>>,
    runner: Weak<RunnerControl>,
}
impl Invocation {
    fn signal(&self) {
        self.cancelled.set(true);
        for waker in self.waiters.take() {
            waker.wake();
        }
    }
    pub(super) fn end(&self) {
        self.ended.set(true);
        self.signal();
        for waker in self.joined.take() {
            waker.wake();
        }
    }
    async fn join(&self) {
        std::future::poll_fn(|cx| {
            if self.ended.get() {
                Poll::Ready(())
            } else {
                let mut waiters = self.joined.borrow_mut();
                if !waiters.iter().any(|w| w.will_wake(cx.waker())) {
                    waiters.push(cx.waker().clone());
                }
                Poll::Pending
            }
        })
        .await
    }
    fn check(&self) -> Result<(), SessionError> {
        let active = self.runner.upgrade().is_some_and(|runner| {
            let state = runner.0.borrow();
            !state.closing
                && !state.finished
                && state
                    .active
                    .upgrade()
                    .is_some_and(|inv| std::ptr::eq(inv.as_ref(), self))
        });
        if self.ended.get() || !active {
            Err(SessionError::Invalid(
                "Task invocation ended or runner closing".into(),
            ))
        } else {
            Ok(())
        }
    }
}

// Driver forfeiture fences writes even while private preparation reads await.
// Closing alone does not cancel a callback that already passed its entry gate.
pub(super) enum PersistenceFence {
    Invocation {
        invocation: Rc<Invocation>,
        ending: bool,
    },
    Drive(Rc<drive::Drive>),
}
impl PersistenceFence {
    pub(super) fn check(&self) -> Result<(), SessionError> {
        let (inv, ending) = match self {
            Self::Drive(drive) => return drive.check(),
            Self::Invocation { invocation, ending } => (invocation, ending),
        };
        let valid = inv.runner.upgrade().is_some_and(|runner| {
            let state = runner.0.borrow();
            !state.finished
                && !state.dropped
                && state
                    .active
                    .upgrade()
                    .is_some_and(|active| Rc::ptr_eq(&active, inv))
        });
        if !valid || (inv.ended.get() && !*ending) {
            Err(SessionError::Invalid(
                "Task invocation ended before persistence".into(),
            ))
        } else {
            Ok(())
        }
    }
}

/// Admission handle. The driver owns all admitted run requests.
#[derive(Clone)]
pub struct TaskRunner {
    control: Rc<RunnerControl>,
    close: RunnerCloseWaiter,
    session: Session,
    registry: TaskRegistry,
}
/// Host-owned local future. Dropping it fences contexts and forfeits settlement.
pub struct TaskDriver {
    work: LocalFuture<'static, ()>,
    control: Rc<RunnerControl>,
}
impl Future for TaskDriver {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.work.as_mut().poll(cx)
    }
}
impl Drop for TaskDriver {
    fn drop(&mut self) {
        let mut state = self.control.0.borrow_mut();
        state.closing = true;
        state.dropped = !state.finished;
        state.finished = true;
        let requests = std::mem::take(&mut state.requests);
        let active = state.active.upgrade();
        if let Some(drive) = state.drive.upgrade() {
            drive.ended.set(true);
        }
        drop(state);
        if let Some(active) = active {
            active.end();
        }
        drop(requests);
    }
}
impl TaskRunner {
    pub fn attach(
        session: &Session,
        registry: TaskRegistry,
    ) -> Result<(Self, TaskDriver), RunError> {
        let mut session_control = session.control.borrow_mut();
        session_control.admission()?;
        if session_control
            .runner
            .upgrade()
            .is_some_and(|r| !r.0.borrow().finished)
        {
            return Err(RunError::AlreadyAttached);
        }
        let control = Rc::new(RunnerControl(RefCell::new(RunnerState::default())));
        session_control.runner = Rc::downgrade(&control);
        session_control.tree = Weak::new();
        drop(session_control);
        let session = session.clone();
        let (sender, receiver) = oneshot::channel();
        let close =
            (Box::pin(async move { receiver.await.unwrap_or(Err(RunError::DriverStopped)) })
                as LocalFuture<'static, _>)
                .shared();
        let owner = control.clone();
        let handle_session = session.clone();
        let handle_registry = registry.clone();
        let work = Box::pin(async move {
            loop {
                let request = std::future::poll_fn(|cx| {
                    let mut state = owner.0.borrow_mut();
                    if let Some(request) = state.requests.pop_front() {
                        Poll::Ready(Some(request))
                    } else if state.closing && state.aborts == 0 {
                        Poll::Ready(None)
                    } else {
                        state.waker = Some(cx.waker().clone());
                        Poll::Pending
                    }
                })
                .await;
                let Some(request) = request else {
                    break;
                };
                if owner.0.borrow().closing {
                    let _ = request.sender.send(Ok(RunResult::Interrupted));
                    continue;
                }
                let drive = Rc::new(drive::Drive {
                    root: request.id,
                    ended: Cell::new(false),
                    runner: Rc::downgrade(&owner),
                });
                owner.0.borrow_mut().drive = Rc::downgrade(&drive);
                let result = AssertUnwindSafe(drive::execute(&session, &registry, drive.clone()))
                    .catch_unwind()
                    .await;
                drive.ended.set(true);
                let active = owner.0.borrow().active.upgrade();
                if let Some(active) = active {
                    active.end();
                }
                let result =
                    result.unwrap_or_else(|panic| Err(RunError::Panicked(panic_message(&*panic))));
                let _ = request.sender.send(result);
            }
            let mut state = owner.0.borrow_mut();
            state.finished = true;
            let result = state
                .session_error
                .clone()
                .map_or(Ok(()), |e| Err(RunError::Session(e)));
            drop(state);
            let _ = sender.send(result);
        });
        Ok((
            Self {
                control: control.clone(),
                close,
                session: handle_session,
                registry: handle_registry,
            },
            TaskDriver { work, control },
        ))
    }
    /// Eager FIFO admission. Dropping the observer cannot cancel a run.
    pub fn run(&self, id: Id) -> RunWaiter {
        let mut state = self.control.0.borrow_mut();
        if state.closing || state.finished {
            let error = if state.dropped {
                RunError::DriverStopped
            } else {
                RunError::Closed
            };
            return RunWaiter(Box::pin(async { Err(error) }));
        }
        let (sender, receiver) = oneshot::channel();
        state.requests.push_back(Request { id, sender });
        let waker = state.waker.take();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
        RunWaiter(Box::pin(async move {
            receiver.await.unwrap_or(Err(RunError::DriverStopped))
        }))
    }
    /// Seal run admission, signal the handler, and join its cooperative settlement.
    /// Does not close Storage. Keep both drivers polling.
    pub fn close(&self) -> RunnerCloseWaiter {
        self.control.seal();
        self.close.clone()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum TaskUpdate {
    Checkpoint(Value),
    Wait {
        checkpoint: Value,
        on: Vec<Id>,
        policy: JoinPolicy,
    },
    Complete(Value),
    Fail(TaskOutcomeError, Option<Value>),
    Abort {
        reason: Option<String>,
        result: Option<Value>,
    },
}
#[derive(Clone)]
pub struct TaskRuntime {
    session: Session,
    invocation: Rc<Invocation>,
    conversation_id: Id,
}
impl TaskRuntime {
    pub fn task_id(&self) -> Id {
        self.invocation.id
    }
    pub fn conversation_id(&self) -> Id {
        self.conversation_id
    }
    pub fn is_cancelled(&self) -> bool {
        self.invocation.cancelled.get()
    }
    pub fn cancelled(&self) -> impl Future<Output = ()> + 'static {
        let invocation = self.invocation.clone();
        std::future::poll_fn(move |cx| {
            if invocation.cancelled.get() {
                Poll::Ready(())
            } else {
                let mut waiters = invocation.waiters.borrow_mut();
                if !waiters.iter().any(|w| w.will_wake(cx.waker())) {
                    waiters.push(cx.waker().clone());
                }
                Poll::Pending
            }
        })
    }
    /// Queue an atomic application change and optional state update on Session.
    pub fn commit<F>(&self, change: F) -> CommitWaiter<()>
    where
        F: for<'a> FnOnce(&'a Tx, TaskRecord) -> TxFuture<'a, Option<TaskUpdate>> + 'static,
    {
        let invocation = self.invocation.clone();
        self.session.commit(move |tx| {
            Box::pin(async move {
                invocation.check()?;
                tx.fence(invocation.clone(), false);
                let record = tx
                    .task(invocation.id)
                    .await?
                    .ok_or_else(|| SessionError::Invalid("Task disappeared".into()))?;
                invocation.check()?;
                if record.status() != TaskStatus::Running
                    || (record.abort_requested && !invocation.abort_mode)
                {
                    return Err(SessionError::Invalid(
                        "Task is not running or is abort-marked".into(),
                    ));
                }
                tx.attribute(invocation.id);
                let update = change(tx, record).await?;
                // Driver drop while an application callback awaits must fence its writes too.
                if invocation.ended.get() {
                    return Err(SessionError::Invalid("Task invocation ended".into()));
                }
                if let Some(update) = update {
                    if invocation.abort_mode && matches!(update, TaskUpdate::Wait { .. }) {
                        return Err(SessionError::Invalid("Abort handlers cannot wait".into()));
                    }
                    tx.update_task(invocation.id, update).await?;
                }
                Ok(())
            })
        })
    }
}

fn panic_message(panic: &(dyn Any + Send)) -> String {
    if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).into()
    } else {
        "Task handler panicked with a non-string payload".into()
    }
}
fn fault(message: impl Into<String>) -> TaskOutcomeError {
    TaskOutcomeError {
        message: message.into(),
        detail: None,
    }
}

mod abort;
pub(super) mod drive;
mod phase;
pub(super) mod tree;

#[cfg(test)]
mod tests;
