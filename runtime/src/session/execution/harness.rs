//! Harness-wide task scheduling above a private Session.
use super::*;
mod observers;
mod submissions;
#[cfg(test)]
mod tests;
use futures_util::{StreamExt, future::Shared, stream::FuturesUnordered};
use observers::Registration;
use std::{collections::BTreeSet, task::Waker};
pub use submissions::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HarnessError {
    Session(SessionError),
    Closed,
    DriverStopped,
}
impl fmt::Display for HarnessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session(error) => error.fmt(f),
            Self::Closed => f.write_str("Harness is closed"),
            Self::DriverStopped => f.write_str("Harness driver stopped before settlement"),
        }
    }
}
impl std::error::Error for HarnessError {}
impl From<SessionError> for HarnessError {
    fn from(error: SessionError) -> Self {
        Self::Session(error)
    }
}

pub struct HarnessOpenWaiter(LocalFuture<'static, Result<Harness, HarnessError>>);
impl Future for HarnessOpenWaiter {
    type Output = Result<Harness, HarnessError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}
pub struct TaskWaiter {
    // Cancel registration before dropping the future and its channel receivers.
    registration: Option<Registration<Id, TaskRecord>>,
    future: LocalFuture<'static, Result<TaskRecord, HarnessError>>,
}
impl Future for TaskWaiter {
    type Output = Result<TaskRecord, HarnessError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = self.future.as_mut().poll(cx);
        if result.is_ready() {
            self.registration = None;
        }
        result
    }
}
pub struct HarnessAbortWaiter(LocalFuture<'static, Result<AbortResult, HarnessError>>);
impl Future for HarnessAbortWaiter {
    type Output = Result<AbortResult, HarnessError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConversationAbortOptions {
    pub background: bool,
}

pub struct ConversationWaiter(
    LocalFuture<'static, Result<Option<ConversationHandle>, HarnessError>>,
);
impl Future for ConversationWaiter {
    type Output = Result<Option<ConversationHandle>, HarnessError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

pub struct IdleWaiter {
    registration: Option<Registration<Option<Id>, ()>>,
    future: LocalFuture<'static, Result<(), HarnessError>>,
}
impl Future for IdleWaiter {
    type Output = Result<(), HarnessError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = self.future.as_mut().poll(cx);
        if result.is_ready() {
            self.registration = None;
        }
        result
    }
}

pub struct ConversationAbortWaiter(LocalFuture<'static, Result<(), HarnessError>>);
impl Future for ConversationAbortWaiter {
    type Output = Result<(), HarnessError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}
pub struct InspectionWaiter(LocalFuture<'static, Result<HarnessInspection, HarnessError>>);
impl Future for InspectionWaiter {
    type Output = Result<HarnessInspection, HarnessError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}
pub type HarnessCloseWaiter = Shared<LocalFuture<'static, Result<(), HarnessError>>>;

#[derive(Clone, Debug, PartialEq)]
pub struct TaskInspection {
    pub task: TaskRecord,
    pub active: bool,
    pub blocked: Option<BlockReason>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct HarnessInspection {
    pub progress_enabled: bool,
    pub registry_generation: u64,
    pub last_commit_seq: Option<Seq>,
    pub tasks: Vec<TaskInspection>,
}

type IdleSenders = BTreeMap<u64, oneshot::Sender<Result<(), HarnessError>>>;

struct HarnessState {
    opened: bool,
    enabled: bool,
    closing: bool,
    finished: bool,
    stopped: bool,
    failure: Option<SessionError>,
    dirty: bool,
    draining: bool,
    handles: usize,
    registry: TaskRegistry,
    registry_generation: u64,
    last_commit_seq: Option<Seq>,
    waker: Option<Waker>,
    next_waiter: u64,
    task_waiters: BTreeMap<Id, BTreeMap<u64, oneshot::Sender<Result<TaskRecord, HarnessError>>>>,
    idle_waiters: BTreeMap<Option<Id>, IdleSenders>,
    submission_waiters:
        BTreeMap<Id, BTreeMap<u64, oneshot::Sender<Result<SubmissionRecord, HarnessError>>>>,
}
struct HarnessControl(RefCell<HarnessState>);
impl HarnessControl {
    fn wake(&self) {
        let waker = self.0.borrow_mut().waker.take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    fn seal(&self, runner: &RunnerControl) {
        let session_error = runner.0.borrow().session_error.clone();
        let (task_waiters, idle_waiters, submission_waiters, waker) = {
            let mut state = self.0.borrow_mut();
            if let Some(error) = session_error
                && state.failure.is_none()
            {
                state.failure = Some(error);
            }
            if state.closing {
                return;
            }
            state.closing = true;
            state.dirty = false;
            (
                std::mem::take(&mut state.task_waiters),
                std::mem::take(&mut state.idle_waiters),
                std::mem::take(&mut state.submission_waiters),
                state.waker.take(),
            )
        };
        runner.seal();
        for sender in task_waiters.into_values().flat_map(BTreeMap::into_values) {
            let _ = sender.send(Err(HarnessError::Closed));
        }
        for sender in idle_waiters.into_values().flat_map(BTreeMap::into_values) {
            let _ = sender.send(Err(HarnessError::Closed));
        }
        for sender in submission_waiters
            .into_values()
            .flat_map(BTreeMap::into_values)
        {
            let _ = sender.send(Err(HarnessError::Closed));
        }
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// Cloneable admission and observation handle. Its Session remains private.
pub struct Harness {
    control: Rc<HarnessControl>,
    runner: Rc<RunnerControl>,
    session: Session,
    close: HarnessCloseWaiter,
}
impl Clone for Harness {
    fn clone(&self) -> Self {
        self.control.0.borrow_mut().handles += 1;
        Self {
            control: self.control.clone(),
            runner: self.runner.clone(),
            session: self.session.clone(),
            close: self.close.clone(),
        }
    }
}
impl Drop for Harness {
    fn drop(&mut self) {
        let last = {
            let mut state = self.control.0.borrow_mut();
            state.handles -= 1;
            state.handles == 0
        };
        if last {
            self.control.seal(&self.runner);
        }
    }
}

/// Stateless orchestration view over one committed conversation.
#[derive(Clone)]
pub struct ConversationHandle {
    id: Id,
    harness: Harness,
}
impl ConversationHandle {
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn wait_for_idle(&self) -> IdleWaiter {
        self.harness.wait_for_idle_scope(Some(self.id))
    }
    pub fn abort(&self, options: ConversationAbortOptions) -> ConversationAbortWaiter {
        self.harness.abort_conversation(self.id, options)
    }
}

/// One host-polled owner for Session persistence, scheduling, and local handlers.
pub struct HarnessDriver {
    work: LocalFuture<'static, ()>,
    control: Rc<HarnessControl>,
    runner: Rc<RunnerControl>,
}
impl Future for HarnessDriver {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.work.as_mut().poll(cx)
    }
}
impl Drop for HarnessDriver {
    fn drop(&mut self) {
        let actives = {
            let mut state = self.runner.0.borrow_mut();
            state.closing = true;
            state.dropped = !state.finished;
            state.finished = true;
            state
                .actives
                .values()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>()
        };
        for invocation in actives {
            invocation.end();
        }
        let (task_waiters, idle_waiters, submission_waiters) = {
            let mut state = self.control.0.borrow_mut();
            state.stopped = !state.finished;
            state.closing = true;
            (
                std::mem::take(&mut state.task_waiters),
                std::mem::take(&mut state.idle_waiters),
                std::mem::take(&mut state.submission_waiters),
            )
        };
        for sender in task_waiters.into_values().flat_map(BTreeMap::into_values) {
            let _ = sender.send(Err(HarnessError::DriverStopped));
        }
        for sender in idle_waiters.into_values().flat_map(BTreeMap::into_values) {
            let _ = sender.send(Err(HarnessError::DriverStopped));
        }
        for sender in submission_waiters
            .into_values()
            .flat_map(BTreeMap::into_values)
        {
            let _ = sender.send(Err(HarnessError::DriverStopped));
        }
    }
}

impl Harness {
    pub fn open(
        storage: impl Storage + 'static,
        registry: TaskRegistry,
    ) -> (HarnessOpenWaiter, HarnessDriver) {
        let (session, session_driver) = Session::new(storage);
        let runner = Rc::new(RunnerControl(RefCell::new(RunnerState::default())));
        {
            let mut session_control = session.control.borrow_mut();
            session_control.runner = Rc::downgrade(&runner);
            session_control.tree = Weak::new();
        }
        let control = Rc::new(HarnessControl(RefCell::new(HarnessState {
            opened: false,
            enabled: false,
            closing: false,
            finished: false,
            stopped: false,
            failure: None,
            dirty: false,
            draining: false,
            handles: 1,
            registry,
            registry_generation: 1,
            last_commit_seq: None,
            waker: None,
            next_waiter: 1,
            task_waiters: BTreeMap::new(),
            idle_waiters: BTreeMap::new(),
            submission_waiters: BTreeMap::new(),
        })));

        let weak_control = Rc::downgrade(&control);
        let weak_runner = Rc::downgrade(&runner);
        session.observe_commits(Rc::new(move |changes| {
            let Some(control) = weak_control.upgrade() else {
                return;
            };
            let runner = weak_runner.upgrade();
            let mut completed = Vec::new();
            let mut settled_submissions = Vec::new();
            let mut signals = Vec::new();
            let waker = {
                let mut state = control.0.borrow_mut();
                state.last_commit_seq = Some(changes.seq);
                state.dirty = true;
                for task in changes.tasks {
                    if task.status() == TaskStatus::Terminal
                        && let Some(waiters) = state.task_waiters.remove(&task.id)
                    {
                        completed
                            .extend(waiters.into_values().map(|sender| (sender, task.clone())));
                    }
                    if (task.abort_requested || task.status() == TaskStatus::Terminal)
                        && let Some(invocation) = runner
                            .as_ref()
                            .and_then(|runner| runner.0.borrow().actives.get(&task.id).cloned())
                            .and_then(|active| active.upgrade())
                        && !invocation.abort_mode
                    {
                        signals.push(invocation);
                    }
                }
                for record in changes.submissions {
                    if submissions::is_terminal(&record)
                        && let Some(waiters) = state.submission_waiters.remove(&record.id)
                    {
                        settled_submissions
                            .extend(waiters.into_values().map(|sender| (sender, record.clone())));
                    }
                }
                let _ = changes.conversations;
                state.waker.take()
            };
            for (sender, task) in completed {
                let _ = sender.send(Ok(task));
            }
            for (sender, record) in settled_submissions {
                let _ = sender.send(Ok(record));
            }
            for invocation in signals {
                invocation.signal();
            }
            if let Some(waker) = waker {
                waker.wake();
            }
        }));

        let settled_control = control.clone();
        let opening = session.commit_options(
            |tx| {
                Box::pin(async move {
                    let tree = tx.tree().await?;
                    let running = tree
                        .tasks
                        .into_values()
                        .filter(|record| record.status() == TaskStatus::Running)
                        .collect::<Vec<_>>();
                    tx.reconcile_terminal_runs().await?;
                    for mut record in running {
                        let TaskState::Running { checkpoint } = record.state else {
                            unreachable!()
                        };
                        record.state = TaskState::Pending { checkpoint };
                        tx.set_task(record).await?;
                    }
                    Ok(())
                })
            },
            true,
            false,
            None,
            Some(Box::new(move |result| {
                let waker = {
                    let mut state = settled_control.0.borrow_mut();
                    match result {
                        Ok(Ok(_)) => {
                            state.opened = true;
                            state.dirty = true;
                        }
                        _ => state.closing = true,
                    }
                    state.waker.take()
                };
                if let Some(waker) = waker {
                    waker.wake();
                }
            })),
        );

        let (close_sender, close_receiver) = oneshot::channel();
        let close = (Box::pin(async move {
            close_receiver
                .await
                .unwrap_or(Err(HarnessError::DriverStopped))
        }) as LocalFuture<'static, _>)
            .shared();
        let harness = Harness {
            control: control.clone(),
            runner: runner.clone(),
            session: session.clone(),
            close: close.clone(),
        };
        let failed_open_close = close.clone();
        let opening_waiter = HarnessOpenWaiter(Box::pin(async move {
            match AssertUnwindSafe(opening).catch_unwind().await {
                Ok(Ok(_)) => Ok(harness),
                Ok(Err(error)) => {
                    let _ = failed_open_close.await;
                    Err(HarnessError::Session(error))
                }
                Err(panic) => {
                    let _ = failed_open_close.await;
                    resume_unwind(panic)
                }
            }
        }));

        let scheduler_control = control.clone();
        let scheduler_runner = runner.clone();
        let scheduler_session = session.clone();
        let scheduler = async move {
            let result = schedule(
                scheduler_session,
                scheduler_control.clone(),
                scheduler_runner.clone(),
            )
            .await;
            let state = scheduler_control.0.borrow();
            let result = if state.stopped {
                Err(HarnessError::DriverStopped)
            } else if let Some(error) = &state.failure {
                Err(HarnessError::Session(error.clone()))
            } else {
                result.map_err(HarnessError::Session)
            };
            drop(state);
            let _ = close_sender.send(result);
        };
        let work = Box::pin(async move {
            let _ = futures_util::future::join(session_driver, scheduler).await;
        });
        (
            opening_waiter,
            HarnessDriver {
                work,
                control,
                runner,
            },
        )
    }

    pub fn commit<T: 'static, F>(&self, callback: F) -> CommitWaiter<T>
    where
        F: for<'a> FnOnce(&'a Tx) -> TxFuture<'a, T> + 'static,
    {
        let state = self.control.0.borrow();
        if state.stopped {
            return CommitWaiter(Box::pin(async { Err(SessionError::DriverStopped) }));
        }
        if state.closing {
            return CommitWaiter(Box::pin(async { Err(SessionError::Closed) }));
        }
        drop(state);
        self.session.commit(callback)
    }

    /// Permanently enable scheduler progress and request a fresh drain.
    pub fn resume(&self) -> Result<(), HarnessError> {
        let waker = {
            let mut state = self.control.0.borrow_mut();
            if state.stopped {
                return Err(HarnessError::DriverStopped);
            }
            if state.closing {
                return Err(HarnessError::Closed);
            }
            state.enabled = true;
            state.dirty = true;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    /// Replace the immutable process-local definition snapshot.
    pub fn replace_registry(&self, registry: TaskRegistry) -> Result<(), HarnessError> {
        let (previous, waker) = {
            let mut state = self.control.0.borrow_mut();
            if state.stopped {
                return Err(HarnessError::DriverStopped);
            }
            if state.closing {
                return Err(HarnessError::Closed);
            }
            let previous = std::mem::replace(&mut state.registry, registry);
            state.registry_generation += 1;
            state.dirty = true;
            (previous, state.waker.take())
        };
        drop(previous);
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    pub fn inspect(&self) -> InspectionWaiter {
        {
            let state = self.control.0.borrow();
            if state.stopped || state.closing {
                let error = if state.stopped {
                    HarnessError::DriverStopped
                } else {
                    HarnessError::Closed
                };
                return InspectionWaiter(Box::pin(async move { Err(error) }));
            }
        }
        let control = self.control.clone();
        let runner = self.runner.clone();
        let waiter = self.session.commit(move |tx| {
            Box::pin(async move {
                let tree = tx.tree().await?;
                let (registry, progress_enabled, registry_generation, last_commit_seq) = {
                    let state = control.0.borrow();
                    (
                        state.registry.clone(),
                        state.enabled,
                        state.registry_generation,
                        state.last_commit_seq,
                    )
                };
                let mut tasks = tree
                    .tasks
                    .values()
                    .filter(|task| task.status() != TaskStatus::Terminal)
                    .map(|task| {
                        let supported = tree
                            .root(task.id)
                            .and_then(|root| tree.scope(root))
                            .is_some();
                        let definition = registry.0.get(&task.kind);
                        let blocked = if !supported {
                            Some(BlockReason::UnsupportedScope)
                        } else if definition.is_none() {
                            Some(BlockReason::MissingDefinition)
                        } else if definition.is_some_and(|d| d.version != task.version) {
                            Some(BlockReason::VersionMismatch)
                        } else if task.status() != TaskStatus::Pending {
                            Some(BlockReason::NotPending)
                        } else {
                            None
                        };
                        TaskInspection {
                            task: task.clone(),
                            active: runner
                                .0
                                .borrow()
                                .actives
                                .get(&task.id)
                                .and_then(Weak::upgrade)
                                .is_some(),
                            blocked,
                        }
                    })
                    .collect::<Vec<_>>();
                tasks.sort_by_key(|inspection| inspection.task.id);
                Ok(HarnessInspection {
                    progress_enabled,
                    registry_generation,
                    last_commit_seq,
                    tasks,
                })
            })
        });
        InspectionWaiter(Box::pin(async move {
            Ok(waiter.await.map_err(HarnessError::Session)?.value)
        }))
    }

    pub fn conversation(&self, id: Id) -> ConversationWaiter {
        {
            let state = self.control.0.borrow();
            if state.stopped || state.closing {
                let error = if state.stopped {
                    HarnessError::DriverStopped
                } else {
                    HarnessError::Closed
                };
                return ConversationWaiter(Box::pin(async move { Err(error) }));
            }
        }
        let harness = self.clone();
        let waiter = self
            .session
            .commit(move |tx| Box::pin(async move { Ok(tx.conversation(id).await?.is_some()) }));
        ConversationWaiter(Box::pin(async move {
            if waiter.await.map_err(HarnessError::Session)?.value {
                Ok(Some(ConversationHandle { id, harness }))
            } else {
                Ok(None)
            }
        }))
    }

    pub fn wait_for_idle(&self) -> IdleWaiter {
        self.wait_for_idle_scope(None)
    }

    fn wait_for_idle_scope(&self, scope: Option<Id>) -> IdleWaiter {
        if let Err(error) = self.resume() {
            return IdleWaiter {
                future: Box::pin(async move { Err(error) }),
                registration: None,
            };
        }
        let registration = Registration::new(&self.control, scope, |state| &mut state.idle_waiters);
        let key = registration.key;
        let callback_cancelled = registration.cancelled.clone();
        let (sender, receiver) = oneshot::channel();
        let control = self.control.clone();
        let waiter = self.session.commit(move |tx| {
            Box::pin(async move {
                let tree = tx.tree().await?;
                if let Some(id) = scope
                    && !tree.conversations.contains_key(&id)
                {
                    return Err(SessionError::Invalid("Conversation does not exist".into()));
                }
                if tree.conversation_idle(scope) || callback_cancelled.get() {
                    return Ok(true);
                }
                let mut state = control.0.borrow_mut();
                if state.closing {
                    return Err(SessionError::Closed);
                }
                state
                    .idle_waiters
                    .entry(scope)
                    .or_default()
                    .insert(key, sender);
                Ok(false)
            })
        });
        IdleWaiter {
            future: Box::pin(async move {
                if waiter.await.map_err(HarnessError::Session)?.value {
                    Ok(())
                } else {
                    receiver.await.unwrap_or(Err(HarnessError::DriverStopped))
                }
            }),
            registration: Some(registration),
        }
    }

    fn abort_conversation(
        &self,
        id: Id,
        options: ConversationAbortOptions,
    ) -> ConversationAbortWaiter {
        if let Err(error) = self.resume() {
            return ConversationAbortWaiter(Box::pin(async move { Err(error) }));
        }
        let waiter = self.session.commit(move |tx| {
            Box::pin(async move {
                let mut tree = tx.tree().await?;
                let reached = tree
                    .conversation_scope(id, options.background)
                    .ok_or_else(|| {
                        SessionError::Invalid("Invalid conversation ownership".into())
                    })?;
                let conversations = tree
                    .conversation_scopes(id, options.background)
                    .ok_or_else(|| {
                        SessionError::Invalid("Invalid conversation ownership".into())
                    })?;
                for conversation in conversations {
                    tx.withdraw_queued_inputs(conversation).await?;
                }
                let before = tree.tasks.clone();
                for task in &reached {
                    if tree.tasks[task].status() != TaskStatus::Terminal {
                        tree.mark(*task);
                    }
                }
                // Reconcile only the reached snapshot. Unrelated roots must not
                // advance merely because this conversation was aborted.
                tree.reconcile(&reached);
                tree.stage_changes(tx, &before).await?;
                Ok(reached)
            })
        });
        let harness = self.clone();
        ConversationAbortWaiter(Box::pin(async move {
            let reached = waiter.await.map_err(HarnessError::Session)?.value;
            if options.background {
                for task in reached {
                    harness.wait_task(task).await?;
                }
            }
            let conversation = harness.conversation(id).await?.ok_or_else(|| {
                HarnessError::Session(SessionError::Invalid("Conversation does not exist".into()))
            })?;
            conversation.wait_for_idle().await
        }))
    }

    pub fn wait_task(&self, id: Id) -> TaskWaiter {
        {
            let state = self.control.0.borrow();
            if state.stopped || state.closing {
                let error = if state.stopped {
                    HarnessError::DriverStopped
                } else {
                    HarnessError::Closed
                };
                return TaskWaiter {
                    future: Box::pin(async move { Err(error) }),
                    registration: None,
                };
            }
        }
        let registration = Registration::new(&self.control, id, |state| &mut state.task_waiters);
        let key = registration.key;
        let callback_cancelled = registration.cancelled.clone();
        let (sender, receiver) = oneshot::channel();
        let control = self.control.clone();
        let waiter = self.session.commit(move |tx| {
            Box::pin(async move {
                let record = tx
                    .task(id)
                    .await?
                    .ok_or_else(|| SessionError::Invalid("Task does not exist".into()))?;
                if record.status() == TaskStatus::Terminal {
                    return Ok(Some(record));
                }
                if callback_cancelled.get() {
                    return Ok(None);
                }
                let mut state = control.0.borrow_mut();
                if state.closing {
                    return Err(SessionError::Closed);
                }
                state
                    .task_waiters
                    .entry(id)
                    .or_default()
                    .insert(key, sender);
                Ok(None)
            })
        });
        TaskWaiter {
            future: Box::pin(async move {
                match waiter.await.map_err(HarnessError::Session)?.value {
                    Some(record) => Ok(record),
                    None => receiver.await.unwrap_or(Err(HarnessError::DriverStopped)),
                }
            }),
            registration: Some(registration),
        }
    }

    pub fn abort(&self, id: Id) -> HarnessAbortWaiter {
        {
            let state = self.control.0.borrow();
            if state.stopped || state.closing {
                let error = if state.stopped {
                    HarnessError::DriverStopped
                } else {
                    HarnessError::Closed
                };
                return HarnessAbortWaiter(Box::pin(async move { Err(error) }));
            }
        }
        let runner = self.runner.clone();
        let waiter = self.session.commit(move |tx| {
            Box::pin(async move {
                let record = tx
                    .task(id)
                    .await?
                    .ok_or_else(|| SessionError::Invalid("Task does not exist".into()))?;
                if record.status() == TaskStatus::Terminal {
                    return Ok((AbortResult::Terminal, None));
                }
                let mut tree = tx.tree().await?;
                let Some(scope) = tree.root(id).and_then(|root| tree.scope(root)) else {
                    return Ok((AbortResult::Blocked(BlockReason::UnsupportedScope), None));
                };
                let before = tree.tasks.clone();
                tree.mark(id);
                tree.reconcile(&scope);
                let normal = runner
                    .0
                    .borrow()
                    .actives
                    .get(&id)
                    .and_then(Weak::upgrade)
                    .filter(|invocation| !invocation.abort_mode);
                tree.stage_changes(tx, &before).await?;
                Ok((AbortResult::Marked, normal))
            })
        });
        HarnessAbortWaiter(Box::pin(async move {
            let (result, normal) = waiter.await.map_err(HarnessError::Session)?.value;
            if let Some(normal) = normal {
                normal.join().await;
            }
            Ok(result)
        }))
    }

    pub fn close(&self) -> HarnessCloseWaiter {
        self.control.seal(&self.runner);
        self.close.clone()
    }
}

fn resolve_idle_waiters(control: &HarnessControl, tree: &tree::Tree) {
    let settled = {
        let mut state = control.0.borrow_mut();
        let scopes = state.idle_waiters.keys().copied().collect::<Vec<_>>();
        let mut settled = Vec::new();
        for scope in scopes {
            if tree.conversation_idle(scope)
                && let Some(waiters) = state.idle_waiters.remove(&scope)
            {
                settled.extend(waiters.into_values());
            }
        }
        settled
    };
    for sender in settled {
        let _ = sender.send(Ok(()));
    }
}

enum SchedulerEvent {
    Drain(Result<Vec<(Rc<Invocation>, TaskRegistry, u64)>, SessionError>),
    Invocation(Id, Rc<Invocation>, Box<Result<RunResult, RunError>>),
}
enum SchedulerAction {
    Drain(TaskRegistry, u64),
    Event(SchedulerEvent),
    Failure(SessionError),
    Close,
}

async fn schedule(
    session: Session,
    control: Rc<HarnessControl>,
    runner: Rc<RunnerControl>,
) -> Result<(), SessionError> {
    let mut events: FuturesUnordered<LocalFuture<'static, SchedulerEvent>> =
        FuturesUnordered::new();
    loop {
        let action = std::future::poll_fn(|cx| {
            if let Poll::Ready(Some(event)) = events.poll_next_unpin(cx) {
                return Poll::Ready(SchedulerAction::Event(event));
            }
            let mut state = control.0.borrow_mut();
            if !state.closing {
                let mut runner_state = runner.0.borrow_mut();
                if runner_state.closing {
                    return Poll::Ready(SchedulerAction::Failure(
                        runner_state
                            .session_error
                            .clone()
                            .unwrap_or(SessionError::Closed),
                    ));
                }
                runner_state.waker = Some(cx.waker().clone());
            }
            if state.closing && !state.draining && events.is_empty() {
                return Poll::Ready(SchedulerAction::Close);
            }
            if state.opened && state.dirty && !state.draining && !state.closing {
                state.dirty = false;
                state.draining = true;
                return Poll::Ready(SchedulerAction::Drain(
                    state.registry.clone(),
                    state.registry_generation,
                ));
            }
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        })
        .await;
        match action {
            SchedulerAction::Drain(registry, registry_generation) => {
                let session = session.clone();
                let runner = runner.clone();
                let weak_control = Rc::downgrade(&control);
                let enabled = control.0.borrow().enabled;
                events.push(Box::pin(async move {
                    SchedulerEvent::Drain(
                        reserve_all(
                            &session,
                            &runner,
                            weak_control,
                            registry,
                            registry_generation,
                            enabled,
                        )
                        .await,
                    )
                }));
            }
            SchedulerAction::Event(SchedulerEvent::Drain(result)) => {
                control.0.borrow_mut().draining = false;
                match result {
                    Ok(reservations) if !control.0.borrow().closing => {
                        for (invocation, registry, registry_generation) in reservations {
                            let session = session.clone();
                            let registry_control = Rc::downgrade(&control);
                            events.push(Box::pin(async move {
                                let id = invocation.id;
                                let refresh = Rc::new(move || {
                                    registry_control.upgrade().map(|control| {
                                        let state = control.0.borrow();
                                        (state.registry_generation, state.registry.clone())
                                    })
                                });
                                let result = phase::execute_reserved(
                                    &session,
                                    registry,
                                    invocation.clone(),
                                    registry_generation,
                                    refresh,
                                )
                                .await;
                                SchedulerEvent::Invocation(id, invocation, Box::new(result))
                            }));
                        }
                    }
                    Ok(reservations) => {
                        let invocations = reservations
                            .into_iter()
                            .map(|(invocation, _, _)| invocation)
                            .collect::<Vec<_>>();
                        {
                            let mut state = runner.0.borrow_mut();
                            for invocation in &invocations {
                                state.actives.remove(&invocation.id);
                            }
                        }
                        for invocation in invocations {
                            invocation.end();
                        }
                    }
                    Err(SessionError::Storage(StorageError::Rejected(_))) => {}
                    Err(SessionError::Closed) if control.0.borrow().closing => {}
                    Err(error) => {
                        if control.0.borrow().failure.is_none() {
                            control.0.borrow_mut().failure = Some(error);
                        }
                        control.seal(&runner);
                    }
                }
                control.wake();
            }
            SchedulerAction::Event(SchedulerEvent::Invocation(id, invocation, result)) => {
                let current = runner
                    .0
                    .borrow_mut()
                    .actives
                    .remove(&id)
                    .and_then(|active| active.upgrade());
                invocation.end();
                if let Some(current) = current
                    && !Rc::ptr_eq(&current, &invocation)
                {
                    current.end();
                }
                let error = match *result {
                    Err(RunError::Session(SessionError::Closed)) if control.0.borrow().closing => {
                        None
                    }
                    Err(RunError::Session(error)) => Some(error),
                    Err(other) => Some(SessionError::Invalid(other.to_string())),
                    Ok(_) => None,
                };
                if let Some(error) = error {
                    if control.0.borrow().failure.is_none() {
                        control.0.borrow_mut().failure = Some(error);
                    }
                    control.seal(&runner);
                } else if !control.0.borrow().closing {
                    let mut state = control.0.borrow_mut();
                    state.dirty = true;
                    let waker = state.waker.take();
                    drop(state);
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                }
            }
            SchedulerAction::Failure(error) => {
                control.0.borrow_mut().failure = Some(error);
                control.seal(&runner);
            }
            SchedulerAction::Close => break,
        }
    }
    let result = session.close().await;
    let runner_error = {
        let mut state = runner.0.borrow_mut();
        state.finished = true;
        state.session_error.clone()
    };
    let mut state = control.0.borrow_mut();
    state.finished = true;
    if state.failure.is_none() {
        state.failure = runner_error;
    }
    result
}

// Group before computing scopes: each scope walks the ownership graph.
fn reservation_roots(tree: &tree::Tree) -> BTreeSet<Id> {
    tree.tasks
        .values()
        .filter(|task| task.status() != TaskStatus::Terminal)
        .filter_map(|task| tree.root(task.id))
        .collect()
}

async fn reserve_all(
    session: &Session,
    runner: &Rc<RunnerControl>,
    control: Weak<HarnessControl>,
    registry: TaskRegistry,
    registry_generation: u64,
    enabled: bool,
) -> Result<Vec<(Rc<Invocation>, TaskRegistry, u64)>, SessionError> {
    let tentative = Rc::new(RefCell::new(Vec::<Rc<Invocation>>::new()));
    let callback_tentative = tentative.clone();
    let callback_runner = runner.clone();
    let callback_control = control.clone();
    let definitions = registry.clone();
    let receipt = session
        .commit(move |tx| {
            Box::pin(async move {
                if callback_control
                    .upgrade()
                    .is_none_or(|control| control.0.borrow().closing)
                {
                    return Ok(Vec::new());
                }
                let mut tree = tx.tree().await?;
                let Some(control) = callback_control.upgrade() else {
                    return Ok(Vec::new());
                };
                if control.0.borrow().closing {
                    return Ok(Vec::new());
                }
                resolve_idle_waiters(&control, &tree);
                let before = tree.tasks.clone();
                let mut scopes = BTreeMap::<Id, BTreeSet<Id>>::new();
                for root in reservation_roots(&tree) {
                    if let Some(scope) = tree.scope(root) {
                        scopes.insert(root, scope);
                    }
                }
                for scope in scopes.values() {
                    tree.reconcile(scope);
                }
                let supported = scopes
                    .values()
                    .flat_map(|scope| scope.iter().copied())
                    .collect::<BTreeSet<_>>();
                let ids = tree.tasks.keys().copied().collect::<Vec<_>>();
                let mut selected = Vec::new();
                for id in ids {
                    let record = &tree.tasks[&id];
                    if !supported.contains(&id)
                        || record.status() != TaskStatus::Pending
                        || callback_runner
                            .0
                            .borrow()
                            .actives
                            .get(&id)
                            .and_then(Weak::upgrade)
                            .is_some()
                        || (record.abort_requested && tree.owned_live(id))
                    {
                        continue;
                    }
                    let definition = definitions.0.get(&record.kind);
                    let exact = definition.is_some_and(|d| d.version == record.version);
                    if !exact {
                        if record.abort_requested {
                            let record = tree.tasks.get_mut(&id).unwrap();
                            record.state = TaskState::Terminal {
                                outcome: TaskOutcome::Orphaned {
                                    reason: "Missing task definition or exact version for abort"
                                        .into(),
                                },
                            };
                            record.memos = None;
                        }
                        continue;
                    }
                    if !enabled {
                        continue;
                    }
                    let record = tree.tasks.get_mut(&id).unwrap();
                    let TaskState::Pending { checkpoint } = &record.state else {
                        unreachable!()
                    };
                    record.state = TaskState::Running {
                        checkpoint: checkpoint.clone(),
                    };
                    let invocation = Rc::new(Invocation {
                        id,
                        abort_mode: record.abort_requested,
                        joined: RefCell::new(Vec::new()),
                        ended: Cell::new(false),
                        cancelled: Cell::new(false),
                        waiters: RefCell::new(Vec::new()),
                        runner: Rc::downgrade(&callback_runner),
                    });
                    callback_runner
                        .0
                        .borrow_mut()
                        .actives
                        .insert(id, Rc::downgrade(&invocation));
                    callback_tentative.borrow_mut().push(invocation.clone());
                    selected.push(invocation);
                }
                tree.stage_changes(tx, &before).await?;
                if callback_control
                    .upgrade()
                    .is_none_or(|control| control.0.borrow().closing)
                {
                    return Err(SessionError::Closed);
                }
                Ok(selected)
            })
        })
        .await;
    match receipt {
        Ok(receipt) => Ok(receipt
            .value
            .into_iter()
            .map(|invocation| (invocation, registry.clone(), registry_generation))
            .collect()),
        Err(error) => {
            let invocations = tentative.borrow_mut().drain(..).collect::<Vec<_>>();
            {
                let mut state = runner.0.borrow_mut();
                for invocation in &invocations {
                    if state
                        .actives
                        .get(&invocation.id)
                        .and_then(Weak::upgrade)
                        .is_some_and(|active| Rc::ptr_eq(&active, invocation))
                    {
                        state.actives.remove(&invocation.id);
                    }
                }
            }
            for invocation in invocations {
                invocation.end();
            }
            Err(error)
        }
    }
}
