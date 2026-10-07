//! Harness-wide task scheduling above a private Session.
use super::*;
use futures_util::{StreamExt, future::Shared, stream::FuturesUnordered};
use std::{collections::BTreeSet, task::Waker};

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
pub struct TaskWaiter(LocalFuture<'static, Result<TaskRecord, HarnessError>>);
impl Future for TaskWaiter {
    type Output = Result<TaskRecord, HarnessError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}
pub struct HarnessAbortWaiter(LocalFuture<'static, Result<AbortResult, HarnessError>>);
impl Future for HarnessAbortWaiter {
    type Output = Result<AbortResult, HarnessError>;
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

struct HarnessState {
    opened: bool,
    enabled: bool,
    closing: bool,
    finished: bool,
    stopped: bool,
    dirty: bool,
    draining: bool,
    handles: usize,
    registry: TaskRegistry,
    registry_generation: u64,
    last_commit_seq: Option<Seq>,
    waker: Option<Waker>,
    task_waiters: BTreeMap<Id, Vec<oneshot::Sender<Result<TaskRecord, HarnessError>>>>,
}
struct HarnessControl(RefCell<HarnessState>);
impl HarnessControl {
    fn wake(&self) {
        if let Some(waker) = self.0.borrow_mut().waker.take() {
            waker.wake();
        }
    }
    fn seal(&self, runner: &RunnerControl) {
        let (waiters, waker) = {
            let mut state = self.0.borrow_mut();
            if state.closing {
                return;
            }
            state.closing = true;
            state.dirty = false;
            (std::mem::take(&mut state.task_waiters), state.waker.take())
        };
        runner.seal();
        for sender in waiters.into_values().flatten() {
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
        let waiters = {
            let mut state = self.control.0.borrow_mut();
            state.stopped = !state.finished;
            state.closing = true;
            std::mem::take(&mut state.task_waiters)
        };
        for sender in waiters.into_values().flatten() {
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
            dirty: false,
            draining: false,
            handles: 1,
            registry,
            registry_generation: 1,
            last_commit_seq: None,
            waker: None,
            task_waiters: BTreeMap::new(),
        })));

        let weak_control = Rc::downgrade(&control);
        let weak_runner = Rc::downgrade(&runner);
        session.observe_commits(Rc::new(move |changes| {
            let Some(control) = weak_control.upgrade() else {
                return;
            };
            let runner = weak_runner.upgrade();
            let mut completed = Vec::new();
            let mut signals = Vec::new();
            let waker = {
                let mut state = control.0.borrow_mut();
                state.last_commit_seq = Some(changes.seq);
                state.dirty = true;
                for task in changes.tasks {
                    if task.status() == TaskStatus::Terminal
                        && let Some(waiters) = state.task_waiters.remove(&task.id)
                    {
                        completed.extend(waiters.into_iter().map(|sender| (sender, task.clone())));
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
                let _ = changes.conversations;
                state.waker.take()
            };
            for (sender, task) in completed {
                let _ = sender.send(Ok(task));
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
        let opening_waiter = HarnessOpenWaiter(Box::pin(async move {
            opening.await.map_err(HarnessError::Session)?;
            Ok(harness)
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
            let result = if scheduler_control.0.borrow().stopped {
                Err(HarnessError::DriverStopped)
            } else {
                result.map_err(HarnessError::Session)
            };
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
        let waker = {
            let mut state = self.control.0.borrow_mut();
            if state.stopped {
                return Err(HarnessError::DriverStopped);
            }
            if state.closing {
                return Err(HarnessError::Closed);
            }
            state.registry = registry;
            state.registry_generation += 1;
            state.dirty = true;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    pub fn inspect(&self) -> InspectionWaiter {
        let control = self.control.clone();
        let runner = self.runner.clone();
        let registry = control.0.borrow().registry.clone();
        let waiter = self.session.commit(move |tx| {
            Box::pin(async move {
                let tree = tx.tree().await?;
                let state = control.0.borrow();
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
                    progress_enabled: state.enabled,
                    registry_generation: state.registry_generation,
                    last_commit_seq: state.last_commit_seq,
                    tasks,
                })
            })
        });
        InspectionWaiter(Box::pin(async move {
            Ok(waiter.await.map_err(HarnessError::Session)?.value)
        }))
    }

    pub fn wait_task(&self, id: Id) -> TaskWaiter {
        let (sender, receiver) = oneshot::channel();
        let sender = Rc::new(RefCell::new(Some(sender)));
        let control = self.control.clone();
        let callback_sender = sender.clone();
        let waiter = self.session.commit(move |tx| {
            Box::pin(async move {
                let record = tx
                    .task(id)
                    .await?
                    .ok_or_else(|| SessionError::Invalid("Task does not exist".into()))?;
                if record.status() == TaskStatus::Terminal {
                    return Ok(Some(record));
                }
                let mut state = control.0.borrow_mut();
                if state.closing {
                    return Err(SessionError::Closed);
                }
                state
                    .task_waiters
                    .entry(id)
                    .or_default()
                    .push(callback_sender.borrow_mut().take().unwrap());
                Ok(None)
            })
        });
        TaskWaiter(Box::pin(async move {
            match waiter.await.map_err(HarnessError::Session)?.value {
                Some(record) => Ok(record),
                None => receiver.await.unwrap_or(Err(HarnessError::DriverStopped)),
            }
        }))
    }

    pub fn abort(&self, id: Id) -> HarnessAbortWaiter {
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

enum SchedulerEvent {
    Drain(Result<Vec<(Rc<Invocation>, TaskRegistry)>, SessionError>),
    Invocation(Id, Rc<Invocation>),
}
enum SchedulerAction {
    Drain(TaskRegistry),
    Event(SchedulerEvent),
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
        if runner.0.borrow().closing && !control.0.borrow().closing {
            control.seal(&runner);
        }
        let action = std::future::poll_fn(|cx| {
            if let Poll::Ready(Some(event)) = events.poll_next_unpin(cx) {
                return Poll::Ready(SchedulerAction::Event(event));
            }
            let mut state = control.0.borrow_mut();
            if state.closing && !state.draining && events.is_empty() {
                return Poll::Ready(SchedulerAction::Close);
            }
            if state.opened && state.dirty && !state.draining && !state.closing {
                state.dirty = false;
                state.draining = true;
                return Poll::Ready(SchedulerAction::Drain(state.registry.clone()));
            }
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        })
        .await;
        match action {
            SchedulerAction::Drain(registry) => {
                let session = session.clone();
                let runner = runner.clone();
                let enabled = control.0.borrow().enabled;
                events.push(Box::pin(async move {
                    SchedulerEvent::Drain(reserve_all(&session, &runner, registry, enabled).await)
                }));
            }
            SchedulerAction::Event(SchedulerEvent::Drain(result)) => {
                control.0.borrow_mut().draining = false;
                match result {
                    Ok(reservations) if !control.0.borrow().closing => {
                        for (invocation, registry) in reservations {
                            let session = session.clone();
                            events.push(Box::pin(async move {
                                let id = invocation.id;
                                let _ = phase::execute_reserved(
                                    &session,
                                    &registry,
                                    invocation.clone(),
                                )
                                .await;
                                SchedulerEvent::Invocation(id, invocation)
                            }));
                        }
                    }
                    Ok(reservations) => {
                        let mut state = runner.0.borrow_mut();
                        for (invocation, _) in reservations {
                            invocation.end();
                            state.actives.remove(&invocation.id);
                        }
                    }
                    Err(_) => {}
                }
                control.wake();
            }
            SchedulerAction::Event(SchedulerEvent::Invocation(id, invocation)) => {
                invocation.end();
                let mut state = runner.0.borrow_mut();
                if state
                    .actives
                    .get(&id)
                    .and_then(Weak::upgrade)
                    .is_some_and(|active| Rc::ptr_eq(&active, &invocation))
                {
                    state.actives.remove(&id);
                }
                drop(state);
                let mut state = control.0.borrow_mut();
                state.dirty = true;
                let waker = state.waker.take();
                drop(state);
                if let Some(waker) = waker {
                    waker.wake();
                }
            }
            SchedulerAction::Close => break,
        }
    }
    let result = session.close().await;
    runner.0.borrow_mut().finished = true;
    control.0.borrow_mut().finished = true;
    result
}

async fn reserve_all(
    session: &Session,
    runner: &Rc<RunnerControl>,
    registry: TaskRegistry,
    enabled: bool,
) -> Result<Vec<(Rc<Invocation>, TaskRegistry)>, SessionError> {
    let tentative = Rc::new(RefCell::new(Vec::<Rc<Invocation>>::new()));
    let callback_tentative = tentative.clone();
    let callback_runner = runner.clone();
    let definitions = registry.clone();
    let receipt = session
        .commit(move |tx| {
            Box::pin(async move {
                let mut tree = tx.tree().await?;
                let before = tree.tasks.clone();
                let mut scopes = BTreeMap::<Id, BTreeSet<Id>>::new();
                for task in tree.tasks.values() {
                    if task.status() == TaskStatus::Terminal {
                        continue;
                    }
                    if let Some(root) = tree.root(task.id)
                        && let Some(scope) = tree.scope(root)
                    {
                        scopes.entry(root).or_insert(scope);
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
                Ok(selected)
            })
        })
        .await;
    match receipt {
        Ok(receipt) => Ok(receipt
            .value
            .into_iter()
            .map(|invocation| (invocation, registry.clone()))
            .collect()),
        Err(error) => {
            let mut state = runner.0.borrow_mut();
            for invocation in tentative.borrow_mut().drain(..) {
                invocation.end();
                if state
                    .actives
                    .get(&invocation.id)
                    .and_then(Weak::upgrade)
                    .is_some_and(|active| Rc::ptr_eq(&active, &invocation))
                {
                    state.actives.remove(&invocation.id);
                }
            }
            Err(error)
        }
    }
}
