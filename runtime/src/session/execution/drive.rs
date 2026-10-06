//! A run request owns one tree until terminal, close, error, or quiescence.
use super::*;

pub(in crate::session) struct Drive {
    pub root: Id,
    pub ended: Cell<bool>,
    pub runner: Weak<RunnerControl>,
}
impl Drive {
    pub fn live(&self) -> bool {
        !self.ended.get()
            && self.runner.upgrade().is_some_and(|runner| {
                let state = runner.0.borrow();
                !state.finished
                    && !state.dropped
                    && state
                        .drive
                        .upgrade()
                        .is_some_and(|d| std::ptr::eq(d.as_ref(), self))
            })
    }
    pub fn check(&self) -> Result<(), SessionError> {
        if self.live() {
            Ok(())
        } else {
            Err(SessionError::Invalid(
                "Task drive ended before persistence".into(),
            ))
        }
    }
}
// A pass carries at most one detached root receipt; no extra allocation is needed.
#[allow(clippy::large_enum_variant)]
enum Pass {
    Run(Id),
    Done(RunResult),
}
pub(super) async fn execute(
    session: &Session,
    registry: &TaskRegistry,
    drive: Rc<Drive>,
) -> Result<RunResult, RunError> {
    let mut first = true;
    loop {
        let owner = drive.runner.upgrade().ok_or(RunError::DriverStopped)?;
        if owner.0.borrow().closing {
            return Ok(RunResult::Interrupted);
        }
        let d = drive.clone();
        let runner = owner.clone();
        let definitions = registry.clone();
        let control = Rc::downgrade(&session.control);
        let pass = session
            .commit(move |tx| {
                Box::pin(async move {
                    tx.drive_fence(d.clone());
                    d.check()?;
                    // Admission is eager, but a queued system callback may not
                    // start reconciliation after close. Once past this entry
                    // gate, graceful close does not revoke its persistence fence.
                    {
                        let state = runner.0.borrow();
                        if let Some(error) = &state.session_error {
                            return Err(error.clone());
                        }
                        if state.closing {
                            return Ok(Pass::Done(RunResult::Interrupted));
                        }
                    }
                    let mut tree = tx.tree().await?;
                    d.check()?;
                    let Some(root) = tree.tasks.get(&d.root) else {
                        return Ok(Pass::Done(RunResult::Blocked(BlockReason::MissingTask)));
                    };
                    if first && root.status() == TaskStatus::Terminal {
                        return Ok(Pass::Done(RunResult::Blocked(BlockReason::NotPending)));
                    }
                    let Some(scope) = tree.scope(d.root) else {
                        return Ok(Pass::Done(RunResult::Blocked(
                            BlockReason::UnsupportedScope,
                        )));
                    };
                    // A Running record without our invocation requires explicit startup recovery.
                    if scope
                        .iter()
                        .any(|id| tree.tasks[id].status() == TaskStatus::Running)
                    {
                        return Ok(Pass::Done(RunResult::Blocked(BlockReason::NotPending)));
                    }
                    control
                        .upgrade()
                        .ok_or(SessionError::DriverStopped)?
                        .borrow_mut()
                        .tree = Rc::downgrade(&d);
                    let before = tree.tasks.clone();
                    tree.reconcile(&scope);
                    let mut selected = None;
                    let mut blocked = None;
                    for id in std::iter::once(d.root)
                        .chain(scope.iter().copied().filter(|id| *id != d.root))
                    {
                        let record = &tree.tasks[&id];
                        if record.status() != TaskStatus::Pending
                            || (record.abort_requested && tree.owned_live(id))
                        {
                            continue;
                        }
                        let definition = definitions.0.get(&record.kind);
                        let reason = if definition.is_none() {
                            Some(BlockReason::MissingDefinition)
                        } else if definition.is_some_and(|d| d.version != record.version) {
                            Some(BlockReason::VersionMismatch)
                        } else {
                            None
                        };
                        if reason.is_some() && !record.abort_requested {
                            if id == d.root {
                                blocked = reason;
                            }
                            continue;
                        }
                        selected = Some(id);
                        break;
                    }
                    tree.stage_changes(tx, &before).await?;
                    let root = tree.tasks[&d.root].clone();
                    Ok(if root.status() == TaskStatus::Terminal {
                        Pass::Done(RunResult::Terminal(root))
                    } else if let Some(id) = selected {
                        Pass::Run(id)
                    } else if scope.len() == 1
                        && before == tree.tasks
                        && let Some(reason) = blocked
                    {
                        Pass::Done(RunResult::Blocked(reason))
                    } else {
                        Pass::Done(RunResult::Suspended(root))
                    })
                })
            })
            .await;
        let pass = match pass {
            Ok(receipt) => receipt.value,
            Err(SessionError::Closed) => return Ok(RunResult::Interrupted),
            Err(error) => return Err(error.into()),
        };
        first = false;
        let Pass::Run(id) = pass else {
            let Pass::Done(result) = pass else {
                unreachable!()
            };
            return Ok(result);
        };
        if owner.0.borrow().closing {
            return Ok(RunResult::Interrupted);
        }
        let invocation = Rc::new(Invocation {
            id,
            abort_mode: false,
            joined: RefCell::new(Vec::new()),
            ended: Cell::new(false),
            cancelled: Cell::new(false),
            waiters: RefCell::new(Vec::new()),
            runner: Rc::downgrade(&owner),
        });
        owner.0.borrow_mut().active = Rc::downgrade(&invocation);
        let result = phase::execute(session, registry, invocation.clone()).await;
        let active = owner.0.borrow().active.upgrade();
        if let Some(active) = active {
            active.end();
        }
        invocation.end();
        match result? {
            RunResult::Terminal(record) if record.id == drive.root => {
                return Ok(RunResult::Terminal(record));
            }
            RunResult::Terminal(_) | RunResult::Suspended(_) => {}
            // Abort or a host mutation can overtake selection. A blocked reservation
            // is observable, rather than spinning or silently resuming Running.
            result => return Ok(result),
        }
    }
}
