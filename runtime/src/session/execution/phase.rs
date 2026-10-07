//! Reservation and phase-boundary decisions always run on the Session line.
use super::*;

enum Decision {
    Ready(TaskRecord, TaskDefinition),
    Done(RunResult),
}
pub(super) async fn execute(
    session: &Session,
    registry: &TaskRegistry,
    invocation: Rc<Invocation>,
) -> Result<RunResult, RunError> {
    execute_inner(session, registry, invocation, false).await
}

pub(super) async fn execute_reserved(
    session: &Session,
    registry: &TaskRegistry,
    invocation: Rc<Invocation>,
) -> Result<RunResult, RunError> {
    execute_inner(session, registry, invocation, true).await
}

async fn execute_inner(
    session: &Session,
    registry: &TaskRegistry,
    mut invocation: Rc<Invocation>,
    already_reserved: bool,
) -> Result<RunResult, RunError> {
    if !already_reserved {
        let definitions = registry.clone();
        let inv = invocation.clone();
        let reservation = session
            .commit_reservation(invocation.clone(), move |tx| {
                Box::pin(async move {
                    tx.fence(inv.clone(), false);
                    if inv.check().is_err() {
                        return Ok(Some(RunResult::Interrupted));
                    }
                    let Some(mut record) = tx.task(inv.id).await? else {
                        return Ok(Some(RunResult::Blocked(BlockReason::MissingTask)));
                    };
                    if record.status() == TaskStatus::Terminal {
                        // Initial terminal roots were blocked by the drive. Here an
                        // acknowledged abort may have orphaned a selected task.
                        return Ok(Some(RunResult::Terminal(record)));
                    }
                    if record.status() != TaskStatus::Pending {
                        return Ok(Some(RunResult::Blocked(BlockReason::NotPending)));
                    }
                    if !supported(tx, &record).await? {
                        return Ok(Some(RunResult::Blocked(BlockReason::UnsupportedScope)));
                    }
                    if record.abort_requested && tx.tree().await?.owned_live(record.id) {
                        return Ok(Some(RunResult::Suspended(record)));
                    }
                    let definition = definitions.0.get(&record.kind);
                    let missing = definition.is_none();
                    let mismatched = definition.is_some_and(|d| d.version != record.version);
                    if missing || mismatched {
                        if record.abort_requested {
                            record.state = TaskState::Terminal {
                                outcome: TaskOutcome::Orphaned {
                                    reason: "Missing task definition or exact version for abort"
                                        .into(),
                                },
                            };
                            record.memos = None;
                            tx.set_task(record.clone()).await?;
                            return Ok(Some(RunResult::Terminal(record)));
                        }
                        return Ok(Some(RunResult::Blocked(if missing {
                            BlockReason::MissingDefinition
                        } else {
                            BlockReason::VersionMismatch
                        })));
                    }
                    if inv.check().is_err() {
                        return Ok(Some(RunResult::Interrupted));
                    }
                    let TaskState::Pending { checkpoint } = record.state else {
                        unreachable!()
                    };
                    record.state = TaskState::Running { checkpoint };
                    tx.set_task(record).await?;
                    Ok(None)
                })
            })
            .await?
            .value;
        if let Some(result) = reservation {
            return Ok(result);
        }
    }
    loop {
        // A fresh Session-line decision is essential: abort can be queued behind
        // reservation while its storage acknowledgement is still pending.
        let inv = invocation.clone();
        let definitions = registry.clone();
        let dispatch = session
            .commit_join(invocation.clone(), move |tx| {
                Box::pin(async move {
                    tx.fence(inv.clone(), true);
                    let mut record = tx
                        .task(inv.id)
                        .await?
                        .ok_or_else(|| SessionError::Invalid("Task disappeared".into()))?;
                    if record.status() == TaskStatus::Terminal {
                        inv.end();
                        return Ok(Decision::Done(RunResult::Terminal(record)));
                    }
                    if inv.check().is_err() || record.status() != TaskStatus::Running {
                        inv.end();
                        return Ok(Decision::Done(RunResult::Interrupted));
                    }
                    if record.abort_requested && tx.tree().await?.owned_live(record.id) {
                        if let TaskState::Running { checkpoint } = record.state {
                            record.state = TaskState::Pending { checkpoint };
                        }
                        tx.set_task(record.clone()).await?;
                        inv.end();
                        return Ok(Decision::Done(RunResult::Suspended(record)));
                    }
                    let definition = definitions
                        .0
                        .get(&record.kind)
                        .filter(|d| d.version == record.version)
                        .cloned()
                        .ok_or_else(|| {
                            SessionError::Invalid("Running task definition changed".into())
                        })?;
                    Ok(Decision::Ready(record, definition))
                })
            })
            .await;
        let (current, definition) = match dispatch {
            Ok(receipt) => match receipt.value {
                Decision::Done(result) => return Ok(result),
                Decision::Ready(record, definition) => (record, definition),
            },
            Err(SessionError::Closed) => return Ok(RunResult::Interrupted),
            Err(error) => return Err(error.into()),
        };
        if invocation.check().is_err() {
            return Ok(RunResult::Interrupted);
        }
        if invocation.cancelled.get() && !invocation.abort_mode && !current.abort_requested {
            continue; // A durable abort acknowledgement overtook the dispatch receipt.
        }
        if current.abort_requested && !invocation.abort_mode {
            invocation = abort::handoff(session, &invocation);
        }
        let runtime = TaskRuntime {
            session: session.clone(),
            invocation: invocation.clone(),
            conversation_id: current.conversation_id,
        };
        let TaskState::Running { checkpoint } = &current.state else {
            unreachable!()
        };
        let previous = checkpoint.clone();
        let handler = if invocation.abort_mode {
            Some(definition.abort_handler.clone())
        } else {
            checkpoint
                .as_object()
                .and_then(|v| v.get("phase"))
                .and_then(Value::as_str)
                .and_then(|phase| definition.phases.get(phase))
                .cloned()
        };
        if invocation.check().is_err() {
            return Ok(RunResult::Interrupted);
        }
        let failure = if let Some(handler) = handler {
            match AssertUnwindSafe(async { handler(current, runtime).await })
                .catch_unwind()
                .await
            {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error),
                Err(panic) => Some(fault(panic_message(&*panic))),
            }
        } else {
            Some(fault("Task checkpoint has a malformed or unknown phase"))
        };
        let inv = invocation.clone();
        // Join every admitted runtime/abort mutation, including dropped waiters.
        let decision = session
            .commit_join(invocation.clone(), move |tx| {
                Box::pin(async move {
                    tx.fence(inv.clone(), true);
                    let mut record = tx
                        .task(inv.id)
                        .await?
                        .ok_or_else(|| SessionError::Invalid("Task disappeared".into()))?;
                    if record.status() != TaskStatus::Running {
                        inv.end();
                        return Ok(Some(if record.status() == TaskStatus::Terminal {
                            RunResult::Terminal(record)
                        } else {
                            RunResult::Suspended(record)
                        }));
                    }
                    if inv.check().is_err() {
                        inv.end();
                        return Ok(Some(RunResult::Interrupted));
                    }
                    // Durable intent suppresses even a normal handler's error.
                    if record.abort_requested && !inv.abort_mode {
                        return Ok(None);
                    }
                    let TaskState::Running { checkpoint } = &record.state else {
                        unreachable!()
                    };
                    let failure = failure.or_else(|| {
                        if inv.abort_mode {
                            Some(fault(
                                "Task abort handler returned without a durable outcome",
                            ))
                        } else {
                            json_equal(checkpoint, &previous)
                                .then(|| fault("Task phase returned without durable progress"))
                        }
                    });
                    if let Some(error) = failure {
                        let held = tx.tree().await?.owned_live(record.id);
                        let outcome = TaskOutcome::Faulted { error };
                        record.state = if held {
                            TaskState::Completing { outcome }
                        } else {
                            TaskState::Terminal { outcome }
                        };
                        record.memos = None;
                        tx.set_task(record.clone()).await?;
                        inv.end();
                        Ok(Some(if held {
                            RunResult::Suspended(record)
                        } else {
                            RunResult::Terminal(record)
                        }))
                    } else {
                        Ok(None)
                    }
                })
            })
            .await;
        match decision {
            Ok(receipt) => {
                if let Some(result) = receipt.value {
                    return Ok(result);
                }
            }
            Err(SessionError::Closed) => return Ok(RunResult::Interrupted),
            Err(error) => return Err(error.into()),
        }
    }
}
pub(super) async fn supported(tx: &Tx, record: &TaskRecord) -> Result<bool, SessionError> {
    let tree = tx.tree().await?;
    Ok(tree
        .root(record.id)
        .and_then(|root| tree.scope(root))
        .is_some())
}

// Numeric equality follows JSON value semantics rather than integer/float encoding.
pub(super) fn json_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(a), Value::Number(b)) => {
            let integer = |n: &serde_json::Number| {
                n.as_i64()
                    .map(i128::from)
                    .or_else(|| n.as_u64().map(i128::from))
            };
            match (integer(a), integer(b)) {
                (Some(a), Some(b)) => a == b,
                (Some(i), None) | (None, Some(i)) => {
                    let f = if integer(a).is_none() {
                        a.as_f64()
                    } else {
                        b.as_f64()
                    }
                    .unwrap();
                    f.fract() == 0.0
                        && f >= i64::MIN as f64
                        && f < 18446744073709551616.0
                        && f as i128 == i
                }
                (None, None) => a.as_f64() == b.as_f64(),
            }
        }
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| json_equal(a, b))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(key, a)| b.get(key).is_some_and(|b| json_equal(a, b)))
        }
        _ => left == right,
    }
}
