//! Reservation and phase-boundary decisions always run on the Session line.
use super::*;

enum Reservation {
    Ready(TaskRecord, TaskDefinition),
    Done(RunResult),
}
pub(super) async fn execute(
    session: &Session,
    registry: &TaskRegistry,
    invocation: Rc<Invocation>,
) -> Result<RunResult, RunError> {
    let registry = registry.clone();
    let inv = invocation.clone();
    let control = Rc::downgrade(&session.control);
    let reservation = session
        .commit_reservation(invocation.clone(), move |tx| {
            Box::pin(async move {
                tx.fence(inv.clone(), false);
                if inv.check().is_err() {
                    return Ok(Reservation::Done(RunResult::Interrupted));
                }
                let Some(mut record) = tx.task(inv.id).await? else {
                    return Ok(Reservation::Done(RunResult::Blocked(
                        BlockReason::MissingTask,
                    )));
                };
                let blocked = if record.status() != TaskStatus::Pending {
                    Some(BlockReason::NotPending)
                } else if record.abort_requested {
                    Some(BlockReason::AbortRequested)
                } else if record.background || record.owner.is_some() {
                    Some(BlockReason::UnsupportedScope)
                } else {
                    None
                };
                if let Some(reason) = blocked {
                    return Ok(Reservation::Done(RunResult::Blocked(reason)));
                }
                let Some(definition) = registry.0.get(&record.kind).cloned() else {
                    return Ok(Reservation::Done(RunResult::Blocked(
                        BlockReason::MissingDefinition,
                    )));
                };
                if definition.version != record.version {
                    return Ok(Reservation::Done(RunResult::Blocked(
                        BlockReason::VersionMismatch,
                    )));
                }
                let conversation = tx.conversation(record.conversation_id).await?;
                if conversation.is_none_or(|c| c.owner.is_some())
                    || has_owned(tx, record.id).await?
                {
                    return Ok(Reservation::Done(RunResult::Blocked(
                        BlockReason::UnsupportedScope,
                    )));
                }
                if inv.check().is_err() {
                    return Ok(Reservation::Done(RunResult::Interrupted));
                }
                let TaskState::Pending { checkpoint } = record.state else {
                    unreachable!()
                };
                record.state = TaskState::Running { checkpoint };
                // Register on the mutation line before adoption; final assembly of every
                // Session transaction consults this guard, regardless of staging order.
                control
                    .upgrade()
                    .ok_or(SessionError::DriverStopped)?
                    .borrow_mut()
                    .leaf = Rc::downgrade(&inv);
                tx.set_task(record.clone()).await?;
                Ok(Reservation::Ready(record, definition))
            })
        })
        .await?
        .value;
    let Reservation::Ready(mut current, definition) = reservation else {
        let Reservation::Done(result) = reservation else {
            unreachable!()
        };
        return Ok(result);
    };
    let runtime = TaskRuntime {
        session: session.clone(),
        invocation: invocation.clone(),
        conversation_id: current.conversation_id,
    };
    loop {
        let TaskState::Running { checkpoint } = &current.state else {
            unreachable!()
        };
        let previous = checkpoint.clone();
        let phase = checkpoint
            .as_object()
            .and_then(|v| v.get("phase"))
            .and_then(Value::as_str);
        let handler = phase
            .and_then(|phase| definition.phases.get(phase))
            .cloned();
        let failure = if invocation.check().is_err() {
            None
        } else if let Some(handler) = handler {
            match AssertUnwindSafe(async { handler(current, runtime.clone()).await })
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
        // Eagerly queued behind every runtime commit, including abandoned waiters.
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
                        return Ok(Decision::Done(if record.status() == TaskStatus::Terminal {
                            RunResult::Terminal(record)
                        } else {
                            RunResult::Interrupted
                        }));
                    }
                    if inv.check().is_err() || record.abort_requested {
                        inv.end();
                        return Ok(Decision::Done(RunResult::Interrupted));
                    }
                    let TaskState::Running { checkpoint } = &record.state else {
                        unreachable!()
                    };
                    let failure = failure.or_else(|| {
                        json_equal(checkpoint, &previous)
                            .then(|| fault("Task phase returned without durable progress"))
                    });
                    if let Some(error) = failure {
                        record.state = TaskState::Terminal {
                            outcome: TaskOutcome::Faulted { error },
                        };
                        record.memos = None;
                        tx.set_task(record.clone()).await?;
                        inv.end();
                        Ok(Decision::Done(RunResult::Terminal(record)))
                    } else {
                        Ok(Decision::Continue(record))
                    }
                })
            })
            .await;
        match decision {
            Ok(receipt) => match receipt.value {
                Decision::Done(result) => return Ok(result),
                Decision::Continue(record) => current = record,
            },
            Err(SessionError::Closed) => return Ok(RunResult::Interrupted),
            Err(error) => return Err(error.into()),
        }
    }
}
enum Decision {
    Done(RunResult),
    Continue(TaskRecord),
}
async fn has_owned(tx: &Tx, task: Id) -> Result<bool, SessionError> {
    if !tx
        .scan_conversations(
            ConversationQuery {
                owner_task_id: Some(task),
                ..Default::default()
            },
            1,
            None,
        )
        .await?
        .items
        .is_empty()
    {
        return Ok(true);
    }
    let mut cursor = None;
    loop {
        let page = tx.scan_tasks(TaskQuery::default(), 128, cursor).await?;
        if page.items.iter().any(|record| record.owner == Some(task)) {
            return Ok(true);
        }
        cursor = page.next;
        if cursor.is_none() {
            return Ok(false);
        }
    }
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
