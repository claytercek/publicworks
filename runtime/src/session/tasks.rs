use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskOwnership {
    Conversation,
    Task(Id),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Owner {
    Ownerless,
    Task(Id),
}
#[derive(Clone, Debug)]
pub struct TaskOptions {
    pub ownership: TaskOwnership,
    pub conversation_id: Option<Id>,
    pub background: bool,
}
impl Session {
    /// Admit Running -> Pending normalization before returning any usable handle.
    /// Poll the driver concurrently. Dropping the waiter requests close, but the
    /// admitted normalization and cleanup remain owned by the driver.
    pub fn open_recovered(
        storage: impl Storage + 'static,
    ) -> (CommitWaiter<Session>, SessionDriver) {
        let (session, driver) = Self::new(storage);
        let normalization = session.commit_owned(
            |tx| {
                Box::pin(async move {
                    let mut cursor = None;
                    let mut records = Vec::new();
                    loop {
                        let page = tx
                            .scan_tasks(
                                TaskQuery {
                                    status: Some(TaskStatus::Running),
                                    ..TaskQuery::default()
                                },
                                128,
                                cursor,
                            )
                            .await?;
                        records.extend(page.items);
                        cursor = page.next;
                        if cursor.is_none() {
                            break;
                        }
                    }
                    // All reads precede writes: changing status cannot disturb pagination.
                    for mut record in records {
                        if let TaskState::Running { checkpoint } = record.state {
                            record.state = TaskState::Pending { checkpoint };
                            tx.set_task(record).await?;
                        }
                    }
                    Ok(())
                })
            },
            true,
        );
        let waiter = CommitWaiter(Box::pin(async move {
            // This observer is outside the job queue: awaiting close here cannot
            // deadlock the driver's drain. The job requests close independently,
            // so dropping this observer never cancels cleanup. Hold the original
            // error/panic until cleanup settles, even if close itself fails.
            match AssertUnwindSafe(normalization).catch_unwind().await {
                Ok(Ok(receipt)) => Ok(CommitReceipt {
                    value: session,
                    seq: receipt.seq,
                }),
                Ok(Err(error)) => {
                    let _ = session.close().await;
                    Err(error)
                }
                Err(panic) => {
                    let _ = session.close().await;
                    resume_unwind(panic)
                }
            }
        }));
        (waiter, driver)
    }
}
