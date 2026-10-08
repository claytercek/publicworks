//! Committed submission observation. Admission and inbox policy live above Harness.
use super::*;

/// Cloneable, stateless view of one durable submission. Retains a Harness handle.
#[derive(Clone)]
pub struct Submission {
    id: Id,
    harness: Harness,
}
impl Submission {
    pub fn id(&self) -> Id {
        self.id
    }

    /// Read the current committed receipt without enabling scheduler progress.
    pub fn status(&self) -> impl Future<Output = Result<SubmissionRecord, HarnessError>> + use<> {
        self.read()
    }

    /// Alias for `status`; returned records are detached values, not cached state.
    pub fn read(&self) -> impl Future<Output = Result<SubmissionRecord, HarnessError>> + use<> {
        let id = self.id;
        let waiter = self.harness.commit(move |tx| {
            Box::pin(async move {
                tx.submission(id)
                    .await?
                    .ok_or_else(|| SessionError::Invalid("Submission does not exist".into()))
            })
        });
        async move { Ok(waiter.await.map_err(observation_error)?.value) }
    }

    /// Enable progress and observe a terminal committed receipt. Dropping the
    /// waiter cancels only observation, even before registration runs.
    pub fn wait(&self) -> SubmissionWaiter {
        self.harness.wait_submission(self.id)
    }

    /// Atomically withdraw a queued receipt and its inbox item. Does not resume
    /// scheduling or cancel a run that has already placed this submission.
    pub fn abort(&self) -> impl Future<Output = Result<WithdrawalResult, HarnessError>> + use<> {
        self.harness.withdraw_submission(self.id, None)
    }

    pub fn withdraw(&self) -> impl Future<Output = Result<WithdrawalResult, HarnessError>> + use<> {
        self.abort()
    }
}

fn observation_error(error: SessionError) -> HarnessError {
    match error {
        SessionError::Closed => HarnessError::Closed,
        SessionError::DriverStopped => HarnessError::DriverStopped,
        error => HarnessError::Session(error),
    }
}

pub(super) fn is_terminal(record: &SubmissionRecord) -> bool {
    matches!(
        record.status(),
        SubmissionStatus::Done | SubmissionStatus::Unanswered
    )
}

pub struct SubmissionWaiter {
    registration: Option<Registration<Id, SubmissionRecord>>,
    future: LocalFuture<'static, Result<SubmissionRecord, HarnessError>>,
}
impl Future for SubmissionWaiter {
    type Output = Result<SubmissionRecord, HarnessError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = self.future.as_mut().poll(cx);
        if result.is_ready() {
            self.registration = None;
        }
        result
    }
}

impl Harness {
    /// Reacquire a committed receipt without enabling scheduler progress.
    pub fn submission(
        &self,
        id: Id,
    ) -> impl Future<Output = Result<Option<Submission>, HarnessError>> + use<> {
        self.submission_in(id, None)
    }

    fn submission_in(
        &self,
        id: Id,
        conversation: Option<Id>,
    ) -> impl Future<Output = Result<Option<Submission>, HarnessError>> + use<> {
        let harness = self.clone();
        let waiter = self.commit(move |tx| {
            Box::pin(async move {
                Ok(tx.submission(id).await?.is_some_and(|record| {
                    conversation.is_none_or(|id| id == record.conversation_id)
                }))
            })
        });
        async move {
            Ok(waiter
                .await
                .map_err(observation_error)?
                .value
                .then_some(Submission { id, harness }))
        }
    }

    /// Withdraw on the Session line, optionally restricting conversation identity.
    /// Like the reference abort path, this does not enable scheduler progress.
    pub fn withdraw_submission(
        &self,
        id: Id,
        conversation: Option<Id>,
    ) -> impl Future<Output = Result<WithdrawalResult, HarnessError>> + use<> {
        let waiter = self.commit(move |tx| {
            Box::pin(async move { tx.withdraw_submission(id, conversation).await })
        });
        async move { Ok(waiter.await.map_err(observation_error)?.value) }
    }

    fn wait_submission(&self, id: Id) -> SubmissionWaiter {
        if let Err(error) = self.resume() {
            return SubmissionWaiter {
                future: Box::pin(async move { Err(error) }),
                registration: None,
            };
        }
        let registration =
            Registration::new(&self.control, id, |state| &mut state.submission_waiters);
        let key = registration.key;
        let callback_cancelled = registration.cancelled.clone();
        let (sender, receiver) = oneshot::channel();
        let control = self.control.clone();
        let waiter = self.session.commit(move |tx| {
            Box::pin(async move {
                let record = tx
                    .submission(id)
                    .await?
                    .ok_or_else(|| SessionError::Invalid("Submission does not exist".into()))?;
                // The storage read can yield. Recheck close on the same line as
                // terminal detection and registration, before releasing the Tx.
                let mut state = control.0.borrow_mut();
                if state.closing {
                    return Err(SessionError::Closed);
                }
                if is_terminal(&record) {
                    return Ok(Some(record));
                }
                if !callback_cancelled.get() {
                    state
                        .submission_waiters
                        .entry(id)
                        .or_default()
                        .insert(key, sender);
                }
                Ok(None)
            })
        });
        SubmissionWaiter {
            future: Box::pin(async move {
                match waiter.await.map_err(observation_error)?.value {
                    Some(record) => Ok(record),
                    None => receiver.await.unwrap_or(Err(HarnessError::DriverStopped)),
                }
            }),
            registration: Some(registration),
        }
    }
}

impl ConversationHandle {
    /// Read-only reacquisition restricted to this conversation.
    pub fn submission(
        &self,
        id: Id,
    ) -> impl Future<Output = Result<Option<Submission>, HarnessError>> + use<> {
        self.harness.submission_in(id, Some(self.id))
    }

    /// Atomic withdrawal restricted to this conversation; no run cancellation.
    pub fn withdraw_submission(
        &self,
        id: Id,
    ) -> impl Future<Output = Result<WithdrawalResult, HarnessError>> + use<> {
        self.harness.withdraw_submission(id, Some(self.id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::future::{block_on, zip};

    #[test]
    fn dropped_observers_leave_no_registration_before_or_after_admission() {
        block_on(async {
            let (opening, driver) = Harness::open(MemoryStorage::new(), TaskRegistry::default());
            zip(
                async move {
                    let harness = opening.await.unwrap();
                    let id = harness
                        .commit(|tx| {
                            Box::pin(async move {
                                let conversation = tx.create_conversation().await?;
                                Ok(tx
                                    .create_submission(conversation.id, SubmissionType::Input, None)
                                    .await?
                                    .id)
                            })
                        })
                        .await
                        .unwrap()
                        .value;
                    let submission = harness.submission(id).await.unwrap().unwrap();
                    drop(submission.wait());
                    harness
                        .commit(|_| Box::pin(async { Ok(()) }))
                        .await
                        .unwrap();
                    assert!(harness.control.0.borrow().submission_waiters.is_empty());

                    let first = submission.wait();
                    let second = submission.wait();
                    harness
                        .commit(|_| Box::pin(async { Ok(()) }))
                        .await
                        .unwrap();
                    assert_eq!(harness.control.0.borrow().submission_waiters[&id].len(), 2);
                    drop(first);
                    assert_eq!(harness.control.0.borrow().submission_waiters[&id].len(), 1);
                    drop(second);
                    assert!(harness.control.0.borrow().submission_waiters.is_empty());
                    assert_eq!(
                        submission.status().await.unwrap().status(),
                        SubmissionStatus::Queued
                    );
                    harness.close().await.unwrap();
                },
                driver,
            )
            .await;
        });
    }
}
