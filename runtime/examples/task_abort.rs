//! Durable abort of an explicit foreground leaf task.
//! Run with: cargo run -p publicworks-runtime --example task_abort
use futures_lite::future::{block_on, zip};
use publicworks_runtime::*;
use serde_json::json;
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    rc::Rc,
    task::Waker,
};

/// A small local-executor signal. The RefCell borrow is released before wake,
/// so a synchronously polled waiter cannot re-enter while it is borrowed.
#[derive(Clone, Default)]
struct Signal(Rc<RefCell<(bool, Option<Waker>)>>);
impl Signal {
    async fn wait(&self) {
        std::future::poll_fn(|cx| {
            let mut state = self.0.borrow_mut();
            if state.0 {
                std::task::Poll::Ready(())
            } else {
                state.1 = Some(cx.waker().clone());
                std::task::Poll::Pending
            }
        })
        .await
    }

    fn fire(&self) {
        let waker = {
            let mut state = self.0.borrow_mut();
            state.0 = true;
            state.1.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

fn handler_error(message: impl Into<String>) -> TaskOutcomeError {
    TaskOutcomeError {
        message: message.into(),
        detail: None,
    }
}

fn definition(normal_started: Signal, normal_joined: Rc<Cell<bool>>) -> TaskDefinition {
    let mut phases: BTreeMap<String, PhaseHandler> = BTreeMap::new();
    phases.insert(
        "work".into(),
        Rc::new(move |_, runtime| {
            let normal_started = normal_started.clone();
            let normal_joined = normal_joined.clone();
            Box::pin(async move {
                normal_started.fire();
                runtime.cancelled().await;
                normal_joined.set(true);
                // Once abort is durable, a late normal-handler error is ignored.
                Err(handler_error("normal work stopped cooperatively"))
            })
        }),
    );

    let abort_handler: PhaseHandler = Rc::new(|_, runtime| {
        Box::pin(async move {
            runtime
                .commit(|tx, task| {
                    Box::pin(async move {
                        tx.append_entry(task.conversation_id, EntryDraft::new("abort cleanup"))
                            .await?;
                        Ok(Some(TaskUpdate::Abort {
                            reason: Some("operator request".into()),
                            result: None,
                        }))
                    })
                })
                .await
                .map_err(|error| handler_error(error.to_string()))?;
            Ok(())
        })
    });

    TaskDefinition::new("abort-example", 1, |_| Ok(json!({"phase":"work"})), phases)
        .with_abort_handler(abort_handler)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    block_on(async {
        let normal_started = Signal::default();
        let normal_joined = Rc::new(Cell::new(false));
        let definition = definition(normal_started.clone(), normal_joined.clone());
        let (session, session_driver) = Session::new(MemoryStorage::new());

        let (result, ()) = zip(
            async {
                let work = async {
                    let registry = TaskRegistry::new([definition.clone()])?;
                    let (runner, task_driver) = TaskRunner::attach(&session, registry)?;
                    let (result, ()) = zip(
                        async {
                            let command = async {
                                let task = session
                                    .commit(move |tx| {
                                        Box::pin(async move {
                                            let conversation = tx.create_conversation().await?;
                                            tx.create_task(
                                                definition,
                                                json!(null),
                                                TaskOptions {
                                                    conversation_id: Some(conversation.id),
                                                    ownership: TaskOwnership::Conversation,
                                                    background: false,
                                                },
                                            )
                                            .await
                                        })
                                    })
                                    .await
                                    .map_err(RunError::Session)?
                                    .value;

                                let run = runner.run(task.id);
                                normal_started.wait().await;
                                let acknowledgement = runner.abort(task.id).await?;
                                // The acknowledgement waits for the observed normal invocation,
                                // not for the abort handler. The run waits through cleanup.
                                assert_eq!(acknowledgement, AbortResult::Marked);
                                assert!(normal_joined.get());
                                run.await
                            }
                            .await;

                            // Always close the runner before closing its Session. Preserve a
                            // command error while still performing both cleanup steps.
                            let runner_closed = runner.close().await;
                            let result = command?;
                            runner_closed?;
                            Ok::<_, RunError>(result)
                        },
                        task_driver,
                    )
                    .await;
                    result
                }
                .await;

                let session_closed = session.close().await;
                let result = work?;
                session_closed.map_err(RunError::Session)?;
                println!("{result:#?}");
                Ok::<_, RunError>(())
            },
            session_driver,
        )
        .await;
        result.map_err(Into::into)
    })
}
