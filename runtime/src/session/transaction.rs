use super::*;
use futures_util::future::{Either, select};
use std::collections::BTreeMap;
mod tasks;
use tasks::TaskCandidate;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Head {
    SelfEntry,
    Entry(Id),
}

/// Input without caller-selected identity or task attribution.
#[derive(Clone, Debug)]
pub struct EntryDraft {
    pub kind: String,
    pub model: Option<Vec<Value>>,
    pub data: Option<Value>,
    pub head: Option<Head>,
    pub edits: Option<Vec<ContextEdit>>,
}
impl EntryDraft {
    pub fn new(kind: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            model: None,
            data: None,
            head: None,
            edits: None,
        }
    }
}

type Operation = Box<dyn for<'a> FnOnce(&'a mut dyn Storage) -> LocalFuture<'a, ()>>;
#[derive(Default)]
struct State {
    writes: Vec<StorageWrite>,
    tasks: BTreeMap<Id, TaskCandidate>,
    operations: VecDeque<Operation>,
    pending: usize,
    mutated: bool,
    read_only: bool,
    sealed: bool,
    waker: Option<Waker>,
    by_task_id: Option<Id>,
    fence: Option<execution::PersistenceFence>,
}
impl State {
    fn open(&self) -> Result<(), SessionError> {
        if self.sealed {
            Err(SessionError::TransactionSettled)
        } else {
            Ok(())
        }
    }
}

/// Callback-scoped transaction. Methods admit operations synchronously, including
/// mutation attempts whose futures are never polled. Adapter operations belong
/// to the driver, not to disposable method waiters. Reads are committed-only.
pub struct Tx {
    state: Rc<RefCell<State>>,
}
impl Tx {
    pub(super) fn fence(&self, invocation: Rc<execution::Invocation>, ending: bool) {
        self.state.borrow_mut().fence =
            Some(execution::PersistenceFence::Invocation { invocation, ending });
    }
    pub(super) fn drive_fence(&self, drive: Rc<execution::drive::Drive>) {
        self.state.borrow_mut().fence = Some(execution::PersistenceFence::Drive(drive));
    }
    pub(super) fn tree(&self) -> TxFuture<'_, execution::tree::Tree> {
        self.operation(false, move |storage, _| {
            Box::pin(execution::tree::Tree::load(storage))
        })
    }
    pub(super) fn attribute(&self, task: Id) {
        self.state.borrow_mut().by_task_id = Some(task);
    }
    pub(super) fn make_read_only(&self) {
        self.state.borrow_mut().read_only = true;
    }

    fn operation<T: 'static, F>(&self, mutation: bool, operation: F) -> TxFuture<'_, T>
    where
        F: for<'a> FnOnce(&'a mut dyn Storage, Rc<RefCell<State>>) -> TxFuture<'a, T> + 'static,
    {
        let mut state = self.state.borrow_mut();
        let admission = state.open().and_then(|()| {
            if mutation {
                state.mutated = true;
                if state.read_only {
                    Err(SessionError::Invalid(
                        "TaskRuntime read callbacks cannot mutate".into(),
                    ))
                } else {
                    Ok(())
                }
            } else if state.mutated {
                Err(SessionError::ReadAfterWrite)
            } else {
                Ok(())
            }
        });
        if let Err(error) = admission {
            return Box::pin(async { Err(error) });
        }
        let (sender, receiver) = oneshot::channel();
        let owner = self.state.clone();
        state.pending += 1;
        state.operations.push_back(Box::new(move |storage| {
            Box::pin(async move {
                let result = AssertUnwindSafe(async {
                    owner.borrow().open()?;
                    operation(storage, owner.clone()).await
                })
                .catch_unwind()
                .await;
                owner.borrow_mut().pending -= 1;
                let _ = sender.send(result);
            })
        }));
        let waker = state.waker.take();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
        Box::pin(receive(receiver))
    }

    pub fn create_conversation(&self) -> TxFuture<'_, ConversationRecord> {
        self.create_conversation_owned(Owner::Ownerless)
    }
    pub fn fork_conversation(&self, parent: Id, at: Id) -> TxFuture<'_, ConversationRecord> {
        self.fork_conversation_owned(parent, at, Owner::Ownerless)
    }
    pub fn create_conversation_owned(&self, owner: Owner) -> TxFuture<'_, ConversationRecord> {
        self.stage_conversation(None, owner)
    }
    pub fn fork_conversation_owned(
        &self,
        parent: Id,
        at: Id,
        owner: Owner,
    ) -> TxFuture<'_, ConversationRecord> {
        self.stage_conversation(
            Some(ParentLink {
                conversation_id: parent,
                at,
            }),
            owner,
        )
    }
    fn stage_conversation(
        &self,
        parent: Option<ParentLink>,
        owner: Owner,
    ) -> TxFuture<'_, ConversationRecord> {
        self.operation(true, move |storage, state| {
            Box::pin(async move {
                let id = storage.mint_id().await?;
                state.borrow().open()?;
                if let Some(parent) = &parent {
                    if storage
                        .visible_entry(parent.conversation_id, parent.at)
                        .await?
                        .is_none()
                    {
                        return Err(SessionError::Invalid(format!(
                            "Entry {} is not visible from conversation {}",
                            parent.at, parent.conversation_id
                        )));
                    }
                    state.borrow().open()?;
                }
                let owner = match owner {
                    Owner::Ownerless => None,
                    Owner::Task(task_id) => {
                        let task = tasks::current_task(storage, &state, task_id)
                            .await?
                            .ok_or_else(|| {
                                SessionError::Invalid(format!("Unknown owner task: {task_id}"))
                            })?;
                        state.borrow().open()?;
                        Some(OwnerLink {
                            conversation_id: task.conversation_id,
                            task_id,
                        })
                    }
                };
                let record = ConversationRecord { id, parent, owner };
                state
                    .borrow_mut()
                    .writes
                    .push(StorageWrite::Conversation(record.clone()));
                Ok(record)
            })
        })
    }
    pub fn append_entry(&self, conversation: Id, draft: EntryDraft) -> TxFuture<'_, EntryRecord> {
        self.operation(true, move |storage, state| {
            Box::pin(async move {
                let staged = state.borrow().writes.iter().any(|write|
                matches!(write, StorageWrite::Conversation(record) if record.id == conversation));
                if !staged && storage.conversation(conversation).await?.is_none() {
                    return Err(SessionError::Invalid(format!(
                        "Unknown conversation: {conversation}"
                    )));
                }
                state.borrow().open()?;
                let id = storage.mint_id().await?;
                state.borrow().open()?;
                let entry = EntryRecord {
                    id,
                    conversation_id: conversation,
                    kind: draft.kind,
                    model: draft.model,
                    data: draft.data,
                    edits: draft.edits,
                    by_task_id: state.borrow().by_task_id,
                    head: draft.head.map(|head| match head {
                        Head::SelfEntry => id,
                        Head::Entry(id) => id,
                    }),
                };
                entry.validate_payloads()?;
                state
                    .borrow_mut()
                    .writes
                    .push(StorageWrite::Entry(entry.clone()));
                Ok(entry)
            })
        })
    }
    pub fn conversation(&self, id: Id) -> TxFuture<'_, Option<ConversationRecord>> {
        self.operation(false, move |storage, _| {
            Box::pin(async move { Ok(storage.conversation(id).await?) })
        })
    }
    pub fn entry(&self, id: Id) -> TxFuture<'_, Option<StoredEntry>> {
        self.operation(false, move |storage, _| {
            Box::pin(async move { Ok(storage.entry(id).await?) })
        })
    }
    pub fn visible_entry(&self, conversation: Id, id: Id) -> TxFuture<'_, Option<StoredEntry>> {
        self.operation(false, move |storage, _| {
            Box::pin(async move { Ok(storage.visible_entry(conversation, id).await?) })
        })
    }
    pub fn scan_conversations(
        &self,
        query: ConversationQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> TxFuture<'_, Page<ConversationRecord>> {
        self.operation(false, move |storage, _| {
            Box::pin(async move { Ok(storage.scan_conversations(query, limit, cursor).await?) })
        })
    }
    pub fn scan_entries(
        &self,
        query: EntryQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> TxFuture<'_, Page<EntryRecord>> {
        self.operation(false, move |storage, _| {
            Box::pin(async move { Ok(storage.scan_entries(query, limit, cursor).await?) })
        })
    }
    pub fn find_latest_head_marker(
        &self,
        conversation: Id,
        at: Option<Id>,
    ) -> TxFuture<'_, Option<EntryRecord>> {
        self.operation(false, move |storage, _| {
            Box::pin(async move { Ok(storage.find_latest_head_marker(conversation, at).await?) })
        })
    }
}

pub(super) async fn prepare<T: 'static, F>(
    storage: &mut dyn Storage,
    callback: F,
    control: Rc<RefCell<Control>>,
) -> Outcome<(T, Vec<StorageWrite>, Option<execution::PersistenceFence>)>
where
    F: for<'a> FnOnce(&'a Tx) -> TxFuture<'a, T>,
{
    let tx = Tx {
        state: Rc::new(RefCell::new(State::default())),
    };
    let state = tx.state.clone();
    let pump = Box::pin(async {
        loop {
            let operation = std::future::poll_fn(|cx| {
                let mut state = state.borrow_mut();
                if let Some(operation) = state.operations.pop_front() {
                    Poll::Ready(Some(operation))
                } else if state.sealed {
                    Poll::Ready(None)
                } else {
                    state.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            })
            .await;
            match operation {
                Some(operation) => operation(storage).await,
                None => break,
            }
        }
    });
    // The callback is polled first: seal as soon as it settles, before driving
    // more operations. Catch construction as well as polling panics.
    let callback = Box::pin(AssertUnwindSafe(async { callback(&tx).await }).catch_unwind());
    let (result, pump) = match select(callback, pump).await {
        Either::Left(done) => done,
        Either::Right(_) => unreachable!("operation pump runs until callback settlement seals it"),
    };
    let (pending, waker) = {
        let mut state = state.borrow_mut();
        state.sealed = true;
        (state.pending != 0, state.waker.take())
    };
    if let Some(waker) = waker {
        waker.wake();
    }
    // Complete any already-started adapter future without borrowing it from a
    // caller waiter. Sealing prevents that operation starting another adapter call.
    pump.await;
    match result {
        Ok(Ok(_)) if pending => Ok(Err(SessionError::PendingOperations)),
        Ok(Ok(value)) => {
            AssertUnwindSafe(async {
                let (writes, tasks) = {
                    let mut state = state.borrow_mut();
                    (
                        std::mem::take(&mut state.writes),
                        std::mem::take(&mut state.tasks),
                    )
                };
                let writes = tasks::assemble(storage, writes, tasks, control).await?;
                let fence = state.borrow_mut().fence.take();
                Ok((value, writes, fence))
            })
            .catch_unwind()
            .await
        }
        Ok(Err(error)) => Ok(Err(error)),
        Err(panic) => Err(panic),
    }
}
