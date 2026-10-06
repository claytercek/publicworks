use super::*;

pub(super) struct TaskCandidate {
    created: bool,
    record: TaskRecord,
}
pub(super) async fn current_task(
    storage: &mut dyn Storage,
    state: &Rc<RefCell<State>>,
    id: Id,
) -> Result<Option<TaskRecord>, SessionError> {
    let staged = state.borrow().tasks.get(&id).map(|v| v.record.clone());
    if staged.is_some() {
        return Ok(staged);
    }
    let record = storage.task(id).await?;
    state.borrow().open()?;
    Ok(record)
}
async fn require_conversation(
    storage: &mut dyn Storage,
    state: &Rc<RefCell<State>>,
    id: Id,
) -> Result<(), SessionError> {
    let staged = state
        .borrow()
        .writes
        .iter()
        .any(|w| matches!(w, StorageWrite::Conversation(r) if r.id == id));
    if !staged && storage.conversation(id).await?.is_none() {
        return Err(SessionError::Invalid(format!("Unknown conversation: {id}")));
    }
    state.borrow().open()
}
impl Tx {
    pub fn task(&self, id: Id) -> TxFuture<'_, Option<TaskRecord>> {
        self.operation(false, move |storage, _| {
            Box::pin(async move { Ok(storage.task(id).await?) })
        })
    }
    pub fn scan_tasks(
        &self,
        query: TaskQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> TxFuture<'_, Page<TaskRecord>> {
        self.operation(false, move |storage, _| {
            Box::pin(async move { Ok(storage.scan_tasks(query, limit, cursor).await?) })
        })
    }
    pub fn create_task(
        &self,
        definition: TaskDefinition,
        input: Value,
        options: TaskOptions,
    ) -> TxFuture<'_, TaskRecord> {
        self.operation(true, move |storage, state| {
            Box::pin(async move {
                let owner = match options.ownership {
                    TaskOwnership::Conversation => None,
                    TaskOwnership::Task(id) => {
                        if options.background {
                            return Err(SessionError::Invalid(
                                "A child task cannot be background".into(),
                            ));
                        }
                        Some(current_task(storage, &state, id).await?.ok_or_else(|| {
                            SessionError::Invalid(format!("Unknown task owner: {id}"))
                        })?)
                    }
                };
                let conversation_id = if let Some(owner) = &owner {
                    if options
                        .conversation_id
                        .is_some_and(|id| id != owner.conversation_id)
                    {
                        return Err(SessionError::Invalid(
                            "Child task must inherit owner conversation".into(),
                        ));
                    }
                    owner.conversation_id
                } else {
                    options.conversation_id.ok_or_else(|| {
                        SessionError::Invalid(
                            "Conversation-owned task requires conversation_id".into(),
                        )
                    })?
                };
                require_conversation(storage, &state, conversation_id).await?;
                let checkpoint = definition.initial(&input)?;
                let id = storage.mint_id().await?;
                state.borrow().open()?;
                let record = TaskRecord {
                    id,
                    conversation_id,
                    kind: definition.kind().to_owned(),
                    version: definition.version(),
                    input,
                    owner: owner.map(|r| r.id),
                    background: options.background,
                    abort_requested: false,
                    state: TaskState::Pending { checkpoint },
                    memos: None,
                };
                record.validate_payloads()?;
                state.borrow_mut().tasks.insert(
                    id,
                    TaskCandidate {
                        created: true,
                        record: record.clone(),
                    },
                );
                Ok(record)
            })
        })
    }
    pub(in crate::session) fn update_task(&self, id: Id, update: TaskUpdate) -> TxFuture<'_, ()> {
        self.operation(true, move |storage, state| {
            Box::pin(async move {
                let mut record = current_task(storage, &state, id)
                    .await?
                    .ok_or_else(|| SessionError::Invalid("Task disappeared".into()))?;
                record.state = match update {
                    TaskUpdate::Checkpoint(checkpoint) => TaskState::Running { checkpoint },
                    TaskUpdate::Complete(result) => TaskState::Terminal {
                        outcome: TaskOutcome::Completed { result },
                    },
                    TaskUpdate::Abort { reason, result } => TaskState::Terminal {
                        outcome: TaskOutcome::Aborted { reason, result },
                    },
                    TaskUpdate::Fail(error, result) => TaskState::Terminal {
                        outcome: TaskOutcome::Failed { error, result },
                    },
                };
                if record.status() == TaskStatus::Terminal {
                    record.memos = None;
                }
                record.validate_payloads()?;
                let mut state = state.borrow_mut();
                let previous = state.tasks.get(&id);
                if let Some(previous) = previous {
                    replaceable(&previous.record, &record)?;
                }
                let created = previous.is_some_and(|v| v.created);
                state.tasks.insert(id, TaskCandidate { created, record });
                Ok(())
            })
        })
    }
    /// Reserved for runtime-owned transitions, not an application state escape.
    pub(crate) fn set_task(&self, record: TaskRecord) -> TxFuture<'_, ()> {
        self.operation(true, move |_, state| {
            Box::pin(async move {
                record.validate_payloads()?;
                let mut state = state.borrow_mut();
                let previous = state.tasks.get(&record.id);
                if let Some(previous) = previous {
                    replaceable(&previous.record, &record)?;
                }
                let created = previous.is_some_and(|v| v.created);
                state
                    .tasks
                    .insert(record.id, TaskCandidate { created, record });
                Ok(())
            })
        })
    }
}
fn replaceable(previous: &TaskRecord, next: &TaskRecord) -> Result<(), SessionError> {
    if previous.status() == TaskStatus::Terminal {
        return Err(SessionError::Invalid(format!(
            "Task {} is already terminal",
            previous.id
        )));
    }
    if previous.conversation_id != next.conversation_id {
        return Err(SessionError::Invalid(
            "Task cannot change conversations".into(),
        ));
    }
    Ok(())
}
async fn final_task(
    storage: &mut dyn Storage,
    tasks: &BTreeMap<Id, TaskCandidate>,
    id: Id,
) -> Result<TaskRecord, SessionError> {
    if let Some(candidate) = tasks.get(&id) {
        return Ok(candidate.record.clone());
    }
    storage
        .task(id)
        .await?
        .ok_or_else(|| SessionError::Invalid(format!("Unknown task: {id}")))
}
pub(super) async fn assemble(
    storage: &mut dyn Storage,
    mut writes: Vec<StorageWrite>,
    tasks: BTreeMap<Id, TaskCandidate>,
    control: Rc<RefCell<Control>>,
) -> Result<Vec<StorageWrite>, SessionError> {
    let mut owners = Vec::new();
    for write in &writes {
        if let StorageWrite::Conversation(record) = write
            && let Some(owner) = &record.owner
        {
            owners.push((owner.task_id, owner.conversation_id));
        }
    }
    for candidate in tasks.values() {
        let record = &candidate.record;
        if candidate.created {
            let staged = writes.iter().any(
                |w| matches!(w, StorageWrite::Conversation(r) if r.id == record.conversation_id),
            );
            if !staged
                && storage
                    .conversation(record.conversation_id)
                    .await?
                    .is_none()
            {
                return Err(SessionError::Invalid(
                    "Task conversation does not exist".into(),
                ));
            }
        }
        if !candidate.created {
            let committed = storage.task(record.id).await?.ok_or_else(|| {
                SessionError::Invalid(format!("Task {} does not exist", record.id))
            })?;
            replaceable(&committed, record)?;
        } else if let Some(owner) = record.owner {
            if record.background {
                return Err(SessionError::Invalid(
                    "A child task cannot be background".into(),
                ));
            }
            owners.push((owner, record.conversation_id));
        }
    }
    for (id, conversation) in owners {
        if control
            .borrow()
            .leaf
            .upgrade()
            .is_some_and(|inv| inv.guards(id))
        {
            return Err(SessionError::Invalid(
                "Reserved leaf task cannot acquire owned work".into(),
            ));
        }
        let owner = final_task(storage, &tasks, id).await?;
        if matches!(
            owner.status(),
            TaskStatus::Completing | TaskStatus::Terminal
        ) || owner.abort_requested
        {
            return Err(SessionError::Invalid(format!(
                "Task owner {id} cannot acquire new work"
            )));
        }
        if owner.conversation_id != conversation {
            return Err(SessionError::Invalid("Owner conversation mismatch".into()));
        }
    }
    writes.extend(tasks.into_values().map(|v| StorageWrite::Task(v.record)));
    Ok(writes)
}
