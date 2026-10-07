use super::*;
use std::collections::BTreeSet;

/// Replacement contents for the dedicated state; Tx owns its stable identity.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConversationStateDraft {
    pub run: Option<ConversationRun>,
    pub inbox: Vec<InboxItem>,
    pub agent_config: Option<Value>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WithdrawalResult {
    Aborted,
    AlreadyPlaced,
    Settled,
    NotFound,
}

pub(super) struct SubmissionCandidate {
    // Retain the committed baseline through all private replacements. This is
    // separate from the latest record so assembly can validate coalesced writes.
    original: Option<SubmissionRecord>,
    record: SubmissionRecord,
}

fn invalid(message: impl Into<String>) -> SessionError {
    SessionError::Invalid(message.into())
}

async fn current_submission(
    storage: &mut dyn Storage,
    state: &Rc<RefCell<State>>,
    id: Id,
) -> Result<Option<SubmissionRecord>, SessionError> {
    let candidate = state
        .borrow()
        .submissions
        .get(&id)
        .map(|c| c.record.clone());
    if candidate.is_some() {
        return Ok(candidate);
    }
    let record = storage.submission(id).await?;
    state.borrow().open()?;
    Ok(record)
}

async fn current_state(
    storage: &mut dyn Storage,
    state: &Rc<RefCell<State>>,
    conversation: Id,
) -> Result<Option<ConversationStateRecord>, SessionError> {
    let candidate = state
        .borrow()
        .conversation_states
        .get(&conversation)
        .cloned();
    if candidate.is_some() {
        return Ok(candidate);
    }
    let record = storage.conversation_state(conversation).await?;
    state.borrow().open()?;
    Ok(record)
}

fn same_submission(
    previous: &SubmissionRecord,
    next: &SubmissionRecord,
) -> Result<(), SessionError> {
    if previous.id != next.id
        || previous.conversation_id != next.conversation_id
        || previous.request_id != next.request_id
        || previous.submission_type() != next.submission_type()
    {
        return Err(invalid(
            "Submission identity, type, and request ID are immutable",
        ));
    }
    Ok(())
}

// Validate each private replacement, not just the coalesced endpoints: queued ->
// placed -> done is legal in one transaction, queued -> done directly is not.
fn transition(previous: &SubmissionRecord, next: &SubmissionRecord) -> Result<(), SessionError> {
    same_submission(previous, next)?;
    use SubmissionState::*;
    let legal = match (&previous.state, &next.state) {
        (InputQueued, InputPlaced { .. } | InputUnanswered { entry: None, .. })
        | (WriteQueued, WriteDone { .. } | WriteUnanswered { .. }) => true,
        (
            InputPlaced { entry },
            InputDone { entry: next, .. }
            | InputUnanswered {
                entry: Some(next), ..
            },
        ) => entry == next,
        _ => false,
    };
    if !legal {
        return Err(invalid(
            "Illegal submission transition (terminal settlements are immutable)",
        ));
    }
    next.validate_payloads()?;
    Ok(())
}

fn stage_submission(
    state: &Rc<RefCell<State>>,
    previous: &SubmissionRecord,
    next: SubmissionRecord,
) -> Result<(), SessionError> {
    transition(previous, &next)?;
    let mut state = state.borrow_mut();
    let original = state
        .submissions
        .get(&next.id)
        .map(|c| c.original.clone())
        .unwrap_or_else(|| Some(previous.clone()));
    state.submissions.insert(
        next.id,
        SubmissionCandidate {
            original,
            record: next,
        },
    );
    Ok(())
}

fn same_state(
    previous: &ConversationStateRecord,
    next: &ConversationStateRecord,
) -> Result<(), SessionError> {
    if previous.id != next.id || previous.conversation_id != next.conversation_id {
        return Err(invalid("Conversation state identity is immutable"));
    }
    Ok(())
}

impl Tx {
    pub fn submission(&self, id: Id) -> TxFuture<'_, Option<SubmissionRecord>> {
        self.operation(false, move |storage, _| {
            Box::pin(async move { Ok(storage.submission(id).await?) })
        })
    }

    pub fn scan_submissions(
        &self,
        query: SubmissionQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> TxFuture<'_, Page<SubmissionRecord>> {
        self.operation(false, move |storage, _| {
            Box::pin(async move { Ok(storage.scan_submissions(query, limit, cursor).await?) })
        })
    }

    pub fn submission_by_request(
        &self,
        conversation: Id,
        request_id: impl Into<String>,
    ) -> TxFuture<'_, Option<SubmissionRecord>> {
        let request_id = request_id.into();
        self.operation(false, move |storage, _| {
            Box::pin(async move {
                Ok(storage
                    .submission_by_request(conversation, &request_id)
                    .await?)
            })
        })
    }

    pub fn conversation_state(
        &self,
        conversation: Id,
    ) -> TxFuture<'_, Option<ConversationStateRecord>> {
        self.operation(false, move |storage, _| {
            Box::pin(async move { Ok(storage.conversation_state(conversation).await?) })
        })
    }

    /// Create a queued receipt. A request ID is unique within its conversation;
    /// use the committed lookup before mutation for admission deduplication.
    pub fn create_submission(
        &self,
        conversation: Id,
        kind: SubmissionType,
        request_id: Option<String>,
    ) -> TxFuture<'_, SubmissionRecord> {
        self.operation(true, move |storage, state| {
            Box::pin(async move {
                let id = storage.mint_id().await?;
                state.borrow().open()?;
                let record = SubmissionRecord {
                    id,
                    conversation_id: conversation,
                    request_id,
                    state: match kind {
                        SubmissionType::Input => SubmissionState::InputQueued,
                        SubmissionType::Write => SubmissionState::WriteQueued,
                    },
                };
                if state.borrow().submissions.contains_key(&id) {
                    return Err(invalid("Duplicate submission ID"));
                }
                state.borrow_mut().submissions.insert(
                    id,
                    SubmissionCandidate {
                        original: None,
                        record: record.clone(),
                    },
                );
                Ok(record)
            })
        })
    }

    /// Place a queued input, or finish a queued passive write at its entry.
    /// References are checked against the final transaction, independent of order.
    pub fn place_submission(&self, id: Id, entry: Id) -> TxFuture<'_, SubmissionRecord> {
        self.operation(true, move |storage, state| {
            Box::pin(async move {
                let previous = current_submission(storage, &state, id)
                    .await?
                    .ok_or_else(|| invalid(format!("Unknown submission: {id}")))?;
                let mut record = previous.clone();
                record.state = match previous.state {
                    SubmissionState::InputQueued => SubmissionState::InputPlaced { entry },
                    SubmissionState::WriteQueued => SubmissionState::WriteDone { entry },
                    _ => return Err(invalid("Only queued submissions can be placed")),
                };
                stage_submission(&state, &previous, record.clone())?;
                Ok(record)
            })
        })
    }

    /// First writer wins: an unknown or terminal receipt rejects. Only a placed
    /// input accepts Done; queued inputs and writes can settle Unanswered.
    pub fn settle_submission(
        &self,
        id: Id,
        settlement: SubmissionSettlement,
    ) -> TxFuture<'_, SubmissionRecord> {
        self.operation(true, move |storage, state| {
            Box::pin(async move {
                settlement.validate_payloads()?;
                let previous = current_submission(storage, &state, id)
                    .await?
                    .ok_or_else(|| invalid(format!("Unknown submission: {id}")))?;
                let mut record = previous.clone();
                use SubmissionState::*;
                record.state = match (&previous.state, settlement) {
                    (InputPlaced { entry }, SubmissionSettlement::Done { answer }) => InputDone {
                        entry: *entry,
                        answer,
                    },
                    (
                        InputQueued | InputPlaced { .. },
                        SubmissionSettlement::Unanswered { reason, detail },
                    ) => InputUnanswered {
                        entry: match previous.state {
                            InputPlaced { entry } => Some(entry),
                            _ => None,
                        },
                        reason,
                        detail,
                    },
                    (WriteQueued, SubmissionSettlement::Unanswered { reason, detail }) => {
                        WriteUnanswered { reason, detail }
                    }
                    _ => return Err(invalid("Illegal submission settlement")),
                };
                stage_submission(&state, &previous, record.clone())?;
                Ok(record)
            })
        })
    }

    /// Create or replace contents while retaining the Tx-allocated state ID.
    pub fn set_conversation_state(
        &self,
        conversation: Id,
        draft: ConversationStateDraft,
    ) -> TxFuture<'_, ConversationStateRecord> {
        self.update_conversation_state(conversation, move |record| {
            record.run = draft.run;
            record.inbox = draft.inbox;
            record.agent_config = draft.agent_config;
        })
    }

    /// Update the private candidate (or an empty, newly allocated state). The
    /// synchronous callback must preserve id and conversation_id. Public reads
    /// still never expose staged data. Failed updates leave the candidate intact.
    pub fn update_conversation_state<F>(
        &self,
        conversation: Id,
        update: F,
    ) -> TxFuture<'_, ConversationStateRecord>
    where
        F: FnOnce(&mut ConversationStateRecord) + 'static,
    {
        self.operation(true, move |storage, state| {
            Box::pin(async move {
                let previous = match current_state(storage, &state, conversation).await? {
                    Some(record) => record,
                    None => {
                        let id = storage.mint_id().await?;
                        state.borrow().open()?;
                        ConversationStateRecord {
                            id,
                            conversation_id: conversation,
                            run: None,
                            inbox: Vec::new(),
                            agent_config: None,
                        }
                    }
                };
                let mut record = previous.clone();
                update(&mut record);
                same_state(&previous, &record)?;
                record.validate_payloads()?;
                state
                    .borrow_mut()
                    .conversation_states
                    .insert(conversation, record.clone());
                Ok(record)
            })
        })
    }

    /// Runtime recovery: include stale terminal markers in final assembly even
    /// when no task transition is needed. Never re-execute application code.
    pub(crate) fn reconcile_terminal_runs(&self) -> TxFuture<'_, ()> {
        Box::pin(async move {
            let tree = self.tree().await?;
            let mut records = Vec::new();
            let mut conversations = BTreeSet::new();
            for task in tree
                .tasks
                .values()
                .filter(|task| task.status() == TaskStatus::Terminal)
            {
                if let Some(record) = self.conversation_state(task.conversation_id).await?
                    && record
                        .run
                        .as_ref()
                        .is_some_and(|run| run.task_id == task.id)
                {
                    records.push(record);
                }
                if task.abort_requested || execution::tree::Tree::failed(task) {
                    conversations.extend(tree.owned_conversations(task.id));
                }
            }
            // Recovery reads precede all writes. Re-stage only matching markers;
            // final assembly applies the same fallback as an ordinary outcome.
            for record in records {
                self.set_conversation_state(
                    record.conversation_id,
                    ConversationStateDraft {
                        run: record.run,
                        inbox: record.inbox,
                        agent_config: record.agent_config,
                    },
                )
                .await?;
            }
            for conversation in conversations {
                self.withdraw_queued_inputs(conversation).await?;
            }
            Ok(())
        })
    }

    /// Withdraw only queued inputs in a reached ownership scope. Writes are
    /// passive and remain queued. Private candidates participate in the batch.
    pub(crate) fn withdraw_queued_inputs(&self, conversation: Id) -> TxFuture<'_, ()> {
        self.operation(true, move |storage, state| {
            Box::pin(async move {
                let candidates = state
                    .borrow()
                    .submissions
                    .iter()
                    .map(|(id, candidate)| (*id, candidate.record.clone()))
                    .collect();
                let changes =
                    queued_input_withdrawals(storage, &candidates, conversation, Some(&state))
                        .await?;
                state.borrow().open()?;
                let mut inbox = current_state(storage, &state, conversation).await?;
                if let Some(inbox) = &mut inbox {
                    let withdrawn = changes
                        .iter()
                        .map(|(_, record)| record.id)
                        .collect::<BTreeSet<_>>();
                    inbox
                        .inbox
                        .retain(|item| !withdrawn.contains(&item.submission_id));
                }
                if changes.is_empty() {
                    return Ok(());
                }
                if let Some(inbox) = &inbox {
                    inbox.validate_payloads()?;
                }
                for (previous, record) in changes {
                    stage_submission(&state, &previous, record)?;
                }
                if let Some(inbox) = inbox {
                    state
                        .borrow_mut()
                        .conversation_states
                        .insert(conversation, inbox);
                }
                Ok(())
            })
        })
    }

    /// Atomically abort a queued receipt and remove its inbox member, if any.
    /// The optional conversation filter treats mismatches as NotFound.
    pub fn withdraw_submission(
        &self,
        id: Id,
        conversation: Option<Id>,
    ) -> TxFuture<'_, WithdrawalResult> {
        self.operation(true, move |storage, state| {
            Box::pin(async move {
                let Some(previous) = current_submission(storage, &state, id).await? else {
                    return Ok(WithdrawalResult::NotFound);
                };
                if conversation.is_some_and(|id| id != previous.conversation_id) {
                    return Ok(WithdrawalResult::NotFound);
                }
                match previous.status() {
                    SubmissionStatus::Placed => return Ok(WithdrawalResult::AlreadyPlaced),
                    SubmissionStatus::Done | SubmissionStatus::Unanswered => {
                        return Ok(WithdrawalResult::Settled);
                    }
                    SubmissionStatus::Queued => {}
                }
                let mut inbox = current_state(storage, &state, previous.conversation_id).await?;
                let mut record = previous.clone();
                record.state = match record.submission_type() {
                    SubmissionType::Input => SubmissionState::InputUnanswered {
                        entry: None,
                        reason: "aborted".into(),
                        detail: None,
                    },
                    SubmissionType::Write => SubmissionState::WriteUnanswered {
                        reason: "aborted".into(),
                        detail: None,
                    },
                };
                // Finish all fallible work before staging either half.
                transition(&previous, &record)?;
                if let Some(inbox) = &mut inbox {
                    inbox.inbox.retain(|item| item.submission_id != id);
                    inbox.validate_payloads()?;
                }
                stage_submission(&state, &previous, record)?;
                let mut state = state.borrow_mut();
                if let Some(inbox) = inbox {
                    state
                        .conversation_states
                        .insert(inbox.conversation_id, inbox);
                }
                Ok(WithdrawalResult::Aborted)
            })
        })
    }
}

async fn queued_input_withdrawals(
    storage: &mut dyn Storage,
    candidates: &BTreeMap<Id, SubmissionRecord>,
    conversation: Id,
    state: Option<&Rc<RefCell<State>>>,
) -> Result<Vec<(SubmissionRecord, SubmissionRecord)>, SessionError> {
    let mut records = BTreeMap::new();
    let mut cursor = None;
    loop {
        let page = storage
            .scan_submissions(
                SubmissionQuery {
                    conversation_id: Some(conversation),
                    status: Some(SubmissionStatus::Queued),
                },
                128,
                cursor,
            )
            .await?;
        if let Some(state) = state {
            state.borrow().open()?;
        }
        records.extend(page.items.into_iter().map(|record| (record.id, record)));
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    for record in candidates
        .values()
        .filter(|record| record.conversation_id == conversation)
    {
        records.insert(record.id, record.clone());
    }
    let mut changes = Vec::new();
    for previous in records.into_values() {
        if previous.state != SubmissionState::InputQueued {
            continue;
        }
        let mut record = previous.clone();
        record.state = SubmissionState::InputUnanswered {
            entry: None,
            reason: "aborted".into(),
            detail: None,
        };
        transition(&previous, &record)?;
        changes.push((previous, record));
    }
    Ok(changes)
}

async fn final_submission(
    storage: &mut dyn Storage,
    submissions: &BTreeMap<Id, SubmissionCandidate>,
    id: Id,
) -> Result<SubmissionRecord, SessionError> {
    if let Some(candidate) = submissions.get(&id) {
        return Ok(candidate.record.clone());
    }
    storage
        .submission(id)
        .await?
        .ok_or_else(|| invalid(format!("Unknown submission: {id}")))
}

async fn require_conversation(
    storage: &mut dyn Storage,
    writes: &[StorageWrite],
    id: Id,
) -> Result<(), SessionError> {
    if !writes
        .iter()
        .any(|w| matches!(w, StorageWrite::Conversation(r) if r.id == id))
        && storage.conversation(id).await?.is_none()
    {
        return Err(invalid(format!("Unknown conversation: {id}")));
    }
    Ok(())
}

async fn require_entry(
    storage: &mut dyn Storage,
    writes: &[StorageWrite],
    conversation: Id,
    id: Id,
) -> Result<(), SessionError> {
    if let Some(entry) = writes.iter().find_map(|w| match w {
        StorageWrite::Entry(r) if r.id == id => Some(r),
        _ => None,
    }) {
        if entry.conversation_id == conversation {
            return Ok(());
        }
    } else {
        // A newly forked conversation is not yet known to the adapter. Fork
        // cutoffs and parents are committed-only, so resolve through that link.
        let staged = writes.iter().find_map(|w| match w {
            StorageWrite::Conversation(r) if r.id == conversation => Some(r),
            _ => None,
        });
        let source = match staged {
            Some(record) => record
                .parent
                .as_ref()
                .filter(|parent| id <= parent.at)
                .map(|parent| parent.conversation_id),
            None => Some(conversation),
        };
        if let Some(source) = source
            && storage.visible_entry(source, id).await?.is_some()
        {
            return Ok(());
        }
    }
    Err(invalid(format!(
        "Entry {id} is not visible from conversation {conversation}"
    )))
}

pub(super) async fn assemble(
    storage: &mut dyn Storage,
    mut writes: Vec<StorageWrite>,
    mut submissions: BTreeMap<Id, SubmissionCandidate>,
    mut states: BTreeMap<Id, ConversationStateRecord>,
) -> Result<Vec<StorageWrite>, SessionError> {
    // Abort/failure candidates cascade into owned conversation nodes even when
    // those nodes have no live tasks. The task's own conversation is excluded:
    // direct run abort preserves its later queue. Background anchors are cuts.
    let cascades = writes
        .iter()
        .filter_map(|write| match write {
            StorageWrite::Task(task)
                if task.abort_requested || execution::tree::Tree::failed(task) =>
            {
                Some(task.id)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if !cascades.is_empty() {
        let mut tree = execution::tree::Tree::load(storage).await?;
        for write in &writes {
            match write {
                StorageWrite::Task(task) => {
                    tree.tasks.insert(task.id, task.clone());
                }
                StorageWrite::Conversation(record) => {
                    tree.conversations.insert(record.id, record.clone());
                }
                _ => {}
            }
        }
        let conversations = cascades
            .into_iter()
            .flat_map(|id| tree.owned_conversations(id))
            .collect::<BTreeSet<_>>();
        let candidates = submissions
            .iter()
            .map(|(id, candidate)| (*id, candidate.record.clone()))
            .collect();
        for conversation in conversations {
            let changes =
                queued_input_withdrawals(storage, &candidates, conversation, None).await?;
            if changes.is_empty() {
                continue;
            }
            let mut record = match states.get(&conversation) {
                Some(record) => Some(record.clone()),
                None => storage.conversation_state(conversation).await?,
            };
            for (previous, next) in changes {
                if let Some(record) = &mut record {
                    record.inbox.retain(|item| item.submission_id != next.id);
                }
                let original = submissions
                    .get(&next.id)
                    .map(|c| c.original.clone())
                    .unwrap_or_else(|| Some(previous));
                submissions.insert(
                    next.id,
                    SubmissionCandidate {
                        original,
                        record: next,
                    },
                );
            }
            if let Some(record) = record {
                states.insert(conversation, record);
            }
        }
    }
    // Task assembly has already held outcomes with live descendants. Only
    // FINAL terminal candidates may settle a run. Also inspect staged state
    // during recovery or when a caller points it at an already-terminal task.
    let mut conversations = states.keys().copied().collect::<BTreeSet<_>>();
    conversations.extend(writes.iter().filter_map(|write| match write {
        StorageWrite::Task(task) if task.status() == TaskStatus::Terminal => {
            Some(task.conversation_id)
        }
        _ => None,
    }));
    for conversation in conversations {
        let Some(mut record) = (match states.get(&conversation) {
            Some(record) => Some(record.clone()),
            None => storage.conversation_state(conversation).await?,
        }) else {
            continue;
        };
        let Some(run) = &record.run else { continue };
        let task = match writes.iter().find_map(|write| match write {
            StorageWrite::Task(task) if task.id == run.task_id => Some(task.clone()),
            _ => None,
        }) {
            Some(task) => Some(task),
            None => storage.task(run.task_id).await?,
        };
        let Some(task) = task else { continue };
        let TaskState::Terminal { outcome } = task.state else {
            continue;
        };
        if task.conversation_id != conversation {
            return Err(invalid("Run task conversation mismatch"));
        }
        let reason = if matches!(outcome, TaskOutcome::Aborted { .. }) {
            "aborted"
        } else {
            "faulted"
        };
        let mut ids = BTreeSet::new();
        for id in &run.input_submission_ids {
            if !ids.insert(*id) {
                return Err(invalid("Duplicate run input submission ID"));
            }
            let previous = final_submission(storage, &submissions, *id).await?;
            if previous.conversation_id != conversation {
                return Err(invalid("Run input conversation mismatch"));
            }
            if !matches!(
                previous.state,
                SubmissionState::InputPlaced { .. }
                    | SubmissionState::InputDone { .. }
                    | SubmissionState::InputUnanswered { entry: Some(_), .. }
            ) {
                return Err(invalid(
                    "Run inputs must be placed inputs in its conversation",
                ));
            }
            // A specific settlement already staged by the agent wins.
            if let SubmissionState::InputPlaced { entry } = previous.state {
                let mut next = previous.clone();
                next.state = SubmissionState::InputUnanswered {
                    entry: Some(entry),
                    reason: reason.into(),
                    detail: None,
                };
                transition(&previous, &next)?;
                let original = submissions
                    .get(id)
                    .map(|c| c.original.clone())
                    .unwrap_or_else(|| Some(previous));
                submissions.insert(
                    *id,
                    SubmissionCandidate {
                        original,
                        record: next,
                    },
                );
            }
        }
        // We inspected the latest candidate marker, never a superseded run.
        record.run = None;
        states.insert(conversation, record);
    }
    let mut requests = BTreeSet::new();
    let mut affected = BTreeSet::new();
    for (id, candidate) in &submissions {
        let record = &candidate.record;
        if *id != record.id {
            return Err(invalid("Submission candidate key mismatch"));
        }
        record.validate_payloads()?;
        require_conversation(storage, &writes, record.conversation_id).await?;
        affected.insert(record.conversation_id);
        let committed = storage.submission(record.id).await?;
        if committed != candidate.original {
            return Err(invalid("Submission creation/replacement baseline mismatch"));
        }
        if let Some(previous) = &committed {
            same_submission(previous, record)?;
            // A queued input may traverse placement and settlement in this Tx.
            // All other coalesced endpoints must be a single legal transition.
            if !matches!(
                (&previous.state, &record.state),
                (
                    SubmissionState::InputQueued,
                    SubmissionState::InputDone { .. }
                        | SubmissionState::InputUnanswered { entry: Some(_), .. }
                )
            ) {
                transition(previous, record)?;
            }
        }
        if let Some(request) = &record.request_id {
            if !requests.insert((record.conversation_id, request.clone())) {
                return Err(invalid("Duplicate request ID in conversation"));
            }
            if let Some(existing) = storage
                .submission_by_request(record.conversation_id, request)
                .await?
                && existing.id != record.id
            {
                return Err(invalid("Duplicate request ID in conversation"));
            }
        }
        use SubmissionState::*;
        let (entry, answer) = match &record.state {
            InputPlaced { entry } | WriteDone { entry } => (Some(*entry), None),
            InputDone { entry, answer } => (Some(*entry), Some(*answer)),
            InputUnanswered { entry, .. } => (*entry, None),
            _ => (None, None),
        };
        for id in entry.into_iter().chain(answer) {
            require_entry(storage, &writes, record.conversation_id, id).await?;
        }
    }
    for (conversation, record) in &states {
        if *conversation != record.conversation_id {
            return Err(invalid("Conversation state key mismatch"));
        }
        if let Some(previous) = storage.conversation_state(*conversation).await? {
            same_state(&previous, record)?;
        }
        affected.insert(*conversation);
    }
    // Revalidate committed state too: settling/placing a submission must not
    // strand its old inbox or run reference when no state update was supplied.
    for conversation in affected {
        require_conversation(storage, &writes, conversation).await?;
        let record = match states.get(&conversation) {
            Some(record) => Some(record.clone()),
            None => storage.conversation_state(conversation).await?,
        };
        let Some(record) = record else { continue };
        record.validate_payloads()?;
        let mut ids = BTreeSet::new();
        for item in &record.inbox {
            if !ids.insert(item.submission_id) {
                return Err(invalid("Duplicate inbox submission ID"));
            }
            let submission = final_submission(storage, &submissions, item.submission_id).await?;
            if submission.conversation_id != conversation
                || submission.status() != SubmissionStatus::Queued
            {
                return Err(invalid(
                    "Inbox must reference queued submissions in its conversation",
                ));
            }
        }
        if let Some(run) = &record.run {
            let task = match writes.iter().find_map(|w| match w {
                StorageWrite::Task(r) if r.id == run.task_id => Some(r),
                _ => None,
            }) {
                Some(record) => Some(record.clone()),
                None => storage.task(run.task_id).await?,
            }
            .ok_or_else(|| invalid("Unknown run task"))?;
            if task.conversation_id != conversation {
                return Err(invalid("Run task conversation mismatch"));
            }
            let mut ids = BTreeSet::new();
            for id in &run.input_submission_ids {
                if !ids.insert(*id) {
                    return Err(invalid("Duplicate run input submission ID"));
                }
                let submission = final_submission(storage, &submissions, *id).await?;
                if submission.conversation_id != conversation
                    || !matches!(submission.state, SubmissionState::InputPlaced { .. })
                {
                    return Err(invalid(
                        "Run inputs must be placed inputs in its conversation",
                    ));
                }
            }
        }
    }
    writes.extend(
        submissions
            .into_values()
            .map(|c| StorageWrite::Submission(c.record)),
    );
    writes.extend(states.into_values().map(StorageWrite::ConversationState));
    let mut ids = BTreeSet::new();
    for write in &writes {
        if !ids.insert(write.id()) {
            return Err(invalid("Conflicting candidate IDs"));
        }
    }
    Ok(writes)
}

#[cfg(test)]
mod tests;
