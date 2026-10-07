//! Agent-owned admission and inbox policy. All reads precede the first mutation.
use crate::wire::validate_entry;
use crate::{
    engine::{decode_input, message_draft},
    *,
};
use publicworks_runtime::*;
use serde_json::json;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BusyMode {
    Reject,
    Steer,
    #[default]
    FollowUp,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum QueueMode {
    #[default]
    One,
    All,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueueConfig {
    pub steer: QueueMode,
    pub follow_up: QueueMode,
}
#[derive(Clone, Debug, Default)]
pub struct SubmitOptions {
    pub request_id: Option<String>,
    pub busy: BusyMode,
}
/// Compare the active context head at placement, not the transcript tail.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExpectedHead {
    #[default]
    Any,
    Exact(Option<Id>),
}
#[derive(Clone, Debug, Default)]
pub struct WriteOptions {
    pub request_id: Option<String>,
    pub expected_head: ExpectedHead,
}

impl Agent {
    /// Admit immediately on the Harness line and enable progress. Dropping the
    /// returned observer does not cancel admission. Keep polling the driver.
    pub fn submit<T: Into<String>>(
        &self,
        harness: &Harness,
        conversation: Id,
        text: T,
        config: TurnConfig,
        options: SubmitOptions,
    ) -> impl Future<Output = Result<Submission, HarnessError>> + use<T> {
        let agent = self.clone();
        let text = text.into();
        let waiter =
            harness.commit(move |tx| agent.admit_input(tx, conversation, text, config, options));
        let enabled = harness.resume();
        let harness = harness.clone();
        async move {
            enabled?;
            let id = waiter.await.map_err(HarnessError::Session)?.value;
            harness
                .submission(id)
                .await?
                .ok_or_else(|| HarnessError::Session(invalid("Missing admitted submission")))
        }
    }

    /// Passive writes never enable scheduling. Busy writes wait for a boundary.
    pub fn write(
        &self,
        harness: &Harness,
        conversation: Id,
        draft: EntryDraft,
        options: WriteOptions,
    ) -> impl Future<Output = Result<Submission, HarnessError>> + use<> {
        let agent = self.clone();
        let waiter = harness.commit(move |tx| agent.admit_write(tx, conversation, draft, options));
        let harness = harness.clone();
        async move {
            let id = waiter.await.map_err(HarnessError::Session)?.value;
            harness
                .submission(id)
                .await?
                .ok_or_else(|| HarnessError::Session(invalid("Missing admitted submission")))
        }
    }

    /// Persist queue policy. Boundaries read it on the Session line, rather than
    /// pinning it in a task checkpoint or in the process-local installation.
    pub fn configure_queues(
        &self,
        harness: &Harness,
        conversation: Id,
        config: QueueConfig,
    ) -> CommitWaiter<()> {
        harness.commit(move |tx| {
            Box::pin(async move {
                crate::config::merge(
                    tx,
                    conversation,
                    [
                        ("steer", Some(json!(mode(config.steer)))),
                        ("followUp", Some(json!(mode(config.follow_up)))),
                    ],
                )
                .await
            })
        })
    }

    /// Low-level admission for hosts composing a Session transaction. Unlike
    /// `submit`, this does not enable progress. Returns the durable receipt ID.
    pub fn admit_input<'a>(
        &self,
        tx: &'a Tx,
        conversation: Id,
        text: impl Into<String>,
        config: TurnConfig,
        options: SubmitOptions,
    ) -> TxFuture<'a, Id> {
        let agent = self.clone();
        let text = text.into();
        Box::pin(async move {
            if let Some(id) = dedup(
                tx,
                conversation,
                options.request_id.as_deref(),
                SubmissionType::Input,
            )
            .await?
            {
                return Ok(id);
            }
            let mut boundary = Boundary::read(tx, conversation).await?;
            if boundary.state.run.is_some() && options.busy == BusyMode::Reject {
                return Err(invalid("Conversation already has a live agent run"));
            }
            let input = agent.turn_input(config)?;
            let draft = message_draft(
                "agent.user",
                ModelMessage::User { text: text.clone() },
                None,
            );
            validate_entry(draft.model.as_deref(), None)?;
            let submission = tx
                .create_submission(conversation, SubmissionType::Input, options.request_id)
                .await?;
            if boundary.state.run.is_none() && boundary.state.inbox.is_empty() {
                let entry = tx
                    .append_entry(
                        conversation,
                        message_draft("agent.user", ModelMessage::User { text }, None),
                    )
                    .await?;
                tx.place_submission(submission.id, entry.id).await?;
                boundary.state.run = Some(
                    agent
                        .start_run(tx, conversation, vec![submission.id], input)
                        .await?,
                );
            } else {
                boundary.state.inbox.push(InboxItem {
                    submission_id: submission.id,
                    payload: json!({
                        "mode": if options.busy == BusyMode::Steer { "steer" } else { "followUp" },
                        "text": text,
                        "input": input,
                    }),
                });
                if boundary.state.run.is_none() {
                    agent
                        .place_boundary(tx, &mut boundary, Position::Final)
                        .await?;
                }
            }
            boundary.save(tx).await?;
            Ok(submission.id)
        })
    }

    /// Low-level passive admission for a Session transaction; returns the
    /// durable receipt ID and never enables progress on its own.
    pub fn admit_write<'a>(
        &self,
        tx: &'a Tx,
        conversation: Id,
        draft: EntryDraft,
        options: WriteOptions,
    ) -> TxFuture<'a, Id> {
        let agent = self.clone();
        Box::pin(async move {
            if let Some(id) = dedup(
                tx,
                conversation,
                options.request_id.as_deref(),
                SubmissionType::Write,
            )
            .await?
            {
                return Ok(id);
            }
            let mut boundary = Boundary::read(tx, conversation).await?;
            validate_entry(draft.model.as_deref(), draft.data.as_ref())?;
            let payload = encode_write(&draft, options.expected_head)?;
            let submission = tx
                .create_submission(conversation, SubmissionType::Write, options.request_id)
                .await?;
            boundary.state.inbox.push(InboxItem {
                submission_id: submission.id,
                payload,
            });
            if boundary.state.run.is_none() {
                agent
                    .place_boundary(tx, &mut boundary, Position::Final)
                    .await?;
            }
            boundary.save(tx).await?;
            Ok(submission.id)
        })
    }

    async fn start_run(
        &self,
        tx: &Tx,
        conversation: Id,
        inputs: Vec<Id>,
        input: Value,
    ) -> Result<ConversationRun, SessionError> {
        let task = tx
            .create_task(
                self.turn_definition(),
                input,
                TaskOptions {
                    ownership: TaskOwnership::Conversation,
                    conversation_id: Some(conversation),
                    background: false,
                },
            )
            .await?;
        Ok(ConversationRun {
            task_id: task.id,
            input_submission_ids: inputs,
        })
    }

    pub(crate) async fn place_boundary(
        &self,
        tx: &Tx,
        boundary: &mut Boundary,
        at: Position,
    ) -> Result<bool, SessionError> {
        let mut retained = Vec::new();
        let mut inputs = Vec::new();
        let mut reset = false;
        // Writes always precede selected inputs, including writes queued later.
        for item in std::mem::take(&mut boundary.state.inbox) {
            if item.payload["mode"] != "write" {
                retained.push(item);
                continue;
            }
            let (draft, expected) = decode_write(&item.payload)?;
            if matches!(expected, ExpectedHead::Exact(head) if head != boundary.head) {
                tx.settle_submission(item.submission_id, unanswered("stale"))
                    .await?;
                continue;
            }
            let head = draft.head;
            let is_reset = draft.kind == "agent.reset";
            let entry = tx.append_entry(boundary.conversation, draft).await?;
            if head.is_some() {
                boundary.head = entry.head;
            }
            reset |= is_reset;
            tx.place_submission(item.submission_id, entry.id).await?;
        }
        let at = if reset { Position::Final } else { at };
        if reset && boundary.state.run.is_some() {
            boundary.settle(tx, unanswered("reset")).await?;
        }
        let selected_mode = match at {
            Position::PostTools => "steer",
            Position::Final => "followUp",
        };
        let queue_mode = match at {
            Position::PostTools => boundary.config.steer,
            Position::Final => boundary.config.follow_up,
        };
        let mut next_input = None;
        for item in retained {
            if item.payload["mode"] == selected_mode
                && (queue_mode == QueueMode::All || inputs.is_empty())
            {
                let text = crate::wire::string(&item.payload, "text")?;
                let input = item
                    .payload
                    .get("input")
                    .ok_or_else(|| invalid("Missing queued turn configuration"))?
                    .clone();
                decode_input(&input)?;
                let entry = tx
                    .append_entry(
                        boundary.conversation,
                        message_draft("agent.user", ModelMessage::User { text }, None),
                    )
                    .await?;
                tx.place_submission(item.submission_id, entry.id).await?;
                inputs.push(item.submission_id);
                if next_input.is_none() {
                    next_input = Some(input);
                }
            } else {
                boundary.state.inbox.push(item);
            }
        }
        if !inputs.is_empty() {
            if let Some(run) = &mut boundary.state.run {
                run.input_submission_ids.extend(inputs);
            } else {
                boundary.state.run = Some(
                    self.start_run(
                        tx,
                        boundary.conversation,
                        inputs,
                        next_input.expect("selected input"),
                    )
                    .await?,
                );
            }
        }
        Ok(reset)
    }
}

async fn dedup(
    tx: &Tx,
    conversation: Id,
    request: Option<&str>,
    kind: SubmissionType,
) -> Result<Option<Id>, SessionError> {
    if let Some(request) = request
        && let Some(record) = tx.submission_by_request(conversation, request).await?
    {
        if record.submission_type() != kind {
            return Err(invalid(
                "Request ID already names a different submission type",
            ));
        }
        return Ok(Some(record.id));
    }
    Ok(None)
}
fn mode(mode: QueueMode) -> &'static str {
    match mode {
        QueueMode::One => "one",
        QueueMode::All => "all",
    }
}
pub(crate) fn unanswered(reason: &str) -> SubmissionSettlement {
    SubmissionSettlement::Unanswered {
        reason: reason.into(),
        detail: None,
    }
}
#[derive(Clone, Copy)]
pub(crate) enum Position {
    PostTools,
    Final,
}
pub(crate) struct Boundary {
    conversation: Id,
    pub state: ConversationStateDraft,
    config: QueueConfig,
    head: Option<Id>,
}
impl Boundary {
    pub async fn read(tx: &Tx, conversation: Id) -> Result<Self, SessionError> {
        let state = tx
            .conversation_state(conversation)
            .await?
            .map(|s| ConversationStateDraft {
                run: s.run,
                inbox: s.inbox,
                agent_config: s.agent_config,
            })
            .unwrap_or_default();
        let config = crate::config::queues(state.agent_config.as_ref())?;
        let head = tx
            .find_latest_head_marker(conversation, None)
            .await?
            .and_then(|e| e.head);
        Ok(Self {
            conversation,
            state,
            config,
            head,
        })
    }
    pub fn require_run(&self, task: Id) -> Result<(), SessionError> {
        if self.state.run.as_ref().is_none_or(|r| r.task_id != task) {
            return Err(invalid("Turn does not own live run marker"));
        }
        Ok(())
    }
    pub async fn settle(
        &mut self,
        tx: &Tx,
        settlement: SubmissionSettlement,
    ) -> Result<(), SessionError> {
        if let Some(run) = self.state.run.take() {
            for id in run.input_submission_ids {
                tx.settle_submission(id, settlement.clone()).await?;
            }
        }
        Ok(())
    }
    pub async fn save(self, tx: &Tx) -> Result<(), SessionError> {
        tx.set_conversation_state(self.conversation, self.state)
            .await?;
        Ok(())
    }
}

// Optional keys preserve omitted versus JSON null for entry data and
// expected empty head. EntryDraft itself is intentionally not a storage type.
fn encode_write(draft: &EntryDraft, expected: ExpectedHead) -> Result<Value, SessionError> {
    let mut value = json!({"mode":"write","kind":draft.kind});
    if let Some(model) = &draft.model {
        value["model"] = json!(model);
    }
    if let Some(data) = &draft.data {
        value["data"] = data.clone();
    }
    if let Some(edits) = &draft.edits {
        value["edits"] = json!(edits);
    }
    if let Some(head) = draft.head {
        value["head"] = match head {
            Head::SelfEntry => json!("self"),
            Head::Entry(id) => json!(id),
        };
    }
    if let ExpectedHead::Exact(head) = expected {
        value["expectedHead"] = json!(head);
    }
    validate_entry(None, Some(&value))?;
    Ok(value)
}
fn decode_write(value: &Value) -> Result<(EntryDraft, ExpectedHead), SessionError> {
    let mut draft = EntryDraft::new(crate::wire::string(value, "kind")?);
    // Clone opaque values directly: serde's Value deserializer can reinterpret
    // reserved-key objects when arbitrary_precision is unified by a consumer.
    draft.model = value.get("model").map(messages).transpose()?;
    draft.data = value.get("data").cloned();
    draft.edits = value.get("edits").map(decode_edits).transpose()?;
    draft.head = match value.get("head") {
        None => None,
        Some(Value::String(s)) if s == "self" => Some(Head::SelfEntry),
        Some(_) => Some(Head::Entry(crate::wire::id(value, "head")?)),
    };
    let expected = match value.get("expectedHead") {
        None => ExpectedHead::Any,
        Some(Value::Null) => ExpectedHead::Exact(None),
        Some(_) => ExpectedHead::Exact(Some(crate::wire::id(value, "expectedHead")?)),
    };
    Ok((draft, expected))
}

fn messages(value: &Value) -> Result<Vec<Value>, SessionError> {
    value
        .as_array()
        .cloned()
        .ok_or_else(|| invalid("Expected model message array"))
}
fn decode_edits(value: &Value) -> Result<Vec<ContextEdit>, SessionError> {
    value
        .as_array()
        .ok_or_else(|| invalid("Expected context edit array"))?
        .iter()
        .map(|edit| {
            let target = crate::wire::id(edit, "target")?;
            match edit.get("action").and_then(Value::as_str) {
                Some("omit") => Ok(ContextEdit::Omit { target }),
                Some("replace") => Ok(ContextEdit::Replace {
                    target,
                    messages: messages(&edit["messages"])?,
                }),
                _ => Err(invalid("Invalid context edit action")),
            }
        })
        .collect()
}
