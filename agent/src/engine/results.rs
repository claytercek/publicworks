//! Raw execution results, never projected transcript messages. The first valid
//! matching entry wins, including host-inserted results. Metadata and the single
//! model message must agree; malformed receipts never trigger a fallback.
use super::*;

#[derive(Clone, Debug)]
pub(super) struct RecordedResult {
    pub id: Id,
    pub result: ToolResult,
}
fn decode_result(
    entry: EntryRecord,
    conversation: Id,
    assistant: Id,
    call_id: &str,
) -> Result<RecordedResult, SessionError> {
    let data = entry
        .data
        .as_ref()
        .ok_or_else(|| invalid("Missing result metadata"))?;
    if entry.conversation_id != conversation
        || entry.id <= assistant
        || entry.kind != "agent.toolResult"
        || id(data, "assistantEntryId")? != assistant
        || string(data, "callId")? != call_id
    {
        return Err(invalid("Invalid tool result association"));
    }
    let messages = entry
        .model
        .ok_or_else(|| invalid("Missing result message"))?;
    if messages.len() != 1 {
        return Err(invalid("Invalid result messages"));
    }
    let ModelMessage::ToolResult {
        call_id: model_id,
        content,
        is_error,
    } = decode_message(&messages[0])?
    else {
        return Err(invalid("Invalid result message"));
    };
    if model_id != call_id {
        return Err(invalid("Result metadata differs from model call"));
    }
    Ok(RecordedResult {
        id: entry.id,
        result: ToolResult {
            content,
            is_error,
            usage: data.get("usage").cloned(),
        },
    })
}

/// Scan only the current batch's raw suffix, not unrelated conversation history.
/// Used for observers, host-supplied results, and missing-receipt recovery.
pub(super) async fn collect_results(
    tx: &Tx,
    conversation: Id,
    assistant: Id,
) -> Result<BTreeMap<String, RecordedResult>, SessionError> {
    let mut query = EntryQuery::new(conversation);
    query.min_entry_id = Some(assistant);
    let mut cursor = None;
    let mut results = BTreeMap::new();
    loop {
        let page = tx.scan_entries(query.clone(), 128, cursor).await?;
        for entry in page.items {
            if entry.kind != "agent.toolResult"
                || entry
                    .data
                    .as_ref()
                    .and_then(|d| d.get("assistantEntryId"))
                    .and_then(Value::as_u64)
                    != Some(assistant.get())
            {
                continue;
            }
            let call_id = string(entry.data.as_ref().expect("checked data"), "callId")?;
            let result = decode_result(entry, conversation, assistant, &call_id)?;
            let existing = results.entry(call_id).or_insert_with(|| result.clone());
            if result.id < existing.id {
                *existing = result;
            }
        }
        cursor = page.next;
        if cursor.is_none() {
            return Ok(results);
        }
    }
}

pub(super) async fn child_receipt(
    tx: &Tx,
    root: &TaskRecord,
    batch: &ToolBatch,
    call: &ToolCall,
) -> Result<Option<RecordedResult>, SessionError> {
    let child = tx
        .task(batch.child)
        .await?
        .ok_or_else(|| invalid("Missing tool child"))?;
    if child.kind != TOOL_KIND
        || child.version != TOOL_VERSION
        || child.owner != Some(root.id)
        || child.conversation_id != root.conversation_id
        || child.background
        || ToolInput::decode(&child.input)?
            != (ToolInput {
                assistant: batch.assistant,
                index: batch.index,
            })
    {
        return Err(invalid("Invalid tool child association"));
    }
    let TaskState::Terminal { outcome } = child.state else {
        return Err(invalid("Tool child not settled"));
    };
    let result = match outcome {
        TaskOutcome::Completed { result } => Some(result),
        TaskOutcome::Failed { result, .. } | TaskOutcome::Aborted { result, .. } => result,
        TaskOutcome::Faulted { .. } | TaskOutcome::Orphaned { .. } => return Ok(None),
    }
    .ok_or_else(|| invalid("Missing tool result receipt"))?;
    let entry_id = id(&result, "resultEntryId")?;
    let entry = tx
        .visible_entry(root.conversation_id, entry_id)
        .await?
        .ok_or_else(|| invalid("Missing receipted tool result"))?
        .entry;
    decode_result(entry, root.conversation_id, batch.assistant, &call.id).map(Some)
}

pub(super) fn unavailable() -> ToolResult {
    ToolResult {
        content: "Tool result unavailable; the operation may have run.".into(),
        is_error: true,
        usage: None,
    }
}
pub(super) async fn append_result(
    tx: &Tx,
    conversation: Id,
    assistant: Id,
    call_id: &str,
    result: ToolResult,
    code: Option<&str>,
) -> Result<Id, SessionError> {
    let mut data = json!({"assistantEntryId":assistant.get(),"callId":call_id});
    if let Some(code) = code {
        data["code"] = Value::String(code.into());
    }
    if let Some(usage) = result.usage {
        data["usage"] = usage;
    }
    Ok(tx
        .append_entry(
            conversation,
            message_draft(
                "agent.toolResult",
                ModelMessage::ToolResult {
                    call_id: call_id.into(),
                    content: result.content,
                    is_error: result.is_error,
                },
                Some(data),
            ),
        )
        .await?
        .id)
}
pub(super) enum End {
    Complete,
    Fail(String),
    Abort,
}
pub(super) async fn finish_tool(
    runtime: &TaskRuntime,
    input: &ToolInput,
    call_id: &str,
    result: ToolResult,
    code: Option<&str>,
    end: End,
) -> Result<(), SessionError> {
    let input = input.clone();
    let call_id = call_id.to_owned();
    let code = code.map(str::to_owned);
    runtime
        .commit(move |tx, task| {
            Box::pin(async move {
                let recorded = collect_results(tx, task.conversation_id, input.assistant).await?;
                let entry = match recorded.get(&call_id) {
                    Some(existing) => existing.id,
                    None => {
                        append_result(
                            tx,
                            task.conversation_id,
                            input.assistant,
                            &call_id,
                            result,
                            code.as_deref(),
                        )
                        .await?
                    }
                };
                let result = json!({"resultEntryId":entry.get()});
                Ok(Some(match end {
                    End::Complete => TaskUpdate::Complete(result),
                    End::Fail(message) => TaskUpdate::Fail(
                        TaskOutcomeError {
                            message,
                            detail: None,
                        },
                        Some(result),
                    ),
                    End::Abort => TaskUpdate::Abort {
                        reason: Some("Tool aborted".into()),
                        result: Some(result),
                    },
                }))
            })
        })
        .await?;
    Ok(())
}
