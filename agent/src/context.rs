//! Read-only projection of the durable text/tool-call transcript subset.
//!
//! Bounds, edits, and tool-result order are transcript semantics. The wire
//! format is the Rust agent subset; entry metadata adds status and result
//! association.
use crate::{ModelMessage, decode_message, invalid};
use publicworks_runtime::{ContextEdit, EntryQuery, EntryRecord, Id, SessionError, Tx, TxFuture};
use serde_json::Value;
use std::collections::BTreeMap;

const SCAN_PAGE_SIZE: usize = 128;
const MISSING_RESULT_TEXT: &str = "Tool result unavailable; the operation may have run.";

/// A derived view. Synthetic missing results are never appended to storage.
#[derive(Clone, Debug, PartialEq)]
pub struct ContextProjection {
    pub messages: Vec<ModelMessage>,
    pub cutoff: Option<Id>,
}

/// Project visible history inside a gated transaction, before any staged writes.
/// `None` captures the latest visible entry; `Some` reconstructs that exact tail.
/// An empty history has no cutoff and contributes no messages.
pub fn project_context(
    tx: &Tx,
    conversation: Id,
    cutoff: Option<Id>,
) -> TxFuture<'_, ContextProjection> {
    Box::pin(async move {
        let (cutoff, head, range) = load_entries(tx, conversation, cutoff).await?;
        Ok(ContextProjection {
            messages: project_messages(head.as_ref(), &range)?,
            cutoff,
        })
    })
}

struct Contribution<'a> {
    entry_id: Id,
    source_id: Id,
    data: Option<&'a Value>,
    value: &'a Value,
}

/// Return the active contributions in oldest-first order, with the latest head
/// first. Edits on discarded older head markers still participate. Keeping the
/// source entry alongside each value lets decoding report useful diagnostics.
fn contributions<'a>(
    head: Option<&'a EntryRecord>,
    range: &'a [EntryRecord],
) -> Vec<Contribution<'a>> {
    let mut edits = BTreeMap::new();
    for entry in range {
        for edit in entry.edits.iter().flatten() {
            let target = match edit {
                ContextEdit::Omit { target } | ContextEdit::Replace { target, .. } => target,
            };
            edits.insert(*target, (entry.id, edit));
        }
    }
    let active = head.into_iter().chain(
        range
            .iter()
            .filter(|entry| head.is_none() || entry.head.is_none()),
    );
    active
        .flat_map(|entry| match edits.get(&entry.id) {
            Some((_, ContextEdit::Omit { .. })) => Vec::new(),
            Some((source, ContextEdit::Replace { messages, .. })) => messages
                .iter()
                .map(|value| Contribution {
                    entry_id: entry.id,
                    source_id: *source,
                    data: None,
                    value,
                })
                .collect(),
            None => entry
                .model
                .iter()
                .flatten()
                .map(|value| Contribution {
                    entry_id: entry.id,
                    source_id: entry.id,
                    data: entry.data.as_ref(),
                    value,
                })
                .collect(),
        })
        .collect()
}

struct ProjectedMessage {
    entry_id: Id,
    assistant_id: Option<Id>,
    message: ModelMessage,
}

fn project_messages(
    head: Option<&EntryRecord>,
    range: &[EntryRecord],
) -> Result<Vec<ModelMessage>, SessionError> {
    let mut messages = Vec::new();
    for contribution in contributions(head, range) {
        let decoded = decode_contribution(&contribution).map_err(|error| {
            invalid(format!(
                "Entry {} (model source entry {}): {error}",
                contribution.entry_id, contribution.source_id
            ))
        })?;
        if let Some(message) = decoded {
            messages.push(message);
        }
    }
    Ok(order_tool_results(&messages))
}

fn decode_contribution(
    contribution: &Contribution<'_>,
) -> Result<Option<ProjectedMessage>, SessionError> {
    // Decode even excluded assistants: unsupported model-bearing history must
    // fail rather than silently discard malformed wire content.
    let message = decode_message(contribution.value)?;
    let mut assistant_id = None;
    match &message {
        ModelMessage::Assistant { .. } => {
            if let Some(status) = contribution.data.and_then(|data| data.get("status")) {
                match status.as_str() {
                    Some("failed" | "aborted") => return Ok(None),
                    Some("completed") => {}
                    _ => return Err(invalid("Invalid assistant status")),
                }
            }
        }
        ModelMessage::ToolResult { call_id, .. } => {
            let data = contribution.data;
            let associated_id = data.and_then(|data| data.get("assistantEntryId"));
            let associated_call = data.and_then(|data| data.get("callId"));
            if let Some(associated_call) = associated_call {
                if associated_call.as_str() != Some(call_id) {
                    return Err(invalid("Tool result callId metadata disagrees with model"));
                }
                if associated_id.is_none() {
                    return Err(invalid("Tool result association missing assistantEntryId"));
                }
            }
            if let Some(value) = associated_id {
                assistant_id =
                    Some(Id::new(value.as_u64().ok_or_else(|| {
                        invalid("Invalid tool result assistantEntryId")
                    })?)?);
            }
        }
        ModelMessage::User { .. } => {}
    }
    Ok(Some(ProjectedMessage {
        entry_id: contribution.entry_id,
        assistant_id,
        message,
    }))
}

/// Each surviving assistant owns the following non-assistant window. The first
/// matching result wins; results never cross the next assistant, even when an
/// explicit association points backward. Unmatched results are discarded.
fn order_tool_results(messages: &[ProjectedMessage]) -> Vec<ModelMessage> {
    let mut ordered = Vec::with_capacity(messages.len());
    let mut index = 0;
    while index < messages.len() {
        let projected = &messages[index];
        match &projected.message {
            ModelMessage::ToolResult { .. } => {
                index += 1;
            }
            ModelMessage::User { .. } => {
                ordered.push(projected.message.clone());
                index += 1;
            }
            ModelMessage::Assistant { tool_calls, .. } => {
                ordered.push(projected.message.clone());
                let mut results = BTreeMap::new();
                let mut following = Vec::new();
                let mut end = index + 1;
                while end < messages.len() {
                    let candidate = &messages[end];
                    match &candidate.message {
                        ModelMessage::Assistant { .. } => break,
                        ModelMessage::ToolResult { call_id, .. }
                            if candidate
                                .assistant_id
                                .is_none_or(|id| id == projected.entry_id) =>
                        {
                            results
                                .entry(call_id.as_str())
                                .or_insert(&candidate.message);
                        }
                        ModelMessage::User { .. } => following.push(candidate.message.clone()),
                        ModelMessage::ToolResult { .. } => {}
                    }
                    end += 1;
                }
                for call in tool_calls {
                    ordered.push(results.get(call.id.as_str()).map_or_else(
                        || ModelMessage::ToolResult {
                            call_id: call.id.clone(),
                            content: MISSING_RESULT_TEXT.into(),
                            is_error: true,
                        },
                        |message| (*message).clone(),
                    ));
                }
                ordered.extend(following);
                index = end;
            }
        }
    }
    ordered
}

/// Capture the latest visible tail, or validate a previously captured tail.
/// All queries are made through the caller's gated transaction, before writes.
async fn load_entries(
    tx: &Tx,
    conversation: Id,
    cutoff: Option<Id>,
) -> Result<(Option<Id>, Option<EntryRecord>, Vec<EntryRecord>), SessionError> {
    let cutoff = match cutoff {
        Some(cutoff) => {
            if tx.visible_entry(conversation, cutoff).await?.is_none() {
                return Err(crate::invalid(format!(
                    "Entry {cutoff} is not visible from conversation {conversation}"
                )));
            }
            cutoff
        }
        None => {
            let latest = tx
                .scan_entries(EntryQuery::new(conversation), 1, None)
                .await?;
            let Some(latest) = latest.items.first() else {
                return Ok((None, None, Vec::new()));
            };
            latest.id
        }
    };
    let head = tx
        .find_latest_head_marker(conversation, Some(cutoff))
        .await?;
    let query = EntryQuery {
        conversation_id: conversation,
        min_entry_id: head.as_ref().and_then(|entry| entry.head),
        max_entry_id: Some(cutoff),
    };
    let mut range = Vec::new();
    let mut cursor = None;
    loop {
        let page = tx
            .scan_entries(query.clone(), SCAN_PAGE_SIZE, cursor)
            .await?;
        range.extend(page.items);
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    range.reverse();
    Ok((Some(cutoff), head, range))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::future::{block_on, zip};
    use publicworks_runtime::{
        ConversationRecord, MemoryStorage, ParentLink, Session, Storage, StorageWrite,
    };
    use serde_json::json;

    fn id(value: u64) -> Id {
        Id::new(value).unwrap()
    }

    fn record(number: u64, model: Vec<Value>) -> EntryRecord {
        let mut entry = EntryRecord::new(id(number), id(2), "application-kind");
        entry.model = Some(model);
        entry
    }

    fn user(text: &str) -> Value {
        json!({"role":"user", "text":text})
    }

    fn replacement(target: u64, text: &str) -> ContextEdit {
        ContextEdit::Replace {
            target: id(target),
            messages: vec![user(text)],
        }
    }

    fn texts(head: Option<&EntryRecord>, range: &[EntryRecord]) -> Vec<String> {
        contributions(head, range)
            .iter()
            .map(|contribution| contribution.value["text"].as_str().unwrap().to_owned())
            .collect()
    }

    fn root() -> StorageWrite {
        StorageWrite::Conversation(ConversationRecord {
            id: id(2),
            parent: None,
            owner: None,
        })
    }

    async fn load_fixture(
        writes: Vec<StorageWrite>,
        conversation: Id,
        cutoff: Option<Id>,
    ) -> Result<(Option<Id>, Option<EntryRecord>, Vec<EntryRecord>), SessionError> {
        let mut storage = MemoryStorage::new();
        storage.commit(writes).await.unwrap();
        let (session, driver) = Session::new(storage);
        zip(
            async {
                let result = session
                    .commit(move |tx| {
                        Box::pin(async move { load_entries(tx, conversation, cutoff).await })
                    })
                    .await;
                session.close().await.unwrap();
                result.map(|receipt| {
                    assert!(receipt.seq.is_none(), "projection must not write");
                    receipt.value
                })
            },
            driver,
        )
        .await
        .0
    }

    #[test]
    fn latest_head_first_older_heads_removed_but_their_edits_count() {
        let first = record(3, vec![user("old")]);
        let mut reset = record(4, vec![user("fresh start")]);
        reset.head = Some(id(4));
        let after = record(5, vec![user("after reset")]);
        assert_eq!(
            texts(Some(&reset), &[reset.clone(), after.clone()]),
            ["fresh start", "after reset"]
        );

        let mut older_head = record(6, vec![user("discard this head")]);
        older_head.head = Some(id(5));
        older_head.edits = Some(vec![replacement(5, "edited by discarded head")]);
        let mut summary = record(7, vec![user("summary")]);
        summary.head = Some(id(5));
        let tail = record(8, vec![user("next")]);
        assert_eq!(
            texts(Some(&summary), &[after, older_head, summary.clone(), tail]),
            ["summary", "edited by discarded head", "next"]
        );
        assert_eq!(texts(None, &[first]), ["old"]);
    }

    #[test]
    fn newest_edit_per_target_and_last_edit_within_entry_win() {
        let first = record(3, vec![user("first")]);
        let second = record(4, vec![user("second")]);
        let mut edit1 = record(5, vec![]);
        edit1.edits = Some(vec![replacement(3, "first v2")]);
        let mut edit2 = record(6, vec![]);
        edit2.edits = Some(vec![
            replacement(3, "first v3"),
            ContextEdit::Omit { target: id(4) },
        ]);
        assert_eq!(
            texts(None, &[first.clone(), second.clone(), edit1, edit2.clone()]),
            ["first v3"]
        );
        edit2
            .edits
            .as_mut()
            .unwrap()
            .push(replacement(4, "second v2"));
        assert_eq!(
            texts(None, &[first, second, edit2]),
            ["first v3", "second v2"]
        );
    }

    #[test]
    fn paginated_history_and_cutoff_reconstruction_ignore_later_edits_and_heads() {
        block_on(async {
            let mut writes = vec![root()];
            for number in 3..=302 {
                writes.push(StorageWrite::Entry(record(
                    number,
                    vec![user(&number.to_string())],
                )));
            }
            let (cutoff, head, range) = load_fixture(writes.clone(), id(2), None).await.unwrap();
            assert_eq!(cutoff, Some(id(302)));
            assert!(head.is_none());
            assert_eq!(range.len(), 300);
            assert_eq!(range.first().unwrap().id, id(3));
            assert_eq!(range.last().unwrap().id, id(302));
            let projected = project_fixture(writes.clone(), id(2), cutoff)
                .await
                .unwrap();
            assert_eq!(projected.messages.len(), 300);
            assert_eq!(
                projected.messages[0],
                ModelMessage::User { text: "3".into() }
            );
            assert_eq!(
                projected.messages[299],
                ModelMessage::User { text: "302".into() }
            );

            let mut edit = record(303, vec![]);
            edit.edits = Some(vec![ContextEdit::Omit { target: id(3) }]);
            let mut head = record(304, vec![user("new head")]);
            head.head = Some(id(304));
            writes.extend([StorageWrite::Entry(edit), StorageWrite::Entry(head)]);
            // Fresh Session/storage with the same durable history plus later writes.
            let reconstructed = load_fixture(writes.clone(), id(2), cutoff).await.unwrap();
            assert_eq!(reconstructed, (cutoff, None, range));
            let latest = load_fixture(writes, id(2), None).await.unwrap();
            assert_eq!(latest.0, Some(id(304)));
            assert_eq!(texts(latest.1.as_ref(), &latest.2), ["new head"]);
        });
    }

    #[test]
    fn fork_visibility_and_active_range_bound_edits() {
        block_on(async {
            let first = record(3, vec![user("first")]);
            let mut early_edit = record(4, vec![]);
            early_edit.edits = Some(vec![replacement(6, "outside range")]);
            let mut old_head = record(5, vec![user("old summary")]);
            old_head.head = Some(id(3));
            let second = record(6, vec![user("kept")]);
            let mut head = record(7, vec![user("summary")]);
            head.head = Some(id(6));
            let mut late_edit = record(8, vec![]);
            late_edit.edits = Some(vec![replacement(6, "past fork cutoff")]);
            let child = StorageWrite::Conversation(ConversationRecord {
                id: id(9),
                parent: Some(ParentLink {
                    conversation_id: id(2),
                    at: id(7),
                }),
                owner: None,
            });
            let mut child_entry = record(10, vec![user("child")]);
            child_entry.conversation_id = id(9);
            let mut writes = vec![root(), child];
            writes.extend(
                [
                    first,
                    early_edit,
                    old_head,
                    second,
                    head,
                    late_edit,
                    child_entry,
                ]
                .into_iter()
                .map(StorageWrite::Entry),
            );
            let (_, head, range) = load_fixture(writes.clone(), id(9), None).await.unwrap();
            assert_eq!(texts(head.as_ref(), &range), ["summary", "kept", "child"]);
            assert!(
                load_fixture(writes, id(9), Some(id(8)))
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("not visible")
            );
        });
    }

    fn assistant(text: &str, calls: &[&str]) -> Value {
        json!({"role":"assistant", "text":text, "toolCalls":calls.iter().map(|call|
            json!({"id":call, "name":format!("tool-{call}"), "arguments":{}})
        ).collect::<Vec<_>>()})
    }

    fn result(call: &str, content: &str) -> Value {
        json!({"role":"toolResult", "callId":call, "content":content, "isError":false})
    }

    fn associated_result(number: u64, assistant_id: u64, call: &str, content: &str) -> EntryRecord {
        let mut entry = record(number, vec![result(call, content)]);
        entry.data = Some(json!({"assistantEntryId":assistant_id, "callId":call}));
        entry
    }

    fn describe(messages: &[ModelMessage]) -> Vec<String> {
        messages
            .iter()
            .map(|message| match message {
                ModelMessage::User { text } => format!("user:{text}"),
                ModelMessage::Assistant { text, .. } => format!("assistant:{text}"),
                ModelMessage::ToolResult {
                    call_id,
                    content,
                    is_error,
                } => {
                    format!(
                        "result:{call_id}:{}",
                        if *is_error { "error" } else { content }
                    )
                }
            })
            .collect()
    }

    async fn project_fixture(
        writes: Vec<StorageWrite>,
        conversation: Id,
        cutoff: Option<Id>,
    ) -> Result<ContextProjection, SessionError> {
        let mut storage = MemoryStorage::new();
        storage.commit(writes).await.unwrap();
        let (session, driver) = Session::new(storage);
        zip(
            async {
                let result = session
                    .commit(move |tx| project_context(tx, conversation, cutoff))
                    .await;
                session.close().await.unwrap();
                result.map(|receipt| {
                    assert!(receipt.seq.is_none(), "projection must not write");
                    receipt.value
                })
            },
            driver,
        )
        .await
        .0
    }

    #[test]
    fn active_head_can_be_replaced_or_omitted_by_in_range_edits() {
        let kept = record(3, vec![user("kept")]);
        let mut head = record(4, vec![user("summary")]);
        head.head = Some(id(3));
        let mut edit = record(5, vec![]);
        edit.edits = Some(vec![replacement(4, "revised summary")]);
        assert_eq!(
            describe(
                &project_messages(Some(&head), &[kept.clone(), head.clone(), edit.clone()])
                    .unwrap()
            ),
            ["user:revised summary", "user:kept"]
        );
        edit.edits = Some(vec![ContextEdit::Omit { target: id(4) }]);
        assert_eq!(
            describe(&project_messages(Some(&head), &[kept, head.clone(), edit]).unwrap()),
            ["user:kept"]
        );
    }

    #[test]
    fn results_follow_assistant_in_call_order_not_commit_order() {
        let range = [
            record(3, vec![result("orphan", "drop")]),
            record(4, vec![user("run tools")]),
            record(5, vec![assistant("calling", &["b", "a"])]),
            associated_result(6, 5, "a", "first a"),
            record(7, vec![user("interposed")]),
            associated_result(8, 5, "b", "b"),
            associated_result(9, 5, "a", "duplicate a"),
            record(10, vec![result("zz", "drop")]),
            record(11, vec![assistant("done", &[])]),
        ];
        assert_eq!(
            describe(&project_messages(None, &range).unwrap()),
            [
                "user:run tools",
                "assistant:calling",
                "result:b:b",
                "result:a:first a",
                "user:interposed",
                "assistant:done",
            ]
        );
    }

    #[test]
    fn explicit_associations_never_fall_back_or_cross_next_assistant() {
        let range = [
            record(3, vec![assistant("first", &["x"])]),
            associated_result(4, 99, "x", "wrong explicit association"),
            record(5, vec![assistant("second", &["x"])]),
            associated_result(6, 3, "x", "too late for first"),
            record(7, vec![result("x", "generic for second")]),
            associated_result(8, 5, "x", "duplicate for second"),
        ];
        let messages = project_messages(None, &range).unwrap();
        assert_eq!(
            describe(&messages),
            [
                "assistant:first",
                "result:x:error",
                "assistant:second",
                "result:x:generic for second"
            ]
        );
        assert_eq!(
            messages[1],
            ModelMessage::ToolResult {
                call_id: "x".into(),
                content: MISSING_RESULT_TEXT.into(),
                is_error: true,
            }
        );
    }

    #[test]
    fn failed_and_aborted_original_assistants_are_excluded_not_their_replacements() {
        let mut failed = record(3, vec![assistant("failed", &["x"])]);
        failed.data = Some(json!({"status":"failed"}));
        let mut aborted = record(4, vec![assistant("aborted", &["y"])]);
        aborted.data = Some(json!({"status":"aborted"}));
        let mut completed = record(5, vec![assistant("done", &[])]);
        completed.data = Some(json!({"status":"completed"}));
        let orphan = associated_result(6, 3, "x", "failed assistant result");
        let mut range = vec![failed, aborted, completed, orphan];
        assert_eq!(
            describe(&project_messages(None, &range).unwrap()),
            ["assistant:done"]
        );
        let mut edit = record(7, vec![]);
        edit.edits = Some(vec![ContextEdit::Replace {
            target: id(3),
            messages: vec![assistant("replacement", &[])],
        }]);
        range.push(edit);
        assert_eq!(
            describe(&project_messages(None, &range).unwrap()),
            ["assistant:replacement", "assistant:done"]
        );
    }

    #[test]
    fn replacements_ignore_original_result_metadata_and_use_new_call_id() {
        let calling = record(3, vec![assistant("calling", &["new"])]);
        let original = associated_result(4, 99, "old", "original");
        let mut edit = record(5, vec![]);
        edit.edits = Some(vec![ContextEdit::Replace {
            target: id(4),
            messages: vec![result("new", "replacement")],
        }]);
        assert_eq!(
            describe(&project_messages(None, &[calling, original, edit]).unwrap()),
            ["assistant:calling", "result:new:replacement"]
        );
    }

    #[test]
    fn model_less_is_harmless_but_unsupported_or_malformed_model_is_an_error() {
        let mut note = EntryRecord::new(id(3), id(2), "agent.diagnostic");
        note.data =
            Some(json!({"status":false, "assistantEntryId":"not a runtime ID", "whatever":42}));
        assert!(project_messages(None, &[note]).unwrap().is_empty());
        let invalid_messages = [
            json!({"role":"system", "text":"unsupported"}),
            json!({"role":"user", "content":"Foreign wire is unsupported"}),
            json!({"role":"user", "text":42}),
            json!({"role":"assistant", "text":"bad", "toolCalls":[], "stopReason":"error"}),
            assistant("duplicate IDs", &["x", "x"]),
            json!({"role":"assistant", "text":"bad", "toolCalls":[{"id":"x", "name":"t"}]}),
        ];
        for value in invalid_messages {
            let error = project_messages(None, &[record(42, vec![value])])
                .unwrap_err()
                .to_string();
            assert!(error.contains("Entry 42"), "{error}");
        }
        let mut bad_replacement = record(99, vec![]);
        bad_replacement.edits = Some(vec![ContextEdit::Replace {
            target: id(42),
            messages: vec![json!({"role":"unknown"})],
        }]);
        let error = project_messages(None, &[record(42, vec![user("ok")]), bad_replacement])
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("Entry 42 (model source entry 99)"),
            "{error}"
        );
    }

    #[test]
    fn invalid_association_metadata_reports_its_entry() {
        for data in [
            json!({"assistantEntryId":3, "callId":"wrong"}),
            json!({"assistantEntryId":"3", "callId":"x"}),
            json!({"assistantEntryId":0, "callId":"x"}),
            json!({"callId":"x"}),
        ] {
            let mut entry = record(42, vec![result("x", "result")]);
            entry.data = Some(data);
            let error = project_messages(None, &[entry]).unwrap_err().to_string();
            assert!(error.contains("Entry 42"), "{error}");
        }
    }

    #[test]
    fn fork_cut_synthesizes_missing_results_and_head_cut_drops_orphans() {
        block_on(async {
            let mut writes = vec![root()];
            writes.extend(
                [
                    record(3, vec![user("go")]),
                    record(4, vec![assistant("calling", &["x", "y"])]),
                    associated_result(5, 4, "x", "x"),
                    associated_result(6, 4, "y", "y"),
                ]
                .into_iter()
                .map(StorageWrite::Entry),
            );
            writes.push(StorageWrite::Conversation(ConversationRecord {
                id: id(7),
                parent: Some(ParentLink {
                    conversation_id: id(2),
                    at: id(4),
                }),
                owner: None,
            }));
            let child = project_fixture(writes.clone(), id(7), None).await.unwrap();
            assert_eq!(child.cutoff, Some(id(4)));
            assert_eq!(
                describe(&child.messages),
                [
                    "user:go",
                    "assistant:calling",
                    "result:x:error",
                    "result:y:error"
                ]
            );
            let mut head = record(8, vec![]);
            head.head = Some(id(6));
            writes.push(StorageWrite::Entry(head));
            assert!(
                project_fixture(writes.clone(), id(2), None)
                    .await
                    .unwrap()
                    .messages
                    .is_empty()
            );
            let pinned = project_fixture(writes, id(2), Some(id(4))).await.unwrap();
            assert_eq!(pinned, child);
        });
    }

    #[test]
    fn empty_history_projects_without_a_cutoff() {
        block_on(async {
            assert_eq!(
                project_fixture(vec![root()], id(2), None).await.unwrap(),
                ContextProjection {
                    messages: vec![],
                    cutoff: None
                }
            );
        });
    }

    #[test]
    fn opaque_tool_arguments_preserve_native_numbers_and_reserved_key_objects() {
        block_on(async {
            let opaque = json!({
                "unsigned":u64::MAX, "signed":i64::MIN, "float":0.125,
                "reserved":{"$serde_json::private::Number":"184467440737095516160000"},
                "raw":{"$serde_json::private::RawValue":"{not JSON}"},
                "nested":[null, {"type":"assistant", "content":{"arbitrary":true}}],
            });
            let mut message = assistant("call", &["provider-string-not-runtime-id"]);
            message["toolCalls"][0]["arguments"] = opaque.clone();
            let projection = project_fixture(
                vec![
                    root(),
                    StorageWrite::Entry(record(3, vec![message.clone()])),
                ],
                id(2),
                None,
            )
            .await
            .unwrap();
            let ModelMessage::Assistant { tool_calls, .. } = &projection.messages[0] else {
                panic!("expected assistant")
            };
            assert_eq!(tool_calls[0].arguments, opaque);
            assert_eq!(tool_calls[0].arguments["unsigned"].as_u64(), Some(u64::MAX));
            assert_eq!(tool_calls[0].arguments["signed"].as_i64(), Some(i64::MIN));
            assert!(tool_calls[0].arguments["reserved"].is_object());
            assert_eq!(crate::encode_message(&projection.messages[0]), message);

            // Replacement payloads take the same direct-Value path.
            let mut edit = record(4, vec![]);
            edit.edits = Some(vec![ContextEdit::Replace {
                target: id(3),
                messages: vec![message.clone()],
            }]);
            let replaced = project_messages(None, &[record(3, vec![user("old")]), edit]).unwrap();
            assert_eq!(crate::encode_message(&replaced[0]), message);
        });
    }
}
