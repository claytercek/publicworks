//! Decode directly from Value: tagged serde content buffers lose opaque numbers
//! when serde_json/arbitrary_precision is enabled. Never stringify caller JSON.
use crate::*;
use serde_json::{Map, json};
use std::collections::BTreeSet;

pub(crate) fn object(value: &Value) -> Result<&Map<String, Value>, SessionError> {
    value
        .as_object()
        .ok_or_else(|| invalid("Expected JSON object"))
}
pub(crate) fn string(value: &Value, key: &str) -> Result<String, SessionError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| invalid(format!("Missing or invalid {key}")))
}
pub(crate) fn number(value: &Value, key: &str) -> Result<u64, SessionError> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid(format!("Missing or invalid {key}")))
}
pub(crate) fn id(value: &Value, key: &str) -> Result<publicworks_runtime::Id, SessionError> {
    Ok(publicworks_runtime::Id::new(number(value, key)?)?)
}
fn keys(value: &Value, allowed: &[&str]) -> Result<(), SessionError> {
    if object(value)?
        .keys()
        .any(|key| !allowed.contains(&key.as_str()))
    {
        return Err(invalid("Unsupported message field"));
    }
    Ok(())
}
pub(crate) fn encode_call(call: &ToolCall) -> Value {
    json!({"id":call.id,"name":call.name,"arguments":call.arguments})
}
pub(crate) fn decode_call(value: &Value) -> Result<ToolCall, SessionError> {
    keys(value, &["id", "name", "arguments"])?;
    let call = ToolCall {
        id: string(value, "id")?,
        name: string(value, "name")?,
        arguments: value
            .get("arguments")
            .cloned()
            .ok_or_else(|| invalid("Missing arguments"))?,
    };
    if call.id.is_empty() || call.name.is_empty() {
        return Err(invalid("Empty tool call ID or name"));
    }
    Ok(call)
}
pub fn encode_message(message: &ModelMessage) -> Value {
    match message {
        ModelMessage::User { text } => json!({"role":"user","text":text}),
        ModelMessage::Assistant { text, tool_calls } => json!({"role":"assistant","text":text,
            "toolCalls":tool_calls.iter().map(encode_call).collect::<Vec<_>>()}),
        ModelMessage::ToolResult {
            call_id,
            content,
            is_error,
        } => json!({"role":"toolResult","callId":call_id,"content":content,"isError":is_error}),
    }
}
pub fn decode_message(value: &Value) -> Result<ModelMessage, SessionError> {
    validate_entry(Some(std::slice::from_ref(value)), None)?;
    match string(value, "role")?.as_str() {
        "user" => {
            keys(value, &["role", "text"])?;
            Ok(ModelMessage::User {
                text: string(value, "text")?,
            })
        }
        "assistant" => {
            keys(value, &["role", "text", "toolCalls"])?;
            let calls = value
                .get("toolCalls")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid("Invalid toolCalls"))?
                .iter()
                .map(decode_call)
                .collect::<Result<Vec<_>, _>>()?;
            let mut ids = BTreeSet::new();
            if calls.iter().any(|call| !ids.insert(&call.id)) {
                return Err(invalid("Duplicate tool call ID"));
            }
            Ok(ModelMessage::Assistant {
                text: string(value, "text")?,
                tool_calls: calls,
            })
        }
        "toolResult" => {
            keys(value, &["role", "callId", "content", "isError"])?;
            let call_id = string(value, "callId")?;
            if call_id.is_empty() {
                return Err(invalid("Empty tool result ID"));
            }
            Ok(ModelMessage::ToolResult {
                call_id,
                content: string(value, "content")?,
                is_error: value
                    .get("isError")
                    .and_then(Value::as_bool)
                    .ok_or_else(|| invalid("Invalid isError"))?,
            })
        }
        _ => Err(invalid("Unsupported model message")),
    }
}
pub(crate) fn declaration(value: &ToolDeclaration) -> Value {
    json!({"name":value.name,"version":value.version,"description":value.description,"parameters":value.parameters})
}
pub(crate) fn decode_declaration(value: &Value) -> Result<ToolDeclaration, SessionError> {
    keys(value, &["name", "version", "description", "parameters"])?;
    let result = ToolDeclaration {
        name: string(value, "name")?,
        version: number(value, "version")?,
        description: string(value, "description")?,
        parameters: value
            .get("parameters")
            .cloned()
            .ok_or_else(|| invalid("Missing parameters"))?,
    };
    if result.name.is_empty() {
        return Err(invalid("Empty tool name"));
    }
    Ok(result)
}
pub(crate) fn declarations(value: &Value) -> Result<Vec<ToolDeclaration>, SessionError> {
    let tools = value
        .as_array()
        .ok_or_else(|| invalid("Invalid tools"))?
        .iter()
        .map(decode_declaration)
        .collect::<Result<Vec<_>, _>>()?;
    let mut names = BTreeSet::new();
    if tools.iter().any(|tool| !names.insert(&tool.name)) {
        return Err(invalid("Duplicate tool name"));
    }
    Ok(tools)
}
/// Use the kernel's native JSON domain checks, including wrapper depth.
pub(crate) fn validate_entry(
    model: Option<&[Value]>,
    data: Option<&Value>,
) -> Result<(), SessionError> {
    for message in model.into_iter().flatten() {
        publicworks_runtime::validate_native_json_value(message, 1)?;
    }
    if let Some(data) = data {
        publicworks_runtime::validate_native_json_value(data, 0)?;
    }
    Ok(())
}
