//! Stateless Responses wire mapping. Never deserialize opaque JSON through Value's
//! serde visitor: feature-unified arbitrary_precision reserves object keys there.
use crate::error;
use publicworks_agent::{
    FinishReason, ModelError, ModelMessage, ModelRequest, ModelResponse, ToolCall,
};
use publicworks_runtime::{decode_native_json, validate_native_json_value};
use serde_json::{Value, json};
use std::collections::BTreeSet;

fn name_valid(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
}

#[allow(
    clippy::result_large_err,
    reason = "Match the agent Model error contract"
)]
pub(super) fn encode_request(request: &ModelRequest) -> Result<Vec<u8>, ModelError> {
    let invalid = || error("Invalid OpenAI model request");
    if request.model.trim().is_empty() || request.messages.is_empty() {
        return Err(invalid());
    }
    let mut names = BTreeSet::new();
    let mut tools = Vec::new();
    for tool in &request.tools {
        if !name_valid(&tool.name)
            || !names.insert(&tool.name)
            || !tool.parameters.is_object()
            || tool.parameters.get("type").and_then(Value::as_str) != Some("object")
        {
            return Err(invalid());
        }
        tools.push(json!({"type":"function", "name":tool.name,
            "description":tool.description, "parameters":tool.parameters, "strict":false}));
    }
    let mut input = Vec::new();
    let mut pending = BTreeSet::new();
    for message in &request.messages {
        match message {
            ModelMessage::User { text } => {
                if !pending.is_empty() {
                    return Err(invalid());
                }
                input.push(json!({"role":"user", "content":[{"type":"input_text", "text":text}]}));
            }
            ModelMessage::Assistant { text, tool_calls } => {
                if !pending.is_empty() {
                    return Err(invalid());
                }
                if !text.is_empty() || tool_calls.is_empty() {
                    input.push(
                        json!({"role":"assistant", "content":[{"type":"input_text", "text":text}]}),
                    );
                }
                for call in tool_calls {
                    if call.id.is_empty()
                        || !name_valid(&call.name)
                        || !pending.insert(call.id.as_str())
                    {
                        return Err(invalid());
                    }
                    // Validate the original native value before stringification can
                    // hide an out-of-domain number/depth in a JSON string.
                    validate_payload(&call.arguments).map_err(|_| invalid())?;
                    let arguments =
                        serde_json::to_string(&call.arguments).map_err(|_| invalid())?;
                    input.push(json!({"type":"function_call", "call_id":call.id,
                        "name":call.name, "arguments":arguments}));
                }
            }
            ModelMessage::ToolResult {
                call_id,
                content,
                is_error,
            } => {
                if !pending.remove(call_id.as_str()) {
                    return Err(invalid());
                }
                let output = if *is_error {
                    format!("[tool_error]\n{content}")
                } else {
                    content.clone()
                };
                input.push(
                    json!({"type":"function_call_output", "call_id":call_id, "output":output}),
                );
            }
        }
    }
    if !pending.is_empty() {
        return Err(invalid());
    }
    let body = json!({"model":request.model, "instructions":request.instructions,
        "input":input, "tools":tools, "store":false, "stream":false, "parallel_tool_calls":true});
    validate_payload(&body).map_err(|_| invalid())?;
    serde_json::to_vec(&body).map_err(|_| invalid())
}

fn validate_payload(value: &Value) -> Result<(), publicworks_runtime::StorageError> {
    validate_native_json_value(value, 0)
}

#[allow(
    clippy::result_large_err,
    reason = "Match the agent Model error contract"
)]
pub(super) fn decode_response(bytes: &[u8]) -> Result<ModelResponse, ModelError> {
    let value = decode_native_json(bytes).map_err(|_| error("Invalid OpenAI response JSON"))?;
    let usage = value.get("usage").cloned();
    let mut text = String::new();
    let mut calls = Vec::new();
    let parsed = decode_output(&value, &mut text, &mut calls);
    let failure = if !value.is_object() {
        Some("Invalid OpenAI response envelope")
    } else if value.get("status").and_then(Value::as_str) != Some("completed") {
        Some("OpenAI response was not completed")
    } else if value.get("error").is_some_and(|error| !error.is_null()) {
        Some("OpenAI response reported an error")
    } else if value
        .get("incomplete_details")
        .is_some_and(|details| !details.is_null())
    {
        Some("OpenAI response reported incomplete output")
    } else {
        parsed.err()
    };
    if let Some(message) = failure {
        // A partial is diagnostic text only. Never leak executable calls from an
        // invalid response, even when earlier calls were individually complete.
        let partial_response = (!text.is_empty()).then(|| ModelResponse {
            message: ModelMessage::Assistant {
                text,
                tool_calls: Vec::new(),
            },
            finish_reason: FinishReason::Stop,
            usage: None,
        });
        return Err(ModelError {
            message: message.into(),
            partial_response,
            usage,
        });
    }
    Ok(ModelResponse {
        finish_reason: if calls.is_empty() {
            FinishReason::Stop
        } else {
            FinishReason::ToolCalls
        },
        message: ModelMessage::Assistant {
            text,
            tool_calls: calls,
        },
        usage,
    })
}

fn decode_output(
    value: &Value,
    text: &mut String,
    calls: &mut Vec<ToolCall>,
) -> Result<(), &'static str> {
    let output = value
        .get("output")
        .and_then(Value::as_array)
        .ok_or("Invalid OpenAI output array")?;
    let mut ids = BTreeSet::new();
    for item in output {
        match item.get("type").and_then(Value::as_str) {
            Some("message") => {
                if item.get("role").and_then(Value::as_str) != Some("assistant") {
                    return Err("Invalid OpenAI output message role");
                }
                let content = item
                    .get("content")
                    .and_then(Value::as_array)
                    .ok_or("Invalid OpenAI message content")?;
                for part in content {
                    match part.get("type").and_then(Value::as_str) {
                        Some("output_text") => text.push_str(
                            part.get("text")
                                .and_then(Value::as_str)
                                .ok_or("Invalid OpenAI output text")?,
                        ),
                        Some("refusal") => return Err("OpenAI refusal is unsupported"),
                        _ => return Err("Unsupported OpenAI message content"),
                    }
                }
                if item.get("status").and_then(Value::as_str) != Some("completed") {
                    return Err("OpenAI output message was not completed");
                }
            }
            Some("function_call") => {
                if item.get("status").and_then(Value::as_str) != Some("completed") {
                    return Err("OpenAI function call was not completed");
                }
                let id = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .ok_or("Invalid OpenAI function call ID")?;
                let name = item
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|name| name_valid(name))
                    .ok_or("Invalid OpenAI function name")?;
                if !ids.insert(id) {
                    return Err("Duplicate OpenAI function call ID");
                }
                let arguments = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .ok_or("Invalid OpenAI function arguments")?;
                let arguments = decode_native_json(arguments.as_bytes())
                    .map_err(|_| "Invalid OpenAI function arguments JSON")?;
                calls.push(ToolCall {
                    id: id.into(),
                    name: name.into(),
                    arguments,
                });
            }
            Some("reasoning") => return Err("OpenAI reasoning output is unsupported"),
            _ => return Err("Unsupported OpenAI output item"),
        }
    }
    Ok(())
}
