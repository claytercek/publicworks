//! Decode directly from Value: tagged serde content buffers lose opaque numbers
//! when serde_json/arbitrary_precision is enabled. Never stringify caller JSON.
use crate::*;
use serde_json::{Map, json};
use std::collections::BTreeSet;

/// Containers enclosing tool arguments in a stored model message:
/// model array, message object, toolCalls array, and call object.
const TOOL_ARGUMENT_WRAPPER_DEPTH: usize = 4;

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

fn validate_call_parts(id: &str, name: &str, arguments: &Value) -> Result<(), SessionError> {
    if id.is_empty() || name.is_empty() {
        return Err(invalid("Empty tool call ID or name"));
    }
    publicworks_runtime::validate_native_json_value(arguments, TOOL_ARGUMENT_WRAPPER_DEPTH)?;
    Ok(())
}

/// Validate a call in its model-message wire envelope without encoding it.
pub(crate) fn validate_call(call: &ToolCall) -> Result<(), SessionError> {
    validate_call_parts(&call.id, &call.name, &call.arguments)
}

pub(crate) fn validate_calls(calls: &[ToolCall]) -> Result<(), SessionError> {
    for call in calls {
        validate_call(call)?;
    }
    validate_unique(
        calls.iter().map(|call| call.id.as_str()),
        "Duplicate tool call ID",
    )
}

fn validate_unique<'a>(
    values: impl Iterator<Item = &'a str>,
    error: &str,
) -> Result<(), SessionError> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(invalid(error));
        }
    }
    Ok(())
}

/// Validate one message in the stored model array envelope without encoding it.
pub(crate) fn validate_message(message: &ModelMessage) -> Result<(), SessionError> {
    match message {
        ModelMessage::User { .. } => Ok(()),
        ModelMessage::Assistant { tool_calls, .. } => validate_calls(tool_calls),
        ModelMessage::ToolResult { call_id, .. } if call_id.is_empty() => {
            Err(invalid("Empty tool result ID"))
        }
        ModelMessage::ToolResult { .. } => Ok(()),
    }
}

pub(crate) fn validate_messages(messages: &[ModelMessage]) -> Result<(), SessionError> {
    for message in messages {
        validate_message(message)?;
    }
    Ok(())
}

/// Validate a declaration with the number of containers that enclose the
/// declaration itself. A provider request uses two: its object and tools array.
pub(crate) fn validate_declaration(
    declaration: &ToolDeclaration,
    wrapper_depth: usize,
) -> Result<(), SessionError> {
    validate_declaration_parts(&declaration.name, &declaration.parameters, wrapper_depth)
}

fn validate_declaration_parts(
    name: &str,
    parameters: &Value,
    wrapper_depth: usize,
) -> Result<(), SessionError> {
    if name.is_empty() {
        return Err(invalid("Empty tool name"));
    }
    let parameter_wrapper_depth = wrapper_depth
        .checked_add(1)
        .ok_or_else(|| invalid("JSON payload nesting exceeds 64"))?;
    publicworks_runtime::validate_native_json_value(parameters, parameter_wrapper_depth)?;
    Ok(())
}

pub(crate) fn validate_declarations(
    declarations: &[ToolDeclaration],
    wrapper_depth: usize,
) -> Result<(), SessionError> {
    for declaration in declarations {
        validate_declaration(declaration, wrapper_depth)?;
    }
    validate_unique(
        declarations.iter().map(|tool| tool.name.as_str()),
        "Duplicate tool name",
    )
}

pub(crate) fn encode_call(call: &ToolCall) -> Value {
    json!({"id":call.id,"name":call.name,"arguments":call.arguments})
}
fn decode_call_fields(value: &Value) -> Result<ToolCall, SessionError> {
    keys(value, &["id", "name", "arguments"])?;
    let id = string(value, "id")?;
    let name = string(value, "name")?;
    let arguments = value
        .get("arguments")
        .ok_or_else(|| invalid("Missing arguments"))?;
    // Value::clone is recursive. Reject excessive depth while still borrowing.
    validate_call_parts(&id, &name, arguments)?;
    Ok(ToolCall {
        id,
        name,
        arguments: arguments.clone(),
    })
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
    let message = match string(value, "role")?.as_str() {
        "user" => {
            keys(value, &["role", "text"])?;
            ModelMessage::User {
                text: string(value, "text")?,
            }
        }
        "assistant" => {
            keys(value, &["role", "text", "toolCalls"])?;
            let tool_calls = value
                .get("toolCalls")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid("Invalid toolCalls"))?
                .iter()
                .map(decode_call_fields)
                .collect::<Result<Vec<_>, _>>()?;
            ModelMessage::Assistant {
                text: string(value, "text")?,
                tool_calls,
            }
        }
        "toolResult" => {
            keys(value, &["role", "callId", "content", "isError"])?;
            ModelMessage::ToolResult {
                call_id: string(value, "callId")?,
                content: string(value, "content")?,
                is_error: value
                    .get("isError")
                    .and_then(Value::as_bool)
                    .ok_or_else(|| invalid("Invalid isError"))?,
            }
        }
        _ => return Err(invalid("Unsupported model message")),
    };
    match &message {
        // Each call was validated before cloning; only list uniqueness remains.
        ModelMessage::Assistant { tool_calls, .. } => validate_unique(
            tool_calls.iter().map(|call| call.id.as_str()),
            "Duplicate tool call ID",
        )?,
        _ => validate_message(&message)?,
    }
    Ok(message)
}
pub(crate) fn declaration(value: &ToolDeclaration) -> Value {
    json!({"name":value.name,"version":value.version,"description":value.description,"parameters":value.parameters})
}
fn decode_declaration_fields(value: &Value) -> Result<ToolDeclaration, SessionError> {
    keys(value, &["name", "version", "description", "parameters"])?;
    let name = string(value, "name")?;
    let version = number(value, "version")?;
    let description = string(value, "description")?;
    let parameters = value
        .get("parameters")
        .ok_or_else(|| invalid("Missing parameters"))?;
    // This decoder is used in the declarations array, one enclosing container.
    validate_declaration_parts(&name, parameters, 1)?;
    Ok(ToolDeclaration {
        name,
        version,
        description,
        parameters: parameters.clone(),
    })
}
pub(crate) fn declarations(value: &Value) -> Result<Vec<ToolDeclaration>, SessionError> {
    let tools = value
        .as_array()
        .ok_or_else(|| invalid("Invalid tools"))?
        .iter()
        .map(decode_declaration_fields)
        .collect::<Result<Vec<_>, _>>()?;
    validate_unique(
        tools.iter().map(|tool| tool.name.as_str()),
        "Duplicate tool name",
    )?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn nested(depth: usize) -> Value {
        (0..depth).fold(Value::Null, |value, _| json!([value]))
    }

    fn call(arguments: Value) -> ToolCall {
        ToolCall {
            id: "call".into(),
            name: "tool".into(),
            arguments,
        }
    }

    fn declaration_with(parameters: Value) -> ToolDeclaration {
        ToolDeclaration {
            name: "tool".into(),
            version: u64::MAX,
            description: String::new(),
            parameters,
        }
    }

    // Both construction and teardown must avoid Value's recursive Clone/Drop.
    fn deeply_nested() -> Value {
        (0..50_000).fold(Value::Null, |value, _| Value::Array(vec![value]))
    }

    fn drop_iteratively(value: Value) {
        let mut pending = vec![value];
        while let Some(value) = pending.pop() {
            match value {
                Value::Array(values) => pending.extend(values),
                Value::Object(values) => pending.extend(values.into_values()),
                _ => {}
            }
        }
    }

    #[test]
    fn message_decoder_rejects_deep_arguments_before_cloning() {
        let mut message = json!({"role":"assistant","text":"","toolCalls":[{
            "id":"call","name":"tool","arguments":null
        }]});
        message["toolCalls"][0]["arguments"] = deeply_nested();
        let result = crate::decode_message(&message);
        drop_iteratively(message);
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("JSON payload nesting exceeds")
        );
    }

    #[test]
    fn declaration_decoder_rejects_deep_parameters_before_cloning() {
        let mut tools = json!([{"name":"tool","version":1,"description":"","parameters":null}]);
        tools[0]["parameters"] = deeply_nested();
        let result = declarations(&tools);
        drop_iteratively(tools);
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("JSON payload nesting exceeds")
        );
    }

    #[test]
    fn borrowed_message_validation_matches_direct_decoding() {
        let valid = ModelMessage::Assistant {
            text: String::new(),
            tool_calls: vec![call(json!({
                "max": u64::MAX,
                "min": i64::MIN,
                "float": 1.25,
                "$serde_json::private::Number": "opaque",
                "null": null
            }))],
        };
        assert!(validate_message(&valid).is_ok());
        assert_eq!(decode_message(&encode_message(&valid)).unwrap(), valid);

        for invalid in [
            ModelMessage::Assistant {
                text: String::new(),
                tool_calls: vec![ToolCall {
                    id: String::new(),
                    ..call(Value::Null)
                }],
            },
            ModelMessage::Assistant {
                text: String::new(),
                tool_calls: vec![call(Value::Null), call(Value::Null)],
            },
            ModelMessage::ToolResult {
                call_id: String::new(),
                content: String::new(),
                is_error: false,
            },
        ] {
            assert!(validate_message(&invalid).is_err());
            assert!(decode_message(&encode_message(&invalid)).is_err());
        }
    }

    #[test]
    fn borrowed_declaration_validation_matches_array_decoding() {
        let valid = declaration_with(json!({
            "max": u64::MAX,
            "min": i64::MIN,
            "float": 1.25,
            "$serde_json::private::Number": "opaque",
            "null": null
        }));
        assert!(validate_declarations(std::slice::from_ref(&valid), 1).is_ok());
        assert_eq!(
            declarations(&json!([declaration(&valid)])).unwrap(),
            vec![valid]
        );

        let mut empty = declaration_with(Value::Null);
        empty.name.clear();
        assert!(validate_declarations(std::slice::from_ref(&empty), 1).is_err());
        assert!(declarations(&json!([declaration(&empty)])).is_err());

        let duplicates = vec![declaration_with(Value::Null), declaration_with(Value::Null)];
        assert!(validate_declarations(&duplicates, 1).is_err());
        assert!(
            declarations(&json!(
                duplicates.iter().map(declaration).collect::<Vec<_>>()
            ))
            .is_err()
        );
    }

    #[test]
    fn borrowed_validators_count_actual_wire_wrappers() {
        let accepted_arguments = nested(publicworks_runtime::MAX_JSON_DEPTH - 4);
        assert!(validate_call(&call(accepted_arguments.clone())).is_ok());
        assert!(
            decode_message(&encode_message(&ModelMessage::Assistant {
                text: String::new(),
                tool_calls: vec![call(accepted_arguments)],
            }))
            .is_ok()
        );
        assert!(validate_call(&call(nested(publicworks_runtime::MAX_JSON_DEPTH - 3))).is_err());
        assert!(
            decode_message(&encode_message(&ModelMessage::Assistant {
                text: String::new(),
                tool_calls: vec![call(nested(publicworks_runtime::MAX_JSON_DEPTH - 3))],
            }))
            .is_err()
        );

        // The array decoder counts the tools array and declaration object.
        for (depth, accepted) in [
            (publicworks_runtime::MAX_JSON_DEPTH - 2, true),
            (publicworks_runtime::MAX_JSON_DEPTH - 1, false),
        ] {
            let tools = Value::Array(vec![declaration(&declaration_with(nested(depth)))]);
            assert_eq!(declarations(&tools).is_ok(), accepted);
        }

        let accepted_parameters = nested(publicworks_runtime::MAX_JSON_DEPTH - 3);
        let accepted = declaration_with(accepted_parameters);
        assert!(validate_declaration(&accepted, 2).is_ok());
        assert!(
            validate_declaration(
                &declaration_with(nested(publicworks_runtime::MAX_JSON_DEPTH - 2)),
                2,
            )
            .is_err()
        );
        // A standalone declaration has only its own object around parameters.
        assert!(
            validate_declaration(
                &declaration_with(nested(publicworks_runtime::MAX_JSON_DEPTH - 1)),
                0,
            )
            .is_ok()
        );
    }

    #[test]
    fn decoders_reject_unknown_fields_before_shared_semantics() {
        let mut message = encode_message(&ModelMessage::User {
            text: "hello".into(),
        });
        message["future"] = json!(true);
        assert!(decode_message(&message).is_err());

        let mut tool = declaration(&declaration_with(json!({})));
        tool["future"] = json!(true);
        assert!(declarations(&json!([tool])).is_err());
    }

    #[test]
    fn borrowed_validators_reject_retained_non_native_number_tokens() {
        for token in ["18446744073709551616", "1e999", "1.000", "1e0"] {
            let Ok(value) = serde_json::from_str::<Value>(token) else {
                continue;
            };
            let retained_token = value.to_string();
            if retained_token == token {
                assert!(validate_call(&call(value.clone())).is_err(), "{token}");
                assert!(
                    validate_declaration(&declaration_with(value), 2).is_err(),
                    "{token}"
                );
            }
        }
    }
}
