use super::*;
use publicworks_agent::{ModelMessage, ToolCall, ToolDeclaration};
use publicworks_runtime::decode_native_json;
use serde_json::{Value, json};

fn request() -> ModelRequest {
    ModelRequest {
        model: "gpt-4.1-mini".into(),
        instructions: "Be brief".into(),
        tools: vec![ToolDeclaration {
            name: "weather".into(),
            version: 99,
            description: "Look up weather".into(),
            parameters: json!({"type":"object", "properties":{"city":{"type":"string"}}}),
        }],
        messages: vec![ModelMessage::User {
            text: "Hello".into(),
        }],
    }
}
fn encode(request: &ModelRequest) -> Value {
    decode_native_json(&wire::encode_request(request).unwrap()).unwrap()
}
fn call(id: &str) -> Value {
    json!({"type":"function_call", "status":"completed", "id":format!("item_{id}"),
        "call_id":id, "name":"weather", "arguments":"{\"city\":\"Boston\"}"})
}
fn text(text: &str) -> Value {
    json!({"type":"message", "role":"assistant", "status":"completed",
        "content":[{"type":"output_text", "text":text, "annotations":[]}]})
}
fn envelope(output: Vec<Value>) -> Value {
    json!({"object":"response", "status":"completed", "error":null, "output":output})
}
#[allow(
    clippy::result_large_err,
    reason = "Exercise the agent Model error contract"
)]
fn decode(value: &Value) -> Result<publicworks_agent::ModelResponse, ModelError> {
    wire::decode_response(&serde_json::to_vec(value).unwrap())
}
fn assert_no_partial_calls(error: &ModelError) {
    if let Some(partial) = &error.partial_response {
        assert!(partial.tool_calls.is_empty());
    }
}

#[test]
fn request_uses_only_persisted_semantics_and_explicit_non_strict_tools() {
    let encoded = encode(&request());
    assert_eq!(encoded["model"], "gpt-4.1-mini");
    assert_eq!(encoded["instructions"], "Be brief");
    assert_eq!(encoded["store"], false);
    assert_eq!(encoded["stream"], false);
    assert_eq!(encoded["parallel_tool_calls"], true);
    assert_eq!(encoded["tools"][0]["strict"], false);
    assert_eq!(encoded["tools"][0]["type"], "function");
    assert!(encoded["tools"][0].get("version").is_none());
    assert!(encoded["tools"][0]["parameters"].get("required").is_none());
    for absent in [
        "previous_response_id",
        "conversation",
        "max_output_tokens",
        "temperature",
    ] {
        assert!(encoded.get(absent).is_none());
    }
}

#[test]
fn request_history_uses_call_ids_and_error_text_without_vendor_item_ids() {
    let mut req = request();
    req.messages.extend([
        ModelMessage::Assistant {
            text: "Checking".into(),
            tool_calls: ["b", "a"]
                .map(|id| ToolCall {
                    id: id.into(),
                    name: "weather".into(),
                    arguments: json!({"city":"Boston"}),
                })
                .into(),
        },
        ModelMessage::ToolResult {
            call_id: "a".into(),
            content: "bad".into(),
            is_error: true,
        },
        ModelMessage::ToolResult {
            call_id: "b".into(),
            content: "72 F".into(),
            is_error: false,
        },
    ]);
    let encoded = encode(&req);
    let input = encoded["input"].as_array().unwrap();
    assert_eq!(input[1]["role"], "assistant");
    assert_eq!(input[2]["call_id"], "b");
    assert_eq!(input[3]["call_id"], "a");
    assert_eq!(input[2]["arguments"], "{\"city\":\"Boston\"}");
    assert_eq!(input[4]["output"], "[tool_error]\nbad");
    assert_eq!(input[5]["output"], "72 F");
    assert!(input.iter().all(|v| v.get("id").is_none()));
}

#[test]
fn direct_requests_reject_invalid_schema_names_and_history() {
    let mut cases = Vec::new();
    let mut req = request();
    req.model.clear();
    cases.push(req);
    let mut req = request();
    req.messages.clear();
    cases.push(req);
    let mut req = request();
    req.tools[0].parameters = json!(true);
    cases.push(req);
    let mut req = request();
    req.tools[0].parameters = json!({"type":"array"});
    cases.push(req);
    let mut req = request();
    req.tools[0].name = "bad name".into();
    cases.push(req);
    let mut req = request();
    req.tools.push(req.tools[0].clone());
    cases.push(req);
    let mut req = request();
    req.messages.push(ModelMessage::ToolResult {
        call_id: "orphan".into(),
        content: "".into(),
        is_error: false,
    });
    cases.push(req);
    let mut req = request();
    req.messages.push(ModelMessage::Assistant {
        text: "".into(),
        tool_calls: vec![ToolCall {
            id: "x".into(),
            name: "weather".into(),
            arguments: json!({}),
        }],
    });
    cases.push(req);
    for req in cases {
        assert!(wire::encode_request(&req).is_err());
    }
}

#[test]
fn completed_text_and_multiple_calls_keep_order_and_ignore_item_ids() {
    let response = decode(&envelope(vec![
        text("First"),
        call("z"),
        call("a"),
        text("Second"),
    ]))
    .unwrap();
    assert_eq!(response.text, "FirstSecond");
    assert_eq!(
        response
            .tool_calls
            .iter()
            .map(|c| c.id.as_str())
            .collect::<Vec<_>>(),
        ["z", "a"]
    );
    assert_eq!(response.tool_calls[0].arguments, json!({"city":"Boston"}));
    assert!(
        decode(&envelope(vec![self::text("Done")]))
            .unwrap()
            .tool_calls
            .is_empty()
    );
}

#[test]
fn usage_preserves_absent_null_and_opaque_native_values_on_success_and_error() {
    for usage in [None, Some(Value::Null), Some(opaque())] {
        let mut value = envelope(vec![text("Good")]);
        if let Some(usage) = &usage {
            value["usage"] = usage.clone();
        }
        assert_eq!(decode(&value).unwrap().usage, usage);
        value["output"]
            .as_array_mut()
            .unwrap()
            .push(json!({"type":"reasoning"}));
        let error = decode(&value).unwrap_err();
        assert_eq!(error.usage, usage);
        assert_eq!(error.partial_response.as_ref().unwrap().text, "Good");
        assert_no_partial_calls(&error);
    }
}

fn opaque() -> Value {
    json!({"i":i64::MIN, "u":u64::MAX, "f":0.12345678901234568_f64,
        "number":{"$serde_json::private::Number":"not a number"},
        "raw":{"$serde_json::private::RawValue":"not JSON"}})
}

#[test]
fn native_json_arguments_and_reserved_objects_survive_both_directions() {
    let mut item = call("native");
    item["arguments"] = Value::String(serde_json::to_string(&opaque()).unwrap());
    let result = decode(&envelope(vec![item])).unwrap();
    let tool_calls = result.tool_calls;
    assert_eq!(tool_calls[0].arguments, opaque());
    assert_eq!(tool_calls[0].arguments["i"].as_i64(), Some(i64::MIN));
    assert_eq!(tool_calls[0].arguments["u"].as_u64(), Some(u64::MAX));
    let mut req = request();
    req.messages.extend([
        ModelMessage::Assistant {
            text: "".into(),
            tool_calls,
        },
        ModelMessage::ToolResult {
            call_id: "native".into(),
            content: "ok".into(),
            is_error: false,
        },
    ]);
    let encoded = encode(&req);
    let args = encoded["input"][1]["arguments"].as_str().unwrap();
    assert_eq!(decode_native_json(args.as_bytes()).unwrap(), opaque());
}

#[test]
fn all_invalid_output_fails_closed_without_partial_calls_and_keeps_usage() {
    let mut incomplete_call = call("b");
    incomplete_call["status"] = json!("in_progress");
    let mut missing_id = call("b");
    missing_id.as_object_mut().unwrap().remove("call_id");
    let mut bad_args = call("b");
    bad_args["arguments"] = json!("{broken");
    let mut args_object = call("b");
    args_object["arguments"] = json!({});
    let mut invalid_role = text("bad");
    invalid_role["role"] = json!("user");
    let mut incomplete_text = text("partial");
    incomplete_text["status"] = json!("incomplete");
    let mut refusal = text("");
    refusal["content"] = json!([{"type":"refusal", "refusal":"private"}]);
    let mut unknown_content = text("");
    unknown_content["content"] = json!([{"type":"audio"}]);
    let mut no_status = call("b");
    no_status.as_object_mut().unwrap().remove("status");
    for bad in [
        call("a"),
        incomplete_call,
        missing_id,
        bad_args,
        args_object,
        invalid_role,
        incomplete_text,
        refusal,
        unknown_content,
        no_status,
        json!({"type":"reasoning"}),
        json!({"type":"future_type"}),
        json!(null),
    ] {
        let mut value = envelope(vec![text("safe"), call("a"), bad]);
        value["usage"] = opaque();
        let error = decode(&value).unwrap_err();
        assert_eq!(error.usage, Some(opaque()));
        assert!(
            error
                .partial_response
                .as_ref()
                .unwrap()
                .text
                .starts_with("safe")
        );
        assert_no_partial_calls(&error);
        assert!(!error.message.contains("private"));
    }
}

#[test]
fn bad_envelopes_and_non_completed_responses_never_succeed() {
    for status in [
        "incomplete",
        "failed",
        "cancelled",
        "in_progress",
        "queued",
        "unknown",
    ] {
        let mut value = envelope(vec![text("partial"), call("x")]);
        value["status"] = json!(status);
        value["usage"] = json!(null);
        let error = decode(&value).unwrap_err();
        assert_eq!(error.usage, Some(Value::Null));
        assert_eq!(error.partial_response.as_ref().unwrap().text, "partial");
        assert_no_partial_calls(&error);
    }
    for bad in [
        json!(null),
        json!([]),
        json!({}),
        json!({"status":"completed", "output":{}}),
    ] {
        assert!(decode(&bad).is_err());
    }
    let mut value = envelope(vec![call("x")]);
    value["error"] = json!({"message":"secret"});
    let error = decode(&value).unwrap_err();
    assert!(!error.message.contains("secret"));
    assert_no_partial_calls(&error);
    for bytes in [
        b"not JSON".as_slice(),
        b"{\"output\":[],\"status\":\"completed\",\"usage\":1e999}",
    ] {
        assert!(wire::decode_response(bytes).is_err());
    }
}

#[test]
fn malformed_native_arguments_and_excess_depth_reject() {
    for args in [
        "1e999".to_owned(),
        format!("{}0{}", "[".repeat(65), "]".repeat(65)),
    ] {
        let mut item = call("x");
        item["arguments"] = json!(args);
        assert!(decode(&envelope(vec![item])).is_err());
    }
}

#[test]
fn endpoint_rejects_authority_repair_and_hidden_userinfo() {
    for endpoint in [
        "https:///@example.com/responses",
        "https:////@example.com/responses",
        "https:///example.com/responses",
        "http:///@127.0.0.1/responses",
        "https://\\\\@example.com/responses",
        "https://example.com\\\\@other.example/responses",
        "https://\t@example.com/responses",
        "https://exa\nmple.com/responses",
        "https://example.com/responses\r",
        " https://example.com/responses",
        "https://example.com/a b",
        "https://example.com/\0",
        r"https://\@example.com/responses",
    ] {
        assert_eq!(
            OpenAiResponses::with_config(
                "fake-key",
                Config {
                    endpoint: endpoint.into(),
                    ..Config::default()
                }
            )
            .unwrap_err(),
            ConfigError::InvalidEndpoint,
            "accepted ambiguous endpoint {endpoint:?}"
        );
    }
    // Legitimate normalization and percent-encoded path data remain supported.
    for endpoint in [
        "HTTPS://EXAMPLE.COM/responses",
        "https://example.com/a%20b",
        "https://example.com/@path",
    ] {
        assert!(
            OpenAiResponses::with_config(
                "fake-key",
                Config {
                    endpoint: endpoint.into(),
                    ..Config::default()
                }
            )
            .is_ok()
        );
    }
}

#[test]
fn configuration_validation_and_debug_never_echo_secrets() {
    for endpoint in [
        "http://example.com/v1/responses",
        "http://localhost/v1/responses",
        "https://user:secret@example.com/v1/responses",
        "https://@example.com/v1/responses",
        "https://example.com/?secret=x",
        "https://example.com/#secret",
        "file:///secret",
        "not a URL",
    ] {
        let error = OpenAiResponses::with_config(
            "private-key",
            Config {
                endpoint: endpoint.into(),
                ..Config::default()
            },
        )
        .unwrap_err();
        assert_eq!(error, ConfigError::InvalidEndpoint);
        assert!(!format!("{error:?} {error}").contains("secret"));
    }
    for key in ["", "   ", "secret\nheader"] {
        assert_eq!(
            OpenAiResponses::new(key).unwrap_err(),
            ConfigError::InvalidApiKey
        );
    }
    for endpoint in [
        "https://example.com/secret-path",
        "http://127.0.0.1:1234/responses",
        "http://[::1]:1234/responses",
    ] {
        let config = Config {
            endpoint: endpoint.into(),
            ..Config::default()
        };
        assert!(!format!("{config:?}").contains("secret-path"));
        let provider = OpenAiResponses::with_config("private-key", config).unwrap();
        let debug = format!("{provider:?}");
        assert!(!debug.contains("private-key"));
        assert!(!debug.contains(endpoint));
        assert!(provider.authorization.is_sensitive());
    }
    assert_eq!(
        OpenAiResponses::with_config(
            "k",
            Config {
                timeout: Duration::ZERO,
                ..Config::default()
            }
        )
        .unwrap_err(),
        ConfigError::InvalidLimits
    );
    assert_eq!(
        OpenAiResponses::with_config(
            "k",
            Config {
                max_response_bytes: 0,
                ..Config::default()
            }
        )
        .unwrap_err(),
        ConfigError::InvalidLimits
    );
}
