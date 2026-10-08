use super::*;

fn nested(depth: usize) -> Value {
    (0..depth).fold(Value::Null, |value, _| json!([value]))
}

#[test]
fn execute_intent_is_strict_and_preserves_effective_payload_and_policy() {
    for policy in [ReplayPolicy::Safe, ReplayPolicy::Unsafe] {
        let arguments = json!({"n":u64::MAX,"$serde_json::private::Number":"literal","null":null});
        let state = ToolCheckpoint::Execute { arguments, policy };
        let encoded = ToolCheckpoint::Execute {
            arguments: match &state {
                ToolCheckpoint::Execute { arguments, .. } => arguments.clone(),
                _ => unreachable!(),
            },
            policy,
        }
        .encode()
        .unwrap();
        assert_eq!(ToolCheckpoint::decode(&encoded).unwrap(), state);
        for key in ["phase", "arguments", "replay"] {
            let mut bad = encoded.clone();
            bad.as_object_mut().unwrap().remove(key);
            assert!(ToolCheckpoint::decode(&bad).is_err(), "missing {key}");
        }
        for (key, value) in [
            ("phase", json!("call")),
            ("replay", json!("unknown")),
            ("replay", Value::Null),
            ("callId", json!("old-schema")),
            ("extra", Value::Null),
        ] {
            let mut bad = encoded.clone();
            bad[key] = value;
            assert!(ToolCheckpoint::decode(&bad).is_err(), "invalid {key}");
        }
    }
    let intent = |arguments| json!({"phase":"execute","arguments":arguments,"replay":"safe"});
    assert!(ToolCheckpoint::decode(&intent(nested(MAX_JSON_DEPTH - 4))).is_ok());
    assert!(ToolCheckpoint::decode(&intent(nested(MAX_JSON_DEPTH - 3))).is_err());
    for raw in ["18446744073709551616", "1e400"] {
        if let Ok(arguments) = serde_json::from_str::<Value>(raw) {
            // With arbitrary_precision these tokens remain non-native; a native
            // parse may instead normalize the first token to a finite f64.
            if publicworks_runtime::validate_native_json_value(&arguments, 4).is_err() {
                assert!(ToolCheckpoint::decode(&intent(arguments)).is_err());
            }
        }
    }
}

#[test]
fn compact_inputs_and_turn_checkpoints_reject_old_schema_and_versions() {
    let input = ToolInput {
        assistant: Id::new(10).unwrap(),
        index: 0,
    };
    assert_eq!(ToolInput::decode(&input.encode()).unwrap(), input);
    for bad in [
        json!({"assistantEntryId":10}),
        json!({"assistantEntryId":10,"index":-1}),
        json!({"assistantEntryId":0,"index":0}),
        json!({"assistantEntryId":10,"index":0,"root":2}),
    ] {
        assert!(ToolInput::decode(&bad).is_err());
    }
    let mut root = fixture().0;
    assert!(TurnCheckpoint::decode(&root).is_ok());
    root.version -= 1;
    assert!(TurnCheckpoint::decode(&root).is_err());
    root.version = TURN_VERSION;
    let TaskState::Waiting { checkpoint, .. } = &mut root.state else {
        unreachable!()
    };
    checkpoint["calls"] = json!([]);
    assert!(TurnCheckpoint::decode(&root).is_err());
}

#[test]
fn prepared_request_snapshot_preserves_values_and_pinned_config() {
    let mut root = fixture().0;
    let config = decode_input(&root.input).unwrap();
    let request = ModelRequest {
        model: config.model,
        instructions: config.instructions,
        tools: vec![ToolDeclaration {
            name: "t".into(),
            version: 7,
            description: "".into(),
            parameters: json!({"$serde_json::private::Number":"literal","n":u64::MAX}),
        }],
        messages: vec![ModelMessage::User {
            text: "frozen".into(),
        }],
    };
    let cp = TurnCheckpoint::Request {
        round: 1,
        cutoff: Id::new(9).unwrap(),
        request: request.clone(),
    }
    .encode()
    .unwrap();
    root.state = TaskState::Running {
        checkpoint: cp.clone(),
    };
    let (
        _,
        TurnCheckpoint::Request {
            request: decoded, ..
        },
    ) = TurnCheckpoint::decode(&root).unwrap()
    else {
        unreachable!()
    };
    assert_eq!(decoded, request);
    for (key, value) in [
        ("model", json!("different")),
        ("instructions", json!("different")),
        ("round", json!(0)),
        ("cutoff", json!(0)),
    ] {
        let mut bad = cp.clone();
        bad[key] = value;
        root.state = TaskState::Running { checkpoint: bad };
        assert!(TurnCheckpoint::decode(&root).is_err(), "invalid {key}");
    }
}
