mod support;

use futures_lite::future::zip;
use publicworks_agent::{
    Agent, Cancellation, ModelMessage, Tool, ToolCall, ToolDeclaration, ToolFuture, ToolResult,
    TurnConfig, decode_message,
};
use publicworks_provider_openai::{Config, ConfigError, OpenAiResponses};
use publicworks_runtime::{
    EntryQuery, EntryRecord, MemoryStorage, RunResult, Session, TaskOutcome, TaskQuery, TaskRecord,
    TaskRegistry, TaskRunner, TaskState, decode_native_json,
};
use serde_json::{Value, json};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use support::{Reply, Server, body_head, response};
use tokio::sync::Notify;

const API_KEY: &str = "fake-key-for-local-tests";

struct Capture {
    task: TaskRecord,
    entries: Vec<EntryRecord>,
    tasks: Vec<TaskRecord>,
}

fn transport(endpoint: &str) -> Config {
    Config {
        endpoint: endpoint.to_owned(),
        timeout: Duration::from_secs(2),
        max_response_bytes: 1024 * 1024,
    }
}

fn turn_config(model: &str) -> TurnConfig {
    TurnConfig {
        model: model.to_owned(),
        instructions: "Use only the offered calculator.".into(),
        max_model_rounds: 3,
    }
}

fn tool(name: &str, execute: impl Fn(ToolCall, Cancellation) -> ToolFuture + 'static) -> Tool {
    Tool::new(
        ToolDeclaration {
            name: name.into(),
            version: 17,
            description: "Perform a local calculation".into(),
            parameters: json!({
                "type":"object",
                "properties":{"value":{}},
                "required":["value"]
            }),
        },
        |_| Ok(()),
        execute,
    )
}

async fn run_turn(
    provider: OpenAiResponses,
    tools: Vec<Tool>,
    prompt: &str,
    config: TurnConfig,
) -> Capture {
    let agent = Agent::new(provider, tools).unwrap();
    let (session, session_driver) = Session::new(MemoryStorage::new());
    let (runner, task_driver) =
        TaskRunner::attach(&session, TaskRegistry::new(agent.definitions()).unwrap()).unwrap();
    let prompt = prompt.to_owned();
    let work = async {
        let conversation = session
            .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
            .await
            .unwrap()
            .value
            .id;
        let _submission = session
            .commit(move |tx| {
                agent.admit_input(
                    tx,
                    conversation,
                    prompt,
                    config,
                    publicworks_agent::SubmitOptions::default(),
                )
            })
            .await
            .unwrap()
            .value;
        let task_id = session
            .commit(move |tx| {
                Box::pin(async move {
                    Ok(tx
                        .conversation_state(conversation)
                        .await?
                        .unwrap()
                        .run
                        .unwrap()
                        .task_id)
                })
            })
            .await
            .unwrap()
            .value;
        let task = match runner.run(task_id).await.unwrap() {
            RunResult::Terminal(task) => task,
            result => panic!("expected terminal turn, got {result:?}"),
        };
        let (mut entries, tasks) = session
            .commit(move |tx| {
                Box::pin(async move {
                    Ok((
                        tx.scan_entries(EntryQuery::new(conversation), 128, None)
                            .await?
                            .items,
                        tx.scan_tasks(TaskQuery::default(), 128, None).await?.items,
                    ))
                })
            })
            .await
            .unwrap()
            .value;
        entries.reverse();
        runner.close().await.unwrap();
        session.close().await.unwrap();
        Capture {
            task,
            entries,
            tasks,
        }
    };
    tokio::time::timeout(
        Duration::from_secs(5),
        zip(work, zip(session_driver, task_driver)),
    )
    .await
    .expect("agent and both drivers must shut down")
    .0
}

fn failed_message(task: &TaskRecord) -> &str {
    match &task.state {
        TaskState::Terminal {
            outcome: TaskOutcome::Failed { error, .. },
        } => &error.message,
        state => panic!("expected failed task, got {state:?}"),
    }
}

fn diagnostic(capture: &Capture) -> &Value {
    capture
        .entries
        .iter()
        .find(|entry| entry.kind == "agent.diagnostic")
        .and_then(|entry| entry.data.as_ref())
        .expect("failed model request must persist a diagnostic")
}

fn completed(output: &str, usage: &str) -> String {
    format!(
        r#"{{"object":"response","id":"resp_test","status":"completed","output":[{output}],"usage":{usage}}}"#
    )
}

fn text_item(id: &str, text: &str) -> String {
    format!(
        r#"{{"type":"message","id":"{id}","role":"assistant","status":"completed","content":[{{"type":"output_text","text":{text},"annotations":[]}}]}}"#
    )
}

#[tokio::test(flavor = "current_thread")]
async fn public_two_round_turn_maps_requests_calls_results_usage_and_native_json() {
    let number_key = "$serde_json::private::Number";
    let raw_key = "$serde_json::private::RawValue";
    let first = completed(
        &format!(
            r#"{},{{"type":"function_call","id":"fc_vendor_one","status":"completed","call_id":"call_actual_1","name":"calculate","arguments":"{{\"value\":{{\"{number_key}\":\"literal\"}},\"max\":18446744073709551615}}"}},{{"type":"function_call","id":"fc_vendor_two","status":"completed","call_id":"call_actual_2","name":"calculate","arguments":"{{\"value\":{{\"{raw_key}\":\"literal\"}},\"min\":-9223372036854775808}}"}}"#,
            text_item("msg_first", "\"I will calculate.\"")
        ),
        &format!(
            r#"{{"input_tokens":11,"output_tokens":7,"total_tokens":18,"opaque":{{"{raw_key}":"usage-literal"}},"max":18446744073709551615,"min":-9223372036854775808}}"#
        ),
    );
    let second = completed(
        &text_item("msg_final", "\"The results are recorded.\""),
        "null",
    );
    let server = Server::start(
        "/custom/responses",
        vec![
            Reply::Bytes(response(200, &[], &first)),
            Reply::Bytes(response(200, &[], &second)),
        ],
    )
    .await;

    let observed = Rc::new(RefCell::new(Vec::<ToolCall>::new()));
    let calculator = tool("calculate", {
        let observed = observed.clone();
        move |call, _| {
            let is_second = call.id == "call_actual_2";
            observed.borrow_mut().push(call);
            Box::pin(async move {
                Ok(ToolResult {
                    content: if is_second { "unsafe input" } else { "42" }.into(),
                    is_error: is_second,
                    usage: None,
                })
            })
        }
    });
    let provider = OpenAiResponses::with_config(API_KEY, transport(server.endpoint())).unwrap();
    let capture = run_turn(
        provider,
        vec![calculator],
        "Calculate both values.",
        turn_config("pinned-model-not-provider-default"),
    )
    .await;

    assert!(matches!(
        capture.task.state,
        TaskState::Terminal {
            outcome: TaskOutcome::Completed { .. }
        }
    ));
    let calls = observed.borrow();
    assert_eq!(
        calls
            .iter()
            .map(|call| call.id.as_str())
            .collect::<Vec<_>>(),
        ["call_actual_1", "call_actual_2"]
    );
    assert_eq!(calls[0].arguments["value"][number_key], "literal");
    assert_eq!(calls[0].arguments["max"], json!(u64::MAX));
    assert_eq!(calls[1].arguments["value"][raw_key], "literal");
    assert_eq!(calls[1].arguments["min"], json!(i64::MIN));

    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert_eq!(request.method, "POST");
        assert_eq!(request.target, "/custom/responses");
        assert_eq!(
            request.header("authorization"),
            Some("Bearer fake-key-for-local-tests")
        );
        assert_eq!(request.header("content-type"), Some("application/json"));
    }
    let first_body = decode_native_json(&requests[0].body).unwrap();
    assert_eq!(first_body["model"], "pinned-model-not-provider-default");
    assert_eq!(
        first_body["instructions"],
        "Use only the offered calculator."
    );
    assert_eq!(first_body["store"], false);
    assert_eq!(first_body["stream"], false);
    assert_eq!(first_body["parallel_tool_calls"], true);
    assert!(first_body.get("previous_response_id").is_none());
    assert!(first_body.get("conversation").is_none());
    assert_eq!(first_body["input"].as_array().unwrap().len(), 1);
    assert_eq!(first_body["input"][0]["role"], "user");
    assert_eq!(
        first_body["input"][0]["content"][0]["text"],
        "Calculate both values."
    );
    assert_eq!(first_body["tools"].as_array().unwrap().len(), 1);
    let offered = &first_body["tools"][0];
    assert_eq!(offered["type"], "function");
    assert_eq!(offered["name"], "calculate");
    assert_eq!(offered["strict"], false);
    assert!(offered.get("version").is_none());

    let second_body = decode_native_json(&requests[1].body).unwrap();
    let input = second_body["input"].as_array().unwrap();
    assert_eq!(input.len(), 6);
    assert_eq!(input[1]["role"], "assistant");
    assert_eq!(input[1]["content"][0]["text"], "I will calculate.");
    assert_eq!(input[2]["type"], "function_call");
    assert_eq!(input[2]["call_id"], "call_actual_1");
    assert!(input[2].get("id").is_none());
    let first_arguments =
        decode_native_json(input[2]["arguments"].as_str().unwrap().as_bytes()).unwrap();
    assert_eq!(first_arguments["value"][number_key], "literal");
    assert_eq!(first_arguments["max"], json!(u64::MAX));
    let second_arguments =
        decode_native_json(input[3]["arguments"].as_str().unwrap().as_bytes()).unwrap();
    assert_eq!(second_arguments["value"][raw_key], "literal");
    assert_eq!(second_arguments["min"], json!(i64::MIN));
    assert_eq!(
        input[4],
        json!({"type":"function_call_output","call_id":"call_actual_1","output":"42"})
    );
    assert_eq!(
        input[5],
        json!({"type":"function_call_output","call_id":"call_actual_2","output":"[tool_error]\nunsafe input"})
    );

    assert_eq!(
        capture
            .entries
            .iter()
            .map(|entry| entry.kind.as_str())
            .collect::<Vec<_>>(),
        [
            "agent.user",
            "agent.assistant",
            "agent.toolResult",
            "agent.toolResult",
            "agent.assistant"
        ]
    );
    let first_usage = &capture.entries[1].data.as_ref().unwrap()["usage"];
    assert_eq!(first_usage["input_tokens"], 11);
    assert_eq!(first_usage["output_tokens"], 7);
    assert_eq!(first_usage["total_tokens"], 18);
    assert_eq!(first_usage["opaque"][raw_key], "usage-literal");
    assert_eq!(first_usage["max"], json!(u64::MAX));
    assert_eq!(first_usage["min"], json!(i64::MIN));
    assert!(
        capture.entries[4]
            .data
            .as_ref()
            .unwrap()
            .get("usage")
            .unwrap()
            .is_null()
    );
    let final_message = decode_message(&capture.entries[4].model.as_ref().unwrap()[0]).unwrap();
    assert_eq!(
        final_message,
        ModelMessage::Assistant {
            text: "The results are recorded.".into(),
            tool_calls: vec![]
        }
    );
}

#[tokio::test(flavor = "current_thread")]
async fn public_http_errors_are_single_attempt_and_never_persist_server_secrets() {
    let secret_body = format!(
        r#"{{"error":{{"message":"leak {API_KEY} and http://attacker.invalid/private"}}}}"#
    );
    let server = Server::start(
        "/v1/responses",
        [401, 429, 500, 503]
            .into_iter()
            .map(|status| {
                let headers = if status == 503 {
                    vec![("Retry-After", "0")]
                } else {
                    vec![]
                };
                Reply::Bytes(response(status, &headers, &secret_body))
            })
            .collect(),
    )
    .await;

    for status in [401, 429, 500, 503] {
        let provider = OpenAiResponses::with_config(API_KEY, transport(server.endpoint())).unwrap();
        let capture = run_turn(provider, vec![], "fail once", turn_config("gpt-test")).await;
        assert_eq!(
            failed_message(&capture.task),
            format!("OpenAI HTTP status {status}")
        );
        let durable = format!("{:?}", capture.entries);
        assert!(!durable.contains(API_KEY));
        assert!(!durable.contains("attacker.invalid"));
    }
    assert_eq!(
        server.requests().len(),
        4,
        "statuses and Retry-After must not retry"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn public_redirects_are_never_followed_and_authorization_never_leaks() {
    let target = Server::start(
        "/credential-sink",
        vec![Reply::Bytes(response(
            200,
            &[],
            &completed(&text_item("msg", "\"must not arrive\""), "null"),
        ))],
    )
    .await;
    let locations = [301_u16, 302, 307]
        .map(|status| {
            Reply::Bytes(response(
                status,
                &[("Location", target.endpoint())],
                "redirect body",
            ))
        })
        .into_iter()
        .collect();
    let source = Server::start("/v1/responses", locations).await;

    for status in [301, 302, 307] {
        let provider = OpenAiResponses::with_config(API_KEY, transport(source.endpoint())).unwrap();
        let capture = run_turn(provider, vec![], "do not follow", turn_config("gpt-test")).await;
        assert_eq!(
            failed_message(&capture.task),
            format!("OpenAI HTTP status {status}")
        );
    }
    let requests = source.requests();
    assert_eq!(requests.len(), 3);
    assert!(
        requests
            .iter()
            .all(|request| request.header("authorization")
                == Some("Bearer fake-key-for-local-tests"))
    );
    assert!(
        target.requests().is_empty(),
        "redirect target received credentials or a request"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn public_invalid_outputs_preserve_safe_usage_and_never_start_tools() {
    let invalid = [
        "{".to_owned(),
        completed(
            r#"{"type":"function_call","id":"fc_bad","status":"completed","call_id":"call_bad_args","name":"effect","arguments":"{"}"#,
            "null",
        ),
        format!(
            r#"{{"object":"response","status":"completed","output":[{},{{"type":"function_call","id":"fc_ok","status":"completed","call_id":"call_mixed","name":"effect","arguments":"{{}}"}},{{"type":"reasoning","id":"reasoning_1"}}],"usage":{{"input_tokens":9}}}}"#,
            text_item("msg_partial", "\"safe partial text\"")
        ),
        r#"{"object":"response","status":"completed","output":[{"type":"message","id":"msg_refusal","role":"assistant","status":"completed","content":[{"type":"refusal","refusal":"no"}]}],"usage":{"output_tokens":3}}"#.to_owned(),
        r#"{"object":"response","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[{"type":"function_call","id":"fc_incomplete","status":"completed","call_id":"call_incomplete","name":"effect","arguments":"{}"}]}"#.to_owned(),
        r#"{"object":"response","status":"failed","error":{"message":"provider detail"},"output":[],"usage":{"total_tokens":12}}"#.to_owned(),
        r#"{"object":"response","status":"completed","output":[{"type":"message","id":"msg_unknown","role":"assistant","status":"completed","content":[{"type":"future_content","data":"x"}]}]}"#.to_owned(),
    ];
    let server = Server::start(
        "/v1/responses",
        invalid
            .iter()
            .map(|body| Reply::Bytes(response(200, &[], body)))
            .collect(),
    )
    .await;
    let effects = Rc::new(Cell::new(0));
    let effect = tool("effect", {
        let effects = effects.clone();
        move |_, _| {
            effects.set(effects.get() + 1);
            Box::pin(async {
                Ok(ToolResult {
                    content: "ran".into(),
                    is_error: false,
                    usage: None,
                })
            })
        }
    });

    let mut captures = Vec::new();
    for _ in 0..invalid.len() {
        let provider = OpenAiResponses::with_config(API_KEY, transport(server.endpoint())).unwrap();
        captures.push(
            run_turn(
                provider,
                vec![effect.clone()],
                "invalid output",
                turn_config("gpt-test"),
            )
            .await,
        );
    }
    assert_eq!(effects.get(), 0);
    assert_eq!(server.requests().len(), invalid.len());
    assert!(
        captures.iter().all(|capture| capture.tasks.len() == 1),
        "invalid provider output created tool children"
    );

    assert!(diagnostic(&captures[0]).get("usage").is_none());
    assert!(
        diagnostic(&captures[1])["usage"].is_null(),
        "explicit null usage became absence"
    );
    assert_eq!(diagnostic(&captures[2])["usage"], json!({"input_tokens":9}));
    let partial = &diagnostic(&captures[2])["partialResponse"]["message"];
    assert_eq!(partial["text"], "safe partial text");
    assert_eq!(
        partial["toolCalls"],
        json!([]),
        "invalid calls became executable partial output"
    );
    assert_eq!(
        diagnostic(&captures[3])["usage"],
        json!({"output_tokens":3})
    );
    assert!(diagnostic(&captures[4]).get("usage").is_none());
    assert_eq!(
        diagnostic(&captures[5])["usage"],
        json!({"total_tokens":12})
    );
    assert!(captures.iter().all(|capture| {
        capture
            .entries
            .iter()
            .map(|entry| entry.kind.as_str())
            .collect::<Vec<_>>()
            == ["agent.user", "agent.diagnostic"]
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn public_response_limit_covers_content_length_and_chunked_bodies() {
    let oversized = "x".repeat(256);
    let chunked = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n28\r\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n28\r\nyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy\r\n0\r\n\r\n".to_vec();
    let server = Server::start(
        "/v1/responses",
        vec![
            Reply::Bytes(response(200, &[], &oversized)),
            Reply::Bytes(chunked),
        ],
    )
    .await;

    for _ in 0..2 {
        let mut config = transport(server.endpoint());
        config.max_response_bytes = 64;
        let provider = OpenAiResponses::with_config(API_KEY, config).unwrap();
        let capture = run_turn(provider, vec![], "bounded", turn_config("gpt-test")).await;
        assert_eq!(
            failed_message(&capture.task),
            "OpenAI response body exceeds limit"
        );
    }
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn public_timeout_applies_while_waiting_for_headers_and_for_body() {
    let valid = response(
        200,
        &[],
        &completed(&text_item("msg", "\"too late\""), "null"),
    );
    let headers_server = Server::start(
        "/v1/responses",
        vec![Reply::DelayHeaders {
            delay: Duration::from_millis(300),
            bytes: valid.clone(),
        }],
    )
    .await;
    let body = String::from_utf8(valid).unwrap();
    let (_, body_text) = body.split_once("\r\n\r\n").unwrap();
    let body_server = Server::start(
        "/v1/responses",
        vec![Reply::DelayBody {
            head: body_head(body_text.len()),
            prefix: body_text.as_bytes()[..1].to_vec(),
            delay: Duration::from_millis(300),
            suffix: body_text.as_bytes()[1..].to_vec(),
        }],
    )
    .await;

    for endpoint in [headers_server.endpoint(), body_server.endpoint()] {
        let mut config = transport(endpoint);
        config.timeout = Duration::from_millis(40);
        let provider = OpenAiResponses::with_config(API_KEY, config).unwrap();
        let capture = run_turn(provider, vec![], "timeout", turn_config("gpt-test")).await;
        assert_eq!(failed_message(&capture.task), "OpenAI request timed out");
    }
}

async fn wait_for_closed(closed: &AtomicBool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !closed.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("dropping the model future must close the local HTTP connection");
}

#[tokio::test(flavor = "current_thread")]
async fn public_abort_releases_a_header_pending_request_and_session_remains_usable() {
    let entered = Arc::new(Notify::new());
    let closed = Arc::new(AtomicBool::new(false));
    let server = Server::start(
        "/v1/responses",
        vec![Reply::Stall {
            head: None,
            prefix: vec![],
            entered: entered.clone(),
            peer_closed: closed.clone(),
        }],
    )
    .await;
    let provider = OpenAiResponses::with_config(API_KEY, transport(server.endpoint())).unwrap();
    let agent = Agent::new(provider, vec![]).unwrap();
    let (session, session_driver) = Session::new(MemoryStorage::new());
    let (runner, task_driver) =
        TaskRunner::attach(&session, TaskRegistry::new(agent.definitions()).unwrap()).unwrap();
    let work = async {
        let conversation = session
            .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
            .await
            .unwrap()
            .value
            .id;
        let _submission = session
            .commit(move |tx| {
                agent.admit_input(
                    tx,
                    conversation,
                    "cancel",
                    turn_config("gpt-test"),
                    publicworks_agent::SubmitOptions::default(),
                )
            })
            .await
            .unwrap()
            .value;
        let task_id = session
            .commit(move |tx| {
                Box::pin(async move {
                    Ok(tx
                        .conversation_state(conversation)
                        .await?
                        .unwrap()
                        .run
                        .unwrap()
                        .task_id)
                })
            })
            .await
            .unwrap()
            .value;
        let run = runner.run(task_id);
        entered.notified().await;
        let marker = session
            .commit(move |tx| {
                Box::pin(async move {
                    tx.append_entry(
                        conversation,
                        publicworks_runtime::EntryDraft::new("unrelated"),
                    )
                    .await
                })
            })
            .await
            .unwrap()
            .value;
        assert_eq!(marker.kind, "unrelated");
        assert_eq!(
            runner.abort(task_id).await.unwrap(),
            publicworks_runtime::AbortResult::Marked
        );
        let terminal = match run.await.unwrap() {
            RunResult::Terminal(task) => task,
            other => panic!("expected aborted terminal turn, got {other:?}"),
        };
        assert!(matches!(
            terminal.state,
            TaskState::Terminal {
                outcome: TaskOutcome::Aborted { .. }
            }
        ));
        wait_for_closed(&closed).await;
        runner.close().await.unwrap();
        session.close().await.unwrap();
    };
    tokio::time::timeout(
        Duration::from_secs(5),
        zip(work, zip(session_driver, task_driver)),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn public_runner_close_releases_a_body_pending_request_and_leaves_pinned_marker() {
    let entered = Arc::new(Notify::new());
    let closed = Arc::new(AtomicBool::new(false));
    let server = Server::start(
        "/v1/responses",
        vec![Reply::Stall {
            head: Some(body_head(100)),
            prefix: b"{".to_vec(),
            entered: entered.clone(),
            peer_closed: closed.clone(),
        }],
    )
    .await;
    let provider = OpenAiResponses::with_config(API_KEY, transport(server.endpoint())).unwrap();
    let effects = Rc::new(Cell::new(0));
    let effect = tool("effect", {
        let effects = effects.clone();
        move |_, _| {
            effects.set(effects.get() + 1);
            Box::pin(async {
                Ok(ToolResult {
                    content: "must not run".into(),
                    is_error: false,
                    usage: None,
                })
            })
        }
    });
    let agent = Agent::new(provider, vec![effect]).unwrap();
    let (session, session_driver) = Session::new(MemoryStorage::new());
    let (runner, task_driver) =
        TaskRunner::attach(&session, TaskRegistry::new(agent.definitions()).unwrap()).unwrap();
    let work = async {
        let conversation = session
            .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
            .await
            .unwrap()
            .value
            .id;
        let _submission = session
            .commit(move |tx| {
                agent.admit_input(
                    tx,
                    conversation,
                    "close",
                    turn_config("gpt-pinned"),
                    publicworks_agent::SubmitOptions::default(),
                )
            })
            .await
            .unwrap()
            .value;
        let task_id = session
            .commit(move |tx| {
                Box::pin(async move {
                    Ok(tx
                        .conversation_state(conversation)
                        .await?
                        .unwrap()
                        .run
                        .unwrap()
                        .task_id)
                })
            })
            .await
            .unwrap()
            .value;
        let run = runner.run(task_id);
        entered.notified().await;
        runner.close().await.unwrap();
        assert_eq!(run.await.unwrap(), RunResult::Interrupted);
        wait_for_closed(&closed).await;
        let (task, marker) = session
            .commit(move |tx| {
                Box::pin(async move {
                    let task = tx.task(task_id).await?.unwrap();
                    let marker = tx
                        .append_entry(
                            conversation,
                            publicworks_runtime::EntryDraft::new("after-close"),
                        )
                        .await?;
                    Ok((task, marker))
                })
            })
            .await
            .unwrap()
            .value;
        assert_eq!(marker.kind, "after-close");
        let checkpoint = match task.state {
            TaskState::Pending { checkpoint }
            | TaskState::Running { checkpoint }
            | TaskState::Waiting { checkpoint, .. } => checkpoint,
            state => panic!("closed request marker was not recoverable: {state:?}"),
        };
        assert_eq!(checkpoint["phase"], "request");
        assert_eq!(checkpoint["model"], "gpt-pinned");
        assert_eq!(checkpoint["messages"][0]["text"], "close");
        assert_eq!(server.requests().len(), 1);
        assert_eq!(effects.get(), 0, "a partial response body started a tool");
        session.close().await.unwrap();
    };
    tokio::time::timeout(
        Duration::from_secs(5),
        zip(work, zip(session_driver, task_driver)),
    )
    .await
    .unwrap();
}

#[test]
fn public_config_validation_is_offline_and_debug_diagnostics_are_secret_safe() {
    for key in ["", "   ", "secret\r\nInjected: yes"] {
        let error = OpenAiResponses::new(key).unwrap_err();
        assert_eq!(error, ConfigError::InvalidApiKey);
        assert_eq!(error.to_string(), "Invalid OpenAI API key");
        assert!(!format!("{error:?}").contains("secret"));
    }

    let invalid_endpoints = [
        "http://example.com/v1/responses",
        "http://localhost/v1/responses",
        "https://user:password@example.com/v1/responses",
        "https:///@example.com/responses",
        "https://example.com/v1/responses?secret=query",
        "https://example.com/v1/responses#secret-fragment",
    ];
    for endpoint in invalid_endpoints {
        let config = Config {
            endpoint: endpoint.into(),
            ..Config::default()
        };
        let error = OpenAiResponses::with_config("different-secret", config).unwrap_err();
        assert_eq!(error, ConfigError::InvalidEndpoint);
        assert_eq!(error.to_string(), "Invalid OpenAI endpoint");
        assert!(!format!("{error:?}").contains(endpoint));
    }

    for (timeout, max_response_bytes) in [(Duration::ZERO, 1), (Duration::from_secs(1), 0)] {
        let config = Config {
            endpoint: "http://127.0.0.1:1/v1/responses".into(),
            timeout,
            max_response_bytes,
        };
        assert_eq!(
            OpenAiResponses::with_config("limit-secret", config).unwrap_err(),
            ConfigError::InvalidLimits
        );
    }

    let config = Config {
        endpoint: "http://127.0.0.1:1/private?not-accepted".into(),
        ..Config::default()
    };
    let rendered = format!("{config:?}");
    assert!(!rendered.contains("private"));
    assert!(rendered.contains("[redacted]"));

    let provider = OpenAiResponses::with_config(
        "clone-secret",
        Config {
            endpoint: "http://127.0.0.1:1/v1/responses".into(),
            ..Config::default()
        },
    )
    .unwrap();
    let clone = provider.clone();
    for rendered in [format!("{provider:?}"), format!("{clone:?}")] {
        assert!(!rendered.contains("clone-secret"));
        assert!(!rendered.contains("127.0.0.1"));
    }
}
