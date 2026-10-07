use futures_lite::future::{block_on, zip};
use publicworks_agent::*;
use publicworks_runtime::*;
use publicworks_storage_sqlite::SqliteStorage;
use serde_json::{Value, json};
use std::{
    cell::RefCell,
    collections::BTreeMap,
    future::Future,
    path::PathBuf,
    rc::Rc,
    task::{Poll, Waker},
};

struct Database(PathBuf);
impl Database {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "publicworks-extensions-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn open(&self) -> SqliteStorage {
        SqliteStorage::open(self.0.join("db")).unwrap()
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[derive(Clone, Default)]
struct Gate(Rc<RefCell<(bool, Option<Waker>)>>);
impl Gate {
    async fn wait(&self) {
        std::future::poll_fn(|cx| {
            let mut s = self.0.borrow_mut();
            if s.0 {
                Poll::Ready(())
            } else {
                s.1 = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
    fn release(&self) {
        let wake = {
            let mut s = self.0.borrow_mut();
            s.0 = true;
            s.1.take()
        };
        if let Some(w) = wake {
            w.wake();
        }
    }
}
async fn bounded<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut polls = 0;
    std::future::poll_fn(|cx| {
        polls += 1;
        assert!(polls < 50_000, "extension test exceeded poll budget");
        let result = future.as_mut().poll(cx);
        if result.is_pending() {
            cx.waker().wake_by_ref();
        }
        result
    })
    .await
}
fn turn_config() -> TurnConfig {
    TurnConfig {
        model: "fake".into(),
        instructions: "".into(),
        max_model_rounds: 4,
    }
}
fn response(names: &[&str]) -> ModelResponse {
    ModelResponse {
        message: ModelMessage::Assistant {
            text: "done".into(),
            tool_calls: names
                .iter()
                .enumerate()
                .map(|(i, n)| ToolCall {
                    id: format!("call-{i}"),
                    name: (*n).into(),
                    arguments: json!({}),
                })
                .collect(),
        },
        finish_reason: if names.is_empty() {
            FinishReason::Stop
        } else {
            FinishReason::ToolCalls
        },
        usage: None,
    }
}
fn declaration(name: &str, version: u64) -> ToolDeclaration {
    ToolDeclaration {
        name: name.into(),
        version,
        description: "".into(),
        parameters: json!({}),
    }
}
fn inert(name: &str, version: u64) -> Tool {
    Tool::new(
        declaration(name, version),
        |_| panic!("validator must not run"),
        |_, _| panic!("executor must not run"),
    )
}
fn inert_agent() -> Agent {
    Agent::new(
        |_: ModelRequest, _: Cancellation| -> ModelFuture { panic!("model must not run") },
        vec![inert("inert", 1)],
    )
    .unwrap()
}
async fn conversation(h: &Harness) -> Id {
    h.commit(|tx| Box::pin(async move { Ok(tx.create_conversation().await?.id) }))
        .await
        .unwrap()
        .value
}
async fn config(h: &Harness, c: Id) -> Value {
    h.commit(move |tx| {
        Box::pin(async move {
            Ok(tx
                .conversation_state(c)
                .await?
                .unwrap()
                .agent_config
                .unwrap())
        })
    })
    .await
    .unwrap()
    .value
}
fn settings(selection: ExtensionSelection) -> ExtensionConfig {
    ExtensionConfig {
        selection,
        config: BTreeMap::from([(
            "missing".into(),
            json!({"integer":u64::MAX,"negative":i64::MIN,"float":1.25,"null":null,"object":{"$serde_json::private::Number":"opaque"}}),
        )]),
    }
}

async fn config_roundtrip(storage: impl Storage + 'static) -> (Id, Value) {
    let a = inert_agent();
    let (open, driver) = Harness::open(storage, TaskRegistry::new(a.definitions()).unwrap());
    zip(
        async {
            let h = open.await.unwrap();
            let c = conversation(&h).await;
            // Preserve unknown host fields too, not just the two agent-owned groups.
            h.commit(move |tx| {
                Box::pin(async move {
                    tx.update_conversation_state(c, |s| {
                        s.agent_config = Some(json!({"host":{"keep":true}}))
                    })
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
            for selection in [
                ExtensionSelection::Exact(vec![
                    "missing".into(),
                    "default".into(),
                    "missing".into(),
                ]),
                ExtensionSelection::AddRemove {
                    add: vec!["missing".into()],
                    remove: vec!["default".into()],
                },
                ExtensionSelection::Default,
                ExtensionSelection::Exact(vec![]),
            ] {
                let cfg = settings(selection);
                // Both admissions are synchronous; FIFO patching must retain each other.
                let ext = a.configure_extensions(&h, c, cfg.clone());
                let queue = a.configure_queues(
                    &h,
                    c,
                    QueueConfig {
                        steer: QueueMode::All,
                        follow_up: QueueMode::All,
                    },
                );
                ext.await.unwrap();
                queue.await.unwrap();
                let stored = config(&h, c).await;
                assert_eq!(ExtensionConfig::decode(Some(&stored)).unwrap(), cfg);
                assert_eq!(stored["host"], json!({"keep":true}));
                assert_eq!(stored["steer"], "all");
                let queue = a.configure_queues(
                    &h,
                    c,
                    QueueConfig {
                        steer: QueueMode::One,
                        follow_up: QueueMode::All,
                    },
                );
                let ext = a.configure_extensions(&h, c, cfg.clone());
                queue.await.unwrap();
                ext.await.unwrap();
                let stored = config(&h, c).await;
                assert_eq!(ExtensionConfig::decode(Some(&stored)).unwrap(), cfg);
                assert_eq!(stored["steer"], "one");
                assert_eq!(stored["followUp"], "all");
            }
            let stored = config(&h, c).await;
            let seq = h.inspect().await.unwrap().last_commit_seq;
            for bad in [
                settings(ExtensionSelection::Exact(vec!["".into()])),
                ExtensionConfig {
                    selection: ExtensionSelection::Default,
                    config: BTreeMap::from([("".into(), Value::Null)]),
                },
                ExtensionConfig {
                    selection: ExtensionSelection::Default,
                    config: BTreeMap::from([(
                        "deep".into(),
                        (0..MAX_JSON_DEPTH).fold(Value::Null, |v, _| json!([v])),
                    )]),
                },
            ] {
                assert!(a.configure_extensions(&h, c, bad).await.is_err());
                assert_eq!(config(&h, c).await, stored);
                assert_eq!(h.inspect().await.unwrap().last_commit_seq, seq);
            }
            let mut registry = AgentRegistry::new();
            registry
                .install(Extension::new("local", vec![inert("local", 1)]))
                .unwrap();
            let published = a.publish_snapshot(&h, registry.snapshot(), []).unwrap();
            registry.uninstall("local").unwrap();
            published
                .publish_snapshot(&h, registry.snapshot(), [])
                .unwrap();
            // Duplicate built-ins reject the full projection, with no durable change.
            assert!(
                a.publish_snapshot(&h, registry.snapshot(), a.definitions())
                    .is_err()
            );
            let inspection = h.inspect().await.unwrap();
            assert!(!inspection.progress_enabled);
            assert_eq!(inspection.last_commit_seq, seq);
            assert_eq!(config(&h, c).await, stored);
            h.close().await.unwrap();
            (c, stored)
        },
        driver,
    )
    .await
    .0
}
#[test]
fn memory_configuration_roundtrip_and_publication_are_passive() {
    block_on(bounded(config_roundtrip(MemoryStorage::new())));
}
#[test]
fn sqlite_configuration_roundtrip_coexistence_and_reopen() {
    block_on(bounded(async {
        let db = Database::new();
        let (c, stored) = config_roundtrip(db.open()).await;
        // Reopen with no executable installation at all.
        let (open, driver) = Harness::open(db.open(), TaskRegistry::default());
        zip(
            async {
                let h = open.await.unwrap();
                assert_eq!(config(&h, c).await, stored);
                h.close().await.unwrap();
            },
            driver,
        )
        .await;
    }));
}
#[test]
fn selection_codec_rejects_malformed_values_without_dropping_missing_names() {
    assert_eq!(
        ExtensionSelection::decode(None).unwrap(),
        ExtensionSelection::Default
    );
    for v in [
        json!(null),
        json!("local"),
        json!([null]),
        json!([""]),
        json!({"add":false}),
        json!({"remove":[1]}),
        json!({"typo":[]}),
    ] {
        assert!(ExtensionSelection::decode(Some(&v)).is_err(), "{v}");
    }
    let selected = ExtensionSelection::AddRemove {
        add: vec!["absent".into(), "absent".into()],
        remove: vec!["also-absent".into()],
    };
    assert_eq!(
        ExtensionSelection::decode(selected.encode().unwrap().as_ref()).unwrap(),
        selected
    );
}

async fn selection_order(storage: impl Storage + 'static) {
    let requests = Rc::new(RefCell::new(Vec::<ModelRequest>::new()));
    let mut registry = AgentRegistry::new();
    registry
        .install(Extension::new("a", vec![inert("z", 1), inert("shared", 1)]))
        .unwrap();
    registry
        .install(Extension::new("b", vec![inert("y", 1), inert("shared", 2)]))
        .unwrap();
    let a = Agent::with_registry(
        {
            let requests = requests.clone();
            move |r: ModelRequest, _: Cancellation| -> ModelFuture {
                requests.borrow_mut().push(r);
                Box::pin(async { Ok(response(&[])) })
            }
        },
        registry.snapshot(),
        Some(vec!["b".into(), "a".into()]),
    );
    let (open, driver) = Harness::open(storage, TaskRegistry::new(a.definitions()).unwrap());
    zip(
        async {
            let h = open.await.unwrap();
            let c = conversation(&h).await;
            let cases = [
                (
                    ExtensionSelection::Default,
                    vec![("y", 1), ("shared", 1), ("z", 1)],
                ),
                (
                    ExtensionSelection::Exact(vec![
                        "a".into(),
                        "missing".into(),
                        "b".into(),
                        "a".into(),
                    ]),
                    vec![("z", 1), ("shared", 2), ("y", 1)],
                ),
                (
                    ExtensionSelection::AddRemove {
                        add: vec!["b".into(), "missing".into()],
                        remove: vec!["b".into()],
                    },
                    vec![("z", 1), ("shared", 2), ("y", 1)],
                ),
                (ExtensionSelection::Exact(vec![]), vec![]),
            ];
            for (selection, expected) in cases {
                a.configure_extensions(&h, c, settings(selection))
                    .await
                    .unwrap();
                a.submit(&h, c, "test", turn_config(), SubmitOptions::default())
                    .await
                    .unwrap()
                    .wait()
                    .await
                    .unwrap();
                let requests = requests.borrow();
                assert_eq!(
                    requests
                        .last()
                        .unwrap()
                        .tools
                        .iter()
                        .map(|d| (d.name.as_str(), d.version))
                        .collect::<Vec<_>>(),
                    expected
                );
            }
            // A name missing at configuration time becomes effective on a later
            // publication, without rewriting durable selection or the old snapshot.
            a.configure_extensions(
                &h,
                c,
                settings(ExtensionSelection::Exact(vec![
                    "missing".into(),
                    "a".into(),
                ])),
            )
            .await
            .unwrap();
            let before = config(&h, c).await;
            registry
                .install(Extension::new("missing", vec![inert("m", 3)]))
                .unwrap();
            let newer = a.publish_snapshot(&h, registry.snapshot(), []).unwrap();
            assert!(a.registry_snapshot().get("missing").is_none());
            newer
                .submit(&h, c, "new", turn_config(), SubmitOptions::default())
                .await
                .unwrap()
                .wait()
                .await
                .unwrap();
            assert_eq!(
                requests
                    .borrow()
                    .last()
                    .unwrap()
                    .tools
                    .iter()
                    .map(|d| d.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["m", "z", "shared"]
            );
            assert_eq!(config(&h, c).await, before);
            h.close().await.unwrap();
        },
        driver,
    )
    .await;
}
#[test]
fn memory_selection_controls_prepared_request_order() {
    block_on(bounded(selection_order(MemoryStorage::new())));
}
#[test]
fn sqlite_selection_controls_prepared_request_order() {
    let db = Database::new();
    block_on(bounded(selection_order(db.open())));
}

async fn phase_lifetime(storage: impl Storage + 'static) {
    let requested = Gate::default();
    let model_release = Gate::default();
    let executing = Gate::default();
    let effect_release = Gate::default();
    let trace = Rc::new(RefCell::new(Vec::new()));
    let requests = Rc::new(RefCell::new(Vec::<ModelRequest>::new()));
    let mut registry = AgentRegistry::new();
    registry
        .install(Extension::new(
            "local",
            vec![inert("effect", 1), inert("removed", 1), inert("changed", 1)],
        ))
        .unwrap();
    let a = Agent::with_registry(
        {
            let requested = requested.clone();
            let release = model_release.clone();
            let requests = requests.clone();
            move |r: ModelRequest, _: Cancellation| -> ModelFuture {
                let first = requests.borrow().is_empty();
                requests.borrow_mut().push(r);
                let requested = requested.clone();
                let release = release.clone();
                Box::pin(async move {
                    if first {
                        requested.release();
                        release.wait().await;
                        Ok(response(&["effect", "removed", "changed", "unoffered"]))
                    } else {
                        Ok(response(&[]))
                    }
                })
            }
        },
        registry.snapshot(),
        None,
    );
    let old = a.registry_snapshot();
    let (open, driver) = Harness::open(storage, TaskRegistry::new(a.definitions()).unwrap());
    zip(
        async {
            let h = open.await.unwrap();
            let c = conversation(&h).await;
            let receipt = a
                .submit(&h, c, "test", turn_config(), SubmitOptions::default())
                .await
                .unwrap();
            requested.wait().await;
            // Request declarations remain pinned, but calls resolve current code.
            registry
                .install(Extension::new(
                    "local",
                    vec![
                        Tool::new(
                            declaration("effect", 1),
                            {
                                let trace = trace.clone();
                                move |_| {
                                    trace.borrow_mut().push("new validator");
                                    Ok(())
                                }
                            },
                            {
                                let trace = trace.clone();
                                let executing = executing.clone();
                                let release = effect_release.clone();
                                move |_, _| {
                                    trace.borrow_mut().push("new executor");
                                    executing.release();
                                    let release = release.clone();
                                    Box::pin(async move {
                                        release.wait().await;
                                        Ok(ToolResult {
                                            content: "pinned effect".into(),
                                            is_error: false,
                                            usage: None,
                                        })
                                    })
                                }
                            },
                        ),
                        inert("changed", 2),
                        inert("unoffered", 1),
                    ],
                ))
                .unwrap();
            let newer = a.publish_snapshot(&h, registry.snapshot(), []).unwrap();
            assert_eq!(
                old.get("local").unwrap().tools()[0].declaration().version,
                1
            );
            model_release.release();
            executing.wait().await;
            assert_eq!(*trace.borrow(), vec!["new validator", "new executor"]);
            // Uninstall cannot retarget the accepted call, even while its callback
            // is awaiting. Leave `changed` available at an incompatible version.
            registry.uninstall("local").unwrap();
            registry
                .install(Extension::new(
                    "replacement",
                    vec![inert("changed", 2), inert("unoffered", 1)],
                ))
                .unwrap();
            newer.publish_snapshot(&h, registry.snapshot(), []).unwrap();
            newer
                .configure_extensions(
                    &h,
                    c,
                    settings(ExtensionSelection::Exact(vec!["replacement".into()])),
                )
                .await
                .unwrap();
            effect_release.release();
            receipt.wait().await.unwrap();
            let entries = h
                .commit(move |tx| {
                    Box::pin(async move {
                        Ok(tx.scan_entries(EntryQuery::new(c), 128, None).await?.items)
                    })
                })
                .await
                .unwrap()
                .value;
            let mut results = entries
                .iter()
                .filter(|e| e.kind == "agent.toolResult")
                .map(|e| decode_message(&e.model.as_ref().unwrap()[0]).unwrap())
                .collect::<Vec<_>>();
            results.sort_by_key(|r| match r {
                ModelMessage::ToolResult { call_id, .. } => call_id.clone(),
                _ => unreachable!(),
            });
            let contents = results
                .iter()
                .map(|r| match r {
                    ModelMessage::ToolResult { content, .. } => content.as_str(),
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>();
            assert_eq!(
                contents,
                vec![
                    "pinned effect",
                    "Tool is not installed",
                    "Tool version mismatch",
                    "Tool was not offered"
                ]
            );
            assert_eq!(
                requests.borrow()[0]
                    .tools
                    .iter()
                    .map(|d| d.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["effect", "removed", "changed"]
            );
            assert_eq!(
                requests.borrow()[1]
                    .tools
                    .iter()
                    .map(|d| d.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["changed", "unoffered"]
            );
            assert_eq!(*trace.borrow(), vec!["new validator", "new executor"]);
            h.close().await.unwrap();
        },
        driver,
    )
    .await;
}
#[test]
fn memory_phase_snapshot_pins_effect_and_refreshes_later_calls() {
    block_on(bounded(phase_lifetime(MemoryStorage::new())));
}
#[test]
fn sqlite_phase_snapshot_pins_effect_and_refreshes_later_calls() {
    let db = Database::new();
    block_on(bounded(phase_lifetime(db.open())));
}

async fn publication_wakes_and_deselection_prevents_call(storage: impl Storage + 'static) {
    let entered = Gate::default();
    let release = Gate::default();
    let requests = Rc::new(RefCell::new(Vec::<ModelRequest>::new()));
    let a = Agent::new(
        {
            let entered = entered.clone();
            let release = release.clone();
            let requests = requests.clone();
            move |request: ModelRequest, _: Cancellation| -> ModelFuture {
                let first = requests.borrow().is_empty();
                requests.borrow_mut().push(request);
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    if first {
                        entered.release();
                        release.wait().await;
                        Ok(response(&["effect"]))
                    } else {
                        Ok(response(&[]))
                    }
                })
            }
        },
        vec![inert("effect", 1)],
    )
    .unwrap();
    let (open, driver) = Harness::open(storage, TaskRegistry::default());
    zip(async {
        let h = open.await.unwrap(); let c = conversation(&h).await;
        let receipt = a.submit(&h,c,"blocked",turn_config(),SubmitOptions::default()).await.unwrap();
        assert!(requests.borrow().is_empty());
        // No second resume or durable edit is needed to unblock missing definitions.
        let a = a.publish_snapshot(&h,a.registry_snapshot(),[]).unwrap();
        entered.wait().await;
        a.configure_extensions(&h,c,settings(ExtensionSelection::Exact(vec![]))).await.unwrap();
        release.release();
        receipt.wait().await.unwrap();
        assert_eq!(requests.borrow()[0].tools.len(),1);
        assert!(requests.borrow()[1].tools.is_empty());
        let entries = h.commit(move |tx| Box::pin(async move { Ok(tx.scan_entries(EntryQuery::new(c),128,None).await?.items) })).await.unwrap().value;
        let result = entries.iter().find(|e|e.kind=="agent.toolResult").unwrap();
        assert_eq!(result.data.as_ref().unwrap()["code"], "invalid_tool_call");
        assert!(matches!(decode_message(&result.model.as_ref().unwrap()[0]).unwrap(), ModelMessage::ToolResult {content, is_error:true, ..} if content == "Tool is not installed"));
        h.close().await.unwrap();
    }, driver).await;
}
#[test]
fn memory_publication_wakes_blocked_work_and_deselection_prevents_execution() {
    block_on(bounded(publication_wakes_and_deselection_prevents_call(
        MemoryStorage::new(),
    )));
}
#[test]
fn sqlite_publication_wakes_blocked_work_and_deselection_prevents_execution() {
    let db = Database::new();
    block_on(bounded(publication_wakes_and_deselection_prevents_call(
        db.open(),
    )));
}

#[path = "extensions/hooks.rs"]
mod hooks;

#[path = "extensions/hook_lifetime.rs"]
mod hook_lifetime;
