use super::*;
use crate::ToolDeclaration;
use serde_json::{Value, json};

fn tool(name: &str, version: u64) -> Tool {
    Tool::new(
        ToolDeclaration {
            name: name.into(),
            version,
            description: format!("implementation {version}"),
            parameters: json!({"type":"object"}),
        },
        move |_| Err(format!("validator {version}")),
        |_, _| panic!("registry must not execute tools"),
    )
}
fn extension(name: &str, version: u64) -> Extension {
    Extension::new(name, vec![tool("shared", version)])
}
fn registry() -> AgentRegistry {
    let mut registry = AgentRegistry::new();
    for (i, name) in ["a", "b", "c"].into_iter().enumerate() {
        registry.install(extension(name, i as u64 + 1)).unwrap();
    }
    registry
}
fn names(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).into()).collect()
}
fn extension_names(extensions: &[Rc<Extension>]) -> Vec<&str> {
    extensions.iter().map(|e| e.name()).collect()
}
fn tool_versions(tools: &[Tool]) -> Vec<(&str, u64)> {
    tools
        .iter()
        .map(|t| (t.declaration().name.as_str(), t.declaration().version))
        .collect()
}

#[test]
fn empty_registry_and_empty_bundle_are_valid() {
    let mut registry = AgentRegistry::new();
    let empty = registry.snapshot();
    assert_eq!(empty.generation(), 0);
    assert!(empty.extensions().is_empty());
    assert!(empty.get("missing").is_none());
    let resolved = empty.resolve(&ExtensionSelection::default(), None);
    assert!(resolved.extensions().is_empty());
    assert!(resolved.tools().is_empty());
    registry
        .install(Extension::new("hooks-later", vec![]))
        .unwrap();
    assert_eq!(registry.snapshot().generation(), 1);
    assert!(
        registry
            .snapshot()
            .get("hooks-later")
            .unwrap()
            .tools()
            .is_empty()
    );
    assert!(empty.extensions().is_empty());
}

#[test]
fn replacement_preserves_position_and_reinstall_appends() {
    let mut registry = registry();
    let original = registry.snapshot();
    assert_eq!(original.generation(), 3);
    registry.install(extension("b", 20)).unwrap();
    let replaced = registry.snapshot();
    assert_eq!(replaced.generation(), 4);
    assert_eq!(extension_names(replaced.extensions()), ["a", "b", "c"]);
    assert_eq!(
        replaced.get("b").unwrap().tools()[0].declaration().version,
        20
    );
    assert_eq!(
        original.get("b").unwrap().tools()[0].declaration().version,
        2
    );
    assert!(Rc::ptr_eq(
        original.get("a").unwrap(),
        replaced.get("a").unwrap()
    ));
    assert!(!Rc::ptr_eq(
        original.get("b").unwrap(),
        replaced.get("b").unwrap()
    ));

    // Removal uses a freshly constructed string, not extension object identity.
    assert!(registry.uninstall(&String::from("b")).unwrap());
    let removed = registry.snapshot();
    assert_eq!(removed.generation(), 5);
    assert_eq!(extension_names(removed.extensions()), ["a", "c"]);
    assert!(removed.get("b").is_none());
    assert!(!registry.uninstall("b").unwrap());
    assert!(Rc::ptr_eq(&removed.0, &registry.snapshot().0));

    registry.install(extension("b", 200)).unwrap();
    assert_eq!(registry.snapshot().generation(), 6);
    assert_eq!(
        extension_names(registry.snapshot().extensions()),
        ["a", "c", "b"]
    );
    assert_eq!(extension_names(original.extensions()), ["a", "b", "c"]);
    assert_eq!(extension_names(replaced.extensions()), ["a", "b", "c"]);
    assert_eq!(extension_names(removed.extensions()), ["a", "c"]);
}

#[test]
fn invalid_new_and_replacement_bundles_leave_publication_unchanged() {
    let mut registry = registry();
    let before = registry.snapshot();
    let mut deep = Value::Null;
    for _ in 0..publicworks_runtime::MAX_JSON_DEPTH {
        deep = json!([deep]);
    }
    let mut invalid_parameters = tool("deep", 1);
    invalid_parameters.declaration.parameters = deep;
    for name in ["new", "b"] {
        for tools in [
            vec![tool("valid", 1), tool("", 1)],
            vec![tool("same", 1), tool("same", 2)],
            vec![tool("valid", 1), invalid_parameters.clone()],
        ] {
            assert!(registry.install(Extension::new(name, tools)).is_err());
            assert!(Rc::ptr_eq(&before.0, &registry.snapshot().0));
            assert_eq!(registry.snapshot().generation(), 3);
            assert_eq!(
                extension_names(registry.snapshot().extensions()),
                ["a", "b", "c"]
            );
        }
    }
    assert!(registry.install(extension("", 1)).is_err());
    assert!(Rc::ptr_eq(&before.0, &registry.snapshot().0));
    // A rejection does not poison the registry or consume a position/generation.
    registry.install(extension("new", 4)).unwrap();
    assert_eq!(registry.snapshot().generation(), 4);
    assert_eq!(
        extension_names(registry.snapshot().extensions()),
        ["a", "b", "c", "new"]
    );
}

#[test]
fn generation_exhaustion_is_atomic_for_install_and_uninstall() {
    let mut registry = registry();
    Rc::get_mut(&mut registry.current.0).unwrap().generation = u64::MAX;
    let before = registry.snapshot();
    assert!(registry.install(extension("b", 20)).is_err());
    assert!(registry.install(extension("d", 4)).is_err());
    assert!(registry.uninstall("b").is_err());
    assert!(Rc::ptr_eq(&before.0, &registry.snapshot().0));
    assert!(!registry.uninstall("missing").unwrap());
}

#[test]
fn names_are_case_sensitive_and_not_normalized() {
    let mut registry = AgentRegistry::new();
    for name in ["a", "A", " a "] {
        registry
            .install(Extension::new(
                name,
                vec![tool("x", 0), tool("X", u64::MAX)],
            ))
            .unwrap();
    }
    assert_eq!(
        extension_names(registry.snapshot().extensions()),
        ["a", "A", " a "]
    );
    assert!(!registry.uninstall(" a").unwrap());
    let resolved = registry
        .snapshot()
        .resolve(&ExtensionSelection::Exact(names(&["A", " a"])), None);
    assert_eq!(extension_names(resolved.extensions()), ["A"]);
}

#[test]
fn default_selection_uses_host_default_or_installation_order() {
    let snapshot = registry().snapshot();
    for (host, expected) in [
        (None, names(&["a", "b", "c"])),
        (Some(names(&[])), names(&[])),
        (
            Some(names(&["c", "missing", "a", "c", "b", "a"])),
            names(&["c", "a", "b"]),
        ),
    ] {
        let resolved = snapshot.resolve(&ExtensionSelection::Default, host.as_deref());
        assert_eq!(extension_names(resolved.extensions()), expected);
    }
}

#[test]
fn exact_selection_ignores_host_default_and_first_occurrence_wins() {
    let snapshot = registry().snapshot();
    let host = names(&["a", "c"]);
    for (requested, expected) in [
        (names(&[]), names(&[])),
        (names(&["missing"]), names(&[])),
        (
            names(&["c", "b", "c", "missing", "a", "b"]),
            names(&["c", "b", "a"]),
        ),
    ] {
        let selection = ExtensionSelection::Exact(requested);
        for default in [None, Some(host.as_slice())] {
            let resolved = snapshot.resolve(&selection, default);
            assert_eq!(extension_names(resolved.extensions()), expected);
        }
    }
}

#[test]
fn add_remove_operates_in_order_and_add_can_reintroduce_removed_names() {
    let snapshot = registry().snapshot();
    let host = names(&["b", "missing", "a", "b", "c"]);
    for (default, add, remove, expected) in [
        (None, names(&[]), names(&[]), names(&["a", "b", "c"])),
        (
            None,
            names(&["a", "c", "a"]),
            names(&["a", "a", "missing"]),
            names(&["b", "c", "a"]),
        ),
        (
            None,
            names(&["missing", "missing"]),
            names(&["a", "b", "c"]),
            names(&[]),
        ),
        (
            Some(host.as_slice()),
            names(&["b", "a", "missing", "b"]),
            names(&["b", "c"]),
            names(&["a", "b"]),
        ),
        (
            Some([].as_slice()),
            names(&["c", "c", "a"]),
            names(&["c"]),
            names(&["c", "a"]),
        ),
    ] {
        let selection = ExtensionSelection::AddRemove { add, remove };
        let copy = selection.clone();
        let resolved = snapshot.resolve(&selection, default);
        assert_eq!(extension_names(resolved.extensions()), expected);
        assert_eq!(selection, copy);
    }
}

#[test]
fn missing_names_stay_inert_until_installed_and_survive_uninstall() {
    let mut registry = registry();
    let old = registry.snapshot();
    let host = names(&["future", "a"]);
    let selections = [
        ExtensionSelection::Default,
        ExtensionSelection::Exact(host.clone()),
        ExtensionSelection::AddRemove {
            add: names(&["future"]),
            remove: vec![],
        },
    ];
    for selection in &selections {
        assert_eq!(
            extension_names(old.resolve(selection, Some(&host)).extensions()),
            ["a"]
        );
    }
    registry.install(extension("future", 4)).unwrap();
    let installed = registry.snapshot();
    for selection in &selections {
        assert_eq!(
            extension_names(installed.resolve(selection, Some(&host)).extensions()),
            ["future", "a"]
        );
        assert_eq!(
            extension_names(old.resolve(selection, Some(&host)).extensions()),
            ["a"]
        );
    }
    registry.uninstall("future").unwrap();
    for selection in &selections {
        assert_eq!(
            extension_names(
                registry
                    .snapshot()
                    .resolve(selection, Some(&host))
                    .extensions()
            ),
            ["a"]
        );
    }
    registry.install(extension("future", 5)).unwrap();
    for selection in &selections {
        assert_eq!(
            extension_names(
                registry
                    .snapshot()
                    .resolve(selection, Some(&host))
                    .extensions()
            ),
            ["future", "a"]
        );
    }
    assert_eq!(host, names(&["future", "a"]));
}

#[test]
fn tools_use_selection_order_and_stable_last_winner_replacement() {
    let mut registry = AgentRegistry::new();
    registry
        .install(Extension::new(
            "a",
            vec![tool("shared", 1), tool("first-only", 1)],
        ))
        .unwrap();
    registry
        .install(Extension::new(
            "b",
            vec![tool("second-only", 2), tool("shared", 2)],
        ))
        .unwrap();
    registry
        .install(Extension::new(
            "c",
            vec![tool("shared", 3), tool("third-only", 3)],
        ))
        .unwrap();
    let snapshot = registry.snapshot();
    let resolved = snapshot.resolve(&ExtensionSelection::Default, None);
    assert_eq!(
        tool_versions(resolved.tools()),
        [
            ("shared", 3),
            ("first-only", 1),
            ("second-only", 2),
            ("third-only", 3)
        ]
    );
    let reverse = snapshot.resolve(&ExtensionSelection::Exact(names(&["b", "a", "b"])), None);
    assert_eq!(
        tool_versions(reverse.tools()),
        [("second-only", 2), ("shared", 1), ("first-only", 1)]
    );
    assert!(Rc::ptr_eq(
        &reverse.tools()[1].execute,
        &snapshot.get("a").unwrap().tools()[0].execute
    ));
    assert_eq!(
        (reverse.tools()[1].validate)(&Value::Null),
        Err("validator 1".into())
    );
}

#[test]
fn old_snapshots_and_owned_resolutions_pin_original_callbacks() {
    let mut registry = registry();
    let snapshot = registry.snapshot();
    let resolved = snapshot.resolve(&ExtensionSelection::Exact(names(&["b"])), None);
    registry.install(extension("b", 20)).unwrap();
    let current = registry
        .snapshot()
        .resolve(&ExtensionSelection::Exact(names(&["b"])), None);
    registry.uninstall("b").unwrap();
    drop(registry);
    drop(snapshot);
    assert_eq!(
        (resolved.tools()[0].validate)(&Value::Null),
        Err("validator 2".into())
    );
    assert_eq!(
        (current.tools()[0].validate)(&Value::Null),
        Err("validator 20".into())
    );
    assert_eq!(resolved.extensions()[0].tools()[0].declaration().version, 2);
}

#[test]
fn callback_resources_live_until_last_snapshot_or_resolution_is_dropped() {
    let resource = Rc::new(());
    let weak = Rc::downgrade(&resource);
    let mut retained_tool = tool("local", 1);
    retained_tool.validate = Rc::new(move |_| {
        let _ = &resource;
        Ok(())
    });
    let mut registry = AgentRegistry::new();
    registry
        .install(Extension::new("a", vec![retained_tool]))
        .unwrap();
    let snapshot = registry.snapshot();
    let resolved = snapshot.resolve(&ExtensionSelection::Default, None);
    registry.uninstall("a").unwrap();
    drop(registry);
    assert!(weak.upgrade().is_some());
    drop(snapshot);
    assert!(weak.upgrade().is_some());
    drop(resolved);
    assert!(weak.upgrade().is_none());
}

#[test]
fn publication_and_resolution_never_invoke_callbacks() {
    let mut never_call = tool("local", 1);
    never_call.validate = Rc::new(|_| panic!("must not invoke argument validator"));
    let mut registry = AgentRegistry::new();
    registry
        .install(Extension::new("a", vec![never_call.clone()]))
        .unwrap();
    let old = registry.snapshot();
    registry
        .install(Extension::new("a", vec![never_call.clone()]))
        .unwrap();
    assert_eq!(
        old.resolve(&ExtensionSelection::Default, None)
            .tools()
            .len(),
        1
    );
    assert_eq!(
        registry
            .snapshot()
            .resolve(&ExtensionSelection::Default, None)
            .tools()
            .len(),
        1
    );
    assert!(
        registry
            .install(Extension::new("a", vec![never_call.clone(), never_call]))
            .is_err()
    );
    registry.uninstall("a").unwrap();
}

#[test]
fn selected_hook_objects_survive_replacement_and_uninstall() {
    use crate::LifecycleHooks;
    let callback = Rc::new(
        |_: crate::ModelResponse, _: crate::HookContext| -> crate::HookFuture<()> {
            panic!("publication and selection cannot run hooks")
        },
    );
    let weak = Rc::downgrade(&callback);
    let mut registry = AgentRegistry::new();
    registry
        .install(Extension::new("hooks", vec![]).with_hooks(LifecycleHooks {
            after_response: Some(callback),
            ..LifecycleHooks::default()
        }))
        .unwrap();
    let snapshot = registry.snapshot();
    let selected = snapshot.resolve(&ExtensionSelection::Exact(names(&["hooks", "hooks"])), None);
    assert_eq!(selected.extensions().len(), 1);
    assert!(Rc::ptr_eq(
        snapshot
            .get("hooks")
            .unwrap()
            .hooks()
            .after_response
            .as_ref()
            .unwrap(),
        selected.extensions()[0]
            .hooks()
            .after_response
            .as_ref()
            .unwrap()
    ));
    registry.install(Extension::new("hooks", vec![])).unwrap();
    registry.uninstall("hooks").unwrap();
    drop(snapshot);
    assert!(weak.upgrade().is_some());
    drop(selected);
    assert!(weak.upgrade().is_none());
}
