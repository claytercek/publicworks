//! Durable, name-only configuration. This module never resolves executable code.
use crate::*;
use publicworks_runtime::{CommitWaiter, Harness, Id, Tx};
use serde_json::{Map, json};
use std::collections::BTreeMap;

/// Conversation-local extension policy and opaque JSON settings keyed by name.
/// Missing installations remain durable and become effective upon reinstall.
/// Settings are retained for extension consumers; this slice does not interpret them.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExtensionConfig {
    pub selection: ExtensionSelection,
    pub config: BTreeMap<String, Value>,
}

impl ExtensionSelection {
    fn validate(&self) -> Result<(), SessionError> {
        let validate_names = |names: &[String]| {
            if names.iter().any(String::is_empty) {
                Err(invalid("Expected nonempty extension name"))
            } else {
                Ok(())
            }
        };
        match self {
            Self::Default => Ok(()),
            Self::Exact(names) => validate_names(names),
            Self::AddRemove { add, remove } => {
                validate_names(add)?;
                validate_names(remove)
            }
        }
    }

    /// An absent field means Default; arrays are Exact; objects are AddRemove.
    pub fn decode(value: Option<&Value>) -> Result<Self, SessionError> {
        let selection = match value {
            None => Self::Default,
            Some(value @ Value::Array(_)) => Self::Exact(names(value)?),
            Some(Value::Object(object)) if object.keys().all(|k| k == "add" || k == "remove") => {
                Self::AddRemove {
                    add: object
                        .get("add")
                        .map(names)
                        .transpose()?
                        .unwrap_or_default(),
                    remove: object
                        .get("remove")
                        .map(names)
                        .transpose()?
                        .unwrap_or_default(),
                }
            }
            _ => return Err(invalid("Invalid extension selection")),
        };
        selection.validate()?;
        Ok(selection)
    }

    /// Default removes the selection field rather than pinning today's defaults.
    pub fn encode(&self) -> Result<Option<Value>, SessionError> {
        self.validate()?;
        Ok(match self {
            Self::Default => None,
            Self::Exact(names) => Some(json!(names)),
            Self::AddRemove { add, remove } => Some(json!({"add":add,"remove":remove})),
        })
    }
}

fn names(value: &Value) -> Result<Vec<String>, SessionError> {
    value
        .as_array()
        .ok_or_else(|| invalid("Expected extension names"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| invalid("Expected nonempty extension name"))
        })
        .collect()
}

impl ExtensionConfig {
    fn validate_semantics(&self) -> Result<(), SessionError> {
        self.selection.validate()?;
        if self.config.keys().any(String::is_empty) {
            return Err(invalid("Invalid extension configuration"));
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), SessionError> {
        self.validate_semantics()?;
        // agent_config and extensionConfig are the two containers enclosing
        // each extension-owned opaque value.
        for value in self.config.values() {
            publicworks_runtime::validate_native_json_value(value, 2)?;
        }
        Ok(())
    }

    /// Decode only extension fields from ConversationStateRecord.agent_config.
    pub fn decode(value: Option<&Value>) -> Result<Self, SessionError> {
        if let Some(value) = value {
            // This also validates unknown host fields, which are intentionally
            // retained by merge rather than interpreted here.
            publicworks_runtime::validate_native_json_value(value, 0)?;
        }
        let object = object(value)?;
        let config = match object.get("extensionConfig") {
            None => BTreeMap::new(),
            Some(Value::Object(config)) => {
                config.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
            }
            _ => return Err(invalid("Invalid extension configuration")),
        };
        let result = Self {
            selection: ExtensionSelection::decode(object.get("extensions"))?,
            config,
        };
        result.validate_semantics()?;
        Ok(result)
    }

    /// Encode extension fields only. Queue updates merge rather than replace them.
    pub fn encode(&self) -> Result<Value, SessionError> {
        self.validate()?;
        let mut object = Map::new();
        if let Some(selection) = self.selection.encode()? {
            object.insert("extensions".into(), selection);
        }
        object.insert("extensionConfig".into(), json!(self.config));
        Ok(Value::Object(object))
    }
}

pub(crate) fn object(value: Option<&Value>) -> Result<Map<String, Value>, SessionError> {
    match value {
        None => Ok(Map::new()),
        Some(Value::Object(object)) => Ok(object.clone()),
        _ => Err(invalid("Agent configuration must be an object")),
    }
}

pub(crate) fn queues(value: Option<&Value>) -> Result<QueueConfig, SessionError> {
    let object = object(value)?;
    let mode = |key| match object.get(key) {
        None => Ok(QueueMode::One),
        Some(Value::String(s)) if s == "one" => Ok(QueueMode::One),
        Some(Value::String(s)) if s == "all" => Ok(QueueMode::All),
        _ => Err(invalid("Invalid queue mode")),
    };
    Ok(QueueConfig {
        steer: mode("steer")?,
        follow_up: mode("followUp")?,
    })
}

pub(crate) async fn merge(
    tx: &Tx,
    conversation: Id,
    fields: impl IntoIterator<Item = (&str, Option<Value>)>,
) -> Result<(), SessionError> {
    let state = tx.conversation_state(conversation).await?;
    let mut object = object(state.as_ref().and_then(|s| s.agent_config.as_ref()))?;
    for (key, value) in fields {
        match value {
            Some(value) => {
                object.insert(key.into(), value);
            }
            None => {
                object.remove(key);
            }
        }
    }
    let value = Value::Object(object);
    queues(Some(&value))?;
    ExtensionConfig::decode(Some(&value))?;
    tx.update_conversation_state(conversation, move |state| state.agent_config = Some(value))
        .await?;
    Ok(())
}

impl Agent {
    /// Persist names and JSON only, without resolving extensions, invoking code,
    /// or enabling scheduling. Queue policy and unrelated config fields survive.
    pub fn configure_extensions(
        &self,
        harness: &Harness,
        conversation: Id,
        config: ExtensionConfig,
    ) -> CommitWaiter<()> {
        harness.commit(move |tx| {
            Box::pin(async move {
                let encoded = config.encode()?;
                merge(
                    tx,
                    conversation,
                    [
                        ("extensions", encoded.get("extensions").cloned()),
                        ("extensionConfig", encoded.get("extensionConfig").cloned()),
                    ],
                )
                .await
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nested(depth: usize) -> Value {
        (0..depth).fold(Value::Null, |value, _| json!([value]))
    }

    fn config(value: Value) -> ExtensionConfig {
        ExtensionConfig {
            selection: ExtensionSelection::AddRemove {
                add: vec!["later".into()],
                remove: vec!["default".into()],
            },
            config: BTreeMap::from([("provider".into(), value)]),
        }
    }

    #[test]
    fn typed_config_validation_preserves_native_opaque_values() {
        let expected = config(json!({
            "max": u64::MAX,
            "min": i64::MIN,
            "float": 1.25,
            "reserved": {"$serde_json::private::Number": "ordinary"},
            "null": null
        }));
        let encoded = expected.encode().unwrap();
        assert_eq!(ExtensionConfig::decode(Some(&encoded)).unwrap(), expected);
        assert!(encoded["extensionConfig"]["provider"]["reserved"].is_object());
    }

    #[test]
    fn typed_config_validation_counts_both_config_wrappers() {
        let accepted = config(nested(publicworks_runtime::MAX_JSON_DEPTH - 2));
        assert!(accepted.encode().is_ok());
        let rejected = config(nested(publicworks_runtime::MAX_JSON_DEPTH - 1));
        assert!(rejected.encode().is_err());
    }

    #[test]
    fn decode_validates_unknown_fields_at_the_agent_config_boundary() {
        let accepted = json!({
            "host": nested(publicworks_runtime::MAX_JSON_DEPTH - 1),
            "extensionConfig": {}
        });
        assert!(ExtensionConfig::decode(Some(&accepted)).is_ok());
        let rejected = json!({
            "host": nested(publicworks_runtime::MAX_JSON_DEPTH),
            "extensionConfig": {}
        });
        assert!(ExtensionConfig::decode(Some(&rejected)).is_err());
    }

    #[test]
    fn typed_config_rejects_empty_names_without_roundtripping() {
        for invalid in [
            ExtensionConfig {
                selection: ExtensionSelection::Exact(vec![String::new()]),
                config: BTreeMap::new(),
            },
            ExtensionConfig {
                selection: ExtensionSelection::Default,
                config: BTreeMap::from([(String::new(), Value::Null)]),
            },
        ] {
            assert!(invalid.encode().is_err());
        }
        assert!(ExtensionConfig::decode(Some(&json!({"extensionConfig": null}))).is_err());
        assert_eq!(
            ExtensionConfig::decode(None).unwrap(),
            ExtensionConfig::default()
        );
    }

    #[test]
    fn typed_config_rejects_retained_non_native_number_tokens() {
        for token in ["18446744073709551616", "1e999", "1.000", "1e0"] {
            let Ok(value) = serde_json::from_str::<Value>(token) else {
                continue;
            };
            let retained_token = value.to_string();
            if retained_token == token {
                assert!(config(value).encode().is_err(), "{token}");
            }
        }
    }
}
