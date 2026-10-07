//! Durable, name-only configuration. This module never resolves executable code.
use crate::{wire::validate_entry, *};
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
    /// An absent field means Default; arrays are Exact; objects are AddRemove.
    pub fn decode(value: Option<&Value>) -> Result<Self, SessionError> {
        match value {
            None => Ok(Self::Default),
            Some(value @ Value::Array(_)) => Ok(Self::Exact(names(value)?)),
            Some(Value::Object(object)) if object.keys().all(|k| k == "add" || k == "remove") => {
                Ok(Self::AddRemove {
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
                })
            }
            _ => Err(invalid("Invalid extension selection")),
        }
    }

    /// Default removes the selection field rather than pinning today's defaults.
    pub fn encode(&self) -> Result<Option<Value>, SessionError> {
        let value = match self {
            Self::Default => return Ok(None),
            Self::Exact(names) => json!(names),
            Self::AddRemove { add, remove } => json!({"add":add,"remove":remove}),
        };
        Self::decode(Some(&value))?;
        Ok(Some(value))
    }
}

fn names(value: &Value) -> Result<Vec<String>, SessionError> {
    value
        .as_array()
        .ok_or_else(|| invalid("Expected extension names"))?
        .iter()
        .map(|v| match v.as_str() {
            Some(name) if !name.is_empty() => Ok(name.to_owned()),
            _ => Err(invalid("Expected nonempty extension name")),
        })
        .collect()
}

impl ExtensionConfig {
    /// Decode only extension fields from ConversationStateRecord.agent_config.
    pub fn decode(value: Option<&Value>) -> Result<Self, SessionError> {
        validate_entry(None, value)?;
        let object = object(value)?;
        let config = match object.get("extensionConfig") {
            None => BTreeMap::new(),
            Some(Value::Object(config)) if config.keys().all(|name| !name.is_empty()) => {
                config.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
            }
            _ => return Err(invalid("Invalid extension configuration")),
        };
        Ok(Self {
            selection: ExtensionSelection::decode(object.get("extensions"))?,
            config,
        })
    }

    /// Encode extension fields only. Queue updates merge rather than replace them.
    pub fn encode(&self) -> Result<Value, SessionError> {
        let mut object = Map::new();
        if let Some(selection) = self.selection.encode()? {
            object.insert("extensions".into(), selection);
        }
        object.insert("extensionConfig".into(), json!(self.config));
        let value = Value::Object(object);
        Self::decode(Some(&value))?;
        Ok(value)
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
