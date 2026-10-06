//! Opaque JSON decoding independent of additive serde_json features.
//! RawValue and standard container deserializers do all syntax parsing. Genuine
//! objects never pass through Value's arbitrary_precision reserved-map visitor.
use crate::{ContextEdit, Id, MAX_JSON_DEPTH, StorageError};
use serde::{Deserialize, Deserializer, de::Error};
use serde_json::{Number, Value, value::RawValue};
use std::collections::BTreeMap;

fn invalid(message: &str) -> StorageError {
    StorageError::Other(message.into())
}

/// Only numeric representations produced by baseline native Value are persisted.
/// An arbitrary-precision consumer must not write tokens that a native consumer
/// would round or canonicalize differently on reopening.
pub(crate) fn native_number(number: &Number) -> Result<(), StorageError> {
    let text = number.to_string();
    let canonical = if text.contains(['.', 'e', 'E']) {
        text.parse::<f64>().ok().and_then(Number::from_f64)
    } else {
        text.parse::<i64>()
            .ok()
            .map(Number::from)
            .or_else(|| text.parse::<u64>().ok().map(Number::from))
    };
    if canonical.is_some_and(|n| n.to_string() == text) {
        Ok(())
    } else {
        Err(invalid(
            "JSON number is not a canonical native i64/u64/finite f64 value",
        ))
    }
}

pub(crate) fn decode(raw: &RawValue, parents: usize) -> Result<Value, StorageError> {
    let text = raw.get();
    let parse_error = |e: serde_json::Error| invalid(&e.to_string());
    match text.trim_start().as_bytes().first() {
        Some(b'{') | Some(b'[') if parents >= MAX_JSON_DEPTH => {
            Err(invalid("JSON payload nesting exceeds 64"))
        }
        Some(b'{') => {
            let fields: BTreeMap<String, Box<RawValue>> =
                serde_json::from_str(text).map_err(parse_error)?;
            let values = fields
                .into_iter()
                .map(|(key, value)| Ok((key, decode(&value, parents + 1)?)))
                .collect::<Result<_, StorageError>>()?;
            Ok(Value::Object(values))
        }
        Some(b'[') => {
            let values: Vec<Box<RawValue>> = serde_json::from_str(text).map_err(parse_error)?;
            Ok(Value::Array(
                values
                    .iter()
                    .map(|v| decode(v, parents + 1))
                    .collect::<Result<_, _>>()?,
            ))
        }
        _ => {
            let value = serde_json::from_str(text).map_err(parse_error)?;
            if let Value::Number(number) = &value {
                native_number(number)?;
            }
            Ok(value)
        }
    }
}

// default handles absence; a present null remains Some(Null).
pub(crate) fn data<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Value>, D::Error> {
    decode(&Box::<RawValue>::deserialize(d)?, 0)
        .map(Some)
        .map_err(D::Error::custom)
}
pub(crate) fn model<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<Value>>, D::Error> {
    Option::<Box<RawValue>>::deserialize(d)?
        .map(|raw| messages(&raw, 0))
        .transpose()
        .map_err(D::Error::custom)
}
fn messages(raw: &RawValue, parents: usize) -> Result<Vec<Value>, StorageError> {
    match decode(raw, parents)? {
        Value::Array(values) => Ok(values),
        _ => Err(invalid("Model messages must be an array")),
    }
}

// A normal struct avoids internally-tagged enum deserialization buffering into
// serde Content, which loses RawValue support before opaque messages are read.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EditFields {
    action: String,
    target: Id,
    messages: Option<Box<RawValue>>,
}
impl TryFrom<EditFields> for ContextEdit {
    type Error = StorageError;
    fn try_from(fields: EditFields) -> Result<Self, Self::Error> {
        match (fields.action.as_str(), fields.messages) {
            ("omit", None) => Ok(Self::Omit {
                target: fields.target,
            }),
            ("replace", Some(raw)) => Ok(Self::Replace {
                target: fields.target,
                messages: messages(&raw, 2)?,
            }),
            _ => Err(invalid("Invalid context edit action/messages")),
        }
    }
}

// Retain raw record JSON regardless of whether value precedes type. Serde's
// adjacent-tag Content buffering cannot carry nested RawValue deserializers.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WriteFields {
    #[serde(rename = "type")]
    kind: String,
    value: Box<RawValue>,
}
impl TryFrom<WriteFields> for crate::StorageWrite {
    type Error = StorageError;
    fn try_from(fields: WriteFields) -> Result<Self, Self::Error> {
        let result = match fields.kind.as_str() {
            "conversation" => serde_json::from_str(fields.value.get()).map(Self::Conversation),
            "task" => serde_json::from_str(fields.value.get()).map(Self::Task),
            "entry" => serde_json::from_str(fields.value.get()).map(Self::Entry),
            _ => return Err(invalid("Unknown storage write type")),
        };
        result.map_err(|error| invalid(&error.to_string()))
    }
}

pub(crate) fn validate(mut pending: Vec<(&Value, usize)>) -> Result<(), StorageError> {
    while let Some((value, parents)) = pending.pop() {
        let depth = parents + usize::from(value.is_array() || value.is_object());
        if depth > MAX_JSON_DEPTH {
            return Err(invalid("JSON payload nesting exceeds 64"));
        }
        match value {
            Value::Array(values) => pending.extend(values.iter().map(|v| (v, depth))),
            Value::Object(values) => pending.extend(values.values().map(|v| (v, depth))),
            Value::Number(number) => native_number(number)?,
            _ => {}
        }
    }
    Ok(())
}
