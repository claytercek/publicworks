//! Provider-neutral durable turns on the host-driven Public Works task kernel.
//! Model and tool callbacks are trusted, local futures; no executor is spawned.
use publicworks_runtime::{SessionError, TaskRuntime};
use serde_json::Value;
use std::{future::Future, pin::Pin, rc::Rc};

mod admission;
pub use admission::{BusyMode, ExpectedHead, QueueConfig, QueueMode, SubmitOptions, WriteOptions};
mod context;
mod engine;
mod wire;
pub use context::{ContextProjection, project_context};
pub use engine::{Agent, TOOL_KIND, TURN_KIND};
pub use wire::{decode_message, encode_message};

#[derive(Clone, Debug, PartialEq)]
pub struct TurnConfig {
    pub model: String,
    pub instructions: String,
    pub max_model_rounds: u64,
}
#[derive(Clone, Debug, PartialEq)]
pub struct ToolDeclaration {
    pub name: String,
    pub version: u64,
    pub description: String,
    pub parameters: Value,
}
#[derive(Clone, Debug, PartialEq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}
#[derive(Clone, Debug, PartialEq)]
pub enum ModelMessage {
    User {
        text: String,
    },
    Assistant {
        text: String,
        tool_calls: Vec<ToolCall>,
    },
    ToolResult {
        call_id: String,
        content: String,
        is_error: bool,
    },
}
#[derive(Clone, Debug, PartialEq)]
pub struct ModelRequest {
    pub model: String,
    pub instructions: String,
    pub tools: Vec<ToolDeclaration>,
    pub messages: Vec<ModelMessage>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FinishReason {
    Stop,
    ToolCalls,
}
#[derive(Clone, Debug, PartialEq)]
pub struct ModelResponse {
    pub message: ModelMessage,
    pub finish_reason: FinishReason,
    pub usage: Option<Value>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct ModelError {
    pub message: String,
    pub partial_response: Option<ModelResponse>,
    pub usage: Option<Value>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct ToolResult {
    pub content: String,
    pub is_error: bool,
    pub usage: Option<Value>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct ToolError {
    pub message: String,
    pub usage: Option<Value>,
}
pub type ModelFuture = Pin<Box<dyn Future<Output = Result<ModelResponse, ModelError>>>>;
pub type ToolFuture = Pin<Box<dyn Future<Output = Result<ToolResult, ToolError>>>>;

/// Cooperative interruption. Cancellation does not undo an external effect.
#[derive(Clone)]
pub struct Cancellation(TaskRuntime);
impl Cancellation {
    pub fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }
    pub fn cancelled(&self) -> impl Future<Output = ()> + 'static {
        self.0.cancelled()
    }
}
pub trait Model {
    fn complete(&self, request: ModelRequest, cancellation: Cancellation) -> ModelFuture;
}
impl<F> Model for F
where
    F: Fn(ModelRequest, Cancellation) -> ModelFuture,
{
    fn complete(&self, request: ModelRequest, cancellation: Cancellation) -> ModelFuture {
        self(request, cancellation)
    }
}
type Validator = Rc<dyn Fn(&Value) -> Result<(), String>>;
type Executor = Rc<dyn Fn(ToolCall, Cancellation) -> ToolFuture>;
/// Executable code remains in this immutable installation, never in storage.
#[derive(Clone)]
pub struct Tool {
    pub(crate) declaration: ToolDeclaration,
    pub(crate) validate: Validator,
    pub(crate) execute: Executor,
}
impl Tool {
    pub fn new(
        declaration: ToolDeclaration,
        validate: impl Fn(&Value) -> Result<(), String> + 'static,
        execute: impl Fn(ToolCall, Cancellation) -> ToolFuture + 'static,
    ) -> Self {
        Self {
            declaration,
            validate: Rc::new(validate),
            execute: Rc::new(execute),
        }
    }
    pub fn declaration(&self) -> &ToolDeclaration {
        &self.declaration
    }
}
fn invalid(message: impl Into<String>) -> SessionError {
    SessionError::Invalid(message.into())
}

#[cfg(test)]
mod tests;
