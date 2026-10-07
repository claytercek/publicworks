//! Provider-neutral lifecycle callbacks. Executable hooks are never serialized.
use crate::*;
use futures_util::future::{Either, select};
use publicworks_runtime::EntryDraft;
use serde_json::json;

/// Local callbacks may retain owned inputs across awaits; they need not be Send.
pub type HookFuture<T> = Pin<Box<dyn Future<Output = Result<T, HookError>>>>;
pub type Hook<I, O> = Rc<dyn Fn(I, HookContext) -> HookFuture<O>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HookError {
    /// Stops the phase, never reported as an ordinary error or a tool block.
    Cancelled,
    Failed(String),
}
impl std::fmt::Display for HookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => f.write_str("Lifecycle hook cancelled"),
            Self::Failed(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for HookError {}

/// Only argument values can be rewritten: call identity and tool name are pinned.
#[derive(Clone, Debug, PartialEq)]
pub enum BeforeTool {
    Continue,
    Rewrite(Value),
    Block(String),
}
/// The rewritten call for after_tool; the original model call for after_tools.
/// Batch results are in original call order and include non-executable calls.
#[derive(Clone, Debug, PartialEq)]
pub struct ToolOutcome {
    pub call: ToolCall,
    pub result: ToolResult,
}

/// One optional handler per lifecycle per extension. Dispatch follows selected
/// extension order. Ordinary failures are recorded as `agent.hookError` entries
/// and skipped, except before_tool failures, which block. Cancellation stops
/// dispatch.
///
/// Hooks run outside transactions and may repeat after an unsettled commit.
/// Memos stabilize values, not external side effects.
#[derive(Clone, Default)]
pub struct LifecycleHooks {
    /// Optionally replaces the request before model invocation. The final
    /// request and tool offers are validated and pinned.
    pub before_request: Option<Hook<ModelRequest, Option<ModelRequest>>>,
    /// Observes a successful, validated response before settlement.
    pub after_response: Option<Hook<ModelResponse, ()>>,
    /// Runs after argument validation and before effect intent. Rewrites feed
    /// subsequent hooks and are revalidated before intent.
    pub before_tool: Option<Hook<ToolCall, BeforeTool>>,
    /// Optionally replaces the result after execution. It does not change task
    /// status or replay policy, and is not called for unavailable, blocked,
    /// aborted, or recovered in-flight work.
    pub after_tool: Option<Hook<ToolOutcome, Option<ToolResult>>>,
    /// Observes tool outcomes after the last child settles and before the
    /// post-tools boundary.
    pub after_tools: Option<Hook<Vec<ToolOutcome>, ()>>,
}

/// Provides cancellation and durable first-writer-wins memo access for one
/// hook invocation. Memo keys are scoped to the extension name within the task,
/// so a same-name replacement shares winners. Retaining this context cannot
/// extend the invocation's write lifetime.
#[derive(Clone)]
pub struct HookContext {
    runtime: TaskRuntime,
    extension: String,
}
impl HookContext {
    pub fn cancellation(&self) -> Cancellation {
        Cancellation(self.runtime.clone())
    }
    pub fn extension_name(&self) -> &str {
        &self.extension
    }
    fn key(&self, name: &str) -> String {
        json!(["agent.hook", self.extension, name]).to_string()
    }
    pub async fn memo(&self, name: &str) -> Result<Option<Value>, SessionError> {
        Ok(self.runtime.memo(self.key(name)).await?.value)
    }
    pub async fn memo_or_insert(
        &self,
        name: &str,
        candidate: Value,
    ) -> Result<Value, SessionError> {
        Ok(self
            .runtime
            .memo_or_insert(self.key(name), candidate)
            .await?
            .value)
    }
}

impl ResolvedExtensions {
    // One owned resolution is reused for every dispatch in the phase. The
    // generic loop owns ordering, cancellation, reporting and context creation;
    // lifecycle-specific functions below own composition.
    async fn dispatch<S, I, O>(
        &self,
        runtime: &TaskRuntime,
        name: &'static str,
        mut state: S,
        hook: impl Fn(&LifecycleHooks) -> Option<&Hook<I, O>>,
        input: impl Fn(&S) -> I,
        compose: impl Fn(&mut S, Result<O, String>) -> bool,
    ) -> Result<S, SessionError> {
        for extension in self.extensions() {
            if runtime.is_cancelled() {
                return Err(invalid(HookError::Cancelled.to_string()));
            }
            let Some(hook) = hook(extension.hooks()) else {
                continue;
            };
            let context = HookContext {
                runtime: runtime.clone(),
                extension: extension.name().to_owned(),
            };
            let future = hook(input(&state), context);
            let result = match select(future, Box::pin(runtime.cancelled())).await {
                Either::Left((result, _)) => result,
                Either::Right(_) => Err(HookError::Cancelled),
            };
            if runtime.is_cancelled() || matches!(result, Err(HookError::Cancelled)) {
                return Err(invalid(HookError::Cancelled.to_string()));
            }
            let output = match result {
                Ok(output) => Ok(output),
                Err(HookError::Failed(message)) => {
                    let extension = extension.name().to_owned();
                    let error = message.clone();
                    runtime
                        .commit(move |tx, task| {
                            Box::pin(async move {
                                let mut entry = EntryDraft::new("agent.hookError");
                                entry.data = Some(
                                    json!({"extension":extension,"hook":name,"message":error}),
                                );
                                tx.append_entry(task.conversation_id, entry).await?;
                                Ok(None)
                            })
                        })
                        .await?;
                    Err(message)
                }
                Err(HookError::Cancelled) => unreachable!(),
            };
            if compose(&mut state, output) {
                break;
            }
        }
        if runtime.is_cancelled() {
            return Err(invalid(HookError::Cancelled.to_string()));
        }
        Ok(state)
    }
    pub(crate) async fn before_request(
        &self,
        runtime: &TaskRuntime,
        request: ModelRequest,
    ) -> Result<ModelRequest, SessionError> {
        self.dispatch(
            runtime,
            "beforeRequest",
            request,
            |h| h.before_request.as_ref(),
            Clone::clone,
            |state, replacement| {
                if let Ok(Some(value)) = replacement {
                    *state = value;
                }
                false
            },
        )
        .await
    }
    pub(crate) async fn after_response(
        &self,
        runtime: &TaskRuntime,
        response: ModelResponse,
    ) -> Result<(), SessionError> {
        self.dispatch(
            runtime,
            "afterResponse",
            response,
            |h| h.after_response.as_ref(),
            Clone::clone,
            |_, _| false,
        )
        .await
        .map(|_| ())
    }
    pub(crate) async fn before_tool(
        &self,
        runtime: &TaskRuntime,
        call: ToolCall,
    ) -> Result<(ToolCall, Option<String>), SessionError> {
        self.dispatch(
            runtime,
            "beforeTool",
            (call, None),
            |h| h.before_tool.as_ref(),
            |s| s.0.clone(),
            |state, decision| match decision {
                Ok(BeforeTool::Continue) => false,
                Ok(BeforeTool::Rewrite(arguments)) => {
                    state.0.arguments = arguments;
                    false
                }
                Ok(BeforeTool::Block(message)) | Err(message) => {
                    state.1 = Some(message);
                    true
                }
            },
        )
        .await
    }
    pub(crate) async fn after_tool(
        &self,
        runtime: &TaskRuntime,
        outcome: ToolOutcome,
    ) -> Result<ToolResult, SessionError> {
        self.dispatch(
            runtime,
            "afterTool",
            outcome,
            |h| h.after_tool.as_ref(),
            Clone::clone,
            |state, replacement| {
                if let Ok(Some(value)) = replacement {
                    state.result = value;
                }
                false
            },
        )
        .await
        .map(|s| s.result)
    }
    pub(crate) async fn after_tools(
        &self,
        runtime: &TaskRuntime,
        outcomes: Vec<ToolOutcome>,
    ) -> Result<(), SessionError> {
        self.dispatch(
            runtime,
            "afterTools",
            outcomes,
            |h| h.after_tools.as_ref(),
            Clone::clone,
            |_, _| false,
        )
        .await
        .map(|_| ())
    }
}
