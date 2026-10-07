//! Optional, selected `before_tool` guard for trusted local agent extensions.
//!
//! The returned extension must be installed and selected for a conversation.
//! Omitting or deselecting it runs tools without this guard. This crate provides
//! no approval queue, authorization boundary, or process isolation.
use publicworks_agent::{BeforeTool, Extension, HookContext, HookFuture, LifecycleHooks, ToolCall};
use std::rc::Rc;

/// The conventional extension name used by [`permission_extension`].
pub const EXTENSION_NAME: &str = "permissions";

/// A local policy decision made before durable tool-effect intent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PermissionDecision {
    Allow,
    Deny(String),
}

/// Build the conventional `permissions` extension from a synchronous policy.
///
/// The policy sees the validated model call. A denial becomes an immediate
/// `tool_blocked` result and the tool callback is not invoked. Later selected
/// `before_tool` hooks are skipped because the first block wins.
pub fn permission_extension(
    check: impl Fn(&ToolCall) -> PermissionDecision + 'static,
) -> Extension {
    permission_extension_async(move |call, _| {
        let decision = check(&call);
        Box::pin(async move { Ok(decision) })
    })
}

/// Build the conventional `permissions` extension from an asynchronous policy.
///
/// The callback has the ordinary hook cancellation and memo context. Returning
/// `HookError::Failed` follows `before_tool` fail-closed behavior: the error is
/// recorded and its text blocks the call. `HookError::Cancelled` cancels hook
/// dispatch instead of manufacturing a denial.
pub fn permission_extension_async(
    check: impl Fn(ToolCall, HookContext) -> HookFuture<PermissionDecision> + 'static,
) -> Extension {
    Extension::new(EXTENSION_NAME, vec![]).with_hooks(LifecycleHooks {
        before_tool: Some(Rc::new(move |call, context| {
            let decision = check(call, context);
            Box::pin(async move {
                Ok(match decision.await? {
                    PermissionDecision::Allow => BeforeTool::Continue,
                    PermissionDecision::Deny(message) => BeforeTool::Block(message),
                })
            })
        })),
        ..LifecycleHooks::default()
    })
}
