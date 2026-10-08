# publicworks-agent

Provider-neutral durable model/tool turns for `publicworks-runtime`.

This crate was inspired by [`@earendil-works/pi-durable`](https://github.com/earendil-works/pi/tree/main/packages/durable), an experimental TypeScript durable agent harness. It is an independent Rust implementation with different APIs and provider boundaries.

The host supplies a [`Model`](https://docs.rs/publicworks-agent/latest/publicworks_agent/trait.Model.html),
local [`Tool`](https://docs.rs/publicworks-agent/latest/publicworks_agent/struct.Tool.html)
callbacks, and the runtime driver. The agent persists request intent, projected
conversation context, tool progress, and turn settlement; it does not spawn an
executor or choose a network provider.

## Start with the example

The repository includes a complete turn using a deterministic local model and
tool. It needs no credentials or network access:

```sh
cargo run -p publicworks-agent --example agent_turn
```

Create an `Agent`, open a runtime `Harness` with `agent.definitions()`, and keep
the harness driver polled. `Agent::submit` durably admits an input and enables
progress; the returned submission can be awaited through `Submission::wait`.
Executable models, tools, and extensions are process-local and must be installed
again after restart.

## Tools and recovery

Tool calls execute sequentially after their intent is committed. Tools default
to `ReplayPolicy::Unsafe`: if execution was interrupted after intent but before
a durable result, recovery records an uncertain-effect error instead of running
the tool again. Opt in to `ReplayPolicy::Safe` only when repeating the stored
effective arguments is acceptable. This policy does not provide exactly-once
execution or undo external effects.

Original calls are read from the immutable assistant entry. Turn checkpoints
retain the prepared request or the current batch's assistant reference, child
index, child ID, and effective offered tool versions. Child input contains only
the assistant reference and call index; its execute checkpoint records the
hook-rewritten arguments and replay policy.

The prepared request is persisted before `before_request` runs. Hooks may edit
its model, instructions, messages, and tools for the provider call, but those
edits do not replace the prepared checkpoint. An interrupted request restarts
from the original snapshot and runs the currently selected hooks again. Once a
response commits, the tool batch pins the effective offered names and versions
from the edited request.

A terminal tool outcome records `resultEntryId` atomically with its result.
Normal parent recovery loads that entry directly and validates its association.
Batch observers and fault/orphan recovery use raw entries bounded to the current
assistant's suffix, not projected context. Matching host-inserted results retain
their actual entry IDs; the earliest valid matching entry wins. Result metadata
and the model message must agree on the call ID. Malformed receipts fail closed;
faulted or orphaned children without receipts still receive the uncertain-result
fallback when no matching raw result exists.

Named `Extension` bundles combine tools and lifecycle hooks. Conversation
configuration stores extension names and settings, never executable code.
Registry snapshots are immutable so an active phase keeps stable callbacks while
the host publishes a replacement for later phases.

The `tool_policy` example shows a selected `before_tool` hook:

```sh
cargo run -p publicworks-agent --example tool_policy
```

It is a host-policy recipe, not an authorization boundary or sandbox. Hooks and
tools are trusted in-process code.

## Supported model shape

The provider-neutral message model supports user text, assistant text, local
function calls, and text tool results. Streaming, media, reasoning state,
provider credentials, and transport policy belong in provider crates. Model
requests may replay after interruption, so billing and output are not guaranteed
to occur exactly once.

`ModelResponse` contains assistant `text`, `tool_calls`, and optional `usage`.
An empty call list completes the turn; a nonempty list starts a tool batch.
Models no longer return a message role or `FinishReason`. Providers must report
incomplete or failed output as `ModelError`, which can retain partial assistant
output and usage for diagnostics.

This is an early `0.1` release. Persisted agent checkpoints and definition
versions have no migration guarantee yet. The compact checkpoint formats use
`agent.turn` version **3** and `agent.tool` version **4**. Older checkpoints are
not migrated or interpreted as the new layouts; finish outstanding work with
the old installation before upgrading if it must remain executable.

## License

Licensed under either MIT or Apache-2.0, at your option.
