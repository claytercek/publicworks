# publicworks-agent

Provider-neutral durable model/tool turns for `publicworks-runtime`.

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

This is an early `0.1` release. Persisted agent checkpoints and definition
versions have no migration guarantee yet.

## License

Licensed under either MIT or Apache-2.0, at your option.
