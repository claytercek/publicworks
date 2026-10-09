# Public Works

[![CI](https://github.com/claytercek/publicworks/actions/workflows/release.yml/badge.svg)](https://github.com/claytercek/publicworks/actions/workflows/release.yml)
[![MSRV](https://img.shields.io/badge/MSRV-1.88-blue.svg)](#rust-version)

Public Works is an embeddable Rust runtime for durable conversations, tasks, and
model/tool turns. It stores progress at explicit checkpoints so a host can resume
interrupted work without depending on one async executor or model provider.

> **Yes, agents helped build this.** Public Works is developed with coding agents,
> which seems appropriate for software meant to support them. They do a meaningful
> amount of the typing. A human owns the design, review, and release decisions.

> **Experimental:** Public Works is `0.1` software. Public APIs, persisted records,
> checkpoint formats, and adapter schemas may change between releases.

## Feedback wanted

Public Works is early `0.1` software, and feedback is welcome. In particular, I’d
like to hear whether the host-polled driver model, crate boundaries, and recovery
semantics make sense for the kinds of durable work you build. If you try it, open
an issue with what you were building, where the API helped, and where it got in
your way.

## Who is this for?

Public Works is for developers embedding durable work in Rust applications:
agent backends, long-running workflows, background jobs, and automation that
combines model requests with local tools. Choose it when you need checkpoints
and recovery inside an application that already owns its executor, provider
clients, storage policy, and process lifecycle. The runtime can also drive
non-agent workflows without depending on the agent or provider crates.

It is not a complete agent application, hosted workflow service, job queue, or
sandbox. Public Works does not choose an executor, spawn background threads,
provide authorization, or make external effects exactly once.

## Why Public Works?

Public Works makes a few deliberate choices about embedding and recovery:

- **Host-driven execution.** You poll the driver futures and control their
  lifetime. The runtime does not require Tokio or `Send` futures, so durable work
  can run alongside the rest of your application without adopting a new
  executor. Individual provider adapters may have their own executor requirements.
- **Separately usable layers.** Use the task runtime on its own, add the agent
  layer for model/tool turns, and choose storage and provider adapters separately.
  Model clients, credentials, and executable callbacks stay outside durable
  records and are supplied by the host.
- **Explicit admission and startup.** Admitted agent input has a durable submission
  receipt; dropping its waiter does not withdraw it. Opening a harness recovers
  state without invoking handlers, giving the host time to install definitions
  before enabling progress. Missing or mismatched definitions leave normal work
  pending rather than running unknown code.
- **Conservative tool recovery.** An interrupted external effect may have
  succeeded even when its result was never saved. Tools are not replayed across
  that uncertainty by default; replay requires an explicit safety opt-in. See
  [recovery and side effects](#recovery-and-side-effects).

These choices leave more work to the host: it must keep drivers polled, reinstall
callbacks after a restart, and provide authorization, isolation, and deployment
coordination where needed.

## How it fits together

The host owns the application boundary. Public Works supplies the durable state
and scheduling primitives inside it:

```mermaid
flowchart LR
    H[Host application<br/>executor · providers · tools]
    R[publicworks-runtime<br/>Session · Harness · tasks]
    A[publicworks-agent<br/>model/tool turns · policies]
    S[(Storage adapter<br/>Memory or SQLite)]
    P[Optional provider<br/>OpenAI Responses]

    H --> R
    H --> A
    A --> R
    R --> S
    H --> P
    P --> A
```

A model request or tool call is first represented in durable state. The host
then polls the runtime and supplies the executable model and tool callbacks.
After a restart, the host installs those callbacks again and resumes the
pending work explicitly. See the [runtime architecture and examples](runtime/)
and the [agent turn example](agent/examples/agent_turn.rs).

## Crates

| Package | Purpose | Documentation |
| --- | --- | --- |
| [`publicworks-runtime`](https://crates.io/crates/publicworks-runtime) | Executor-neutral sessions, durable task trees, submissions, and in-memory storage | [API docs](https://docs.rs/publicworks-runtime) · [Source](runtime/) |
| [`publicworks-storage-sqlite`](https://crates.io/crates/publicworks-storage-sqlite) | Bundled SQLite storage for one logical owner | [API docs](https://docs.rs/publicworks-storage-sqlite) · [Source](storage/sqlite/) |
| [`publicworks-agent`](https://crates.io/crates/publicworks-agent) | Provider-neutral durable model/tool turns and extension hooks | [API docs](https://docs.rs/publicworks-agent) · [Source](agent/) |
| [`publicworks-provider-openai`](https://crates.io/crates/publicworks-provider-openai) | Optional non-streaming OpenAI Responses adapter | [API docs](https://docs.rs/publicworks-provider-openai) · [Source](providers/openai/) |

Most agent applications use the runtime, agent, and one storage adapter. Add the
OpenAI provider only when the host sends requests to OpenAI.

## Installation

A durable agent with SQLite storage uses these dependencies:

```toml
[dependencies]
futures-lite = "2"
publicworks-runtime = "0.1"
publicworks-storage-sqlite = "0.1"
publicworks-agent = "0.1"
```

Add the OpenAI adapter when needed:

```toml
publicworks-provider-openai = "0.1"
tokio = { version = "1", features = ["rt", "macros", "net", "io-util", "time"] }
```

The crates can also be added with Cargo:

```sh
cargo add publicworks-runtime publicworks-storage-sqlite publicworks-agent
cargo add publicworks-provider-openai
```

## Open a durable session

The following program creates or opens a SQLite database, appends one entry, and
shuts the session down cleanly:

```rust
use futures_lite::future::{block_on, zip};
use publicworks_runtime::{EntryDraft, Session, SessionError};
use publicworks_storage_sqlite::SqliteStorage;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let storage = SqliteStorage::open("publicworks.db")?;

    block_on(async move {
        let (session, driver) = Session::new(storage);
        let commands = async {
            let receipt = session
                .commit(|tx| Box::pin(async move {
                    let conversation = tx.create_conversation().await?;
                    tx.append_entry(conversation.id, EntryDraft::new("example"))
                        .await
                }))
                .await?;

            session.close().await?;
            Ok::<_, SessionError>(receipt.value)
        };

        let (entry, ()) = zip(commands, driver).await;
        println!("stored entry {}", entry?.id);
        Ok::<_, SessionError>(())
    })?;

    Ok(())
}
```

`Session::new` returns a handle and a driver future. The host must keep the driver
polled while commands run and until `Session::close` completes. Public Works does
not spawn an executor, and its futures do not need to be `Send`.

For durable task execution, start with the
[`publicworks-runtime` examples](runtime/examples/) and the
[`Harness` documentation](https://docs.rs/publicworks-runtime/latest/publicworks_runtime/struct.Harness.html).

## Run durable agent turns

`publicworks-agent` adds checkpointed model requests, local tools, recovery
policies, and lifecycle hooks. The host supplies a model implementation and tools,
then opens a runtime `Harness` with the definitions returned by the agent.

The [complete agent example](agent/examples/agent_turn.rs) uses a deterministic
local model and tool, so it needs no network access or credentials. The
[tool policy example](agent/examples/tool_policy.rs) shows how a host can inspect
or rewrite tool calls before execution.

`publicworks-provider-openai` implements the agent's model interface for the
OpenAI Responses API. It uses asynchronous I/O and must be polled inside a Tokio
runtime. See the [provider guide](providers/openai/README.md) for configuration,
supported response types, and credential handling.

## Recovery and side effects

Task handlers commit progress through the runtime. External effects happen outside
the storage transaction. A crash can therefore leave an effect completed without
a matching durable result.

Agent tools default to `ReplayPolicy::Unsafe`. If execution was interrupted after
its intent was stored, recovery records an uncertain-effect error instead of
running the tool again. Use `ReplayPolicy::Safe` only when repeating the stored
arguments is acceptable. Public Works does not provide exactly-once external I/O.

The built-in storage implementations assume one logical owner. The SQLite adapter
does not provide a multi-process ownership lock and performs synchronous database
work when polled.

## Compatibility and limits

The `0.1` release line has no migration guarantee for stored records, task
checkpoints, agent checkpoints, definition versions, or SQLite schemas. Review the
crate release notes before upgrading an application with unfinished durable work.

Public Works does not provide authorization, sandboxing, forced cancellation,
timers, multi-process coordination, or automatic schema migration. Model
streaming, media, reasoning-state replay, and server-side conversation state are
outside the current OpenAI adapter's scope.

## Origins and inspiration

Public Works was inspired by
[`@earendil-works/pi-durable`](https://github.com/earendil-works/pi/tree/main/packages/durable),
an experimental, embeddable TypeScript durable agent harness. pi-durable is a
library distinct from the Pi coding-agent application; it also exposes session,
storage, and custom-task APIs.

Both projects build on committing conversation and task progress before treating
it as visible or complete. Task trees, durable submissions, explicit resume,
opt-in replay of interrupted tool calls, and keeping executable callbacks out of
stored state are shared design ideas, not features unique to Public Works.

Public Works is an independent implementation, not a compatible port. Its focus
is a host-polled, executor-neutral runtime with separately packaged agent,
storage, and provider layers. pi-durable integrates with `@earendil-works/pi-ai`
for model access and `@earendil-works/chord` for document state. The projects have
different APIs, persistence contracts, and supported feature sets.

Choose Public Works when its Rust driver model and crate boundaries fit your
application. Consider pi-durable when its TypeScript APIs, pi-ai integration,
and document facilities fit better. Both can be embedded in applications and run
custom durable tasks; that capability alone is not a reason to choose one over
the other.

## Rust version

Public Works supports Rust 1.88 and newer. The workspace uses Rust 2024 edition.
A future release may raise the minimum supported Rust version in a minor release
while the project remains below `1.0`.

## Project policies

See [CONTRIBUTING.md](CONTRIBUTING.md) to build and test the workspace. Report
security issues through the process in [SECURITY.md](SECURITY.md).

## License

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option.
