# Public Works

Public Works is our own Rust runtime project, modeled on Pi Durable. The working
slice is a **Session transaction layer for conversations, immutable entries, and
durable tasks**, with in-memory and SQLite storage. A host-polled `Harness` opens and reconciles
all supported foreground task trees, then atomically reserves every eligible task
and polls their local handler futures concurrently after explicit `resume`.
Durable waits, held outcomes, cancellation cascades, task observation, and orderly
join-before-storage-close are included. The lower-level explicit-root `TaskRunner`
remains available. A provider-neutral agent layer adds durable model/tool turns
using host-supplied callbacks. An opt-in OpenAI Responses package provides asynchronous networking
without adding a provider or executor dependency to the core libraries.

## Try the persistence demo

With a Rust toolchain supporting edition 2024, run these commands from the repo
root. Use a new database path to get the example IDs:

```sh
cargo run -p publicworks-cli -- create /tmp/publicworks-demo.db
# {"commitSeq":1,"conversationId":2}
cargo run -p publicworks-cli -- append /tmp/publicworks-demo.db 2 "hello"
# {"commitSeq":2,"conversationId":2,"entryId":3}
cargo run -p publicworks-cli -- show /tmp/publicworks-demo.db 2
```

Each command opens and closes the database. `create` adds a conversation;
`append` stores a `publicworks.text` entry with `data: {"text":"hello"}`;
`show` prints visible entries newest-first. Successful commands print JSON.
Errors go to stderr with a nonzero exit status. Append/show require an existing
file and conversation; they do not create a missing conversation.

The CLI runs create, append, and read-only show through Session transactions.
It is a transcript demo, not a chat agent. Text is one
application payload, not the core entry model. Root conversation ID 1 is reserved
from allocation but is not automatically created; ordinary IDs start at 2.

## Libraries

- `runtime/` — `publicworks-runtime`: records, object-safe async-compatible
  `Storage`, `Session`/`Tx`, host-polled `SessionDriver`, fork-aware reads, and
  task creation/ownership validation, startup normalization, a host-polled
  `Harness`/`HarnessDriver`, the lower-level `TaskRunner`/`TaskDriver`, and public
  `MemoryStorage`. Uses serde/serde_json and small futures primitives;
  no SQL, Tokio, or executor dependency.
- `agent/` — `publicworks-agent`: immutable model/tool installation, atomic turn
  admission, pinned requests, fork-aware context projection, ordered tool children,
  and conservative interrupted-effect recovery. No provider or executor dependency.
- `providers/openai/` — `publicworks-provider-openai`: opt-in, non-streaming
  OpenAI Responses text/function-call adapter using reqwest/rustls. Requires a
  Tokio host; excluded from the workspace's default members.
- `storage/sqlite/` — `publicworks-storage-sqlite`: `SqliteStorage::open(path)`;
  bundled SQLite through rusqlite, no external server.
- `cli/` — `publicworks-cli`: the `publicworks` binary, composing the public
  Session interface and SQLite with futures-lite.

The in-memory and SQLite storage adapters return boxed futures but **block while
polled**. The opt-in HTTP provider uses asynchronous network I/O. SQLite uses WAL,
NORMAL synchronization, and one transaction per batch. This is an embedded,
single-logical-owner foundation, not a multiwriter runtime or a power-loss
persistence guarantee. The current schema is Public Works' own v3. There are no
existing users or schema migrations: incompatible databases reject without being
deleted or rewritten. Use a new path or manually recreate a disposable development
database when the format changes. Backward compatibility is not promised during
this pre-user phase. Atomic write rollback remains required; it prevents partial
batches and is separate from migrating old formats. Payloads use finite
native serde_json numbers (full i64/u64 and round-trippable f64), not arbitrary
precision decimal text, and have a maximum nesting depth of 64. See the contract
for how model/edit and task state/outcome/memo wrappers count toward that limit. Opaque JSON decoding remains
correct when an embedding application enables additional serde_json features.

## Host a Session

The host must poll the driver concurrently with command futures, including
shutdown. This example uses futures-lite; an existing local executor works too.
The runtime and its Storage interface do not require `Send`.

```rust
use futures_lite::future::{block_on, zip};
use publicworks_runtime::{EntryDraft, MemoryStorage, Session};

block_on(async {
    let (session, driver) = Session::new(MemoryStorage::new());
    let command = async {
        let result = session.commit(|tx| Box::pin(async move {
            let conversation = tx.create_conversation().await?;
            tx.append_entry(conversation.id, EntryDraft::new("example")).await
        })).await;
        // Always close, including when the command fails.
        let closed = session.close().await;
        closed?;
        result
    };
    let (result, ()) = zip(command, driver).await;
    let receipt = result.unwrap();
    assert_eq!(receipt.seq.unwrap().get(), 1);
});
```

`commit` admits its owned callback synchronously. Dropping its returned waiter,
including before its first poll, does not cancel the admitted transaction. The
driver owns callback execution and storage settlement. Read-only and empty
transactions skip Storage.commit and return `seq: None`. Returned records are
detached values; public table reads after any mutation attempt reject.

`close()` immediately seals admission, drains admitted transactions, and calls
Storage.close once. Repeated calls share the result; dropping a close waiter does
not stop cleanup. Dropping the last Session handle also requests drain and close.
**Dropping the driver is different:** waiters get `DriverStopped`, and in-flight
persistence or asynchronous cleanup is no longer guaranteed. Never await a
Session commit or close from inside its own callback; use Tx operations there.

Only an explicit no-effect storage rejection leaves a failed commit reusable.
Any other commit error or storage panic poisons the Session; it must be closed.
Callback errors/panics discard staged writes without poisoning. Started Tx
operations are drained even when their method waiters are abandoned; a callback
that returns with pending operations fails rather than committing. There is no
public commit stream or view subscription yet, so no subscriber queue to grow.
See the design contract for the boundary
between inherited Pi behavior and Rust driver/cancellation mapping.

## Schedule all ready task trees with a Harness

`Harness::open(storage, registry)` returns an opening waiter and one
`HarnessDriver`. Poll the driver while awaiting opening and for the full lifetime
of the returned `Harness`. Opening scans the task graph, atomically normalizes
interrupted `running` records to `pending`, and schedules code-free reconciliation.
It never invokes a definition or handler.

The scheduler remains paused until `harness.resume()`. Resume is idempotent and
permanently enables progress. Each drain reconciles committed records and changes
**all** currently eligible tasks from `pending` to `running` in one Session
transaction. Handler futures start only after that batch is acknowledged. They are
then polled concurrently by the same local driver; the runtime spawns no executor
work and requires neither `Send` nor Tokio.

Use `harness.commit` for application transactions, `wait_task` for a committed
terminal record, and `inspect` for a detached view of live tasks and scheduler
blocks. Dropping a task waiter abandons only that observation. Replacing the
immutable `TaskRegistry` snapshot wakes blocked work; every reservation retains
one stable snapshot. Missing or non-exact definitions leave normal work pending,
while abort-marked work without an exact definition becomes orphaned only after
owned work drains.

Close through `harness.close()` while continuing to poll `HarnessDriver`. Close
seals public admission, signals and joins every active invocation, drains admitted
Session work, and only then closes Storage. A noncooperative handler can keep close
pending. Dropping the driver is forfeiture: late runtime access is fenced, but an
in-flight persistence operation or external effect may have an uncertain outcome.

The current Harness intentionally schedules only the foreground ownership topology
already supported by `TaskRunner`. Task-owned conversations, background traversal,
conversation handles, and scoped idle waits remain the next ownership-lifecycle
phase in the implementation plan.

## Create and execute one task tree explicitly

`TaskDefinition::new(kind, version, initial, phases)` combines a reusable
synchronous initial-checkpoint function with a `BTreeMap<String, PhaseHandler>`.
Definitions are immutable and cloneable. `TaskRegistry::new(definitions)` rejects
duplicate kinds. Neither creating a definition nor registering it runs code.

Inside a Session callback, `tx.create_task(definition, input, options)` persists a
pending task and returns a detached `TaskRecord`. `TaskOptions` requires explicit
conversation or task ownership. Task-owned children inherit their owner's
conversation and cannot be background. Use `tx.task` and `tx.scan_tasks` for
committed reads before mutations.

For execution, attach one runner with
`TaskRunner::attach(&session, registry)`, then call `runner.run(task.id)`.
The host must poll **both** the SessionDriver and TaskDriver. The TaskDriver owns
admitted run requests and handler futures; SessionDriver remains the only Storage
owner. A handler awaiting external work does not occupy the Session queue.
Dropping a run waiter does not cancel its request.

`run(root)` drives a foreground root and its task-owned descendants in one
ownerless conversation, with only one handler active at a time. Background work
and task-owned conversations anywhere below the root are unsupported. The root
must be conversation-owned; `run(child)` is not a second hosting mode. Pending,
waiting, and completing roots can be driven, but unrecovered running records
require `Session::open_recovered` first. Terminal roots return `Blocked(NotPending)`.

Normal execution requires an exact definition version. A blocked root does not
prevent eligible children from running. `RunResult::Terminal` is a durable root
receipt; `Suspended(TaskRecord)` is the durable root at quiescence, when no
supported work can execute. `TaskRunner` itself has no global scheduler, timer,
or automatic retry: explicitly run the root again when an observed external
dependency changes, or use a Harness for Harness-wide progress.

A handler receives its detached task and a `TaskRuntime`. It calls
`runtime.commit(|tx, current| ...)` to atomically write entries and return an
optional `TaskUpdate::Checkpoint`, `Wait { checkpoint, on, policy }`, `Complete`,
`Fail`, or `Abort { reason, result }`. Entries receive task
attribution automatically. Each successful phase must change its structural
checkpoint, commit a wait, or commit an outcome; returning without durable progress faults the
task. External effects are not part of the storage transaction and are not
exactly-once.

Run the [leaf host example](runtime/examples/task_execution.rs) or the
[tree example](runtime/examples/task_tree.rs):

```sh
cargo run -p publicworks-runtime --example task_execution
cargo run -p publicworks-runtime --example task_tree
```

On shutdown, await `runner.close()` while polling both drivers, then await
`session.close()`. Runner close signals `runtime.cancelled()`, interrupts queued
unreserved requests, joins the cooperative handler, and drains runner-admitted
mutations. A queued mutation whose callback has not started rejects at the closing
gate; draining does not promise it will apply. A noncooperative handler can keep
runner close pending. Session close signals the runner but does not join external
handler code. Dropping TaskDriver forfeits settlement and fences old contexts.
See the execution contract for details.

## Run a durable agent turn

Install a local `Model` implementation and `Tool` callbacks with `Agent::new` from
`publicworks-agent`, register `agent.definitions()`, and use `agent.admit_turn`
inside a Session transaction. Admission atomically appends user text and creates
the foreground root, rejecting a busy conversation without writes. Explicitly
run that root while polling both drivers.

```sh
cargo run -p publicworks-agent --example agent_turn
```

The example uses a fake provider and tool; no credentials or network are needed.
Requests pin model identity, instructions, offered tool versions, and projected
messages. Calls execute one at a time after durable intent. An interrupted tool
is not replayed: it records an uncertain-effect error, then the turn continues
under its model-round limit. A model can request a similar action with a new call
ID, so approvals and external idempotency remain application responsibilities.
Cancellation does not undo effects. See the
agent contract for the API and limits.

## Opt in to OpenAI Responses

`publicworks-provider-openai` implements the same `Model` interface with a real
asynchronous HTTP client. Use `OpenAiResponses::new(api_key)` or
`OpenAiResponses::with_config(api_key, Config { .. })` and install it in the agent.
Poll the host on a Tokio runtime with I/O and time enabled; a current-thread
runtime is enough. Runtime and agent themselves remain executor-neutral.

The live example uses `gpt-4.1-mini`. It is not run by the test suite and requires
an explicit credential; running it sends data to OpenAI and can incur charges:

```sh
# Set OPENAI_API_KEY securely in your host environment first.
cargo run -p publicworks-provider-openai --example openai_turn
```

Credentials belong in the host environment/memory, never durable turn
configuration. The default endpoint is `https://api.openai.com/v1/responses`,
with a 120-second timeout and an 8 MiB response cap. Configuration accepts a
validated full HTTPS URL (numeric loopback HTTP is allowed for fixtures).
Redirects, retries, and ambient proxies are disabled.

The adapter supports text and local function calls, not every OpenAI model:
reasoning, streaming, images, OAuth, and built-in remote tools are unsupported.
Unknown semantic output, refusals, or incomplete output fail rather than produce
executable calls. Local tools still execute sequentially.

Model inputs, outputs, tool data, partial text, and usage can be persisted; this
adapter is not a secret scrubber. Cancellation and timeouts cannot undo server
work or billing. Recovery may replay a request and bill twice. `store:false`
is sent on every request but is not a zero-retention guarantee. See the
provider contract for the full boundary.

No credentials or real API calls are needed for provider tests:

```sh
cargo test -p publicworks-provider-openai --locked
cargo test -p publicworks-provider-openai --locked --features serde_json/arbitrary_precision
```

## Cancel a task durably

`runner.abort(id)` immediately admits a Session-owned command. Its `AbortWaiter`
returns `Result<AbortResult, RunError>`: `Marked`, `Terminal`, or
`Blocked(BlockReason)`. `Marked` means the mutation was acknowledged and the
target's normal invocation observed on the Session line has ended. It does **not** mean
cleanup finished or the task has an aborted outcome. Missing targets error;
unsupported scopes are untouched. Abort can target the root or a descendant;
it marks that subtree, not unrelated siblings. Terminal targets and unchanged
repeated marks consume no commit sequence. Missing definitions or exact-version
mismatches orphan a target only after its owned descendants have drained.

Affected normal handlers are signalled only after durable acknowledgement,
even when a child is waiting for cancellation while its parent is waiting on it.
Cleanup runs bottom-up with fresh, initially unsignalled abort invocations. Repeated requests do
not cancel cleanup. The default handler commits `Aborted`; use
`definition.with_abort_handler(handler)` to install a `PhaseHandler`-shaped custom
handler. It must commit `Abort`, `Complete`, or `Fail`. Returning without an outcome
faults, even after checkpoint or entry writes. Abort dispatch does not validate
the normal checkpoint's phase.

With the lower-level `TaskRunner`, an **inactive** marked task needs an explicit
`runner.run(root)` to execute cleanup. A resumed Harness instead notices the
committed mark and schedules eligible cleanup automatically. Never await your own
`runner.abort(id)` from its current normal handler: it joins that invocation and
would deadlock. Enqueuing without awaiting is allowed.

Dropping an abort waiter never cancels the admitted command. Dropping TaskDriver
also leaves admitted abort mutations owned by SessionDriver, but forfeits handler
settlement. In contrast, `runner.close()` requests cooperative shutdown without
persisting abort intent; reopening can resume normal work. Close does not start
fresh cleanup. Keep both drivers polling to drain admitted mutations. External
cleanup must tolerate replay after a crash. See the
cancellation contract.

## Reopen interrupted tasks

`Session::open_recovered(storage)` returns `(CommitWaiter<Session>, SessionDriver)`.
Poll the driver concurrently with the opening waiter, then take the usable Session
from the successful receipt's `value`. Its `seq` is present only when startup
normalized running tasks to pending. All running pages are read before one atomic
replacement batch; checkpoints, input, version, owner, flags, and memos survive.
Plain `Session::new` remains unchanged.

Opening runs no task code. Unknown kinds/versions are preserved, and waiting,
completing, and terminal states are not reconciled by this lower-level API. After
opening, explicitly attach a runner and request each desired root run. Prefer
`Harness::open` when the host wants Harness-wide reconciliation, reservation, and
automatic progress after one resume. Neither path performs definition or schema
migration. Dropping the Session opening waiter still leaves normalization and
close owned by the driver. See the task persistence contract.

## Development

```sh
cargo fmt --all -- --check
cargo test --workspace --all-features --locked
cargo test --workspace --all-features --locked --features serde_json/arbitrary_precision
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo clippy --workspace --all-targets --all-features --locked --features serde_json/arbitrary_precision -- -D warnings
```

The shared conformance checks exercise the public `Storage` seam against both
adapters. SQLite tests also cover reopen, allocator continuity, rollback, invalid
schemas, corrupt reads, and Session transactions across reopen. Session tests
cover FIFO settlement, failed callbacks, fork validation, poison classification,
panics, abandoned operations, waiter/driver drops, close, task ownership and
replacement validation, and startup normalization. Execution tests cover phase
progress and error precedence, attribution, drive-lifetime tree guards, invocation fencing,
rejection versus uncertain commits, cooperative shutdown, and SQLite reopen.
Tree tests cover joins, held outcomes, bottom-up cancellation, acknowledgement
races, scope validation, quiescence, and SQLite recovery. Task storage checks run under
both default and feature-unified serde_json. SQLite rejects incompatible schemas
rather than migrating them. The executable
smoke test runs create/append/show in
separate processes. See the contract for what
these checks do and do not establish. Packages remain unpublished; `Cargo.lock`
is retained for reproducible builds.
