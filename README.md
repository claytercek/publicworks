# Public Works

Public Works is our own Rust runtime project, modeled on Pi Durable. The working
slice is a **Session transaction layer for conversations, immutable entries, and
durable tasks**, with in-memory and SQLite storage. Hosts can explicitly run
foreground leaf tasks as checkpointed phase handlers. Startup normalization
changes interrupted running tasks back to pending. There is no automatic scheduler,
LLM integration, or agent executor.

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
  `TaskRunner`/`TaskDriver`, and public `MemoryStorage`. Uses serde/serde_json and small futures primitives;
  no SQL, Tokio, or executor dependency.
- `storage/sqlite/` — `publicworks-storage-sqlite`: `SqliteStorage::open(path)`;
  bundled SQLite through rusqlite, no external server.
- `cli/` — `publicworks-cli`: the `publicworks` binary, composing the public
  Session interface and SQLite with futures-lite.

Both adapters return boxed futures but **block while polled**. SQLite uses WAL,
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

## Create and execute a leaf task

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

Only pending, non-abort-marked, foreground tasks owned by an ownerless conversation
and owning no tasks or conversations can run. The installed definition's version
must exactly match the stored version. Other cases return `RunResult::Blocked`
without writes. There is one active invocation per attached runner, with no
background scheduling, wait/abort execution, or owned-completion supervision.

A handler receives its detached task and a `TaskRuntime`. It calls
`runtime.commit(|tx, current| ...)` to atomically write entries and return an
optional `TaskUpdate::Checkpoint`, `Complete`, or `Fail`. Entries receive task
attribution automatically. Each successful phase must change its structural
checkpoint or commit an outcome; returning without durable progress faults the
task. External effects are not part of the storage transaction and are not
exactly-once.

Run the complete [host example](runtime/examples/task_execution.rs):

```sh
cargo run -p publicworks-runtime --example task_execution
```

On shutdown, await `runner.close()` while polling both drivers, then await
`session.close()`. Runner close signals `runtime.cancelled()`, interrupts queued
unreserved requests, joins the cooperative handler, and drains runner-admitted
mutations. A queued mutation whose callback has not started rejects at the closing
gate; draining does not promise it will apply. A noncooperative handler can keep
runner close pending. Session close signals the runner but does not join external
handler code. Dropping TaskDriver forfeits settlement and fences old contexts.
See the execution contract for details.

## Reopen interrupted tasks

`Session::open_recovered(storage)` returns `(CommitWaiter<Session>, SessionDriver)`.
Poll the driver concurrently with the opening waiter, then take the usable Session
from the successful receipt's `value`. Its `seq` is present only when startup
normalized running tasks to pending. All running pages are read before one atomic
replacement batch; checkpoints, input, version, owner, flags, and memos survive.
Plain `Session::new` remains unchanged.

Opening runs no task code. Unknown kinds/versions are preserved, and waiting,
completing, and terminal states are not reconciled. After opening, explicitly
attach a runner and request each desired run. There is no definition migration or
schema migration framework. Dropping the opening waiter still leaves normalization
and close owned by the driver. See the task persistence contract.

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
progress and error precedence, attribution, leaf guards, invocation fencing,
rejection versus uncertain commits, cooperative shutdown, and SQLite reopen.
Task storage checks run under
both default and feature-unified serde_json. SQLite rejects incompatible schemas
rather than migrating them. The executable
smoke test runs create/append/show in
separate processes. See the contract for what
these checks do and do not establish. Packages remain unpublished; `Cargo.lock`
is retained for reproducible builds.
