# Public Works

Public Works is our own Rust runtime project, modeled on Pi Durable. The working
slice is a **Session transaction layer for conversations and immutable entries**,
with in-memory and SQLite storage. There is no scheduler, LLM integration, or
agent executor yet.

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
  public `MemoryStorage`. Uses serde/serde_json and small futures primitives;
  no SQL, Tokio, or executor dependency.
- `storage/sqlite/` — `publicworks-storage-sqlite`: `SqliteStorage::open(path)`;
  bundled SQLite through rusqlite, no external server.
- `cli/` — `publicworks-cli`: the `publicworks` binary, composing the public
  Session interface and SQLite with futures-lite.

Both adapters return boxed futures but **block while polled**. SQLite uses WAL,
NORMAL synchronization, and one transaction per batch. This is an embedded,
single-logical-owner foundation, not a multiwriter runtime or a power-loss
persistence guarantee. The schema is Public Works' own v2; it cannot open Pi's
JavaScript databases or the initial unpublished v1 format. Payloads use finite
native serde_json numbers (full i64/u64 and round-trippable f64), not arbitrary
precision decimal text, and have a maximum nesting depth of 64. See the contract
for how model/edit wrappers count toward that limit. Opaque JSON decoding remains
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
panics, abandoned operations, waiter/driver drops, and close. The executable
smoke test runs create/append/show in
separate processes. See the contract for what
these checks do and do not establish. Packages remain unpublished; `Cargo.lock`
is retained for reproducible builds.
