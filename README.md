# Public Works

Public Works is our own Rust runtime project, modeled on Pi Durable. The first
working slice is a **conversation and immutable-entry storage kernel** with
in-memory and SQLite adapters. There is no Session, scheduler, LLM integration,
or agent executor yet.

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

This is a low-level transcript/storage demo, not a chat agent. Text is one
application payload, not the core entry model. Root conversation ID 1 is reserved
from allocation but is not automatically created; ordinary IDs start at 2.

## Libraries

- `runtime/` — `publicworks-runtime`: records, object-safe async-compatible
  `Storage`, errors, fork-aware reads, and public `MemoryStorage`. Depends on
  serde/serde_json, not SQL or an async executor.
- `storage/sqlite/` — `publicworks-storage-sqlite`: `SqliteStorage::open(path)`;
  bundled SQLite through rusqlite, no external server.
- `cli/` — `publicworks-cli`: the `publicworks` binary, composing the public
  storage interface and SQLite with futures-lite.

Both adapters return boxed futures but **block while polled**. SQLite uses WAL,
NORMAL synchronization, and one transaction per batch. This is an embedded,
single-logical-owner foundation, not a multiwriter runtime or a power-loss
persistence guarantee. The schema is Public Works' own v2; it cannot open Pi's
JavaScript databases or the initial unpublished v1 format. Payloads use finite
native serde_json numbers (full i64/u64 and round-trippable f64), not arbitrary
precision decimal text, and have a maximum nesting depth of 64. See the contract
for how model/edit wrappers count toward that limit. Opaque JSON decoding remains
correct when an embedding application enables additional serde_json features.

## Development

```sh
cargo fmt --all -- --check
cargo test --workspace --all-features --locked
cargo test --workspace --all-features --locked --features serde_json/arbitrary_precision
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

The shared conformance checks exercise the public `Storage` seam against both
adapters. SQLite tests also cover reopen, allocator continuity, rollback, invalid
schemas, and corrupt reads. The executable smoke test runs create/append/show in
separate processes. See the contract for what
these checks do and do not establish. Packages remain unpublished; `Cargo.lock`
is retained for reproducible builds.
