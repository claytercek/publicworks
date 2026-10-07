# publicworks-storage-sqlite

The bundled SQLite adapter for `publicworks-runtime`.

```rust,no_run
use publicworks_runtime::Session;
use publicworks_storage_sqlite::SqliteStorage;

let storage = SqliteStorage::open("publicworks.db")?;
let (_session, _driver) = Session::new(storage);
# Ok::<(), publicworks_runtime::StorageError>(())
```

`SqliteStorage::open` creates a new database when the path is empty, or validates
an existing Public Works schema before returning. Incompatible or corrupt schemas
are rejected; the adapter never migrates, resets, or repairs them.

Operations are exposed as futures to implement the runtime's `Storage` trait, but
they perform synchronous SQLite work when polled and may block for the configured
busy timeout. Use one logical owner. Concurrent external mutation and
multi-process ownership are unsupported.

The adapter uses bundled SQLite, WAL mode, `synchronous=NORMAL`, atomic write
batches, typed indexed tables, and leased ID ranges. Gaps in durable IDs are
normal. An acknowledged commit is durable according to those SQLite settings,
not a claim of power-loss-proof persistence.

This is an early `0.1` release. The current schema has no migration guarantee;
recreate disposable databases when the schema changes.

## License

Licensed under either MIT or Apache-2.0, at your option.
