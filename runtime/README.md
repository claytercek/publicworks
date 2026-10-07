# publicworks-runtime

An executor-neutral runtime for durable conversations, submissions, and task
trees.

The crate provides three layers:

- [`Session`](https://docs.rs/publicworks-runtime/latest/publicworks_runtime/struct.Session.html)
  serializes storage transactions.
- [`Harness`](https://docs.rs/publicworks-runtime/latest/publicworks_runtime/struct.Harness.html)
  recovers and schedules all eligible task trees after an explicit `resume`.
- [`TaskRunner`](https://docs.rs/publicworks-runtime/latest/publicworks_runtime/struct.TaskRunner.html)
  drives one task tree when the host wants lower-level control.

## Session example

The host must poll the returned driver alongside command and shutdown futures.
Public Works does not spawn an executor.

```rust
use futures_lite::future::{block_on, zip};
use publicworks_runtime::{EntryDraft, MemoryStorage, Session};

block_on(async {
    let (session, driver) = Session::new(MemoryStorage::new());
    let commands = async {
        let receipt = session
            .commit(|tx| Box::pin(async move {
                let conversation = tx.create_conversation().await?;
                tx.append_entry(conversation.id, EntryDraft::new("example")).await
            }))
            .await?;

        session.close().await?;
        Ok::<_, publicworks_runtime::SessionError>(receipt.value)
    };

    let (entry, ()) = zip(commands, driver).await;
    assert_eq!(entry?.kind, "example");
    Ok::<_, publicworks_runtime::SessionError>(())
})?;
# Ok::<(), publicworks_runtime::SessionError>(())
```

Calling `commit` admits the transaction immediately. Dropping its waiter does
not cancel admitted work. Keep the driver alive until `close` finishes; dropping
the driver forfeits in-flight settlement.

## Runtime model

Task definitions are immutable, versioned collections of checkpointed phase
handlers. A handler commits progress through `TaskRuntime`; external effects are
outside the storage transaction. Durable cancellation is cooperative and does
not undo an effect that already ran.

`Harness::open` normalizes interrupted work without invoking handlers. Scheduling
remains paused until `resume` or a progress-enabling wait. Missing or mismatched
definitions leave normal work pending rather than running unknown code.

The `Storage` trait uses one exclusive logical owner and atomic write batches.
`MemoryStorage` is a reference adapter, not persistence. Adapter operations need
not be `Send`; the built-in adapters complete synchronously once polled.

## Compatibility and limits

This is an early `0.1` release. Persisted records, checkpoints, definition
versions, and adapter schemas have no migration guarantee yet. JSON payloads use
serde_json's native finite number domain and permit at most 64 nested containers.
The runtime does not provide exactly-once external effects, forced cancellation,
authorization, sandboxing, timers, or a multi-process ownership lock.

See the crate's `examples/` directory for task execution, trees, cancellation,
and durable memos.

## License

Licensed under either MIT or Apache-2.0, at your option.
