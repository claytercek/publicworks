use futures_lite::future::block_on;
use publicworks_runtime::Storage;
use publicworks_storage_sqlite::SqliteStorage;
#[path = "../../../runtime/tests/support/submission_cleanup.rs"]
mod contracts;
fn storage() -> SqliteStorage {
    SqliteStorage::open(":memory:").unwrap()
}

#[test]
fn terminal_paths() {
    block_on(contracts::terminal_paths(storage));
}
#[test]
fn held_outcome() {
    block_on(contracts::held_outcome(storage()));
}
#[test]
fn conversation_scopes() {
    block_on(contracts::conversation_scopes(storage));
}
#[test]
fn scheduler_cascade() {
    block_on(contracts::scheduler_cascade(storage));
}
#[test]
fn rollback() {
    block_on(contracts::rollback(storage()));
}
#[test]
fn reopen_cleanup() {
    block_on(async {
        for (harness, aborted) in [(false, false), (true, false), (false, true), (true, true)] {
            let path = std::env::temp_dir().join(format!(
                "publicworks-cleanup-{}-{}.db",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let mut store = SqliteStorage::open(&path).unwrap();
            contracts::stale_terminal(&mut store, aborted).await;
            store.close().await.unwrap();
            drop(store);
            contracts::reopened(SqliteStorage::open(&path).unwrap(), harness, aborted).await;
            std::fs::remove_file(path).unwrap();
        }
    });
}

#[test]
fn storage_failures() {
    block_on(contracts::storage_failures(storage));
}
#[test]
fn queued_only_pagination() {
    block_on(contracts::queued_only_pagination(storage()));
}

#[test]
fn explicit_runner() {
    block_on(contracts::explicit_runner(storage));
}
