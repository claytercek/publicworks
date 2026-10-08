use futures_lite::future::block_on;
use publicworks_runtime::{
    Storage, forward_storage_methods,
    test_support::{Gate, TempDatabase},
};
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
            let db = TempDatabase::new("publicworks-cleanup-", "cleanup.db");
            let mut store = SqliteStorage::open(db.path()).unwrap();
            contracts::stale_terminal(&mut store, aborted).await;
            store.close().await.unwrap();
            drop(store);
            contracts::reopened(SqliteStorage::open(db.path()).unwrap(), harness, aborted).await;
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
