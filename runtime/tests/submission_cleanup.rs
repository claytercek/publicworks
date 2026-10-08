use futures_lite::future::block_on;
use publicworks_runtime::*;
#[path = "../src/test_support/scaffold.rs"]
mod scaffold;
#[path = "../src/test_support/storage.rs"]
mod storage_scaffold;
use scaffold::Gate;
#[path = "support/submission_cleanup.rs"]
mod contracts;

#[test]
fn terminal_paths() {
    block_on(contracts::terminal_paths(MemoryStorage::new));
}
#[test]
fn held_outcome() {
    block_on(contracts::held_outcome(MemoryStorage::new()));
}
#[test]
fn conversation_scopes() {
    block_on(contracts::conversation_scopes(MemoryStorage::new));
}
#[test]
fn scheduler_cascade() {
    block_on(contracts::scheduler_cascade(MemoryStorage::new));
}
#[test]
fn rollback() {
    block_on(contracts::rollback(MemoryStorage::new()));
}
#[test]
fn reopen_cleanup() {
    block_on(async {
        for (harness, aborted) in [(false, false), (true, false), (false, true), (true, true)] {
            let mut store = MemoryStorage::new();
            contracts::stale_terminal(&mut store, aborted).await;
            contracts::reopened(store, harness, aborted).await;
        }
    });
}

#[test]
fn storage_failures() {
    block_on(contracts::storage_failures(MemoryStorage::new));
}
#[test]
fn queued_only_pagination() {
    block_on(contracts::queued_only_pagination(MemoryStorage::new()));
}

#[test]
fn explicit_runner() {
    block_on(contracts::explicit_runner(MemoryStorage::new));
}
