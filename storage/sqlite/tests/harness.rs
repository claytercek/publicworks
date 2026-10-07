use futures_lite::future::{block_on, zip};
use publicworks_runtime::*;
use publicworks_storage_sqlite::SqliteStorage;
use serde_json::json;
use std::{collections::BTreeMap, path::PathBuf, rc::Rc};

fn path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "publicworks-harness-{}-{}.db",
        std::process::id(),
        std::thread::current().name().unwrap_or("test")
    ))
}

fn definition() -> TaskDefinition {
    let handler: PhaseHandler = Rc::new(|_, runtime| {
        Box::pin(async move {
            runtime
                .commit(|_, _| Box::pin(async { Ok(Some(TaskUpdate::Complete(json!("done")))) }))
                .await
                .map_err(|error| TaskOutcomeError {
                    message: error.to_string(),
                    detail: None,
                })?;
            Ok(())
        })
    });
    TaskDefinition::new(
        "sqlite-work",
        1,
        |_| Ok(json!({"phase":"run"})),
        BTreeMap::from([("run".into(), handler)]),
    )
}

fn task(id: u64, conversation_id: u64, state: TaskState) -> TaskRecord {
    TaskRecord {
        id: Id::new(id).unwrap(),
        conversation_id: Id::new(conversation_id).unwrap(),
        kind: "sqlite-work".into(),
        version: 1,
        input: json!(id),
        owner: None,
        background: false,
        abort_requested: false,
        state,
        memos: None,
    }
}

#[test]
fn harness_reopens_runs_all_roots_and_does_not_redispatch_terminals() {
    block_on(async {
        let path = path();
        let _ = std::fs::remove_file(&path);
        let mut storage = SqliteStorage::open(&path).unwrap();
        let first = task(
            3,
            2,
            TaskState::Running {
                checkpoint: json!({"phase":"run"}),
            },
        );
        let second = task(
            5,
            4,
            TaskState::Pending {
                checkpoint: json!({"phase":"run"}),
            },
        );
        storage
            .commit(vec![
                StorageWrite::Conversation(ConversationRecord {
                    id: Id::new(2).unwrap(),
                    parent: None,
                    owner: None,
                }),
                StorageWrite::Task(first.clone()),
                StorageWrite::Conversation(ConversationRecord {
                    id: Id::new(4).unwrap(),
                    parent: None,
                    owner: None,
                }),
                StorageWrite::Task(second.clone()),
            ])
            .await
            .unwrap();
        storage.close().await.unwrap();

        let (opening, driver) = Harness::open(
            SqliteStorage::open(&path).unwrap(),
            TaskRegistry::new([definition()]).unwrap(),
        );
        let command = async move {
            let harness = opening.await.unwrap();
            let inspection = harness.inspect().await.unwrap();
            assert_eq!(inspection.tasks.len(), 2);
            assert!(
                inspection
                    .tasks
                    .iter()
                    .all(|task| task.task.status() == TaskStatus::Pending)
            );
            let first_waiter = harness.wait_task(first.id);
            let second_waiter = harness.wait_task(second.id);
            harness.resume().unwrap();
            let (first, second) = zip(first_waiter, second_waiter).await;
            assert_eq!(first.unwrap().status(), TaskStatus::Terminal);
            assert_eq!(second.unwrap().status(), TaskStatus::Terminal);
            harness.close().await.unwrap();
        };
        let ((), ()) = zip(command, driver).await;

        let (opening, driver) = Harness::open(
            SqliteStorage::open(&path).unwrap(),
            TaskRegistry::new([definition()]).unwrap(),
        );
        let command = async move {
            let harness = opening.await.unwrap();
            harness.resume().unwrap();
            assert!(harness.inspect().await.unwrap().tasks.is_empty());
            harness.close().await.unwrap();
        };
        let ((), ()) = zip(command, driver).await;
        let _ = std::fs::remove_file(path);
    });
}
