//! Blocking SQLite adapter for Public Works conversations, entries, and tasks.
//!
//! Unpublished owned schema v3, not compatible with Pi's JavaScript database format.
//! Incompatible schemas are rejected, not migrated or silently reset. Recreate
//! disposable databases manually when the format changes.
use publicworks_runtime::{snapshot::RecordSnapshot, *};
use rusqlite::{Connection, Transaction, TransactionBehavior, params};
use std::{path::Path, time::Duration};

pub struct SqliteStorage {
    connection: Option<Connection>,
}
const SCHEMA: &str = "
CREATE TABLE publicworks_schema (singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL);
INSERT INTO publicworks_schema VALUES (1, 3);
CREATE TABLE publicworks_metadata (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1), next_id INTEGER NOT NULL, next_seq INTEGER NOT NULL
);
INSERT INTO publicworks_metadata VALUES (1, 2, 1);
CREATE TABLE publicworks_records (
    id INTEGER PRIMARY KEY, kind TEXT NOT NULL CHECK(kind IN ('conversation','entry','task')),
    record TEXT NOT NULL, commit_seq INTEGER NOT NULL
);";

fn other(error: impl std::fmt::Display) -> StorageError {
    StorageError::Other(error.to_string())
}

// Explicit rollback: a rollback failure must never retain a no-effect classification.
fn transaction<T>(
    connection: &mut Connection,
    behavior: TransactionBehavior,
    work: impl FnOnce(&Transaction<'_>) -> Result<T, StorageError>,
) -> Result<T, StorageError> {
    let tx = connection
        .transaction_with_behavior(behavior)
        .map_err(other)?;
    match work(&tx) {
        Ok(value) => {
            tx.commit().map_err(other)?;
            Ok(value)
        }
        Err(error) => match tx.rollback() {
            Ok(()) => Err(error),
            Err(rollback) => Err(other(format!("{error}; rollback also failed: {rollback}"))),
        },
    }
}

impl SqliteStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let mut connection = Connection::open(path).map_err(other)?;
        connection
            .busy_timeout(Duration::from_millis(5000))
            .map_err(other)?;
        // Check schema before changing persistent PRAGMAs. Never repair a broken database on reads.
        transaction(&mut connection, TransactionBehavior::Immediate, |tx| {
            let table_count: u64 = tx.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT GLOB 'sqlite_*'",
                [], |r| r.get(0)).map_err(other)?;
            if table_count == 0 {
                tx.execute_batch(SCHEMA).map_err(other)?;
            }
            let versions: Vec<(u64, u64)> = tx
                .prepare("SELECT singleton, version FROM publicworks_schema")
                .map_err(other)?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .map_err(other)?
                .collect::<rusqlite::Result<_>>()
                .map_err(other)?;
            if versions != [(1, 3)] {
                return Err(other(format!(
                    "Unsupported Public Works schema: {versions:?}"
                )));
            }
            validate_schema(tx)?;
            let (next_id, next_seq) = metadata(tx)?;
            if !(2..=MAX_NUMBER + 1).contains(&next_id) || !(1..=MAX_NUMBER + 1).contains(&next_seq)
            {
                return Err(other("Invalid allocator metadata"));
            }
            tx.prepare("SELECT id, kind, record, commit_seq FROM publicworks_records")
                .map_err(other)?;
            Ok(())
        })?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;
            PRAGMA wal_autocheckpoint=1000; PRAGMA foreign_keys=ON;",
            )
            .map_err(other)?;
        Ok(Self {
            connection: Some(connection),
        })
    }
    fn connection(&mut self) -> Result<&mut Connection, StorageError> {
        self.connection
            .as_mut()
            .ok_or_else(|| StorageError::Rejected("Storage is closed".into()))
    }
    // One deferred transaction captures records and fork links together. Deliberately
    // materialized in this first slice; indexed/bounded SQL scans are future optimization.
    fn snapshot(&mut self) -> Result<RecordSnapshot, StorageError> {
        transaction(self.connection()?, TransactionBehavior::Deferred, |tx| {
            let mut snapshot = RecordSnapshot::default();
            let mut statement = tx
                .prepare("SELECT id, kind, record, commit_seq FROM publicworks_records ORDER BY id")
                .map_err(other)?;
            let mut rows = statement.query([]).map_err(other)?;
            while let Some(row) = rows.next().map_err(other)? {
                let id = Id::new(row.get(0).map_err(other)?)?;
                if snapshot.conversations.contains_key(&id)
                    || snapshot.entries.contains_key(&id)
                    || snapshot.tasks.contains_key(&id)
                {
                    return Err(other(format!("Duplicate stored ID: {id}")));
                }
                let kind: String = row.get(1).map_err(other)?;
                let json: String = row.get(2).map_err(other)?;
                let seq = Seq::new(row.get(3).map_err(other)?)?;
                match kind.as_str() {
                    "conversation" => {
                        let record: ConversationRecord =
                            serde_json::from_str(&json).map_err(other)?;
                        if record.id != id {
                            return Err(other("Conversation record ID mismatch"));
                        }
                        snapshot.conversations.insert(id, record);
                    }
                    "entry" => {
                        let entry: EntryRecord = serde_json::from_str(&json).map_err(other)?;
                        entry.validate_payloads()?;
                        if entry.id != id {
                            return Err(other("Entry record ID mismatch"));
                        }
                        snapshot.entries.insert(
                            id,
                            StoredEntry {
                                entry,
                                commit_seq: seq,
                            },
                        );
                    }
                    "task" => {
                        let task: TaskRecord = serde_json::from_str(&json).map_err(other)?;
                        task.validate_payloads()?;
                        if task.id != id {
                            return Err(other("Task record ID mismatch"));
                        }
                        snapshot.tasks.insert(id, task);
                    }
                    _ => return Err(other(format!("Unsupported record kind: {kind}"))),
                }
            }
            Ok(snapshot)
        })
    }
}
// The unpublished owned schema has one exact DDL identity (whitespace ignored).
// Comparing definitions, not just SELECT-able columns, retains PK/NOT NULL/CHECK
// constraints and excludes unexpected triggers, views, indexes, or extra tables.
fn validate_schema(tx: &Transaction<'_>) -> Result<(), StorageError> {
    let normalize = |sql: &str| sql.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut expected: Vec<_> = SCHEMA
        .split(';')
        .map(str::trim)
        .filter(|sql| sql.starts_with("CREATE TABLE "))
        .map(normalize)
        .collect();
    let mut actual: Vec<String> = tx
        .prepare("SELECT sql FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'")
        .map_err(other)?
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(other)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(other)?
        .iter()
        .map(|sql| normalize(sql))
        .collect();
    expected.sort();
    actual.sort();
    if actual != expected {
        return Err(other("Public Works schema definition mismatch"));
    }
    Ok(())
}

fn metadata(tx: &Transaction<'_>) -> Result<(u64, u64), StorageError> {
    tx.query_row(
        "SELECT next_id, next_seq FROM publicworks_metadata WHERE singleton=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .map_err(other)
}

impl Storage for SqliteStorage {
    fn commit(&mut self, writes: Vec<StorageWrite>) -> StorageFuture<'_, Seq> {
        Box::pin(async move {
            self.connection()?;
            for write in &writes {
                match write {
                    StorageWrite::Entry(entry) => entry.validate_payloads()?,
                    StorageWrite::Task(task) => task.validate_payloads()?,
                    StorageWrite::Conversation(_) => {}
                }
            }
            transaction(self.connection()?, TransactionBehavior::Immediate, |tx| {
                let (mut next_id, next_seq) = metadata(tx)?;
                let seq = Seq::new(next_seq)?;
                for write in writes {
                    let id = write.id();
                    let (kind, json) = match write {
                        StorageWrite::Conversation(record) => {
                            ("conversation", serde_json::to_string(&record))
                        }
                        StorageWrite::Entry(record) => ("entry", serde_json::to_string(&record)),
                        StorageWrite::Task(record) => ("task", serde_json::to_string(&record)),
                    };
                    let sql = if kind == "task" {
                        "INSERT INTO publicworks_records(id,kind,record,commit_seq) VALUES (?1,?2,?3,?4)
                         ON CONFLICT(id) DO UPDATE SET record=excluded.record, commit_seq=excluded.commit_seq
                         WHERE publicworks_records.kind='task'"
                    } else {
                        "INSERT INTO publicworks_records(id,kind,record,commit_seq) VALUES (?1,?2,?3,?4)"
                    };
                    let changed = tx
                        .execute(
                            sql,
                            params![id.get(), kind, json.map_err(other)?, seq.get()],
                        )
                        .map_err(other)?;
                    if changed != 1 {
                        return Err(other(format!("ID {id} belongs to another record kind")));
                    }
                    next_id = next_id.max(id.get() + 1);
                }
                tx.execute(
                    "UPDATE publicworks_metadata SET next_id=?1, next_seq=?2 WHERE singleton=1",
                    params![next_id, next_seq + 1],
                )
                .map_err(other)?;
                Ok(seq)
            })
        })
    }
    fn mint_id(&mut self) -> StorageFuture<'_, Id> {
        Box::pin(async move {
            transaction(self.connection()?, TransactionBehavior::Immediate, |tx| {
                let (next_id, _) = metadata(tx)?;
                let id = Id::new(next_id)?;
                tx.execute(
                    "UPDATE publicworks_metadata SET next_id=?1 WHERE singleton=1",
                    [next_id + 1],
                )
                .map_err(other)?;
                Ok(id)
            })
        })
    }
    fn conversation(&mut self, id: Id) -> StorageFuture<'_, Option<ConversationRecord>> {
        Box::pin(async move { Ok(self.snapshot()?.conversations.remove(&id)) })
    }
    fn entry(&mut self, id: Id) -> StorageFuture<'_, Option<StoredEntry>> {
        Box::pin(async move { Ok(self.snapshot()?.entries.remove(&id)) })
    }
    fn task(&mut self, id: Id) -> StorageFuture<'_, Option<TaskRecord>> {
        Box::pin(async move { Ok(self.snapshot()?.tasks.remove(&id)) })
    }
    fn scan_tasks(
        &mut self,
        query: TaskQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<TaskRecord>> {
        Box::pin(async move { self.snapshot()?.scan_tasks(query, limit, cursor) })
    }
    fn scan_conversations(
        &mut self,
        query: ConversationQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<ConversationRecord>> {
        Box::pin(async move { self.snapshot()?.scan_conversations(query, limit, cursor) })
    }
    fn visible_entry(
        &mut self,
        conversation: Id,
        id: Id,
    ) -> StorageFuture<'_, Option<StoredEntry>> {
        Box::pin(async move { self.snapshot()?.visible_entry(conversation, id) })
    }
    fn scan_entries(
        &mut self,
        query: EntryQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<EntryRecord>> {
        Box::pin(async move { self.snapshot()?.scan_entries(query, limit, cursor) })
    }
    fn find_latest_head_marker(
        &mut self,
        conversation: Id,
        at: Option<Id>,
    ) -> StorageFuture<'_, Option<EntryRecord>> {
        Box::pin(async move { self.snapshot()?.find_latest_head_marker(conversation, at) })
    }
    fn close(&mut self) -> StorageFuture<'_, ()> {
        Box::pin(async move {
            let connection = self
                .connection
                .take()
                .ok_or_else(|| StorageError::Rejected("Storage is closed".into()))?;
            connection.close().map_err(|(_, error)| other(error))
        })
    }
}
