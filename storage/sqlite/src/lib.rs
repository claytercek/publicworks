//! Blocking SQLite adapter for Public Works runtime records.
//!
//! Unpublished typed schema v5. Incompatible schemas (including v4) are rejected,
//! never migrated or reset. Open performs a streaming full integrity audit in a
//! transaction; subsequent reads validate selected rows only. Concurrent external
//! mutation is unsupported and is not guaranteed to be detected until reopening.
use publicworks_runtime::*;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use std::{path::Path, time::Duration};
mod query;
mod record;
mod schema;
use query::{Filter, exact, integer, select};
use record::{Kind, Record};
use schema::{SCHEMA, metadata};

pub struct SqliteStorage {
    connection: Option<Connection>,
    lease_next: u64,
    lease_end: u64,
}
const ID_LEASE_SIZE: u64 = 64;
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
        connection
            .execute_batch("PRAGMA foreign_keys=ON")
            .map_err(other)?;
        // Validate before changing persistent PRAGMAs. Never repair corruption.
        transaction(&mut connection, TransactionBehavior::Immediate, |tx| {
            let object_count: u64 = tx
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'",
                    [],
                    |r| r.get(0),
                )
                .map_err(other)?;
            if object_count == 0 {
                tx.execute_batch(SCHEMA).map_err(other)?;
            }
            let versions: Vec<(u64, u64)> = tx
                .prepare("SELECT singleton,version FROM publicworks_schema")
                .map_err(other)?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .map_err(other)?
                .collect::<rusqlite::Result<_>>()
                .map_err(other)?;
            if versions != [(1, 5)] {
                return Err(other(format!(
                    "Unsupported Public Works schema: {versions:?}"
                )));
            }
            schema::validate_schema(tx)?;
            schema::audit(tx)
        })?;
        connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA wal_autocheckpoint=1000;").map_err(other)?;
        Ok(Self {
            connection: Some(connection),
            lease_next: 0,
            lease_end: 0,
        })
    }
    fn connection(&mut self) -> Result<&mut Connection, StorageError> {
        self.connection
            .as_mut()
            .ok_or_else(|| StorageError::Rejected("Storage is closed".into()))
    }
}
impl Storage for SqliteStorage {
    fn commit(&mut self, writes: Vec<StorageWrite>) -> StorageFuture<'_, Seq> {
        Box::pin(async move {
            self.connection()?;
            for write in &writes {
                record::validate(write)?;
            }
            let highest = writes.iter().map(|write| write.id().get()).max();
            let committed = transaction(self.connection()?, TransactionBehavior::Immediate, |tx| {
                let (mut next_id, next_seq) = metadata(tx)?;
                let seq = Seq::new(next_seq)?;
                for write in &writes {
                    record::write(tx, write, seq)?;
                    next_id = next_id.max(write.id().get() + 1);
                }
                record::execute(
                    tx,
                    "UPDATE publicworks_metadata SET next_id=?1,next_seq=?2 WHERE singleton=1",
                    params![next_id, next_seq + 1],
                )
                .map_err(other)?;
                Ok(seq)
            });
            match committed {
                Ok(seq) => {
                    if let Some(highest) = highest
                        && highest >= self.lease_next
                    {
                        if highest < self.lease_end {
                            self.lease_next = highest + 1;
                        } else {
                            self.lease_next = 0;
                            self.lease_end = 0;
                        }
                    }
                    Ok(seq)
                }
                Err(error) => {
                    // An Other result does not guarantee rollback. Abandon the
                    // local range so a later mint starts beyond its durable end.
                    self.lease_next = 0;
                    self.lease_end = 0;
                    Err(error)
                }
            }
        })
    }
    fn mint_id(&mut self) -> StorageFuture<'_, Id> {
        Box::pin(async move {
            self.connection()?;
            if self.lease_next < self.lease_end {
                let id = Id::new(self.lease_next)?;
                self.lease_next += 1;
                return Ok(id);
            }
            let reserved = transaction(self.connection()?, TransactionBehavior::Immediate, |tx| {
                let (next_id, _) = metadata(tx)?;
                let id = Id::new(next_id)?;
                let end = next_id.saturating_add(ID_LEASE_SIZE).min(MAX_NUMBER + 1);
                record::execute(
                    tx,
                    "UPDATE publicworks_metadata SET next_id=?1 WHERE singleton=1",
                    [end],
                )
                .map_err(other)?;
                Ok((id, end))
            });
            match reserved {
                Ok((id, end)) => {
                    self.lease_next = id.get() + 1;
                    self.lease_end = end;
                    Ok(id)
                }
                Err(error) => {
                    self.lease_next = 0;
                    self.lease_end = 0;
                    Err(error)
                }
            }
        })
    }
    fn conversation(&mut self, id: Id) -> StorageFuture<'_, Option<ConversationRecord>> {
        Box::pin(async move { exact(self.connection()?, id) })
    }
    fn entry(&mut self, id: Id) -> StorageFuture<'_, Option<StoredEntry>> {
        Box::pin(async move { exact(self.connection()?, id) })
    }
    fn task(&mut self, id: Id) -> StorageFuture<'_, Option<TaskRecord>> {
        Box::pin(async move { exact(self.connection()?, id) })
    }
    fn submission(&mut self, id: Id) -> StorageFuture<'_, Option<SubmissionRecord>> {
        Box::pin(async move { exact(self.connection()?, id) })
    }
    fn scan_conversations(
        &mut self,
        query: ConversationQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<ConversationRecord>> {
        Box::pin(async move {
            let mut filter = Filter::new(cursor);
            filter.id("owner_conversation_id", query.owner_conversation_id);
            filter.id("owner_task_id", query.owner_task_id);
            filter.page(self.connection()?, limit)
        })
    }
    fn scan_tasks(
        &mut self,
        query: TaskQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<TaskRecord>> {
        Box::pin(async move {
            let mut filter = Filter::new(cursor);
            filter.id("conversation_id", query.conversation_id);
            filter.eq("kind", query.kind.map(Into::into));
            filter.eq(
                "status",
                query
                    .status
                    .map(|s| record::task_status(s).to_owned().into()),
            );
            filter.eq(
                "background",
                query.background.map(|b| integer(u64::from(b))),
            );
            filter.eq(
                "abort_requested",
                query.abort_requested.map(|b| integer(u64::from(b))),
            );
            filter.page(self.connection()?, limit)
        })
    }
    fn scan_submissions(
        &mut self,
        query: SubmissionQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<SubmissionRecord>> {
        Box::pin(async move {
            let mut filter = Filter::new(cursor);
            filter.id("conversation_id", query.conversation_id);
            filter.eq(
                "status",
                query
                    .status
                    .map(|s| record::submission_status(s).to_owned().into()),
            );
            filter.page(self.connection()?, limit)
        })
    }
    fn submission_by_request(
        &mut self,
        conversation_id: Id,
        request_id: &str,
    ) -> StorageFuture<'_, Option<SubmissionRecord>> {
        let request_id = request_id.to_owned();
        Box::pin(async move {
            Ok(select(
                self.connection()?,
                "WHERE t.conversation_id=?1 AND t.request_id=?2 ORDER BY t.id LIMIT 1",
                &[integer(conversation_id.get()), request_id.into()],
            )?
            .pop())
        })
    }
    fn conversation_state(
        &mut self,
        conversation_id: Id,
    ) -> StorageFuture<'_, Option<ConversationStateRecord>> {
        Box::pin(async move {
            Ok(select(
                self.connection()?,
                "WHERE t.conversation_id=?1 ORDER BY t.id LIMIT 1",
                &[integer(conversation_id.get())],
            )?
            .pop())
        })
    }
    fn visible_entry(
        &mut self,
        conversation: Id,
        id: Id,
    ) -> StorageFuture<'_, Option<StoredEntry>> {
        Box::pin(async move {
            transaction(self.connection()?, TransactionBehavior::Deferred, |tx| {
                query::visible_entry(tx, conversation, id)
            })
        })
    }
    fn scan_entries(
        &mut self,
        query: EntryQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> StorageFuture<'_, Page<EntryRecord>> {
        Box::pin(async move {
            transaction(self.connection()?, TransactionBehavior::Deferred, |tx| {
                query::entries(tx, query, limit, cursor)
            })
        })
    }
    fn find_latest_head_marker(
        &mut self,
        conversation: Id,
        at: Option<Id>,
    ) -> StorageFuture<'_, Option<EntryRecord>> {
        Box::pin(async move {
            transaction(self.connection()?, TransactionBehavior::Deferred, |tx| {
                query::head(tx, conversation, at)
            })
        })
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
