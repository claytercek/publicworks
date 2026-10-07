use super::*;
use rusqlite::{params_from_iter, types::Value};
use std::collections::BTreeSet;

pub(crate) fn select<T: Record>(
    tx: &Connection,
    suffix: &str,
    values: &[Value],
) -> Result<Vec<T>, StorageError> {
    let mut statement = tx.prepare_cached(&T::KIND.select(suffix)).map_err(other)?;
    let mut rows = statement.query(params_from_iter(values)).map_err(other)?;
    let mut records = Vec::new();
    while let Some(row) = rows.next().map_err(other)? {
        let (write, seq) = T::KIND.decode(row)?;
        records.push(T::from_stored(write, seq));
    }
    Ok(records)
}
pub(crate) fn exact<T: Record>(tx: &Connection, id: Id) -> Result<Option<T>, StorageError> {
    Ok(select(tx, "WHERE t.id=?1", &[integer(id.get())])?.pop())
}
pub(crate) fn integer(n: u64) -> Value {
    Value::Integer(n as i64)
}
pub(crate) struct Filter {
    sql: String,
    values: Vec<Value>,
}
impl Filter {
    pub fn new(cursor: Option<Cursor>) -> Self {
        Self {
            sql: "WHERE t.id>?".into(),
            values: vec![integer(cursor.map_or(0, |c| c.after().get()))],
        }
    }
    pub fn eq(&mut self, column: &str, value: Option<Value>) {
        if let Some(value) = value {
            self.sql.push_str(&format!(" AND t.{column}=?"));
            self.values.push(value);
        }
    }
    pub fn id(&mut self, column: &str, value: Option<Id>) {
        self.eq(column, value.map(|id| integer(id.get())));
    }
    pub fn page<T: Record>(
        mut self,
        tx: &Connection,
        limit: usize,
    ) -> Result<Page<T>, StorageError> {
        if limit == 0 {
            return Err(other("Scan limit must be positive"));
        }
        self.sql.push_str(" ORDER BY t.id LIMIT ?");
        self.values.push(integer(
            limit.saturating_add(1).min(i64::MAX as usize) as u64
        ));
        let mut items: Vec<T> = select(tx, &self.sql, &self.values)?;
        let next = if items.len() > limit {
            items.pop();
            items.last().map(|r| Cursor::from_after(r.id()))
        } else {
            None
        };
        Ok(Page { items, next })
    }
}

// Do not sort or deduplicate segments. Raw Storage permits dangling links,
// cycles, and child IDs preceding fork cutoffs. Match the shared adapter's
// child-first traversal and ID-only continuation even for those raw graphs.
fn segments(tx: &Connection, query: &EntryQuery) -> Result<Vec<(Id, u64, u64)>, StorageError> {
    let mut current = query.conversation_id;
    let mut upper = query.max_entry_id.map_or(MAX_NUMBER, Id::get);
    let lower = query.min_entry_id.map_or(1, Id::get);
    let mut visited = BTreeSet::new();
    let mut segments = Vec::new();
    loop {
        if !visited.insert(current) {
            return Err(other("Cyclic conversation ancestry"));
        }
        let conversation: ConversationRecord =
            exact(tx, current)?.ok_or_else(|| other(format!("Unknown conversation: {current}")))?;
        if lower > upper {
            break;
        }
        segments.push((current, lower, upper));
        match conversation.parent {
            Some(parent) => {
                upper = upper.min(parent.at.get());
                if upper < lower {
                    break;
                }
                current = parent.conversation_id;
            }
            None => break,
        }
    }
    Ok(segments)
}
fn segment_entries(
    tx: &Connection,
    segment: (Id, u64, u64),
    limit: usize,
    head: bool,
) -> Result<Vec<StoredEntry>, StorageError> {
    let (conversation, lower, upper) = segment;
    let predicate = if head { " AND t.head IS NOT NULL" } else { "" };
    select(
        tx,
        &format!(
            "WHERE t.conversation_id=?1 AND t.id BETWEEN ?2 AND ?3{predicate} ORDER BY t.id DESC LIMIT ?4"
        ),
        &[
            integer(conversation.get()),
            integer(lower),
            integer(upper),
            integer(limit.min(i64::MAX as usize) as u64),
        ],
    )
}
fn before(query: &mut EntryQuery, after: Id) -> Result<(), StorageError> {
    query.max_entry_id = Some(Id::new(
        query
            .max_entry_id
            .map_or(MAX_NUMBER, Id::get)
            .min(after.get() - 1),
    )?);
    Ok(())
}
pub(crate) fn entries(
    tx: &Connection,
    mut query: EntryQuery,
    limit: usize,
    cursor: Option<Cursor>,
) -> Result<Page<EntryRecord>, StorageError> {
    let exhausted = cursor.as_ref().is_some_and(|c| c.after().get() == 1);
    if let Some(cursor) = cursor.filter(|_| !exhausted) {
        before(&mut query, cursor.after())?;
    }
    let visible = segments(tx, &query)?;
    if limit == 0 {
        return Err(other("Scan limit must be positive"));
    }
    let mut items = Vec::new();
    if exhausted {
        return Ok(Page { items, next: None });
    }
    for segment in visible {
        items.extend(
            segment_entries(tx, segment, limit - items.len(), false)?
                .into_iter()
                .map(|r| r.entry),
        );
        if items.len() == limit {
            break;
        }
    }
    let mut next = None;
    if items.len() == limit {
        let after = items.last().expect("positive full page").id;
        if after.get() > 1 {
            before(&mut query, after)?;
            for segment in segments(tx, &query)? {
                if !segment_entries(tx, segment, 1, false)?.is_empty() {
                    next = Some(Cursor::from_after(after));
                    break;
                }
            }
        }
    }
    Ok(Page { items, next })
}
pub(crate) fn visible_entry(
    tx: &Connection,
    conversation_id: Id,
    id: Id,
) -> Result<Option<StoredEntry>, StorageError> {
    let visible = segments(
        tx,
        &EntryQuery {
            conversation_id,
            min_entry_id: Some(id),
            max_entry_id: Some(id),
        },
    )?;
    let record: Option<StoredEntry> = exact(tx, id)?;
    Ok(record.filter(|r| {
        visible.iter().any(|(conversation, lower, upper)| {
            *conversation == r.entry.conversation_id && (*lower..=*upper).contains(&id.get())
        })
    }))
}
pub(crate) fn head(
    tx: &Connection,
    conversation_id: Id,
    at: Option<Id>,
) -> Result<Option<EntryRecord>, StorageError> {
    for segment in segments(
        tx,
        &EntryQuery {
            conversation_id,
            min_entry_id: None,
            max_entry_id: at,
        },
    )? {
        if let Some(record) = segment_entries(tx, segment, 1, true)?.pop() {
            return Ok(Some(record.entry));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(connection: &Connection, kind: Kind, suffix: &str, values: &[Value]) -> String {
        connection
            .prepare(&format!("EXPLAIN QUERY PLAN {}", kind.select(suffix)))
            .unwrap()
            .query_map(params_from_iter(values), |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
            .join("\n")
    }

    #[test]
    fn hot_queries_use_bounded_index_searches_without_temporary_sorts() {
        let mut store = SqliteStorage::open(":memory:").unwrap();
        let connection = store.connection().unwrap();
        for kind in Kind::ALL {
            let plan = plan(connection, kind, "WHERE t.id=?1", &[integer(10)]);
            assert!(
                plan.contains("SEARCH t USING INTEGER PRIMARY KEY"),
                "{plan}"
            );
        }
        for (kind, suffix, values, index) in [
            (
                Kind::Entry,
                "WHERE t.conversation_id=?1 AND t.id BETWEEN ?2 AND ?3 ORDER BY t.id DESC LIMIT ?4",
                vec![integer(1), integer(1), integer(100), integer(10)],
                "entries_conversation",
            ),
            (
                Kind::Entry,
                "WHERE t.conversation_id=?1 AND t.id BETWEEN ?2 AND ?3 AND t.head IS NOT NULL ORDER BY t.id DESC LIMIT ?4",
                vec![integer(1), integer(1), integer(100), integer(1)],
                "entries_head",
            ),
            (
                Kind::Submission,
                "WHERE t.conversation_id=?1 AND t.request_id=?2 ORDER BY t.id LIMIT 1",
                vec![integer(1), "request".to_owned().into()],
                "submissions_request",
            ),
            (
                Kind::ConversationState,
                "WHERE t.conversation_id=?1 ORDER BY t.id LIMIT 1",
                vec![integer(1)],
                "states_conversation",
            ),
        ] {
            let plan = plan(connection, kind, suffix, &values);
            assert!(plan.contains(index), "{plan}");
            assert!(!plan.contains("SCAN t"), "{plan}");
            assert!(!plan.contains("TEMP B-TREE"), "{plan}");
        }
        for (kind, column, value, index) in [
            (
                Kind::Task,
                "conversation_id",
                integer(1),
                "tasks_conversation",
            ),
            (Kind::Task, "owner", integer(1), "tasks_owner"),
            (Kind::Task, "kind", "host".to_owned().into(), "tasks_kind"),
            (
                Kind::Task,
                "status",
                "pending".to_owned().into(),
                "tasks_status",
            ),
            (Kind::Task, "background", integer(1), "tasks_background"),
            (Kind::Task, "abort_requested", integer(1), "tasks_abort"),
            (
                Kind::Submission,
                "conversation_id",
                integer(1),
                "submissions_conversation",
            ),
            (
                Kind::Submission,
                "status",
                "queued".to_owned().into(),
                "submissions_status",
            ),
            (
                Kind::Conversation,
                "owner_conversation_id",
                integer(1),
                "conversations_owner_conversation",
            ),
            (
                Kind::Conversation,
                "owner_task_id",
                integer(1),
                "conversations_owner_task",
            ),
        ] {
            let mut filter = Filter::new(Some(Cursor::from_after(Id::new(5).unwrap())));
            filter.eq(column, Some(value));
            filter.sql.push_str(" ORDER BY t.id LIMIT ?");
            filter.values.push(integer(11));
            let plan = plan(connection, kind, &filter.sql, &filter.values);
            assert!(plan.contains(index), "{plan}");
            assert!(!plan.contains("SCAN t"), "{plan}");
            assert!(!plan.contains("TEMP B-TREE"), "{plan}");
        }
    }
}
