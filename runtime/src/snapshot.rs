//! Shared detached read implementation for adapters. Not a Session snapshot API.
use crate::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Default)]
pub struct RecordSnapshot {
    pub conversations: BTreeMap<Id, ConversationRecord>,
    pub tasks: BTreeMap<Id, TaskRecord>,
    pub entries: BTreeMap<Id, StoredEntry>,
}
impl RecordSnapshot {
    pub fn task(&self, id: Id) -> Option<TaskRecord> {
        self.tasks.get(&id).cloned()
    }
    pub fn scan_tasks(
        &self,
        query: TaskQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> Result<Page<TaskRecord>, StorageError> {
        page(
            self.tasks
                .values()
                .filter(|r| cursor.as_ref().is_none_or(|c| r.id > c.after) && query.matches(r))
                .cloned(),
            limit,
            |r| r.id,
        )
    }

    pub fn scan_conversations(
        &self,
        query: ConversationQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> Result<Page<ConversationRecord>, StorageError> {
        page(
            self.conversations
                .values()
                .filter(|r| {
                    cursor.as_ref().is_none_or(|c| r.id > c.after)
                        && query.owner_conversation_id.is_none_or(|id| {
                            r.owner.as_ref().is_some_and(|o| o.conversation_id == id)
                        })
                        && query
                            .owner_task_id
                            .is_none_or(|id| r.owner.as_ref().is_some_and(|o| o.task_id == id))
                })
                .cloned(),
            limit,
            |r| r.id,
        )
    }

    fn visible(&self, query: EntryQuery) -> Result<Vec<EntryRecord>, StorageError> {
        let mut current = query.conversation_id;
        let mut upper = query.max_entry_id.map_or(MAX_NUMBER, Id::get);
        let lower = query.min_entry_id.map_or(1, Id::get);
        let mut visited = BTreeSet::new();
        let mut records = Vec::new();
        loop {
            if !visited.insert(current) {
                return Err(StorageError::Other("Cyclic conversation ancestry".into()));
            }
            let conversation = self
                .conversations
                .get(&current)
                .ok_or_else(|| StorageError::Other(format!("Unknown conversation: {current}")))?;
            records.extend(
                self.entries
                    .values()
                    .rev()
                    .filter(|r| {
                        r.entry.conversation_id == current
                            && r.entry.id.get() >= lower
                            && r.entry.id.get() <= upper
                    })
                    .map(|r| r.entry.clone()),
            );
            match &conversation.parent {
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
        Ok(records)
    }

    pub fn scan_entries(
        &self,
        mut query: EntryQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> Result<Page<EntryRecord>, StorageError> {
        // A cursor at ID 1 has no predecessor in the supported ID domain.
        if let Some(cursor) = cursor {
            if cursor.after.get() == 1 {
                self.visible(query)?; // Preserve unknown-conversation errors.
                return page(std::iter::empty(), limit, |r: &EntryRecord| r.id);
            }
            let upper = query
                .max_entry_id
                .map_or(MAX_NUMBER, Id::get)
                .min(cursor.after.get() - 1);
            query.max_entry_id = Some(Id::new(upper)?);
        }
        page(self.visible(query)?.into_iter(), limit, |r| r.id)
    }

    pub fn visible_entry(
        &self,
        conversation: Id,
        id: Id,
    ) -> Result<Option<StoredEntry>, StorageError> {
        let query = EntryQuery {
            conversation_id: conversation,
            min_entry_id: Some(id),
            max_entry_id: Some(id),
        };
        Ok(self
            .visible(query)?
            .first()
            .and_then(|r| self.entries.get(&r.id))
            .cloned())
    }

    pub fn find_latest_head_marker(
        &self,
        conversation: Id,
        at: Option<Id>,
    ) -> Result<Option<EntryRecord>, StorageError> {
        let query = EntryQuery {
            conversation_id: conversation,
            min_entry_id: None,
            max_entry_id: at,
        };
        Ok(self.visible(query)?.into_iter().find(|r| r.head.is_some()))
    }
}

fn page<T>(
    values: impl Iterator<Item = T>,
    limit: usize,
    id: impl Fn(&T) -> Id,
) -> Result<Page<T>, StorageError> {
    if limit == 0 {
        return Err(StorageError::Other("Scan limit must be positive".into()));
    }
    let mut iter = values;
    let items: Vec<_> = iter.by_ref().take(limit).collect();
    let next = if iter.next().is_some() {
        items.last().map(|r| Cursor { after: id(r) })
    } else {
        None
    };
    Ok(Page { items, next })
}
