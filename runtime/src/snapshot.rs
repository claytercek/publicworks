//! Shared detached read implementation for adapters. Not a Session snapshot API.
use crate::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Default)]
pub struct RecordSnapshot {
    pub conversations: BTreeMap<Id, ConversationRecord>,
    pub tasks: BTreeMap<Id, TaskRecord>,
    pub submissions: BTreeMap<Id, SubmissionRecord>,
    pub conversation_states: BTreeMap<Id, ConversationStateRecord>,
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

    pub fn submission(&self, id: Id) -> Option<SubmissionRecord> {
        self.submissions.get(&id).cloned()
    }
    pub fn scan_submissions(
        &self,
        query: SubmissionQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> Result<Page<SubmissionRecord>, StorageError> {
        page(
            self.submissions
                .values()
                .filter(|record| {
                    cursor.as_ref().is_none_or(|value| record.id > value.after)
                        && query.matches(record)
                })
                .cloned(),
            limit,
            |record| record.id,
        )
    }
    pub fn submission_by_request(
        &self,
        conversation_id: Id,
        request_id: &str,
    ) -> Option<SubmissionRecord> {
        self.submissions
            .values()
            .find(|record| {
                record.conversation_id == conversation_id
                    && record.request_id.as_deref() == Some(request_id)
            })
            .cloned()
    }
    pub fn conversation_state(&self, conversation_id: Id) -> Option<ConversationStateRecord> {
        self.conversation_states
            .values()
            .find(|record| record.conversation_id == conversation_id)
            .cloned()
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

    fn visible_segments(&self, query: &EntryQuery) -> Result<Vec<(Id, u64, u64)>, StorageError> {
        let mut current = query.conversation_id;
        let mut upper = query.max_entry_id.map_or(MAX_NUMBER, Id::get);
        let lower = query.min_entry_id.map_or(1, Id::get);
        let mut visited = BTreeSet::new();
        let mut segments = Vec::new();
        loop {
            if !visited.insert(current) {
                return Err(StorageError::Other("Cyclic conversation ancestry".into()));
            }
            let conversation = self
                .conversations
                .get(&current)
                .ok_or_else(|| StorageError::Other(format!("Unknown conversation: {current}")))?;
            if lower > upper {
                break;
            }
            segments.push((current, lower, upper));
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
        Ok(segments)
    }

    pub fn scan_entries(
        &self,
        mut query: EntryQuery,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> Result<Page<EntryRecord>, StorageError> {
        let exhausted = cursor
            .as_ref()
            .is_some_and(|cursor| cursor.after.get() == 1);
        if let Some(cursor) = cursor.filter(|_| !exhausted) {
            let upper = query
                .max_entry_id
                .map_or(MAX_NUMBER, Id::get)
                .min(cursor.after.get() - 1);
            query.max_entry_id = Some(Id::new(upper)?);
        }
        let segments = self.visible_segments(&query)?;
        if limit == 0 {
            return Err(StorageError::Other("Scan limit must be positive".into()));
        }
        if exhausted {
            return Ok(Page {
                items: Vec::new(),
                next: None,
            });
        }
        let mut items = Vec::with_capacity(limit.min(segments.len().saturating_mul(8)));
        'segments: for (conversation, lower, upper) in segments {
            for (_, stored) in self
                .entries
                .range(id_bound(lower)?..=id_bound(upper)?)
                .rev()
            {
                if stored.entry.conversation_id == conversation {
                    if items.len() == limit {
                        break 'segments;
                    }
                    items.push(stored.entry.clone());
                }
            }
        }
        let next = if items.len() == limit
            && self.has_visible_after(&query, items.last().expect("nonempty full page").id)?
        {
            items.last().map(|entry| Cursor { after: entry.id })
        } else {
            None
        };
        Ok(Page { items, next })
    }

    fn has_visible_after(&self, query: &EntryQuery, after: Id) -> Result<bool, StorageError> {
        if after.get() == 1 {
            return Ok(false);
        }
        let mut remainder = query.clone();
        let upper = remainder
            .max_entry_id
            .map_or(MAX_NUMBER, Id::get)
            .min(after.get() - 1);
        remainder.max_entry_id = Some(Id::new(upper)?);
        for (conversation, lower, upper) in self.visible_segments(&remainder)? {
            if self
                .entries
                .range(id_bound(lower)?..=id_bound(upper)?)
                .rev()
                .any(|(_, stored)| stored.entry.conversation_id == conversation)
            {
                return Ok(true);
            }
        }
        Ok(false)
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
        let visible = self.visible_segments(&query)?.into_iter().any(
            |(segment_conversation, lower, upper)| {
                (lower..=upper).contains(&id.get())
                    && self
                        .entries
                        .get(&id)
                        .is_some_and(|stored| stored.entry.conversation_id == segment_conversation)
            },
        );
        Ok(visible.then(|| self.entries.get(&id).expect("checked entry").clone()))
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
        for (segment_conversation, lower, upper) in self.visible_segments(&query)? {
            if let Some(entry) = self
                .entries
                .range(id_bound(lower)?..=id_bound(upper)?)
                .rev()
                .map(|(_, stored)| &stored.entry)
                .find(|entry| entry.conversation_id == segment_conversation && entry.head.is_some())
            {
                return Ok(Some(entry.clone()));
            }
        }
        Ok(None)
    }
}

fn id_bound(value: u64) -> Result<Id, StorageError> {
    Id::new(value)
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
