//! Shared detached read implementation for adapters. Not a Session snapshot API.
use crate::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound::{Excluded, Unbounded},
};

/// Detached records indexed by their own IDs.
///
/// Every map key must equal its record's ID (the entry ID for `entries`).
/// Ordered scans use these keys as exclusive continuation boundaries.
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
                .range((cursor.map_or(Unbounded, |c| Excluded(c.after)), Unbounded))
                .filter(|(_, r)| query.matches(r))
                .map(|(_, r)| r.clone()),
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
                .range((cursor.map_or(Unbounded, |c| Excluded(c.after)), Unbounded))
                .filter(|(_, record)| query.matches(record))
                .map(|(_, record)| record.clone()),
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
                .range((cursor.map_or(Unbounded, |c| Excluded(c.after)), Unbounded))
                .filter(|(_, r)| {
                    query
                        .owner_conversation_id
                        .is_none_or(|id| r.owner.as_ref().is_some_and(|o| o.conversation_id == id))
                        && query
                            .owner_task_id
                            .is_none_or(|id| r.owner.as_ref().is_some_and(|o| o.task_id == id))
                })
                .map(|(_, r)| r.clone()),
            limit,
            |r| r.id,
        )
    }

    fn visible_segments(&self, query: &EntryQuery) -> Result<Vec<(Id, u64, u64)>, StorageError> {
        visible_segments(query, |id| Ok(self.conversations.get(&id).cloned()))
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

/// Compute child-first visible ancestry segments using an adapter's lookup.
///
/// Raw storage permits dangling links, cycles, and child IDs preceding fork
/// cutoffs. Do not sort or deduplicate segments: entry continuation is ID-only.
/// Lookups and cycle checks precede the empty-range check; a fork cutoff below
/// the lower bound stops traversal without looking up that ancestor.
/// The adapter must keep the lookups within its consistent read operation.
pub fn visible_segments(
    query: &EntryQuery,
    mut conversation: impl FnMut(Id) -> Result<Option<ConversationRecord>, StorageError>,
) -> Result<Vec<(Id, u64, u64)>, StorageError> {
    let mut current = query.conversation_id;
    let mut upper = query.max_entry_id.map_or(MAX_NUMBER, Id::get);
    let lower = query.min_entry_id.map_or(1, Id::get);
    let mut visited = BTreeSet::new();
    let mut segments = Vec::new();
    loop {
        if !visited.insert(current) {
            return Err(StorageError::Other("Cyclic conversation ancestry".into()));
        }
        let record = conversation(current)?
            .ok_or_else(|| StorageError::Other(format!("Unknown conversation: {current}")))?;
        if lower > upper {
            break;
        }
        segments.push((current, lower, upper));
        match record.parent {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{conversation, id, submission, task};

    fn check_scan<T>(
        mut scan: impl FnMut(usize, Option<Cursor>) -> Result<Page<T>, StorageError>,
        record_id: impl Fn(&T) -> Id,
        matching: &[u64],
    ) {
        for after in [None, Some(1), Some(2), Some(4), Some(MAX_NUMBER)] {
            let cursor = after.map(|n| Cursor::from_after(id(n)));
            for limit in [0, 1, 2, 3, usize::MAX] {
                let result = scan(limit, cursor.clone());
                if limit == 0 {
                    assert!(matches!(result, Err(StorageError::Other(_))));
                    continue;
                }
                let expected: Vec<_> = matching
                    .iter()
                    .copied()
                    .filter(|n| after.is_none_or(|after| *n > after))
                    .map(id)
                    .collect();
                let page = result.unwrap();
                assert_eq!(
                    page.items.iter().map(&record_id).collect::<Vec<_>>(),
                    expected.iter().copied().take(limit).collect::<Vec<_>>()
                );
                assert_eq!(
                    page.next,
                    (expected.len() > limit).then(|| Cursor::from_after(expected[limit - 1]))
                );
            }
        }
    }

    #[test]
    fn ascending_scans_preserve_exclusive_bounds_filters_and_lookahead() {
        let mut snapshot = RecordSnapshot::default();
        for n in [1, 2, 3, 5, MAX_NUMBER] {
            let mut conversation = conversation(n);
            conversation.owner = (n != 2).then(|| OwnerLink {
                conversation_id: id(7),
                task_id: id(if n == 5 { 9 } else { 8 }),
            });
            snapshot.conversations.insert(id(n), conversation);
            let mut task = task(n);
            task.background = n != 2;
            task.conversation_id = id(if n == 5 { 9 } else { 7 });
            snapshot.tasks.insert(id(n), task);
            snapshot.submissions.insert(
                id(n),
                submission(
                    n,
                    if n == 5 { 9 } else { 7 },
                    if n == 2 {
                        SubmissionState::WriteDone { entry: id(8) }
                    } else {
                        SubmissionState::WriteQueued
                    },
                ),
            );
        }
        for snapshot in [RecordSnapshot::default(), snapshot] {
            for filtered in [false, true] {
                let expected: &[u64] = if snapshot.tasks.is_empty() {
                    &[]
                } else if filtered {
                    &[1, 3, MAX_NUMBER]
                } else {
                    &[1, 2, 3, 5, MAX_NUMBER]
                };
                check_scan(
                    |limit, cursor| {
                        snapshot.scan_tasks(
                            TaskQuery {
                                conversation_id: filtered.then(|| id(7)),
                                background: filtered.then_some(true),
                                ..TaskQuery::default()
                            },
                            limit,
                            cursor,
                        )
                    },
                    |r| r.id,
                    expected,
                );
                check_scan(
                    |limit, cursor| {
                        snapshot.scan_submissions(
                            SubmissionQuery {
                                conversation_id: filtered.then(|| id(7)),
                                status: filtered.then_some(SubmissionStatus::Queued),
                            },
                            limit,
                            cursor,
                        )
                    },
                    |r| r.id,
                    expected,
                );
                check_scan(
                    |limit, cursor| {
                        snapshot.scan_conversations(
                            ConversationQuery {
                                owner_conversation_id: filtered.then(|| id(7)),
                                owner_task_id: filtered.then(|| id(8)),
                            },
                            limit,
                            cursor,
                        )
                    },
                    |r| r.id,
                    expected,
                );
            }
        }
    }

    #[test]
    fn ancestry_segments_are_child_first_and_keep_overlapping_bounds() {
        let mut lookups = Vec::new();
        let query = EntryQuery {
            conversation_id: id(3),
            min_entry_id: Some(id(7)),
            max_entry_id: Some(id(100)),
        };
        let segments = visible_segments(&query, |current| {
            lookups.push(current);
            let mut record = conversation(current.get());
            record.parent = match current.get() {
                3 => Some(ParentLink {
                    conversation_id: id(2),
                    at: id(80),
                }),
                2 => Some(ParentLink {
                    conversation_id: id(1),
                    at: id(90),
                }),
                1 => None,
                _ => unreachable!(),
            };
            Ok(Some(record))
        })
        .unwrap();
        assert_eq!(lookups, [id(3), id(2), id(1)]);
        assert_eq!(segments, [(id(3), 7, 100), (id(2), 7, 80), (id(1), 7, 80)]);
    }

    #[test]
    fn ancestry_checks_missing_records_and_cycles_before_empty_bounds() {
        let empty = EntryQuery {
            conversation_id: id(2),
            min_entry_id: Some(id(9)),
            max_entry_id: Some(id(8)),
        };
        assert_eq!(
            visible_segments(&empty, |_| Ok(None)),
            Err(StorageError::Other("Unknown conversation: 2".into()))
        );
        assert!(
            visible_segments(&empty, |_| Ok(Some(conversation(2))))
                .unwrap()
                .is_empty()
        );
        let failure = StorageError::Other("lookup failed".into());
        assert_eq!(
            visible_segments(&empty, |_| Err(failure.clone())),
            Err(failure.clone())
        );

        for parent in [id(2), id(99)] {
            let mut record = conversation(2);
            record.parent = Some(ParentLink {
                conversation_id: parent,
                at: id(8),
            });
            let mut lookups = Vec::new();
            let mut query = EntryQuery {
                max_entry_id: None,
                ..empty.clone()
            };
            // The cutoff is below the lower bound, so neither a missing ancestor
            // nor a cycle is traversed.
            assert_eq!(
                visible_segments(&query, |current| {
                    lookups.push(current);
                    Ok(Some(record.clone()))
                })
                .unwrap(),
                [(id(2), 9, MAX_NUMBER)]
            );
            assert_eq!(lookups, [id(2)]);
            query.min_entry_id = None;
            lookups.clear();
            let result = visible_segments(&query, |current| {
                lookups.push(current);
                Ok((current == id(2)).then(|| record.clone()))
            });
            if parent == id(2) {
                assert_eq!(
                    result,
                    Err(StorageError::Other("Cyclic conversation ancestry".into()))
                );
                assert_eq!(lookups, [id(2)]);
            } else {
                assert_eq!(
                    result,
                    Err(StorageError::Other("Unknown conversation: 99".into()))
                );
                assert_eq!(lookups, [id(2), id(99)]);
            }
        }
    }
}
