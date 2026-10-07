//! One candidate-aware ownership view for validation and tree decisions.
use super::*;
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Up {
    Task(Id),
    Conversation(Id),
}

pub(in crate::session) struct Tree {
    pub tasks: BTreeMap<Id, TaskRecord>,
    pub conversations: BTreeMap<Id, ConversationRecord>,
}
impl Tree {
    pub async fn load(storage: &mut dyn Storage) -> Result<Self, SessionError> {
        let mut tasks = BTreeMap::new();
        let mut cursor = None;
        loop {
            let page = storage
                .scan_tasks(TaskQuery::default(), 128, cursor)
                .await?;
            tasks.extend(page.items.into_iter().map(|r| (r.id, r)));
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        let mut conversations = BTreeMap::new();
        let mut cursor = None;
        loop {
            let page = storage
                .scan_conversations(ConversationQuery::default(), 128, cursor)
                .await?;
            conversations.extend(page.items.into_iter().map(|r| (r.id, r)));
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        Ok(Self {
            tasks,
            conversations,
        })
    }

    fn parent(&self, record: &TaskRecord) -> Option<Up> {
        if let Some(owner) = record.owner {
            let owner = self.tasks.get(&owner)?;
            (owner.conversation_id == record.conversation_id).then_some(Up::Task(owner.id))
        } else {
            self.conversations
                .contains_key(&record.conversation_id)
                .then_some(Up::Conversation(record.conversation_id))
        }
    }

    fn next(&self, node: Up) -> Option<Option<Up>> {
        match node {
            Up::Task(id) => Some(Some(self.parent(self.tasks.get(&id)?)?)),
            Up::Conversation(id) => {
                let conversation = self.conversations.get(&id)?;
                match &conversation.owner {
                    Some(owner) => {
                        let task = self.tasks.get(&owner.task_id)?;
                        (task.conversation_id == owner.conversation_id)
                            .then_some(Some(Up::Task(task.id)))
                    }
                    None => Some(None),
                }
            }
        }
    }

    fn above(&self, start: Up) -> Option<Vec<Up>> {
        let mut seen = BTreeSet::new();
        let mut result = Vec::new();
        let mut current = Some(start);
        while let Some(node) = current {
            if !seen.insert(node) {
                return None;
            }
            result.push(node);
            current = self.next(node)?;
        }
        Some(result)
    }

    /// The scheduling anchor reached through task and conversation ownership.
    /// A background task is an independent anchor. Fork ancestry is never followed.
    pub fn root(&self, id: Id) -> Option<Id> {
        let record = self.tasks.get(&id)?;
        if record.background {
            return record.owner.is_none().then_some(id);
        }
        let mut root = id;
        let mut seen = BTreeSet::new();
        let mut current = Some(self.parent(record)?);
        while let Some(node) = current {
            if !seen.insert(node) {
                return None;
            }
            if let Up::Task(task) = node {
                root = task;
                if self.tasks.get(&task)?.background {
                    return Some(root);
                }
            }
            current = self.next(node)?;
        }
        Some(root)
    }

    fn ordinary_descendant(&self, record: &TaskRecord, owner: Id) -> bool {
        if record.id == owner || record.background {
            return false;
        }
        let Some(parent) = self.parent(record) else {
            return false;
        };
        let mut seen = BTreeSet::new();
        let mut current = Some(parent);
        while let Some(node) = current {
            if !seen.insert(node) {
                return false;
            }
            if let Up::Task(id) = node {
                if id == owner {
                    return true;
                }
                if self.tasks.get(&id).is_some_and(|task| task.background) {
                    return false;
                }
            }
            let Some(next) = self.next(node) else {
                return false;
            };
            current = next;
        }
        false
    }

    pub fn descendants(&self, id: Id) -> BTreeSet<Id> {
        self.tasks
            .values()
            .filter(|record| self.ordinary_descendant(record, id))
            .map(|record| record.id)
            .collect()
    }

    fn in_conversation(
        &self,
        record: &TaskRecord,
        conversation: Id,
        cross_background: bool,
    ) -> Option<bool> {
        if record.background && !cross_background {
            return Some(false);
        }
        let mut seen = BTreeSet::new();
        let mut current = Some(self.parent(record)?);
        while let Some(node) = current {
            if !seen.insert(node) {
                return None;
            }
            match node {
                Up::Conversation(id) if id == conversation => return Some(true),
                Up::Task(id) if !cross_background && self.tasks.get(&id)?.background => {
                    return Some(false);
                }
                _ => {}
            }
            current = self.next(node)?;
        }
        Some(false)
    }

    pub fn conversation_scope(
        &self,
        conversation: Id,
        cross_background: bool,
    ) -> Option<BTreeSet<Id>> {
        self.conversations.get(&conversation)?;
        let mut scope = BTreeSet::new();
        for record in self.tasks.values() {
            if self.in_conversation(record, conversation, cross_background)? {
                scope.insert(record.id);
            }
        }
        Some(scope)
    }

    pub fn conversation_idle(&self, conversation: Option<Id>) -> bool {
        if conversation.is_some_and(|id| !self.conversations.contains_key(&id)) {
            return false;
        }
        self.tasks.values().all(|record| {
            if record.status() == TaskStatus::Terminal || record.background {
                return true;
            }
            match conversation {
                None => false,
                Some(id) => matches!(self.in_conversation(record, id, false), Some(false)),
            }
        })
    }

    pub fn scope(&self, root: Id) -> Option<BTreeSet<Id>> {
        if self.root(root)? != root {
            return None;
        }
        let mut scope = self.descendants(root);
        scope.insert(root);
        if scope.iter().any(|id| self.root(*id) != Some(root)) {
            return None;
        }
        Some(scope)
    }

    pub fn owned_live(&self, id: Id) -> bool {
        self.descendants(id)
            .iter()
            .any(|id| self.tasks[id].status() != TaskStatus::Terminal)
    }

    pub fn validate_wait(&self, record: &TaskRecord) -> Result<(), SessionError> {
        let TaskState::Waiting { on, policy, .. } = &record.state else {
            return Ok(());
        };
        let parent = self
            .parent(record)
            .ok_or_else(|| SessionError::Invalid("Missing task or conversation owner".into()))?;
        let above = self
            .above(parent)
            .ok_or_else(|| SessionError::Invalid("Cyclic or missing ownership".into()))?;
        let ancestors = above
            .into_iter()
            .filter_map(|node| match node {
                Up::Task(id) => Some(id),
                Up::Conversation(_) => None,
            })
            .chain(std::iter::once(record.id))
            .collect::<BTreeSet<_>>();
        for id in on {
            let member = self
                .tasks
                .get(id)
                .ok_or_else(|| SessionError::Invalid(format!("Unknown wait target: {id}")))?;
            if ancestors.contains(id)
                || (*policy == JoinPolicy::FailFast && member.owner != Some(record.id))
            {
                return Err(SessionError::Invalid(
                    "Wait targets cannot be self/ancestors; failFast requires direct children"
                        .into(),
                ));
            }
        }
        Ok(())
    }
    pub fn failed(record: &TaskRecord) -> bool {
        matches!(&record.state, TaskState::Completing { outcome } | TaskState::Terminal { outcome }
            if !matches!(outcome, TaskOutcome::Completed { .. }))
    }
    pub fn mark(&mut self, id: Id) {
        if let Some(record) = self.tasks.get_mut(&id)
            && record.status() != TaskStatus::Terminal
        {
            record.abort_requested = true;
        }
    }
    /// Fixed point over this explicit ordinary scope only. External wait targets are observations.
    pub fn reconcile(&mut self, scope: &BTreeSet<Id>) {
        loop {
            let before = self.tasks.clone();
            for id in scope {
                let record = &self.tasks[id];
                if record.status() != TaskStatus::Terminal
                    && (record.abort_requested || Self::failed(record))
                {
                    for child in self.descendants(*id) {
                        self.mark(child);
                    }
                }
                if let TaskState::Waiting {
                    on,
                    policy: JoinPolicy::FailFast,
                    ..
                } = &self.tasks[id].state
                {
                    let on = on.clone();
                    if on
                        .iter()
                        .any(|id| self.tasks.get(id).is_some_and(Self::failed))
                    {
                        for target in on {
                            // Valid persisted waits name direct children. Never let a malformed
                            // persisted edge turn an observation into out-of-scope mutation.
                            if scope.contains(&target)
                                && self
                                    .tasks
                                    .get(&target)
                                    .is_some_and(|r| r.owner == Some(*id) && !Self::failed(r))
                            {
                                self.mark(target);
                            }
                        }
                    }
                }
            }
            for id in scope {
                let record = &self.tasks[id];
                let next = match &record.state {
                    TaskState::Completing { outcome } if !self.owned_live(*id) => {
                        Some(TaskState::Terminal {
                            outcome: outcome.clone(),
                        })
                    }
                    TaskState::Waiting { checkpoint, on, .. }
                        if if record.abort_requested {
                            !self.owned_live(*id)
                        } else {
                            on.iter().all(|id| {
                                self.tasks
                                    .get(id)
                                    .is_none_or(|r| r.status() == TaskStatus::Terminal)
                            })
                        } =>
                    {
                        Some(TaskState::Pending {
                            checkpoint: checkpoint.clone(),
                        })
                    }
                    _ => None,
                };
                if let Some(next) = next {
                    self.tasks.get_mut(id).unwrap().state = next;
                }
            }
            if self.tasks == before {
                break;
            }
        }
    }
    pub async fn stage_changes(
        &self,
        tx: &Tx,
        before: &BTreeMap<Id, TaskRecord>,
    ) -> Result<(), SessionError> {
        for (id, record) in &self.tasks {
            if before.get(id) != Some(record) {
                tx.set_task(record.clone()).await?;
            }
        }
        Ok(())
    }
}
