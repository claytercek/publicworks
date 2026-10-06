//! One candidate-aware ownership view for validation and tree decisions.
use super::*;
use std::collections::BTreeSet;

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
    /// Missing edges and cycles are errors, never permission to drive a partial tree.
    pub fn root(&self, id: Id) -> Option<Id> {
        let conversation = self.tasks.get(&id)?.conversation_id;
        if self.conversations.get(&conversation)?.owner.is_some() {
            return None;
        }
        let mut seen = BTreeSet::new();
        let mut current = id;
        loop {
            if !seen.insert(current) {
                return None;
            }
            let record = self.tasks.get(&current)?;
            if record.background || record.conversation_id != conversation {
                return None;
            }
            match record.owner {
                Some(owner) => current = owner,
                None => return Some(current),
            }
        }
    }
    pub fn descendants(&self, id: Id) -> BTreeSet<Id> {
        let mut result = BTreeSet::from([id]);
        loop {
            let before = result.len();
            for record in self.tasks.values() {
                if record.owner.is_some_and(|owner| result.contains(&owner)) {
                    result.insert(record.id);
                }
            }
            if before == result.len() {
                break;
            }
        }
        result.remove(&id);
        result
    }
    pub fn scope(&self, root: Id) -> Option<BTreeSet<Id>> {
        if self.root(root)? != root {
            return None;
        }
        let mut scope = self.descendants(root);
        scope.insert(root);
        if scope.iter().any(|id| self.root(*id) != Some(root))
            || self
                .conversations
                .values()
                .any(|c| c.owner.as_ref().is_some_and(|o| scope.contains(&o.task_id)))
        {
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
        let mut ancestors = BTreeSet::from([record.id]);
        let mut owner = record.owner;
        while let Some(id) = owner {
            if !ancestors.insert(id) {
                return Err(SessionError::Invalid("Cyclic task ownership".into()));
            }
            owner = self
                .tasks
                .get(&id)
                .ok_or_else(|| SessionError::Invalid("Missing task owner".into()))?
                .owner;
        }
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
    /// Fixed point over this explicit scope only. External wait targets are observations.
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
