//! Observer ownership only; readiness and admission policy stay with each caller.
use super::*;

pub(super) type Waiters<K, T> =
    BTreeMap<K, BTreeMap<u64, oneshot::Sender<Result<T, HarnessError>>>>;

pub(super) struct Registration<K: Ord, T> {
    control: Weak<HarnessControl>,
    scope: K,
    pub(super) key: u64,
    pub(super) cancelled: Rc<Cell<bool>>,
    table: fn(&mut HarnessState) -> &mut Waiters<K, T>,
}

impl<K: Ord, T> Registration<K, T> {
    pub(super) fn new(
        control: &Rc<HarnessControl>,
        scope: K,
        table: fn(&mut HarnessState) -> &mut Waiters<K, T>,
    ) -> Self {
        let key = {
            let mut state = control.0.borrow_mut();
            let key = state.next_waiter;
            state.next_waiter += 1;
            key
        };
        Self {
            control: Rc::downgrade(control),
            scope,
            key,
            cancelled: Rc::new(Cell::new(false)),
            table,
        }
    }
}

impl<K: Ord, T> Drop for Registration<K, T> {
    fn drop(&mut self) {
        // Also cancels registration if its Session callback has not run yet.
        self.cancelled.set(true);
        if let Some(control) = self.control.upgrade() {
            let removed = {
                let mut state = control.0.borrow_mut();
                let table = (self.table)(&mut state);
                let (removed, empty) = table
                    .get_mut(&self.scope)
                    .map(|waiters| {
                        let removed = waiters.remove(&self.key);
                        (removed, waiters.is_empty())
                    })
                    .unwrap_or((None, false));
                if empty {
                    table.remove(&self.scope);
                }
                removed
            };
            // Sender destruction can wake reentrant code that borrows control.
            drop(removed);
        }
    }
}
