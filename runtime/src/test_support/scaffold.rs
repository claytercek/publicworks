#![allow(dead_code)]

use futures_channel::oneshot;
use futures_util::future::{FutureExt, Shared};
use std::{
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
};

type GateWait = Shared<oneshot::Receiver<()>>;

/// Cloneable one-shot gate for coordinating local async tests.
///
/// Any number of waiters may observe the release. Releasing an already-open gate
/// is harmless.
#[derive(Clone)]
pub struct Gate {
    release: Rc<RefCell<Option<oneshot::Sender<()>>>>,
    wait: GateWait,
    waited: Rc<Cell<bool>>,
}

impl Default for Gate {
    fn default() -> Self {
        let (release, wait) = oneshot::channel();
        Self {
            release: Rc::new(RefCell::new(Some(release))),
            wait: wait.shared(),
            waited: Rc::new(Cell::new(false)),
        }
    }
}

impl Gate {
    pub async fn wait(&self) {
        self.waited.set(true);
        let _ = self.wait.clone().await;
    }

    /// Returns whether any waiter has started waiting on this gate.
    pub fn has_waited(&self) -> bool {
        self.waited.get()
    }

    pub fn released(&self) -> bool {
        self.release.borrow().is_none()
    }

    pub fn release(&self) {
        let release = self.release.borrow_mut().take();
        if let Some(release) = release {
            let _ = release.send(());
        }
    }
}

/// Drives a future with a deterministic poll budget for tests that combine
/// several local drivers and otherwise risk hanging forever.
pub async fn bounded<F: Future>(future: F) -> F::Output {
    bounded_with_budget(50_000, future).await
}

pub async fn bounded_with_budget<F: Future>(max_polls: usize, future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut polls = 0;
    std::future::poll_fn(|cx| {
        polls += 1;
        assert!(polls < max_polls, "test exceeded poll budget");
        let result = future.as_mut().poll(cx);
        if result.is_pending() {
            cx.waker().wake_by_ref();
        }
        result
    })
    .await
}
