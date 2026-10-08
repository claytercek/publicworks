#![allow(dead_code, unused_imports)]

use publicworks_agent::*;
use std::{cell::RefCell, collections::VecDeque, rc::Rc};

pub use publicworks_runtime::test_support::{Gate, TempDatabase, bounded, bounded_with_budget};

/// Scripted model shared by integration tests that need request capture.
#[derive(Clone)]
pub struct FakeModel {
    replies: Rc<RefCell<VecDeque<Result<ModelResponse, ModelError>>>>,
    pub requests: Rc<RefCell<Vec<ModelRequest>>>,
}

impl FakeModel {
    pub fn new(replies: impl IntoIterator<Item = Result<ModelResponse, ModelError>>) -> Self {
        Self {
            replies: Rc::new(RefCell::new(replies.into_iter().collect())),
            requests: Rc::new(RefCell::new(Vec::new())),
        }
    }
}

impl Model for FakeModel {
    fn complete(&self, request: ModelRequest, _: Cancellation) -> ModelFuture {
        self.requests.borrow_mut().push(request);
        let reply = self
            .replies
            .borrow_mut()
            .pop_front()
            .expect("unexpected model call");
        Box::pin(async move { reply })
    }
}
