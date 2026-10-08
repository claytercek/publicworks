/// Implement selected [`Storage`](crate::Storage) methods by forwarding them to
/// a field. Keep fault-injection and observation overrides handwritten beside
/// the invocation.
#[macro_export]
#[doc(hidden)]
macro_rules! forward_storage_methods {
    ($inner:ident; $($method:ident),* $(,)?) => {
        $($crate::forward_storage_methods!(@one $inner, $method);)*
    };
    (@one $inner:ident, commit) => {
        fn commit(&mut self, writes: Vec<$crate::StorageWrite>) -> $crate::StorageFuture<'_, $crate::Seq> {
            self.$inner.commit(writes)
        }
    };
    (@one $inner:ident, mint_id) => {
        fn mint_id(&mut self) -> $crate::StorageFuture<'_, $crate::Id> {
            self.$inner.mint_id()
        }
    };
    (@one $inner:ident, conversation) => {
        fn conversation(&mut self, id: $crate::Id) -> $crate::StorageFuture<'_, Option<$crate::ConversationRecord>> {
            self.$inner.conversation(id)
        }
    };
    (@one $inner:ident, scan_conversations) => {
        fn scan_conversations(
            &mut self,
            query: $crate::ConversationQuery,
            limit: usize,
            cursor: Option<$crate::Cursor>,
        ) -> $crate::StorageFuture<'_, $crate::Page<$crate::ConversationRecord>> {
            self.$inner.scan_conversations(query, limit, cursor)
        }
    };
    (@one $inner:ident, task) => {
        fn task(&mut self, id: $crate::Id) -> $crate::StorageFuture<'_, Option<$crate::TaskRecord>> {
            self.$inner.task(id)
        }
    };
    (@one $inner:ident, scan_tasks) => {
        fn scan_tasks(
            &mut self,
            query: $crate::TaskQuery,
            limit: usize,
            cursor: Option<$crate::Cursor>,
        ) -> $crate::StorageFuture<'_, $crate::Page<$crate::TaskRecord>> {
            self.$inner.scan_tasks(query, limit, cursor)
        }
    };
    (@one $inner:ident, submission) => {
        fn submission(&mut self, id: $crate::Id) -> $crate::StorageFuture<'_, Option<$crate::SubmissionRecord>> {
            self.$inner.submission(id)
        }
    };
    (@one $inner:ident, scan_submissions) => {
        fn scan_submissions(
            &mut self,
            query: $crate::SubmissionQuery,
            limit: usize,
            cursor: Option<$crate::Cursor>,
        ) -> $crate::StorageFuture<'_, $crate::Page<$crate::SubmissionRecord>> {
            self.$inner.scan_submissions(query, limit, cursor)
        }
    };
    (@one $inner:ident, submission_by_request) => {
        fn submission_by_request(
            &mut self,
            conversation_id: $crate::Id,
            request_id: &str,
        ) -> $crate::StorageFuture<'_, Option<$crate::SubmissionRecord>> {
            self.$inner.submission_by_request(conversation_id, request_id)
        }
    };
    (@one $inner:ident, conversation_state) => {
        fn conversation_state(
            &mut self,
            conversation_id: $crate::Id,
        ) -> $crate::StorageFuture<'_, Option<$crate::ConversationStateRecord>> {
            self.$inner.conversation_state(conversation_id)
        }
    };
    (@one $inner:ident, entry) => {
        fn entry(&mut self, id: $crate::Id) -> $crate::StorageFuture<'_, Option<$crate::StoredEntry>> {
            self.$inner.entry(id)
        }
    };
    (@one $inner:ident, visible_entry) => {
        fn visible_entry(
            &mut self,
            conversation: $crate::Id,
            id: $crate::Id,
        ) -> $crate::StorageFuture<'_, Option<$crate::StoredEntry>> {
            self.$inner.visible_entry(conversation, id)
        }
    };
    (@one $inner:ident, scan_entries) => {
        fn scan_entries(
            &mut self,
            query: $crate::EntryQuery,
            limit: usize,
            cursor: Option<$crate::Cursor>,
        ) -> $crate::StorageFuture<'_, $crate::Page<$crate::EntryRecord>> {
            self.$inner.scan_entries(query, limit, cursor)
        }
    };
    (@one $inner:ident, find_latest_head_marker) => {
        fn find_latest_head_marker(
            &mut self,
            conversation: $crate::Id,
            at_or_before: Option<$crate::Id>,
        ) -> $crate::StorageFuture<'_, Option<$crate::EntryRecord>> {
            self.$inner.find_latest_head_marker(conversation, at_or_before)
        }
    };
    (@one $inner:ident, close) => {
        fn close(&mut self) -> $crate::StorageFuture<'_, ()> {
            self.$inner.close()
        }
    };
}

#[allow(unused_imports)]
pub use crate::forward_storage_methods;
