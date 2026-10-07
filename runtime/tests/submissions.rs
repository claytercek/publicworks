use futures_lite::future::{block_on, zip};
use publicworks_runtime::*;

#[path = "../src/test_support/session_submissions.rs"]
mod contracts;

macro_rules! check {
    ($name:ident) => {
        #[test]
        fn $name() {
            block_on(async {
                let (session, driver) = Session::new(MemoryStorage::new());
                zip(
                    async {
                        contracts::$name(&session).await;
                        session.close().await.unwrap();
                    },
                    driver,
                )
                .await;
            });
        }
    };
}
check!(submission_tx_reads);
check!(submission_tx_transitions);
check!(submission_tx_final_candidates);
check!(submission_tx_withdrawal);
check!(submission_tx_json);
