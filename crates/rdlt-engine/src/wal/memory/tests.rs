use super::MemoryWal;
use crate::wal::store::conformance;

#[tokio::test]
async fn a_log_in_memory_keeps_the_store_s_contract() {
    conformance::conforms(&MemoryWal::default()).await;
}
