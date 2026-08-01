//! T052: the bundled memory connectors are certified — "certified = passes
//! conformance" starts with our own connectors (spec SC-007).

use async_trait::async_trait;
use rdlt_testkit_v2::conformance::{destination::verify_destination, source::verify_source};
use rdlt_testkit_v2::{
    MemoryBatch, MemoryDestination, MemorySource, MemoryStream, TableProbe, assert_conformant,
};
use serde_json::json;

#[tokio::test]
async fn memory_source_is_conformant() {
    let source = MemorySource::new(vec![MemoryStream::new(
        rdlt_connector::StreamSpec::new("events"),
        vec![
            MemoryBatch::new(vec![json!({"a": 1}), json!({"a": 2})]).with_checkpoint(1),
            MemoryBatch::new(vec![json!({"a": 3})]).with_checkpoint(2),
            MemoryBatch::new(vec![json!({"a": 4})]).with_checkpoint(3),
        ],
    )]);
    assert_conformant(verify_source(&source).await);
}

struct MemoryProbe(MemoryDestination);

#[async_trait]
impl TableProbe for MemoryProbe {
    async fn count(&self, table: &rdlt_connector::TableName) -> u64 {
        self.0.committed_rows(table.as_str()).len() as u64
    }
}

#[tokio::test]
async fn memory_destination_is_conformant() {
    let dest = MemoryDestination::new();
    let probe = MemoryProbe(dest.clone());
    assert_conformant(verify_destination(&dest, &probe).await);
}
