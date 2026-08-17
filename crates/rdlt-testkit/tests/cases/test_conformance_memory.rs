//! T052: the bundled memory connectors are certified — "certified = passes
//! conformance" starts with our own connectors (spec SC-007).

use async_trait::async_trait;
use rdlt_testkit::conformance::{destination::verify_destination, source::verify_source};
use rdlt_testkit::{
    MemoryBatch, MemoryDestination, MemorySource, MemoryStream, ProbeError, TableProbe,
    assert_conformant,
};
use serde_json::json;

#[tokio::test]
async fn memory_source_is_conformant() {
    let source = MemorySource::new(vec![MemoryStream::new(
        rdlt_connector::source::StreamSpec::new("events"),
        vec![
            MemoryBatch::new(vec![json!({"a": 1}), json!({"a": 2})]).with_checkpoint(1),
            MemoryBatch::new(vec![json!({"a": 3})]).with_checkpoint(2),
            MemoryBatch::new(vec![json!({"a": 4})]).with_checkpoint(3),
        ],
    )]);
    assert_conformant(verify_source(&source).await.expecting_no_skips());
}

/// 5M7's honest side: a read carrying several MiB of ordinary JSON —
/// past the flat factor's ~256 KiB false-negative line, nowhere near the
/// 64 MiB ACTUAL-retention ceiling — must certify. (The flood negative
/// beside this one proves the ceiling still bites.)
#[tokio::test]
async fn an_honest_multi_mib_read_is_conformant() {
    let row = || json!({"filler": "x".repeat(1 << 16), "n": 1});
    let source = MemorySource::new(vec![MemoryStream::new(
        rdlt_connector::source::StreamSpec::new("events"),
        (1..=8)
            .map(|i| {
                // 8 rows × 64 KiB per batch ≈ 512 KiB of wire per push,
                // ~4 MiB retained across the read.
                MemoryBatch::new((0..8).map(|_| row()).collect()).with_checkpoint(i)
            })
            .collect(),
    )]);
    assert_conformant(verify_source(&source).await.expecting_no_skips());
}

struct MemoryProbe(MemoryDestination);

#[async_trait]
impl TableProbe for MemoryProbe {
    async fn count(&self, table: &rdlt_connector::core::id::TableName) -> Result<u64, ProbeError> {
        Ok(self.0.committed_rows(table.as_str()).len() as u64)
    }
}

#[tokio::test]
async fn memory_destination_is_conformant() {
    let dest = MemoryDestination::new();
    let probe = MemoryProbe(dest.clone());
    assert_conformant(verify_destination(&dest, &probe).await.expecting_no_skips());
}
