//! The bundled memory connectors are certified — "certified = passes
//! conformance" starts with our own connectors.

use async_trait::async_trait;
use rdlt_testkit::conformance::destination::{self, ProbeError, TableProbe};
use rdlt_testkit::conformance::{assert_conformant, source};
use rdlt_testkit::memory;
use serde_json::json;

#[tokio::test]
async fn memory_source_is_conformant() {
    let source = memory::Source::new(vec![memory::Stream::new(
        rdlt_connector::source::StreamSpec::new("events"),
        vec![
            memory::Batch::new(vec![json!({"a": 1}), json!({"a": 2})]).with_checkpoint(1),
            memory::Batch::new(vec![json!({"a": 3})]).with_checkpoint(2),
            memory::Batch::new(vec![json!({"a": 4})]).with_checkpoint(3),
        ],
    )]);
    assert_conformant(source::verify(&source).await.expecting_no_skips());
}

/// The retention meter's honest side: a read carrying several MiB of
/// ordinary JSON — far past what a flat wire×factor charge would refuse,
/// nowhere near the 64 MiB actual-retention ceiling — must certify. (The
/// flood negative beside this one proves the ceiling still bites.)
#[tokio::test]
async fn an_honest_multi_mib_read_is_conformant() {
    let row = || json!({"filler": "x".repeat(1 << 16), "n": 1});
    let source = memory::Source::new(vec![memory::Stream::new(
        rdlt_connector::source::StreamSpec::new("events"),
        (1..=8)
            .map(|i| {
                // 8 rows × 64 KiB per batch ≈ 512 KiB of wire per push,
                // ~4 MiB retained across the read.
                memory::Batch::new((0..8).map(|_| row()).collect()).with_checkpoint(i)
            })
            .collect(),
    )]);
    assert_conformant(source::verify(&source).await.expecting_no_skips());
}

struct MemoryProbe(memory::Destination);

#[async_trait]
impl TableProbe for MemoryProbe {
    async fn count(&self, table: &rdlt_connector::core::id::TableName) -> Result<u64, ProbeError> {
        Ok(self.0.committed_rows(table.as_str()).len() as u64)
    }
}

#[tokio::test]
async fn memory_destination_is_conformant() {
    let dest = memory::Destination::new();
    let probe = MemoryProbe(dest.clone());
    assert_conformant(
        destination::verify(&dest, &probe)
            .await
            .expecting_no_skips(),
    );
}
