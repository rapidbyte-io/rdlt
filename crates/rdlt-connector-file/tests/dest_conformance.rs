//! T015: parquet destination — public destination conformance suite (FR-011).

use async_trait::async_trait;
use rdlt_connector_file::ParquetDir;
use rdlt_testkit::conformance::dest::verify_destination;
use rdlt_testkit::{TableProbe, assert_conformant};

struct DirProbe(ParquetDir);

#[async_trait]
impl TableProbe for DirProbe {
    async fn count(&self, table: &rdlt_connector::TableName) -> u64 {
        self.0.count_rows(table.as_str()).unwrap_or(0)
    }
}

#[tokio::test]
async fn parquet_destination_is_conformant() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = ParquetDir::open(dir.path().join("out")).expect("open");
    let probe = DirProbe(ParquetDir::open(dir.path().join("out")).expect("open"));
    assert_conformant(verify_destination(&dest, &probe).await);
}

/// Feature 006 re-assertion: the keyed structured-merge lift does NOT reach a
/// non-`merge`-capable destination — the capability rejection still fires at
/// plan time, even for a stream whose declared key would otherwise qualify.
#[tokio::test]
async fn merge_still_rejected_by_capability_even_with_declared_key() {
    use rdlt_engine::{Engine, EngineConfig};
    use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
    use serde_json::json;

    let dir = tempfile::tempdir().expect("tempdir");
    let dest = ParquetDir::open(dir.path().join("out")).expect("open");
    let source = MemorySource::new(vec![MemoryStream::new(
        rdlt_connector::StreamSpec::new("s").with_primary_key(["id"]),
        vec![MemoryBatch::new(vec![json!({"id": 1})])],
    )]);
    let mut config = EngineConfig::new("pq-merge");
    config.write_mode = rdlt_connector::WriteMode::Merge {
        key: vec!["id".into()],
    };
    let err = Engine::new(config, source, dest)
        .run()
        .await
        .expect_err("parquet has no merge capability");
    let msg = err.to_string();
    assert!(
        msg.contains("Merge") && msg.contains("does not support"),
        "{msg}"
    );
}
