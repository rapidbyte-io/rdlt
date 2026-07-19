//! T015: parquet destination — public destination conformance suite (FR-011).

use async_trait::async_trait;
use rdlt_dest_parquet::ParquetDir;
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
