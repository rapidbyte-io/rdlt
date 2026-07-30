//! A misbehaving connector at the SPI boundary must produce a TYPED error,
//! never a hang: pushing Arrow batches on a stream not declared
//! `structured` is a contract violation, and the host must surface it even
//! while the source sits parked on the byte budget mid-push.

use std::sync::Arc;

use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use async_trait::async_trait;
use rdlt_connector::{ConnectorSpec, ReadRequest, Source, SourceError, StreamSpec};
use rdlt_core::RdltError;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::MemoryDestination;

struct ArrowOnUnstructured;

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .expect("batch")
}

#[async_trait]
impl Source for ArrowOnUnstructured {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("misbehaving", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        // NOT declared structured — pushing Arrow violates the contract.
        Ok(vec![StreamSpec::new("s")])
    }

    async fn read(&self, mut request: ReadRequest) -> Result<(), SourceError> {
        // First push violates the contract; keep pushing so the source is
        // parked on the byte budget when the host breaks out — the exact
        // shape that used to await a reader that could never finish.
        loop {
            if request.out.arrow(batch()).await.is_err() {
                return Ok(());
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn arrow_on_unstructured_stream_fails_typed_and_terminates() {
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        Engine::new(
            EngineConfig::new("contract"),
            ArrowOnUnstructured,
            MemoryDestination::new(),
        )
        .run(),
    )
    .await
    .expect("the run must terminate, not hang");
    let err = outcome.expect_err("contract violation is an error");
    assert!(
        matches!(err, RdltError::Source { .. }) && err.to_string().contains("structured"),
        "typed contract error expected: {err}"
    );
}
