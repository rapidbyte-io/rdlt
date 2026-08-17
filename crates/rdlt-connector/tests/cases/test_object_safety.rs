//! The SPI's object-safety pin.
//!
//! Hosts hold connectors as trait objects (`Arc<dyn Source>`,
//! `Arc<dyn Destination>`, `Box<dyn LoadSession>`), so every trait must
//! stay dyn-compatible. This is a COMPILE-TIME property — the coercions
//! below are the assertion, and the test body only proves the coerced
//! objects still answer. It matters more in this generation than the
//! last: default async trait methods (`check`) are exactly the kind of
//! addition that can quietly disturb dyn-compatibility.

use rdlt_connector::arrow::RecordBatch;
use rdlt_connector::core::commit::{CommitMeta, CommitReceipt, WriteMode};
use rdlt_connector::core::id::{PipelineId, TableName};
use rdlt_connector::core::schema::TableSchema;
use rdlt_connector::core::state::StateDoc;
use rdlt_connector::destination::{Capabilities, Destination, LoadSession, OpenContext};
use rdlt_connector::error::{DestinationError, SourceError};
use rdlt_connector::source::{ReadRequest, Source, StreamSpec};
use rdlt_connector::spec::ConnectorSpec;

struct InertSource;

#[async_trait::async_trait]
impl Source for InertSource {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("inert-source", "0.0.0")
    }
    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![StreamSpec::new("empty")])
    }
    async fn read(&self, _request: ReadRequest) -> Result<(), SourceError> {
        Ok(())
    }
}

struct InertDestination;

struct InertSession;

#[async_trait::async_trait]
impl LoadSession for InertSession {
    async fn ensure_table(
        &mut self,
        _schema: &TableSchema,
        _mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        Ok(())
    }
    async fn write(
        &mut self,
        _table: &TableName,
        _batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        Ok(())
    }
    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        Ok(CommitReceipt {
            load_id: meta.load_id,
            commit_seq: meta.commit_seq,
        })
    }
    async fn read_state(
        &mut self,
        _pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        Ok(None)
    }
}

#[async_trait::async_trait]
impl Destination for InertDestination {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("inert-destination", "0.0.0")
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }
    async fn open(&self, _context: OpenContext) -> Result<Box<dyn LoadSession>, DestinationError> {
        Ok(Box::new(InertSession))
    }
}

/// The coercions ARE the assertion; the calls prove the objects answer —
/// including the defaulted `check`, dispatched through the vtable.
#[tokio::test]
async fn every_spi_trait_stays_dyn_compatible() {
    let source: &dyn Source = &InertSource;
    assert_eq!(source.spec().name, "inert-source");
    source.check().await.expect("default probe through dyn");
    assert_eq!(source.streams().await.expect("streams").len(), 1);

    let destination: &dyn Destination = &InertDestination;
    assert_eq!(destination.spec().name, "inert-destination");
    destination
        .check()
        .await
        .expect("default probe through dyn");
    assert!(!destination.capabilities().merge);

    let mut session: Box<dyn LoadSession> = Box::new(InertSession);
    assert!(
        session
            .read_state(&PipelineId::from("p"))
            .await
            .expect("read_state through dyn")
            .is_none()
    );
}
