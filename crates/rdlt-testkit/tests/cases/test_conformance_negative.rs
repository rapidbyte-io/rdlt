//! T051: deliberately non-compliant connectors FAIL conformance with diagnostics
//! naming the violated clause (spec US5 acceptance scenario 2).

use async_trait::async_trait;
use rdlt_connector::{
    CommitMeta, CommitReceipt, ConnectorSpec, Destination, DestinationCapabilities,
    DestinationError, LoadSession, OpenCtx, PipelineId, ReadRequest, RecordBatch, Source,
    SourceError, StateDoc, StreamSpec, TableName, TableSchema, WriteMode,
};
use rdlt_testkit::conformance::{dest::verify_destination, source::verify_source};
use rdlt_testkit::{MemoryDestination, TableProbe};
use serde_json::json;

/// Violates S1: ignores `since` and re-emits everything from the beginning.
struct AmnesiacSource;

#[async_trait]
impl Source for AmnesiacSource {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("amnesiac", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![StreamSpec::new("events")])
    }

    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
        // NOTE the bug: `req.since` is deliberately ignored.
        for chunk in [vec![json!({"a": 1})], vec![json!({"a": 2})]] {
            if req.out.rows(chunk).await.is_err() {
                return Ok(());
            }
        }
        let _ = req.out.checkpoint(rdlt_connector::Cursor::new(2)).await;
        Ok(())
    }
}

#[tokio::test]
async fn source_ignoring_since_fails_s1_by_name() {
    let failures = verify_source(&AmnesiacSource).await;
    assert!(
        failures.iter().any(|f| f.clause == "S1"),
        "expected an S1 diagnostic, got: {failures:?}"
    );
}

/// Violates D3: every commit returns a fresh receipt (idempotence key ignored).
#[derive(Clone)]
struct ForgetfulDest {
    inner: MemoryDestination,
}

#[async_trait]
impl Destination for ForgetfulDest {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("forgetful", "0.0.0")
    }

    fn capabilities(&self) -> DestinationCapabilities {
        DestinationCapabilities {
            merge: false,
            ..self.inner.capabilities()
        }
    }

    async fn open(&self, ctx: OpenCtx) -> Result<Box<dyn LoadSession>, DestinationError> {
        Ok(Box::new(ForgetfulSession {
            inner: self.inner.open(ctx).await?,
            bump: 0,
        }))
    }
}

struct ForgetfulSession {
    inner: Box<dyn LoadSession>,
    bump: u64,
}

#[async_trait]
impl LoadSession for ForgetfulSession {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        self.inner.ensure_table(schema, mode).await
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        self.inner.write(table, batch).await
    }

    async fn commit(&mut self, mut meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        // NOTE the bug: the idempotency key is destroyed, so a re-commit looks new.
        self.bump += 1;
        meta.commit_seq = meta.commit_seq * 1000 + self.bump;
        self.inner.commit(meta).await
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        self.inner.read_state(pipeline).await
    }
}

struct Probe(MemoryDestination);

#[async_trait]
impl TableProbe for Probe {
    async fn count(&self, table: &TableName) -> u64 {
        self.0.committed_rows(table.as_str()).len() as u64
    }
}

#[tokio::test]
async fn destination_without_idempotent_commit_fails_d3_by_name() {
    let inner = MemoryDestination::new();
    let dest = ForgetfulDest {
        inner: inner.clone(),
    };
    let failures = verify_destination(&dest, &Probe(inner)).await;
    assert!(
        failures.iter().any(|f| f.clause == "D3"),
        "expected a D3 diagnostic, got: {failures:?}"
    );
}
