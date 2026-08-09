//! T051: deliberately non-compliant connectors FAIL conformance with diagnostics
//! naming the violated clause (spec US5 acceptance scenario 2).

use async_trait::async_trait;
use rdlt_connector::{
    CommitMeta, CommitReceipt, ConnectorSpec, Destination, DestinationCapabilities,
    DestinationError, LoadSession, OpenContext, PipelineId, ReadRequest, RecordBatch, Source,
    SourceError, StateDoc, StreamSpec, TableName, TableSchema, WriteMode,
};
use rdlt_testkit::conformance::{destination::verify_destination, source::verify_source};
use rdlt_testkit::{MemoryDestination, ProbeError, TableProbe};
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
    let failures = verify_source(&AmnesiacSource).await.failures;
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
        self.inner.capabilities().with_merge(false)
    }

    async fn open(&self, ctx: OpenContext) -> Result<Box<dyn LoadSession>, DestinationError> {
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
    async fn count(&self, table: &TableName) -> Result<u64, ProbeError> {
        Ok(self.0.committed_rows(table.as_str()).len() as u64)
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

/// Violates S2 (never checkpoints) AND S4 (errors on a closed channel).
/// Pins the generation-1 defect this generation fixes: the S2 `continue`
/// skipped the S4 check, so the second violation was never reported.
/// The stream DECLARES a cursor deliberately — a declared cursor with
/// no checkpoints is S2's violation, where an undeclared one is the
/// snapshot door's honest skip (`test_conformance_snapshot.rs`).
struct DoublyBrokenSource;

#[async_trait]
impl Source for DoublyBrokenSource {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("doubly-broken", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![StreamSpec::new("events").with_cursor_field("a")])
    }

    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
        // NOTE both bugs: no checkpoint ever, and a closed channel is
        // treated as an error instead of cancellation.
        if req.out.rows([json!({"a": 1})]).await.is_err() {
            return Err(SourceError::fatal("channel closed mid-read"));
        }
        Ok(())
    }
}

#[tokio::test]
async fn both_s2_and_s4_are_reported_independently() {
    let failures = verify_source(&DoublyBrokenSource).await.failures;
    for clause in ["S2", "S4"] {
        assert!(
            failures.iter().any(|f| f.clause == clause),
            "expected a {clause} diagnostic, got: {failures:?}"
        );
    }
}

/// A destination whose `open` always fails. Pins the second generation-1
/// defect: every open was labelled D4, sending the author to investigate
/// teardown semantics that were never reached — the setup failure now
/// carries the clause it was setting up.
struct Unopenable;

#[async_trait]
impl Destination for Unopenable {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("unopenable", "0.0.0")
    }

    fn capabilities(&self) -> DestinationCapabilities {
        DestinationCapabilities::default()
    }

    async fn open(&self, _ctx: OpenContext) -> Result<Box<dyn LoadSession>, DestinationError> {
        Err(DestinationError::fatal("credentials rejected"))
    }
}

struct NoProbe;

#[async_trait]
impl TableProbe for NoProbe {
    async fn count(&self, _table: &TableName) -> Result<u64, ProbeError> {
        Ok(0)
    }
}

#[tokio::test]
async fn an_open_failure_is_attributed_to_the_clause_it_sets_up() {
    let failures = verify_destination(&Unopenable, &NoProbe).await;
    let first = failures.first().expect("an unopenable destination fails");
    assert_eq!(
        (first.clause, first.message.starts_with("open failed: ")),
        ("D6", true),
        "the first open serves the D6 fresh-state check, not D4 teardown: {failures:?}"
    );
}

/// A probe whose oracle is itself broken: every count errors. The kit
/// must FAIL the clause under evaluation naming the probe's error — a
/// probe error reading as "0 rows" would certify D1's staging
/// invisibility vacuously, which is exactly the silent zero a fallible
/// count exists to kill.
struct BrokenProbe;

#[async_trait]
impl TableProbe for BrokenProbe {
    async fn count(&self, _table: &TableName) -> Result<u64, ProbeError> {
        Err(ProbeError {
            message: "probe blew up".into(),
        })
    }
}

#[tokio::test]
async fn a_broken_probe_fails_the_clause_under_evaluation_by_name() {
    let failures = verify_destination(&MemoryDestination::new(), &BrokenProbe).await;
    assert!(
        failures
            .iter()
            .any(|f| f.clause == "D1" && f.message.contains("probe blew up")),
        "expected D1 to fail naming the probe error, got: {failures:?}"
    );
}

/// Fails AFTER dropping its output handle: the rows all arrive, the
/// channel drains, and only then does the read return an error. Pins the
/// harness hole round 2 closed: `read_all` used to break on the drained
/// channel and drop the still-pending future, certifying the failure
/// away (and cancelling the source's teardown mid-flight).
struct TeardownFailingSource;

#[async_trait]
impl Source for TeardownFailingSource {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("teardown-failing", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![StreamSpec::new("events")])
    }

    async fn read(&self, req: ReadRequest) -> Result<(), SourceError> {
        let mut out = req.out;
        if out.rows([json!({"a": 1})]).await.is_err() {
            return Ok(());
        }
        let _ = out.checkpoint(rdlt_connector::Cursor::new(1)).await;
        drop(out); // every sender gone — the harness channel drains NOW
        // Yield so the harness OBSERVES the drain before this error
        // exists — the pin must exercise the wait-for-the-reader path,
        // not the result-captured-first path.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        Err(SourceError::fatal("teardown failed after the last push"))
    }
}

#[tokio::test]
async fn a_source_that_fails_after_dropping_its_feed_is_not_certified() {
    let failures = verify_source(&TeardownFailingSource).await.failures;
    assert!(
        failures
            .iter()
            .any(|f| f.clause == "S1" && f.message.contains("teardown failed")),
        "the post-drop error must fail certification, got: {failures:?}"
    );
}
