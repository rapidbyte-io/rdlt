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
    let (failures, _skips) = verify_source(&AmnesiacSource).await.tolerating_skips();
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
    let (failures, _skips) = verify_destination(&dest, &Probe(inner))
        .await
        .tolerating_skips();
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
    let (failures, _skips) = verify_source(&DoublyBrokenSource).await.tolerating_skips();
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
    let (failures, _skips) = verify_destination(&Unopenable, &NoProbe)
        .await
        .tolerating_skips();
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
    let (failures, _skips) = verify_destination(&MemoryDestination::new(), &BrokenProbe)
        .await
        .tolerating_skips();
    assert!(
        failures
            .iter()
            .any(|f| f.clause == "D1" && f.message.contains("probe blew up")),
        "expected D1 to fail naming the probe error, got: {failures:?}"
    );
}

/// The 042 fix-wave docket item: an abort mid-suite must leave every
/// asserted-but-unreached clause with an honest non-verdict, never a
/// silent pass. The broken probe aborts the run at D1's first count, so
/// D4/D2/D3 (and D8 — the memory destination declares merge) were never
/// exercised; each comes back as a skip naming the abort, and the strict
/// fold promotes them to failures.
#[tokio::test]
async fn an_abort_reports_every_unreached_clause_as_a_skip_naming_the_abort() {
    let outcome = verify_destination(&MemoryDestination::new(), &BrokenProbe).await;
    let strict = outcome.expecting_no_skips();
    let outcome = verify_destination(&MemoryDestination::new(), &BrokenProbe).await;
    let (_failures, skips) = outcome.tolerating_skips();
    // D5 is among the unreached (round-7 correction): its clause spans
    // the new-session ensure too, which the D1 abort never reaches.
    for clause in ["D4", "D2", "D3", "D5", "D8"] {
        assert!(
            skips.iter().any(|s| s.clause == clause
                && s.reason == "not run — the suite aborted at D1 before reaching it"),
            "expected {clause} skipped as unreached, got: {skips:?}"
        );
    }
    // D6 concluded before the abort, and D1 carries the failure —
    // neither may double as a skip.
    for clause in ["D6", "D1"] {
        assert!(
            !skips.iter().any(|s| s.clause == clause),
            "{clause} has a real verdict and must not also skip: {skips:?}"
        );
    }
    assert!(
        strict
            .iter()
            .any(|f| f.clause == "D3" && f.message.starts_with("not exercised: ")),
        "the strict fold promotes unreached clauses to failures: {strict:?}"
    );
}

/// D5 spans two sites (round-7 fix): the first session's repeat-ensure
/// loop and the NEW session's ensure. An abort at the D4 re-open lands
/// between them, so D5 must render as unreached — not the full PASS the
/// early conclusion minted.
struct ThirdOpenFails {
    inner: MemoryDestination,
    opens: std::sync::atomic::AtomicU32,
}

#[async_trait]
impl Destination for ThirdOpenFails {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("third-open-fails", "0.0.0")
    }

    fn capabilities(&self) -> DestinationCapabilities {
        self.inner.capabilities()
    }

    async fn open(&self, ctx: OpenContext) -> Result<Box<dyn LoadSession>, DestinationError> {
        let n = self.opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if n >= 2 {
            return Err(DestinationError::fatal("re-open refused"));
        }
        self.inner.open(ctx).await
    }
}

#[tokio::test]
async fn an_abort_at_the_reopen_renders_d5_unreached_not_pass() {
    let dest = ThirdOpenFails {
        inner: MemoryDestination::new(),
        opens: std::sync::atomic::AtomicU32::new(0),
    };
    let outcome = verify_destination(&dest, &NoProbe).await;
    let (failures, skips) = outcome.tolerating_skips();
    assert!(
        failures
            .iter()
            .any(|f| f.clause == "D4" && f.message.starts_with("re-open failed: ")),
        "the abort lands at D4's re-open: {failures:?}"
    );
    assert!(
        skips.iter().any(|s| s.clause == "D5"),
        "D5's second check never ran — it must render unreached, not PASS: {skips:?}"
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
    let (failures, _skips) = verify_source(&TeardownFailingSource)
        .await
        .tolerating_skips();
    assert!(
        failures
            .iter()
            .any(|f| f.clause == "S1" && f.message.contains("teardown failed")),
        "the post-drop error must fail certification, got: {failures:?}"
    );
}

/// Round-9 fix: `try_step!`'s abort must still CLOSE the session the
/// host holds — a lease-class destination (the file destination's
/// shape) releases its session-scoped lease in `close()`, which a plain
/// drop cannot perform, so an abort that skipped the close left the
/// lease held for the life of the certifying process. The destination
/// here records whether its most recent session was closed; the probe
/// errors at its THIRD count (D3's re-commit read-back), aborting the
/// suite while `session2` is live.
struct CloseRecordingDest {
    inner: MemoryDestination,
    last_session_closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

struct CloseRecordingSession {
    inner: Box<dyn LoadSession>,
    closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait]
impl Destination for CloseRecordingDest {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("close-recording", "0.0.0")
    }

    fn capabilities(&self) -> DestinationCapabilities {
        self.inner.capabilities().with_merge(false)
    }

    async fn open(&self, ctx: OpenContext) -> Result<Box<dyn LoadSession>, DestinationError> {
        self.last_session_closed
            .store(false, std::sync::atomic::Ordering::SeqCst);
        Ok(Box::new(CloseRecordingSession {
            inner: self.inner.open(ctx).await?,
            closed: std::sync::Arc::clone(&self.last_session_closed),
        }))
    }
}

#[async_trait]
impl LoadSession for CloseRecordingSession {
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

    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        self.inner.commit(meta).await
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        self.inner.read_state(pipeline).await
    }

    async fn close(&mut self) -> Result<(), DestinationError> {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
        self.inner.close().await
    }
}

/// Fails at its `n`th count, delegating every earlier one to the real
/// read-back.
struct FailsAtCount {
    inner: MemoryDestination,
    calls: std::sync::atomic::AtomicU32,
    fail_at: u32,
}

#[async_trait]
impl TableProbe for FailsAtCount {
    async fn count(&self, table: &TableName) -> Result<u64, ProbeError> {
        let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        if call == self.fail_at {
            return Err(ProbeError {
                message: "probe died mid-suite".into(),
            });
        }
        Ok(self.inner.committed_rows(table.as_str()).len() as u64)
    }
}

#[tokio::test]
async fn an_abort_mid_suite_still_closes_the_held_session() {
    let inner = MemoryDestination::new();
    let last_session_closed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let dest = CloseRecordingDest {
        inner: inner.clone(),
        last_session_closed: std::sync::Arc::clone(&last_session_closed),
    };
    // Counts run D1 → D4 → D3: the third is D3's re-commit read-back,
    // with `session2` open and carrying committed work.
    let probe = FailsAtCount {
        inner,
        calls: std::sync::atomic::AtomicU32::new(0),
        fail_at: 3,
    };

    let (failures, _skips) = verify_destination(&dest, &probe).await.tolerating_skips();

    assert!(
        failures
            .iter()
            .any(|f| f.clause == "D3" && f.message.contains("probe died mid-suite")),
        "the abort lands at D3's count: {failures:?}"
    );
    assert!(
        last_session_closed.load(std::sync::atomic::Ordering::SeqCst),
        "the abort path must close the session it holds — a lease-class destination \
         releases its lease in close(), which the drop the abort used to take cannot do"
    );
}
