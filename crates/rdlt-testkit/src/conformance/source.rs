//! Source conformance. Asserted clauses — EXACTLY these three, no more:
//!
//! - **S1** the resume law: for every checkpoint `c`,
//!   `full_read == rows_covered_by(c) ++ read(since = c)`.
//! - **S2** checkpoint coverage: a stream that never checkpoints cannot
//!   be certified for resume and fails by name — unless it declares no
//!   `cursor_field` at all: an honestly-declared snapshot stream earns
//!   an S2 skip with the reason instead, because its declaration said
//!   up front that there is no resume to certify.
//! - **S4** cancellation: a closed channel means stop-promptly-with-Ok,
//!   never an error, never a hang.
//!
//! Verified black-box against any deterministic [`Source`]. The remaining
//! source clauses have no check here yet; adding one is deferred work,
//! renumbering these is forbidden.

use rdlt_connector::{Cursor, PushPayload, ReadRequest, Source, StreamSpec, records_channel};
use serde_json::Value;

use super::{Conformance, ConformanceFailure, ConformanceSkip};

/// This suite's verdict shape — the shared [`Conformance`]
/// (`failures` + `skips` + the strict `expecting_no_skips` fold),
/// named for the suite that produced it. Today only the S2 snapshot
/// door mints its skips.
pub type SourceConformance = Conformance;

/// Every clause this suite asserts, in module-doc order — THE one
/// clause list (the destination suite's `ASSERTED_CLAUSES` precedent):
/// the terminal conclusion derives from it rather than from a second
/// inline copy, so a clause added to the suite cannot be forgotten in
/// one place and fold as NOT-REACHED at certify for conformant
/// connectors.
pub const ASSERTED_CLAUSES: [&str; 3] = ["S1", "S2", "S4"];

/// Byte budget for the harness's record channel — large enough that a
/// well-behaved source never blocks on backpressure while being certified.
const CHANNEL_BYTE_BUDGET: usize = 16 << 20;

/// The most one read may RETAIN, across everything it observes. The
/// harness keeps every row and checkpoint of a full read because S1 is
/// a CONTENT law — `full_read == rows_covered_by(c) ++ read(since=c)`
/// — and streaming-and-discarding would degrade it to a count check,
/// gutting the clause for every source; retention is the price of the
/// assertion. The channel budget above bounds only what is IN FLIGHT,
/// so without this ceiling a flooding source OOMs the harness instead
/// of failing. A conformance fixture is rows, not a dataset: 64 MiB
/// (four channel budgets) is generous headroom, and a source pushing
/// more fails by name. Arrow rows are charged by their retained opaque
/// values; raw JSON is conservatively expansion-weighted before parse.
const RETENTION_CEILING_BYTES: usize = 64 << 20;

/// Compact JSON expands by well over an order of magnitude when
/// materialized as `serde_json::Value`s. Debit a deliberately
/// conservative factor before parsing so the ceiling also bounds the
/// parser's transient allocation, not only the retained result.
///
/// The arithmetic behind 64, against a glibc-class allocator: a dense
/// scalar array (`[0,0,0,…]`) costs ~2 wire bytes per element and
/// materializes one 32-byte `Value` per element in the root `Vec` —
/// and while that `Vec` doubles, the old and new allocations coexist,
/// so the transient peak reaches ~3 slots' worth ≈ 96 bytes per
/// element ≈ 48× the wire size. Chains of 1-element arrays
/// (`[[[…]]]`, bounded by serde_json's 128-level recursion limit) cost
/// ~2 wire bytes per level against a 32-byte `Value` slot plus its
/// own 1-element `Vec` heap block, which the allocator rounds with
/// header overhead to ~48 bytes — ~40× all told. 64 covers both with
/// headroom, and over-counting is the safe direction here: it refuses
/// an oversized fixture early, where an under-count OOMs the harness.
const RAW_JSON_RETENTION_FACTOR: usize = 64;

/// What one full read produced: row groups separated by checkpoints.
#[derive(Debug, Default)]
struct Observed {
    /// (rows pushed since the previous checkpoint, checkpoint that closed
    /// the group).
    groups: Vec<(Vec<Value>, Option<Cursor>)>,
}

impl Observed {
    /// Every observed row, BY REFERENCE: the S1 fold compares reads
    /// against each other, and cloning whole reads to do it held ~3-4×
    /// the retention ceiling in peak (full + resumed + a materialized
    /// expectation + this clone). Reference vectors keep the peak at
    /// the two reads' own retention plus pointer-sized bookkeeping.
    fn all_rows(&self) -> Vec<&Value> {
        self.groups
            .iter()
            .flat_map(|(rows, _)| rows.iter())
            .collect()
    }

    fn checkpoints(&self) -> Vec<Cursor> {
        self.groups.iter().filter_map(|(_, c)| c.clone()).collect()
    }

    /// Rows covered by (pushed before) checkpoint `cursor` — by
    /// reference, like [`Observed::all_rows`].
    fn rows_covered_by(&self, cursor: &Cursor) -> Vec<&Value> {
        let mut covered = Vec::new();
        for (rows, checkpoint) in &self.groups {
            covered.extend(rows.iter());
            if checkpoint.as_ref() == Some(cursor) {
                return covered;
            }
        }
        covered
    }
}

/// The overall bound on ONE conformance read: a source that parks
/// inside `read()` without ever pushing would otherwise hang a direct
/// harness caller forever — the 5 s post-drain join bound below only
/// covers a source that already dropped its output handle. Generous on
/// purpose (a conformance fixture reads in milliseconds), and
/// deliberately ABOVE the certifier's own 30 s per-clause bound: when
/// the harness runs under the certifier, that outer bound must keep
/// firing first with its own framing.
const READ_ALL_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);

async fn read_all<S: Source>(
    source: &S,
    spec: &StreamSpec,
    since: Option<Cursor>,
) -> Result<Observed, String> {
    read_all_within(source, spec, since, READ_ALL_DEADLINE).await
}

/// [`read_all`] with the deadline as a parameter — the seam that lets
/// the deadline's own pin run in test time rather than sixty seconds.
async fn read_all_within<S: Source>(
    source: &S,
    spec: &StreamSpec,
    since: Option<Cursor>,
    deadline: std::time::Duration,
) -> Result<Observed, String> {
    match tokio::time::timeout(deadline, read_all_unbounded(source, spec, since)).await {
        Ok(result) => result,
        Err(_) => Err(format!(
            "source did not complete read() within the {}s conformance deadline — the whole \
             read phase is bounded so a source parked in read() fails by name instead of \
             hanging the harness",
            deadline.as_secs()
        )),
    }
}

async fn read_all_unbounded<S: Source>(
    source: &S,
    spec: &StreamSpec,
    since: Option<Cursor>,
) -> Result<Observed, String> {
    let (out, mut input) = records_channel(CHANNEL_BYTE_BUDGET);
    let request = ReadRequest::new(spec.clone(), since, out);
    let reader = source.read(request);
    tokio::pin!(reader);

    let mut observed = Observed::default();
    let mut current: Vec<Value> = Vec::new();
    let mut read_result: Option<Result<(), String>> = None;
    let mut retained: usize = 0;
    loop {
        tokio::select! {
            push = input.recv() => match push {
                Some(push) => {
                    retained = retained.saturating_add(match &push.payload {
                        PushPayload::RawJson(bytes) => bytes
                            .len()
                            .saturating_mul(RAW_JSON_RETENTION_FACTOR),
                        PushPayload::Arrow(batch) => batch
                            .num_rows()
                            .saturating_mul(std::mem::size_of::<Value>()),
                        PushPayload::Checkpoint(cursor) => serde_json::to_vec(cursor.as_value())
                            .expect("a cursor Value serializes infallibly")
                            .len()
                            .saturating_mul(RAW_JSON_RETENTION_FACTOR),
                    });
                    if retained > RETENTION_CEILING_BYTES {
                        return Err(format!(
                            "the source exceeded the {RETENTION_CEILING_BYTES}-byte conservative \
                             retained-row budget — raw JSON is charged at \
                             {RAW_JSON_RETENTION_FACTOR}× its wire size before parsing because \
                             serde_json values expand in memory; a conformance fixture must stay \
                             well inside that ceiling"
                        ));
                    }
                    match push.payload {
                        PushPayload::RawJson(bytes) => {
                            for doc in serde_json::Deserializer::from_slice(&bytes).into_iter::<Value>() {
                                match doc.map_err(|e| format!("source pushed invalid JSON: {e}"))? {
                                    Value::Array(items) => current.extend(items),
                                    value => current.push(value),
                                }
                            }
                        }
                        PushPayload::Arrow(batch) => {
                            // Arrow-pushing sources degrade the row comparison to
                            // COUNTS: each row becomes an opaque Null, so the
                            // resume law is certified on cardinality, not content.
                            current.extend((0..batch.num_rows()).map(|_| Value::Null));
                        }
                        PushPayload::Checkpoint(cursor) => {
                            observed.groups.push((std::mem::take(&mut current), Some(cursor)));
                        }
                    }
                }
                None => break,
            },
            result = &mut reader, if read_result.is_none() => {
                read_result = Some(result.map_err(|e| format!("source read failed: {e}")));
            }
        }
    }
    if !current.is_empty() {
        observed.groups.push((std::mem::take(&mut current), None));
    }
    // The channel drains the moment every sender drops — which can happen
    // BEFORE the read future returns (a source that moves its handle out,
    // drops it after the last push, and then fails in teardown). Dropping
    // the future here would cancel that teardown and certify the failure
    // away, so the verdict waits for the read to actually finish — bounded,
    // matching the suite's S4 posture, so a source that hangs after
    // dropping its handle surfaces as a named failure.
    let read_result = match read_result {
        Some(result) => result,
        None => tokio::time::timeout(std::time::Duration::from_secs(5), &mut reader)
            .await
            .map_err(|_| "source did not return after dropping its output handle".to_owned())
            .and_then(|result| result.map_err(|e| format!("source read failed: {e}"))),
    };
    read_result?;
    Ok(observed)
}

/// Run the full source conformance suite (clauses S1/S2/S4 — see the
/// module doc). The source must be deterministic (same data on every
/// uncursored read) and should declare at least one stream with
/// checkpoints — resume clauses cannot be certified without them (a
/// stream declaring no `cursor_field` is the recorded exception: it
/// skips S2 honestly rather than failing it).
///
/// For a source that pushes Arrow batches, the S1 row comparison degrades
/// to row COUNTS (payload content is opaque to the harness); JSON-pushing
/// sources are certified on full row content.
pub async fn verify_source<S: Source>(source: &S) -> SourceConformance {
    let mut failures = Vec::new();
    let mut skips = Vec::new();
    let fail = |clause, message: String| ConformanceFailure { clause, message };

    let streams = match source.streams().await {
        Ok(streams) => streams,
        Err(e) => {
            failures.push(fail("S1", format!("streams() failed: {e}")));
            return nothing_concluded(failures, skips);
        }
    };
    if streams.is_empty() {
        failures.push(fail("S1", "source declares no streams".into()));
        return nothing_concluded(failures, skips);
    }

    // S2's checks — the snapshot door and the resume-law replays — live
    // entirely inside a stream's Ok(full) arm below, so a stream whose
    // baseline read fails skips them wholesale: S2 concludes only when
    // they executed for EVERY stream. (S1 reaches a verdict either way —
    // the failed read IS its failure — and S4 runs unconditionally per
    // stream.)
    let mut s2_ran_for_every_stream = true;
    for spec in &streams {
        // Baseline full read, feeding the resume law. S4 below runs for
        // EVERY stream regardless of this block's outcome — cancellation
        // behavior is independent of whether the stream reads or
        // checkpoints, and skipping it here would under-report a
        // non-conformant stream's failures (generation 1 did exactly
        // that, pinned by `both_s2_and_s4_are_reported_independently`).
        match read_all(source, spec, None).await {
            Err(e) => {
                failures.push(fail("S1", format!("stream `{}`: {e}", spec.name)));
                // The whole Ok(full) arm is skipped for this stream, so
                // S2 must not conclude: concluding would let a consumer
                // mint "PASS S2" from silence while the failed read left
                // its checks unexecuted (the silence-certified-as-pass
                // class, one level below the fold that refuses it).
                s2_ran_for_every_stream = false;
            }
            Ok(full) => {
                let checkpoints = full.checkpoints();
                if checkpoints.is_empty() {
                    // The snapshot door: the door turns on the
                    // DECLARATION, not on the missing checkpoints — a
                    // stream declaring no cursor field said up front it
                    // cannot resume, and the read agreed, so there is
                    // nothing to certify and nothing violated. A
                    // declared cursor that never checkpoints is the
                    // broken promise S2 exists to catch.
                    if spec.cursor_field.is_none() {
                        skips.push(ConformanceSkip {
                            clause: "S2",
                            reason: format!(
                                "stream `{}` declares no cursor_field and never checkpoints — \
                                 an honest snapshot stream: there is no resume to certify, and \
                                 every run re-reads everything",
                                spec.name
                            ),
                        });
                    } else {
                        failures.push(fail(
                            "S2",
                            format!(
                                "stream `{}` never checkpoints — resume (S1) cannot be certified \
                                 and every restart re-reads everything",
                                spec.name
                            ),
                        ));
                    }
                }

                // S1 + S2 as one law: for every checkpoint c,
                //   full_read == rows_covered_by(c) ++ read(since = c).
                for cursor in &checkpoints {
                    let resumed = match read_all(source, spec, Some(cursor.clone())).await {
                        Ok(observed) => observed,
                        Err(e) => {
                            failures.push(fail(
                                "S1",
                                format!("stream `{}` resume from {cursor:?}: {e}", spec.name),
                            ));
                            continue;
                        }
                    };
                    let mut expected = full.rows_covered_by(cursor);
                    expected.extend(resumed.all_rows());
                    if expected != full.all_rows() {
                        failures.push(fail(
                            "S1",
                            format!(
                                "stream `{}`: read(since={cursor:?}) must emit exactly the rows \
                                 after that checkpoint — prefix+resume ({} rows) != full read \
                                 ({} rows, or content differs)",
                                spec.name,
                                expected.len(),
                                full.all_rows().len(),
                            ),
                        ));
                    }
                }
            }
        }

        // S4: a closed channel means cancellation — return promptly, without
        // error.
        let (out, mut input) = records_channel(CHANNEL_BYTE_BUDGET);
        input.close();
        drop(input);
        let request = ReadRequest::new(spec.clone(), None, out);
        match tokio::time::timeout(std::time::Duration::from_secs(5), source.read(request)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => failures.push(fail(
                "S4",
                format!(
                    "stream `{}`: a closed channel is cancellation, not an error — got {e}",
                    spec.name
                ),
            )),
            Err(_) => failures.push(fail(
                "S4",
                format!(
                    "stream `{}`: source hung >5s on a closed channel",
                    spec.name
                ),
            )),
        }
    }

    // Completion concludes a clause only where its checks ran for EVERY
    // stream: S1 reaches a verdict per stream even on a failed read, S4
    // runs unconditionally per stream, and S2 is withheld when a failed
    // baseline read skipped its checks above.
    SourceConformance {
        failures,
        skips,
        concluded: ASSERTED_CLAUSES
            .into_iter()
            .filter(|clause| *clause != "S2" || s2_ran_for_every_stream)
            .collect(),
    }
}

/// The early-return shape for a suite that never got past stream
/// discovery: `concluded` EMPTY deliberately — no clause's checks ran.
/// S1 carries the discovery failure, and S2/S4 must read as
/// never-reached to a consumer, not as silently passed.
fn nothing_concluded(
    failures: Vec<ConformanceFailure>,
    skips: Vec<ConformanceSkip>,
) -> SourceConformance {
    SourceConformance {
        failures,
        skips,
        concluded: Vec::new(),
    }
}

#[cfg(test)]
mod retention_tests {
    use super::*;

    #[test]
    fn compact_json_is_refused_before_a_large_value_graph_can_materialize() {
        let largest_preparse_payload = RETENTION_CEILING_BYTES / RAW_JSON_RETENTION_FACTOR;
        assert_eq!(largest_preparse_payload, 1 << 20);
        assert!(
            largest_preparse_payload
                .saturating_add(1)
                .saturating_mul(RAW_JSON_RETENTION_FACTOR)
                > RETENTION_CEILING_BYTES
        );
    }
}

#[cfg(test)]
mod deadline_tests {
    use rdlt_connector::{ConnectorSpec, ReadRequest, SourceError};

    use super::*;

    /// A source that parks inside `read()` forever without pushing or
    /// dropping its handle — the shape the overall deadline exists for:
    /// the 5 s post-drain bound never fires because the channel never
    /// drains.
    struct ParkedSource;

    #[async_trait::async_trait]
    impl Source for ParkedSource {
        fn spec(&self) -> ConnectorSpec {
            ConnectorSpec::new("parked", "0.0.0")
        }

        async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
            Ok(vec![StreamSpec::new("parked")])
        }

        async fn read(&self, _req: ReadRequest) -> Result<(), SourceError> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn a_source_parked_in_read_fails_the_deadline_by_name() {
        let failed = read_all_within(
            &ParkedSource,
            &StreamSpec::new("parked"),
            None,
            std::time::Duration::from_millis(50),
        )
        .await
        .expect_err("a parked read must fail the deadline, not hang");
        assert!(
            failed.contains("did not complete read() within"),
            "the failure names the read phase: {failed}"
        );
    }
}
