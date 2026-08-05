//! Destination conformance. Asserted clauses — EXACTLY these seven, no
//! more:
//!
//! - **D1** staging invisibility: rows written but not committed are not
//!   reader-visible.
//! - **D2** atomic state: `commit` persists `meta.state` with the data;
//!   `read_state` returns the committed cursor.
//! - **D3** idempotent commits: re-committing the same
//!   `(load_id, commit_seq)` returns the prior receipt and re-publishes
//!   nothing.
//! - **D4** staging teardown: a new session makes a dead predecessor's
//!   staged rows invisible; only the new session's rows publish.
//! - **D5** idempotent `ensure_table`.
//! - **D6** fresh pipelines have no state.
//! - **D8** merge upserts by `_rdlt_id` (asserted only when the
//!   destination declares the merge capability).
//!
//! Verified black-box through the SPI plus one author-supplied
//! [`TableProbe`] — row counting is the only thing the SPI itself cannot
//! do. D7 has no check here yet; adding one is deferred work, renumbering
//! is forbidden.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use rdlt_connector::{
    CommitMeta, Destination, OpenContext, WriteMode,
    core::{
        ColumnDef, ColumnType, CommitCounters, Cursor, LoadId, LogicalType, PipelineId, Provenance,
        StateDoc, StreamName, TableName, TableSchema, schema::system_columns,
    },
};

use super::ConformanceFailure;
use crate::fixtures;

/// The one capability the SPI cannot provide: counting reader-VISIBLE
/// rows in a table (a warehouse query). Implement per destination under
/// test.
#[async_trait]
pub trait TableProbe: Send + Sync {
    /// Rows a reader of `table` would see right now.
    async fn count(&self, table: &TableName) -> u64;
}

/// The suite's three-column logical fixture: two system columns and one
/// value column, enough for every clause including merge identity.
fn fixture_schema(table: &str) -> TableSchema {
    let col = |name: &str, ty, provenance| ColumnDef {
        name: name.to_owned(),
        column_type: ColumnType::scalar(ty),
        nullable: false,
        provenance,
    };
    TableSchema {
        table: TableName::new(table),
        parent: None,
        columns: vec![
            col(
                system_columns::LOAD_ID,
                LogicalType::Utf8,
                Provenance::System,
            ),
            col(system_columns::ID, LogicalType::Utf8, Provenance::System),
            col("v", LogicalType::Int64, Provenance::Inferred),
        ],
    }
}

/// A batch over [`fixture_schema`]'s columns, filled positionally. The
/// Arrow schema comes from the crate's one logical→Arrow derivation
/// ([`fixtures::arrow_schema`]) so the two shapes cannot drift apart.
fn fixture_batch(load_id: &str, ids: &[&str], values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(fixtures::arrow_schema(&fixture_schema("_"))),
        vec![
            Arc::new(StringArray::from(vec![load_id; ids.len()])),
            Arc::new(StringArray::from(ids.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
        ],
    )
    .expect("fixture batch")
}

fn commit_meta(
    pipeline: &PipelineId,
    load_id: &LoadId,
    seq: u64,
    cursor: Option<i64>,
) -> CommitMeta {
    let mut state = StateDoc::new(pipeline.clone(), env!("CARGO_PKG_VERSION"));
    if let Some(c) = cursor {
        state
            .cursors
            .insert(StreamName::new("conf_stream"), Cursor::new(c));
    }
    CommitMeta {
        load_id: load_id.clone(),
        commit_seq: seq,
        state,
        counters: CommitCounters::default(),
    }
}

/// Run the destination conformance suite (clauses D1–D6 and D8 — see the
/// module doc). Uses tables prefixed `rdlt_conf_` — point it at a scratch
/// dataset.
pub async fn verify_destination<D: Destination>(
    dest: &D,
    probe: &dyn TableProbe,
) -> Vec<ConformanceFailure> {
    let mut failures = Vec::new();
    let fail = |clause: &'static str, message: String| ConformanceFailure { clause, message };

    // Run a fallible SPI step; on error, record the clause failure
    // (message `"{prefix}: {error}"`) and return the failures gathered so
    // far. The clause id and prefix are the diagnostic the connector
    // author reads, so they are spelled out verbatim at each call site.
    macro_rules! try_step {
        ($clause:expr, $prefix:expr, $step:expr $(,)?) => {
            match $step {
                Ok(value) => value,
                Err(e) => {
                    failures.push(fail($clause, format!("{}: {e}", $prefix)));
                    return failures;
                }
            }
        };
    }

    let pipeline = PipelineId::new("rdlt-conformance");
    let load_a = LoadId::new("conf-load-a");
    let table = TableName::new("rdlt_conf_t");
    let schema = fixture_schema("rdlt_conf_t");

    // ---- D6: a fresh pipeline has no state ----
    // Setup failures carry the clause they are setting up (here D6, below
    // D1) — generation 1 labelled every open "D4" and sent an author whose
    // open fails to investigate teardown semantics that were never
    // reached.
    let mut session = try_step!(
        "D6",
        "open failed",
        dest.open(OpenContext::new(
            PipelineId::new("rdlt-conf-fresh"),
            load_a.clone()
        ))
        .await
    );
    match session
        .read_state(&PipelineId::new("rdlt-conf-fresh"))
        .await
    {
        Ok(None) => {}
        Ok(Some(_)) => failures.push(fail(
            "D6",
            "read_state returned state for a never-committed pipeline".into(),
        )),
        Err(e) => failures.push(fail("D6", format!("read_state failed: {e}"))),
    }
    // Best-effort, unclaused — same reasoning as `session2`'s close at
    // the very end of this function (037 US2 fix round 2, M3): the kit
    // is a HOST, and a well-behaved host closes every session it opens,
    // not only its last one.
    let _ = session.close().await;

    // ---- D1: staged writes are invisible before commit ----
    let mut session1 = try_step!(
        "D1",
        "open failed",
        dest.open(OpenContext::new(pipeline.clone(), load_a.clone()))
            .await
    );
    // D5: ensure_table is idempotent.
    for attempt in 0..2 {
        try_step!(
            "D5",
            format!("ensure_table attempt {attempt}"),
            session1.ensure_table(&schema, &WriteMode::Append).await
        );
    }
    try_step!(
        "D1",
        "write failed",
        session1
            .write(&table, fixture_batch("conf-load-a", &["r1", "r2"], &[1, 2]))
            .await
    );
    if probe.count(&table).await != 0 {
        failures.push(fail(
            "D1",
            "rows written but not committed are reader-visible (staging must be invisible)".into(),
        ));
    }

    // ---- D4: a new session tears down the previous session's staged data ----
    drop(session1);
    let mut session2 = try_step!(
        "D4",
        "re-open failed",
        dest.open(OpenContext::new(pipeline.clone(), load_a.clone()))
            .await
    );
    try_step!(
        "D5",
        "ensure_table on new session",
        session2.ensure_table(&schema, &WriteMode::Append).await
    );
    try_step!(
        "D4",
        "write on new session",
        session2
            .write(&table, fixture_batch("conf-load-a", &["r3"], &[3]))
            .await
    );
    let receipt1 = try_step!(
        "D2",
        "commit failed",
        session2
            .commit(commit_meta(&pipeline, &load_a, 1, Some(10)))
            .await
    );
    let after_first_commit = probe.count(&table).await;
    if after_first_commit != 1 {
        failures.push(fail(
            "D4",
            format!(
                "expected exactly the new session's 1 row visible after commit, found \
                 {after_first_commit} — orphaned staged data from the dead session leaked in"
            ),
        ));
    }

    // ---- D3: re-committing the same (load_id, commit_seq) is a no-op with
    // the prior receipt ----
    match session2
        .commit(commit_meta(&pipeline, &load_a, 1, Some(10)))
        .await
    {
        Ok(receipt2) => {
            if receipt2 != receipt1 {
                failures.push(fail(
                    "D3",
                    format!("re-commit returned a different receipt: {receipt2:?} vs {receipt1:?}"),
                ));
            }
        }
        Err(e) => failures.push(fail("D3", format!("idempotent re-commit errored: {e}"))),
    }
    if probe.count(&table).await != after_first_commit {
        failures.push(fail("D3", "re-commit re-published data".into()));
    }

    // ---- D2: state persists atomically with the data ----
    match session2.read_state(&pipeline).await {
        Ok(Some(state)) => {
            let cursor = state.cursors.get(&StreamName::new("conf_stream"));
            if cursor != Some(&Cursor::new(10)) {
                failures.push(fail(
                    "D2",
                    format!("committed cursor not returned by read_state (got {cursor:?})"),
                ));
            }
        }
        Ok(None) => failures.push(fail("D2", "state missing after successful commit".into())),
        Err(e) => failures.push(fail("D2", format!("read_state failed: {e}"))),
    }

    // ---- D8: merge replaces by _rdlt_id (only when the capability is
    // declared) ----
    if dest.capabilities().merge {
        let merge_table = TableName::new("rdlt_conf_merge");
        let merge_schema = fixture_schema("rdlt_conf_merge");
        let mode = WriteMode::Merge {
            key: vec!["v".into()],
        };
        let outcome: Result<(), String> = async {
            session2
                .ensure_table(&merge_schema, &mode)
                .await
                .map_err(|e| e.to_string())?;
            session2
                .write(
                    &merge_table,
                    fixture_batch("conf-load-a", &["k1", "k2"], &[1, 2]),
                )
                .await
                .map_err(|e| e.to_string())?;
            session2
                .commit(commit_meta(&pipeline, &load_a, 2, None))
                .await
                .map_err(|e| e.to_string())?;
            // Same _rdlt_id `k1` again with new content: must replace, not
            // append.
            session2
                .write(&merge_table, fixture_batch("conf-load-b", &["k1"], &[99]))
                .await
                .map_err(|e| e.to_string())?;
            session2
                .commit(commit_meta(&pipeline, &load_a, 3, None))
                .await
                .map_err(|e| e.to_string())?;
            Ok(())
        }
        .await;
        match outcome {
            Ok(()) => {
                let count = probe.count(&merge_table).await;
                if count != 2 {
                    failures.push(fail(
                        "D8",
                        format!(
                            "merge on an existing _rdlt_id must upsert: expected 2 rows, found {count}"
                        ),
                    ));
                }
            }
            Err(e) => failures.push(fail("D8", format!("merge flow failed: {e}"))),
        }
    }

    // The kit is itself a HOST: `session2` carried every commit from D2
    // onward, and a well-behaved host closes a session that completed
    // its last commit (037 US2 T7 fix round 1 — the SPI's `close`
    // contract). Best-effort and unclaused deliberately: no clause here
    // certifies `close` itself (a destination with nothing to release
    // on close, which is most of them via the default impl, has
    // nothing to fail), so a close error is not turned into a new
    // failure the negative-test suite would need to account for — it
    // would only ever surface a destination-specific bug the existing
    // clauses cannot name anyway. What this closes is the resource
    // leak: without it, every certified destination's LAST conformance
    // session would stay open (a real cost for one that holds a
    // session-scoped lock, like the file destination's lease) for the
    // life of the process running this suite.
    let _ = session2.close().await;

    failures
}
