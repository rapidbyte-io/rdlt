//! Destination conformance (clauses D1–D8): staging invisibility, atomic state,
//! idempotent commits, staging teardown — verified black-box through the SPI plus one
//! author-supplied probe (row counting is the only thing the SPI itself can't do).

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use rdlt_connector::{
    CommitMeta, Destination, OpenCtx, WriteMode,
    core::{
        ColumnDef, ColumnType, CommitCounters, Cursor, LoadId, LogicalType, PipelineId, Provenance,
        StateDoc, StreamName, TableName, TableSchema, schema::system_columns,
    },
};

use super::ConformanceFailure;

/// The one capability the SPI cannot provide: counting reader-VISIBLE rows in a
/// table (a warehouse query). Implement per destination under test.
#[async_trait]
pub trait TableProbe: Send + Sync {
    async fn count(&self, table: &TableName) -> u64;
}

fn fixture_schema(table: &str) -> TableSchema {
    let col = |name: &str, ty, provenance| ColumnDef {
        name: name.to_owned(),
        ty: ColumnType::scalar(ty),
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

fn fixture_batch(load_id: &str, ids: &[&str], values: &[i64]) -> RecordBatch {
    // Derive the Arrow schema from the logical fixture schema so the two column lists
    // cannot drift apart; the arrays below fill those columns positionally.
    let logical = fixture_schema("_");
    let schema = Schema::new(logical.columns.iter().map(arrow_field).collect::<Vec<_>>());
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(StringArray::from(vec![load_id; ids.len()])),
            Arc::new(StringArray::from(ids.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
        ],
    )
    .expect("fixture batch")
}

/// Map a fixture column to its Arrow field. Only the scalar types the fixture schema
/// uses are handled — any other type is a bug in the fixture, not a runtime input.
fn arrow_field(column: &ColumnDef) -> Field {
    let data_type = match &column.ty {
        ColumnType::Scalar {
            scalar: LogicalType::Utf8,
        } => DataType::Utf8,
        ColumnType::Scalar {
            scalar: LogicalType::Int64,
        } => DataType::Int64,
        other => unreachable!("fixture schema uses only Utf8/Int64 columns, got {other:?}"),
    };
    Field::new(column.name.clone(), data_type, column.nullable)
}

fn meta(pipeline: &PipelineId, load_id: &LoadId, seq: u64, cursor: Option<i64>) -> CommitMeta {
    let mut state = StateDoc::new(pipeline.clone());
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

/// Run the destination conformance suite. Uses tables prefixed `rdlt_conf_` — point
/// it at a scratch dataset.
pub async fn verify_destination<D: Destination>(
    dest: &D,
    probe: &dyn TableProbe,
) -> Vec<ConformanceFailure> {
    let mut failures = Vec::new();
    let fail = |clause: &'static str, message: String| ConformanceFailure { clause, message };

    // Run a fallible SPI step; on error, record the clause failure (message
    // `"{prefix}: {error}"`) and return the failures gathered so far. The clause id
    // and prefix are the diagnostic the connector author reads, so they are spelled
    // out verbatim at each call site.
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
    let mut session = try_step!(
        "D4",
        "open failed",
        dest.open(OpenCtx::new(
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

    // ---- D1: staged writes are invisible before commit ----
    let mut session1 = try_step!(
        "D4",
        "open failed",
        dest.open(OpenCtx::new(pipeline.clone(), load_a.clone()))
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
        dest.open(OpenCtx::new(pipeline.clone(), load_a.clone()))
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
        session2.commit(meta(&pipeline, &load_a, 1, Some(10))).await
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

    // ---- D3: re-committing the same (load_id, commit_seq) is a no-op with the
    // prior receipt ----
    match session2.commit(meta(&pipeline, &load_a, 1, Some(10))).await {
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

    // ---- D8: merge replaces by _rdlt_id (only when the capability is declared) ----
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
                .commit(meta(&pipeline, &load_a, 2, None))
                .await
                .map_err(|e| e.to_string())?;
            // Same _rdlt_id `k1` again with new content: must replace, not append.
            session2
                .write(&merge_table, fixture_batch("conf-load-b", &["k1"], &[99]))
                .await
                .map_err(|e| e.to_string())?;
            session2
                .commit(meta(&pipeline, &load_a, 3, None))
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
                        format!("merge on an existing _rdlt_id must upsert: expected 2 rows, found {count}"),
                    ));
                }
            }
            Err(e) => failures.push(fail("D8", format!("merge flow failed: {e}"))),
        }
    }

    failures
}
