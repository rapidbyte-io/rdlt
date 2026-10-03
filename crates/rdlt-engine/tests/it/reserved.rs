//! Source columns that ask for the name of a metadata column the engine writes: refused when
//! they arrive, whatever the stream's mode then, so no later mode finds its metadata column
//! taken.

use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch};
use rdlt_connector::{Destination, IdentifierCase, ReadMode};
use rdlt_engine::{
    ErrorKind, LocalWal, Nested, RunOutcome, RunStatus, SchemaSettings, StreamPlan, WalStore,
    WriteMode,
};

use crate::support::batches::{BatchStream, batches};
use crate::support::destinations::limited;
use crate::support::{commit_every, logging_engine, memory, pipeline, stream};

/// Checks that `outcome` failed on a column asking for a metadata column's name, loading
/// nothing.
fn refused(outcome: &RunOutcome) {
    assert_eq!(outcome.report.status, RunStatus::Failed);
    let error = outcome.error.as_ref().expect("the run fails");
    assert_eq!(
        (error.kind(), error.code(), error.is_retryable()),
        (ErrorKind::Schema, Some("column_name_reserved"), false),
        "{error}"
    );
    assert_eq!(outcome.report.rows, 0);
}

/// The memory destination at `store`, folding identifiers to `case`.
async fn folding(store: &str, case: IdentifierCase) -> Arc<dyn Destination> {
    limited(memory(store).await, move |capabilities| {
        capabilities.identifiers.case = case;
    })
}

/// A batch of one row: `id`, and a column `name` of 16 bytes.
fn binary(name: &str, id: i64) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![id]));
    let bytes: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&[0xff_u8; 16][..])]));
    RecordBatch::try_from_iter([("id", ids), (name, bytes)]).expect("a batch")
}

/// The modes a stream may switch to once its table exists, each needing metadata columns an
/// appending table lacks.
fn modes() -> Vec<(&'static str, StreamPlan)> {
    vec![
        ("append", stream("events")),
        (
            "merge",
            stream("events")
                .read(ReadMode::Incremental)
                .write(WriteMode::Merge)
                .key(["id"]),
        ),
        (
            "normalize",
            stream("events")
                .schema(SchemaSettings::new().nested(Nested::normalize()))
                .write(WriteMode::Replace),
        ),
        (
            "history",
            stream("events")
                .read(ReadMode::Incremental)
                .write(WriteMode::History)
                .key(["id"]),
        ),
    ]
}

#[tokio::test(start_paused = true)]
async fn a_column_asking_for_a_metadata_name_is_refused_before_any_mode_switch() {
    let names = [
        "_rdlt_id",
        "_rdlt_seq",
        "_rdlt_load_id",
        "_rdlt_valid_from",
        "_rdlt_deleted_at",
        "_rdlt_idx",
    ];
    for name in names {
        let store = format!("reserved_{name}");
        let base = tempfile::tempdir().expect("a temporary directory");
        let wal: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
        let mut outcomes = Vec::new();
        for (mode, plan) in modes() {
            let source = batches(
                &store,
                vec![BatchStream::new("events", vec![binary(name, 1)]).primary_key(&["id"])],
            )
            .await;
            let outcome = logging_engine(commit_every(1), Arc::clone(&wal))
                .run(
                    pipeline(&store, [plan]).with_wal(true),
                    source,
                    memory(&store).await,
                )
                .await;
            outcomes.push((mode, outcome));
        }
        for (mode, outcome) in &outcomes {
            let internal = outcome.error.as_ref().map(rdlt_engine::Error::kind);
            assert_ne!(
                internal,
                Some(ErrorKind::Internal),
                "{name} {mode}: {:?}",
                outcome.error
            );
        }
        for (_, outcome) in &outcomes {
            refused(outcome);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_json_key_asking_for_a_metadata_name_is_refused_under_every_case_rule() {
    let cases = [
        (IdentifierCase::Preserve, r#"{"id":1,"_rdlt_id":"x"}"#),
        (IdentifierCase::Lower, r#"{"id":1,"_RDLT_ID":"x"}"#),
        (IdentifierCase::Upper, r#"{"id":1,"_rdlt_Id":"x"}"#),
    ];
    for (case, push) in cases {
        let store = format!("reserved_json_{case:?}");
        let source = batches(&store, vec![BatchStream::json("events", &[push])]).await;
        let base = tempfile::tempdir().expect("a temporary directory");
        let wal: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
        let outcome = logging_engine(commit_every(1), wal)
            .run(
                pipeline(&store, [stream("events")]),
                source,
                folding(&store, case).await,
            )
            .await;
        refused(&outcome);
    }
}

#[tokio::test(start_paused = true)]
async fn a_key_differing_from_a_metadata_name_only_in_case_loads_where_case_is_kept() {
    let store = "reserved_kept_case";
    let push = r#"{"id":1,"_RDLT_ID":"x"}"#;
    let source = batches(store, vec![BatchStream::json("events", &[push])]).await;
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    let outcome = logging_engine(commit_every(1), wal)
        .run(
            pipeline(store, [stream("events")]),
            source,
            folding(store, IdentifierCase::Preserve).await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.rows, 1);
}
