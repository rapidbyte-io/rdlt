//! Corpus builders and read-back helpers shared across case files.

use std::collections::BTreeMap;

use rdlt_connector::source::StreamSpec;
use rdlt_core::{TableName, schema::system_columns};
use rdlt_testkit::{MemoryBatch, MemoryDestination, MemorySource, MemoryStream, Row};
use serde_json::json;

/// Three two-row batches, each checkpointed — the smallest corpus that
/// exercises multi-batch accounting, commit cadence, and retry restarts.
pub(crate) fn three_batches() -> Vec<MemoryBatch> {
    (0..3)
        .map(|b| {
            MemoryBatch::new(vec![
                json!({"id": b * 2, "name": format!("r{b}a")}),
                json!({"id": b * 2 + 1, "name": format!("r{b}b")}),
            ])
            .with_checkpoint(json!({"b": b}))
        })
        .collect()
}

/// Two checkpointed batches whose second adds a column — the smallest corpus
/// that forces a mid-run schema evolution.
pub(crate) fn evolving_batches() -> Vec<MemoryBatch> {
    vec![
        MemoryBatch::new(vec![json!({"a": 1}), json!({"a": 2})]).with_checkpoint(1),
        MemoryBatch::new(vec![json!({"a": 3, "b": "late"})]).with_checkpoint(2),
    ]
}

/// One-stream source over explicit batches — the shape almost every case file
/// builds (`MemorySource::single_stream` will not do: it drops checkpoints).
pub(crate) fn stream_with_batches(spec: StreamSpec, batches: Vec<MemoryBatch>) -> MemorySource {
    MemorySource::new(vec![MemoryStream::new(spec, batches)])
}

/// The default corpus as a ready source: stream `s` over [`three_batches`].
pub(crate) fn three_batch_source() -> MemorySource {
    stream_with_batches(StreamSpec::new("s"), three_batches())
}

/// Destination content with run-scoped noise removed (`_rdlt_load_id` names
/// the run, so any cross-run comparison must drop it).
pub(crate) fn without_load_id(dest: &MemoryDestination) -> BTreeMap<TableName, Vec<Row>> {
    dest.snapshot()
        .into_iter()
        .map(|(table, rows)| {
            (
                table,
                rows.into_iter()
                    .map(|mut row| {
                        row.remove(system_columns::LOAD_ID);
                        row
                    })
                    .collect(),
            )
        })
        .collect()
}
