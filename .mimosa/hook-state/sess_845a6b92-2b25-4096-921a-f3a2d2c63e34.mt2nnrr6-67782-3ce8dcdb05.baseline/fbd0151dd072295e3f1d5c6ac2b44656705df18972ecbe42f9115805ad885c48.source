//! Corpus builders and read-back helpers shared across case files.

use std::collections::BTreeMap;

use rdlt_connector::source::StreamSpec;
use rdlt_core::id::TableName;
use rdlt_core::schema;
use rdlt_testkit::memory;
use serde_json::json;

use super::support::scripted;

/// Three two-row batches, each checkpointed — the smallest corpus that
/// exercises multi-batch accounting, commit cadence, and retry restarts.
pub(crate) fn three_batches() -> Vec<memory::Batch> {
    (0..3)
        .map(|b| {
            memory::Batch::new(vec![
                json!({"id": b * 2, "name": format!("r{b}a")}),
                json!({"id": b * 2 + 1, "name": format!("r{b}b")}),
            ])
            .with_checkpoint(json!({"b": b}))
        })
        .collect()
}

/// Two checkpointed batches whose second adds a column — the smallest corpus
/// that forces a mid-run schema evolution.
pub(crate) fn evolving_batches() -> Vec<memory::Batch> {
    vec![
        memory::Batch::new(vec![json!({"a": 1}), json!({"a": 2})]).with_checkpoint(1),
        memory::Batch::new(vec![json!({"a": 3, "b": "late"})]).with_checkpoint(2),
    ]
}

/// One-stream source over explicit batches — the shape almost every case file
/// builds (`memory::Source::single_stream` will not do: it drops checkpoints).
/// A scripted source, so its `since` log is available to resume assertions.
pub(crate) fn stream_with_batches(
    spec: StreamSpec,
    batches: Vec<memory::Batch>,
) -> scripted::Source {
    scripted::Source::new(vec![scripted::Stream::new(spec, batches)])
}

/// The default corpus as a ready source: stream `s` over [`three_batches`].
pub(crate) fn three_batch_source() -> scripted::Source {
    stream_with_batches(StreamSpec::new("s"), three_batches())
}

/// Destination content with run-scoped noise removed (`_rdlt_load_id` names
/// the run, so any cross-run comparison must drop it).
pub(crate) fn without_load_id(dest: &memory::Destination) -> BTreeMap<TableName, Vec<memory::Row>> {
    dest.snapshot()
        .into_iter()
        .map(|(table, rows)| {
            (
                table,
                rows.into_iter()
                    .map(|mut row| {
                        row.remove(schema::system::LOAD_ID);
                        row
                    })
                    .collect(),
            )
        })
        .collect()
}
