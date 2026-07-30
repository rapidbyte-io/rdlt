//! Corpus builders shared across case files.

use rdlt_testkit::MemoryBatch;
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
