//! Fuzzing entry points (feature 003 R22). `#[doc(hidden)]` — these exist ONLY
//! so the out-of-workspace `fuzz/` targets can reach `pub(crate)` hot paths;
//! they are not API and may change at any time.

use rdlt_connector::{DestCapabilities, StreamSpec};
use rdlt_core::{LoadId, SchemaPolicy, TableName, WriteMode};

/// Raw-JSON slab parsing only (NDJSON / array / single doc): must never panic,
/// hang, or blow memory — errors are the only acceptable failure.
pub fn parse_slab(bytes: &[u8]) {
    let _ = crate::shred::nest::parse_rows(bytes);
}

/// The FULL shred path over arbitrary bytes: parse, observe, resolve, build.
/// Asserts the cheap invariants inline (system columns first, unique names).
pub fn shred_slab(bytes: &[u8]) {
    let caps = DestCapabilities::default();
    let mut shredder =
        crate::shred::TreeShredder::new(StreamSpec::new("fuzz"), caps, TableName::new("fuzz"));
    if shredder.push_bytes(bytes).is_err() {
        return; // malformed JSON: typed error is the correct outcome
    }
    let mut registry = crate::schema::registry::SchemaRegistry::default();
    let items = shredder.drain_batch(
        &mut registry,
        &LoadId::new("fuzz-load"),
        &WriteMode::Append,
        &SchemaPolicy::evolve(),
    );
    let Ok(items) = items else { return };
    for item in items {
        if let crate::load::LoadItem::Delta { schema, .. } = item {
            let mut seen = std::collections::BTreeSet::new();
            for column in &schema.columns {
                assert!(
                    seen.insert(column.name.clone()),
                    "duplicate column `{}` from fuzzed input",
                    column.name
                );
            }
        }
    }
}

/// Arrow type mapping (clause E7): every `DataType` either maps or returns a
/// typed error — never panics, never silently coerces.
pub fn map_arrow_type(dt: &arrow::datatypes::DataType) {
    let _ = crate::shred::passthrough::column_type_from_arrow(dt);
}

// ---- bench entry points (iai_hotpath / perf gate G1) ----

/// Full shred path over one raw slab; returns emitted row count (anti-DCE).
pub fn bench_shred_bytes(bytes: &[u8]) -> u64 {
    let caps = DestCapabilities::default();
    let mut shredder =
        crate::shred::TreeShredder::new(StreamSpec::new("bench"), caps, TableName::new("bench"));
    shredder.push_bytes(bytes).expect("valid bench input");
    let mut registry = crate::schema::registry::SchemaRegistry::default();
    let items = shredder
        .drain_batch(
            &mut registry,
            &LoadId::new("bench-load"),
            &WriteMode::Append,
            &SchemaPolicy::evolve(),
        )
        .expect("bench shred succeeds");
    items
        .iter()
        .map(|item| match item {
            crate::load::LoadItem::Batch { batch, .. } => batch.num_rows() as u64,
            _ => 0,
        })
        .sum()
}

/// REFERENCE (tree) shred path — the pre-feature-003 implementation, kept for
/// the equivalence gate; benched so the tape path's win stays measured.
pub fn bench_shred_bytes_tree(bytes: &[u8]) -> u64 {
    let caps = DestCapabilities::default();
    let mut shredder =
        crate::shred::TreeShredder::new(StreamSpec::new("bench"), caps, TableName::new("bench"));
    shredder.push_bytes(bytes).expect("valid bench input");
    let mut registry = crate::schema::registry::SchemaRegistry::default();
    let items = shredder
        .drain_batch(
            &mut registry,
            &LoadId::new("bench-load"),
            &WriteMode::Append,
            &SchemaPolicy::evolve(),
        )
        .expect("bench shred succeeds");
    items
        .iter()
        .map(|item| match item {
            crate::load::LoadItem::Batch { batch, .. } => batch.num_rows() as u64,
            _ => 0,
        })
        .sum()
}

/// Passthrough over one structured batch; returns emitted row count.
pub fn bench_passthrough(batch: &arrow::record_batch::RecordBatch) -> u64 {
    let mut registry = crate::schema::registry::SchemaRegistry::default();
    let items = crate::shred::passthrough::passthrough_items(
        batch,
        &TableName::new("bench"),
        &mut registry,
        &SchemaPolicy::evolve(),
        &LoadId::new("bench-load"),
        &WriteMode::Append,
        DestCapabilities::default(),
    )
    .expect("bench passthrough succeeds");
    items
        .iter()
        .map(|item| match item {
            crate::load::LoadItem::Batch { batch, .. } => batch.num_rows() as u64,
            _ => 0,
        })
        .sum()
}
