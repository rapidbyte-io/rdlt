//! Fuzzing entry points. `#[doc(hidden)]` — these exist ONLY
//! so the out-of-workspace `fuzz/` targets can reach `pub(crate)` hot paths;
//! they are not API and may change at any time.

use rdlt_connector::{DestinationCapabilities, StreamSpec};
use rdlt_core::{LoadId, SchemaPolicy, TableName, WriteMode};

/// Raw-JSON slab parsing only (NDJSON / array / single doc) through the
/// PRODUCTION arena parser: must never panic, hang, or blow memory —
/// errors are the only acceptable failure.
pub fn parse_slab(bytes: &[u8]) {
    let mut arena = crate::shred::arena::Arena::default();
    let _ = arena.parse_rows(bytes);
}

/// The FULL (tape) shred path over arbitrary bytes: parse, observe, resolve,
/// build. Asserts the cheap invariants inline (unique destination names).
pub fn shred_slab(bytes: &[u8]) {
    let capabilities = DestinationCapabilities::default();
    let mut shredder = crate::shred::TapeShredder::new(
        StreamSpec::new("fuzz"),
        capabilities,
        TableName::new("fuzz"),
    );
    let mut registry = crate::schema::registry::SchemaRegistry::default();
    let (load_id, mode, policy) = (
        LoadId::new("fuzz-load"),
        WriteMode::Append,
        SchemaPolicy::evolve(),
    );
    let items = shredder.push_and_drain(
        bytes,
        crate::shred::ShredContext {
            registry: &mut registry,
            load_id: &load_id,
            mode: &mode,
            policy: &policy,
        },
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

/// Arrow type mapping: every `DataType` either maps or returns a
/// typed error — never panics, never silently coerces.
pub fn map_arrow_type(dt: &arrow::datatypes::DataType) {
    let _ = crate::shred::passthrough::column_type_from_arrow(dt);
}

// ---- bench entry points (iai_hotpath / perf gate) ----

/// Production (tape) shred path over one raw slab; returns emitted row count.
pub fn bench_shred_bytes(bytes: &[u8]) -> u64 {
    let capabilities = DestinationCapabilities::default();
    let mut shredder = crate::shred::TapeShredder::new(
        StreamSpec::new("bench"),
        capabilities,
        TableName::new("bench"),
    );
    let mut registry = crate::schema::registry::SchemaRegistry::default();
    let (load_id, mode, policy) = (
        LoadId::new("bench-load"),
        WriteMode::Append,
        SchemaPolicy::evolve(),
    );
    let items = shredder
        .push_and_drain(
            bytes,
            crate::shred::ShredContext {
                registry: &mut registry,
                load_id: &load_id,
                mode: &mode,
                policy: &policy,
            },
        )
        .unwrap_or_else(|_| panic!("bench shred succeeds"));
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
    let (load_id, mode, policy) = (
        LoadId::new("bench-load"),
        WriteMode::Append,
        SchemaPolicy::evolve(),
    );
    let items = crate::shred::passthrough::passthrough_items(
        batch,
        &TableName::new("bench"),
        crate::shred::ShredContext {
            registry: &mut registry,
            load_id: &load_id,
            mode: &mode,
            policy: &policy,
        },
        DestinationCapabilities::default(),
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
