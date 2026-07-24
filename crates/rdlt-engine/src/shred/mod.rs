//! The shredder: raw JSON → typed, lineage-stamped Arrow batches, under the
//! schema-change policy.
//!
//! The tape path (`tape`: slab arena, no per-row trees) feeds [`drain_tables`]
//! — the generic resolve/policy/build pipeline over the [`view::JsonView`]
//! seam. The seam stays generic even with one production
//! path: the `&serde_json::Value` view backs the unit tests, and everything
//! semantics-bearing remains representation-independent by construction.

pub(crate) mod arena;
pub(crate) mod build;
pub(crate) mod canon;
pub(crate) mod infer;
pub(crate) mod passthrough;
pub(crate) mod table;
pub(crate) mod tape;
pub(crate) mod view;

use std::collections::BTreeSet;

use rdlt_core::{LoadId, PolicyAction, RdltError, RowId, SchemaChange, SchemaPolicy, WriteMode};

use crate::load::LoadItem;
use crate::schema::contracts::{change_column, value_fits, violation_for};
use crate::schema::registry::SchemaRegistry;
use infer::ColState;
use table::TableBuffer;
pub(crate) use tape::TapeShredder;
use view::JsonView;

/// The per-batch shred context: the mutable schema registry plus the run-scoped
/// load id, write mode, and schema policy. One bundle, one field order — shared
/// by the tape shred path (`TapeShredder::push_and_drain`) and the structured
/// passthrough path (`passthrough::passthrough_items`), which previously threaded
/// these same four values in two different argument orders.
pub(crate) struct ShredCtx<'a> {
    pub(crate) registry: &'a mut SchemaRegistry,
    pub(crate) load_id: &'a LoadId,
    pub(crate) mode: &'a WriteMode,
    pub(crate) policy: &'a SchemaPolicy,
}

/// One row inside the drain: a view value + lineage + the DiscardValue overlay.
/// Both paths lower their buffered rows into this before draining.
pub(crate) struct DrainRow<V> {
    pub(crate) value: V,
    pub(crate) id: RowId,
    pub(crate) parent_id: Option<RowId>,
    pub(crate) root_id: Option<RowId>,
    pub(crate) pos: Option<u64>,
    /// Source keys nulled by `DiscardValue` policy enforcement. An overlay
    /// instead of value mutation, so borrowed (arena) rows work identically.
    pub(crate) nulled: Vec<String>,
}

impl<V: Copy> DrainRow<V> {
    /// Top-level field extraction honoring the DiscardValue overlay.
    pub(crate) fn get_top<'a>(&self, key: &str) -> Option<V>
    where
        V: JsonView<'a>,
    {
        if self.nulled.iter().any(|k| k == key) {
            return None;
        }
        self.value.obj_get(key)
    }
}

/// One table's slice of a drain: its buffer, its buffered rows, and its
/// pre-batch column snapshot — bound together so an index can never pair the
/// wrong snapshot (or row vector) with a buffer. Built once by zipping the
/// previously-parallel slices; the drain loop then only ever touches one bundle.
struct TableDrain<'a, V> {
    buffer: &'a mut TableBuffer,
    rows: &'a mut Vec<DrainRow<V>>,
    /// Column snapshot to roll back to on Discard*; `None` for a table that did
    /// not exist before this batch (nothing to revert to).
    pre: Option<&'a [(String, ColState)]>,
}

/// The shared drain: cascade filtering, schema resolution, policy enforcement,
/// registry diff/apply, Arrow building — identical for both shred paths.
pub(crate) fn drain_tables<'v, V: JsonView<'v>>(
    tables: &mut [TableBuffer],
    rows: &mut [Vec<DrainRow<V>>],
    pre_batch: &[Vec<(String, ColState)>],
    registry: &mut SchemaRegistry,
    load_id: &LoadId,
    mode: &WriteMode,
    policy: &SchemaPolicy,
) -> Result<Vec<LoadItem>, RdltError> {
    let mut items = Vec::new();
    // Rows discarded in earlier (parent) tables cascade into their descendants.
    let mut discarded_ids: BTreeSet<RowId> = BTreeSet::new();

    // Pair the three index-aligned inputs once; `pre_batch.get(idx)` is resolved
    // here and never again, so the loop below cannot misalign them.
    let mut drains: Vec<TableDrain<V>> = tables
        .iter_mut()
        .zip(rows.iter_mut())
        .enumerate()
        .map(|(idx, (buffer, rows))| TableDrain {
            buffer,
            rows,
            pre: pre_batch.get(idx).map(Vec::as_slice),
        })
        .collect();

    for d in &mut drains {
        // Cascade: drop rows whose parent or root was discarded upstream. A
        // cascade-dropped row's OWN id joins the set, so its descendants at any
        // depth cascade too (parent-first table order makes one pass complete);
        // cascade drops are counted — never silent.
        if !discarded_ids.is_empty() {
            let mut cascade_dropped = 0u64;
            let mut kept = Vec::with_capacity(d.rows.len());
            for row in d.rows.drain(..) {
                let doomed = row
                    .parent_id
                    .as_ref()
                    .is_some_and(|p| discarded_ids.contains(p))
                    || row
                        .root_id
                        .as_ref()
                        .is_some_and(|r| discarded_ids.contains(r));
                if doomed {
                    discarded_ids.insert(row.id);
                    cascade_dropped += 1;
                } else {
                    kept.push(row);
                }
            }
            *d.rows = kept;
            if cascade_dropped > 0 {
                items.push(LoadItem::Discarded {
                    table: d.buffer.table.clone(),
                    rows: cascade_dropped,
                    values: 0,
                });
            }
        }

        let has_rows = !d.rows.is_empty();
        let observed = table::resolve_schema(d.buffer);
        let table = observed.table.clone();
        if !has_rows && registry.get(&table).is_none() {
            continue;
        }

        let changes = registry.diff(&observed);

        // ---- Policy resolution per change ----
        let mut discard: Vec<(SchemaChange, PolicyAction)> = Vec::new();
        let mut kept: Vec<SchemaChange> = Vec::new();
        for change in changes {
            // Table creation is the first version, not evolution — always allowed.
            let action = if matches!(change, SchemaChange::CreateTable { .. }) {
                PolicyAction::Evolve
            } else {
                policy.action_for(&table, change_column(&change))
            };
            match action {
                PolicyAction::Evolve => kept.push(change),
                PolicyAction::Freeze => {
                    // Nothing of this batch has been emitted: fail before any row
                    // of the violating batch is written.
                    return Err(RdltError::Schema(violation_for(&table, &change)));
                }
                PolicyAction::DiscardRow | PolicyAction::DiscardValue => {
                    discard.push((change, action));
                }
            }
        }

        let (observed, kept) = if discard.is_empty() {
            (observed, kept)
        } else {
            enforce_discards(
                d.buffer,
                d.rows,
                d.pre,
                &discard,
                &mut discarded_ids,
                &mut items,
            );
            // Re-resolve after rollback + filtering; everything left is approved.
            let observed = table::resolve_schema(d.buffer);
            let changes = registry.diff(&observed);
            (observed, changes)
        };

        if let Some((delta, current)) = registry.apply(observed, kept) {
            items.push(LoadItem::Delta {
                schema: current,
                delta,
                mode: mode.clone(),
            });
        }

        if !d.rows.is_empty() {
            let schema = registry
                .get(&d.buffer.table)
                .expect("schema registered before building")
                .clone();
            let batch =
                build::build_batch(&schema, d.buffer.name_map(), d.rows.as_slice(), load_id)
                    .map_err(|e| RdltError::config(format!("arrow build: {e}")))?;
            d.rows.clear();
            items.push(LoadItem::Batch {
                table: d.buffer.table.clone(),
                batch,
            });
        }
    }
    Ok(items)
}

/// Apply Discard* actions for one table: roll offending columns back to their
/// pre-batch states, then drop rows / null values (via the overlay) that
/// required the refused changes. Counts are emitted as a `LoadItem::Discarded`
/// (never silent).
fn enforce_discards<'v, V: JsonView<'v>>(
    buffer: &mut TableBuffer,
    rows: &mut Vec<DrainRow<V>>,
    snapshot: Option<&[(String, ColState)]>,
    discard: &[(SchemaChange, PolicyAction)],
    discarded_ids: &mut BTreeSet<RowId>,
    items: &mut Vec<LoadItem>,
) {
    let table_name = buffer.table.clone();

    // Resolve, per offending change: (source key, offense test input).
    struct Offense {
        source_key: String,
        /// `None` = new column (any non-null value offends);
        /// `Some(ty)` = must fit this (the pre-change) type.
        must_fit: Option<rdlt_core::ColumnType>,
        action: PolicyAction,
    }
    let mut offenses: Vec<Offense> = Vec::new();
    for (change, action) in discard {
        let Some(normalized) = change_column(change) else {
            continue;
        };
        let source_key = buffer
            .source_key_for(normalized)
            .unwrap_or(normalized)
            .to_owned();
        buffer.revert_column(&source_key, snapshot);
        let must_fit = match change {
            SchemaChange::WidenColumn { from, .. } => Some(from.clone()),
            _ => None,
        };
        offenses.push(Offense {
            source_key,
            must_fit,
            action: *action,
        });
    }

    let mut dropped_rows = 0u64;
    let mut nulled_values = 0u64;
    rows.retain_mut(|row| {
        let mut keep = true;
        for offense in &offenses {
            let value = row.get_top(&offense.source_key);
            let offends = match (&value, &offense.must_fit) {
                (None, _) => false,
                (Some(v), None) => !v.is_null(),
                (Some(v), Some(ty)) => !value_fits(*v, ty),
            };
            if !offends {
                continue;
            }
            match offense.action {
                PolicyAction::DiscardRow => {
                    dropped_rows += 1;
                    discarded_ids.insert(row.id);
                    keep = false;
                    break;
                }
                PolicyAction::DiscardValue => {
                    row.nulled.push(offense.source_key.clone());
                    nulled_values += 1;
                }
                PolicyAction::Evolve | PolicyAction::Freeze => unreachable!("filtered above"),
            }
        }
        keep
    });

    if dropped_rows > 0 || nulled_values > 0 {
        items.push(LoadItem::Discarded {
            table: table_name,
            rows: dropped_rows,
            values: nulled_values,
        });
    }
}
