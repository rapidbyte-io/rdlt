//! The drain: cascade filtering, schema resolution, policy enforcement,
//! registry diff/apply, Arrow building — the shared back half of both shred
//! paths.

use std::collections::BTreeSet;

use rdlt_core::{LoadId, PolicyAction, RdltError, RowId, SchemaChange, SchemaPolicy, WriteMode};

use crate::{
    load::LoadItem,
    schema::{
        contracts::{change_column, inherited_action, value_fits, violation_for},
        registry::SchemaRegistry,
    },
};

use super::{build, infer::ColumnState, table, table::TableBuffer, view::JsonView};

/// The per-batch shred context: the mutable schema registry plus the run-scoped
/// load id, write mode, and schema policy. One bundle, one field order — shared
/// by the tape shred path (`TapeShredder::push_and_drain`) and the structured
/// passthrough path (`passthrough::passthrough_items`), which previously threaded
/// these same four values in two different argument orders.
pub(crate) struct ShredContext<'a> {
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
    pub(crate) fn top_level<'a>(&self, key: &str) -> Option<V>
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
    rollback_snapshot: Option<&'a [(String, ColumnState)]>,
}

/// The shared drain: cascade filtering, schema resolution, policy enforcement,
/// registry diff/apply, Arrow building — identical for both shred paths.
pub(crate) fn drain_tables<'v, V: JsonView<'v>>(
    tables: &mut [TableBuffer],
    rows: &mut [Vec<DrainRow<V>>],
    rollback_snapshot: &[Vec<(String, ColumnState)>],
    ctx: ShredContext,
) -> Result<Vec<LoadItem>, RdltError> {
    let ShredContext {
        registry,
        load_id,
        mode,
        policy,
    } = ctx;
    let mut items = Vec::new();
    // Rows discarded in earlier (parent) tables cascade into their descendants.
    let mut discarded_ids: BTreeSet<RowId> = BTreeSet::new();

    // Pair the three index-aligned inputs once; `rollback_snapshot.get(idx)` is resolved
    // here and never again, so the loop below cannot misalign them.
    let mut drains: Vec<TableDrain<V>> = tables
        .iter_mut()
        .zip(rows.iter_mut())
        .enumerate()
        .map(|(idx, (buffer, rows))| TableDrain {
            buffer,
            rows,
            rollback_snapshot: rollback_snapshot.get(idx).map(Vec::as_slice),
        })
        .collect();

    // Captured BEFORE any table is applied: everything this drain establishes for
    // a stream that has none yet is its initial shape, however many tables that
    // needs. A table appearing after that is a change TO the shape, not the shape
    // itself.
    let bootstrapping = registry.is_empty();

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
            let action = if matches!(change, SchemaChange::CreateTable { .. }) {
                // Establishing the stream's initial shape is not evolution,
                // however many tables that shape needs. But a table that appears
                // LATER is drift — a new nested collection creates one, and
                // treating it as a bootstrap would make a frozen contract depend
                // on which shape the first batch happened to have.
                if bootstrapping {
                    PolicyAction::Evolve
                } else {
                    inherited_action(policy, registry, &observed, None)
                }
            } else {
                inherited_action(policy, registry, &observed, change_column(&change))
            };
            match action {
                PolicyAction::Evolve => kept.push(change),
                PolicyAction::Freeze => {
                    // Nothing of this batch has been emitted: fail before any row
                    // of the violating batch is written.
                    return Err(RdltError::Schema(violation_for(&table, &change)));
                }
                PolicyAction::DiscardRow | PolicyAction::DiscardValue => {
                    if matches!(change, SchemaChange::CreateTable { .. }) {
                        // A table that does not exist yet has no column to null
                        // and no prior shape to roll back to, so `enforce_discards`
                        // has nothing to act on and would skip it — silently
                        // creating the very table the policy refused. Discarding
                        // a table creation means discarding its rows, counted;
                        // their ids cascade so descendants go with them.
                        let dropped = d.rows.len() as u64;
                        for row in d.rows.iter() {
                            discarded_ids.insert(row.id);
                        }
                        d.rows.clear();
                        // Mutation note: `dropped == 0` is unreachable here, so
                        // `> 0` versus `>= 0` is an equivalent mutant. The
                        // CreateTable change EXISTS because this drain observed
                        // rows for a table that did not exist; if the cascade
                        // above had emptied them there would be no observation
                        // and no change to refuse. Kept as a defensive guard —
                        // it costs nothing and states that a discard report is
                        // only ever emitted for a real discard.
                        if dropped > 0 {
                            items.push(LoadItem::Discarded {
                                table: table.clone(),
                                rows: dropped,
                                values: 0,
                            });
                        }
                        continue;
                    }
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
                d.rollback_snapshot,
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
                .expect("schema registered before building");
            let (batch, misfits) = build::build_batch(
                schema,
                d.buffer.source_to_normalized(),
                d.rows.as_slice(),
                load_id,
            )
            .map_err(|e| RdltError::internal(format!("arrow build: {e}")))?;
            d.rows.clear();
            // A value that cannot be represented under its column's type is
            // nulled by the builder. Declared columns never reach the policy
            // layer — nothing observes them — so this is the only place such a
            // loss can be counted, and counting it is what keeps "counted,
            // never silent" true for them.
            if misfits > 0 {
                items.push(LoadItem::Discarded {
                    table: d.buffer.table.clone(),
                    rows: 0,
                    values: misfits,
                });
            }
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
    rollback_snapshot: Option<&[(String, ColumnState)]>,
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
        buffer.revert_column(&source_key, rollback_snapshot);
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
            let value = row.top_level(&offense.source_key);
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

    // Mutation note: both comparisons here are equivalent mutants, for the same
    // structural reason as the creation guard above. `enforce_discards` is only
    // CALLED when the discard set is non-empty — that is, when a real violation
    // was found — and acting on a real violation drops at least one row or nulls
    // at least one value. Reaching this line with both counters at zero is not
    // constructible. Kept defensive: emitting a zero-valued `Discarded` would
    // make the event useless to a consumer watching it to detect data loss.
    if dropped_rows > 0 || nulled_values > 0 {
        items.push(LoadItem::Discarded {
            table: table_name,
            rows: dropped_rows,
            values: nulled_values,
        });
    }
}
