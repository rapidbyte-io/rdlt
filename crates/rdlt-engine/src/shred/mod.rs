//! The shredder: raw JSON → typed, lineage-stamped Arrow batches, under the
//! schema-change policy.

pub(crate) mod build;
pub(crate) mod canon;
pub(crate) mod infer;
pub(crate) mod nest;

use std::collections::BTreeSet;

use rdlt_core::{LoadId, PolicyAction, RdltError, RowId, SchemaChange, SchemaPolicy, WriteMode};

use crate::load::LoadItem;
use crate::schema::contracts::{change_column, value_fits, violation_for};
use crate::schema::registry::SchemaRegistry;
pub(crate) use nest::StreamShredder;

impl StreamShredder {
    /// Finalize the current micro-batch: resolve schemas, enforce the schema policy
    /// (Freeze fails before anything is emitted; Discard* filters and counts), diff
    /// against the registry (delta-before-batch order), build Arrow batches.
    pub(crate) fn drain_batch(
        &mut self,
        registry: &mut SchemaRegistry,
        load_id: &LoadId,
        mode: &WriteMode,
        policy: &SchemaPolicy,
    ) -> Result<Vec<LoadItem>, RdltError> {
        let mut items = Vec::new();
        // Rows discarded in earlier (parent) tables cascade into their descendants.
        let mut discarded_ids: BTreeSet<RowId> = BTreeSet::new();

        for idx in 0..self.tables.len() {
            // Cascade: drop rows whose parent or root was discarded upstream. A
            // cascade-dropped row's OWN id joins the set, so its descendants at any
            // depth cascade too (parent-first table order makes one pass complete);
            // cascade drops are counted — never silent (review finding #6).
            if !discarded_ids.is_empty() {
                let buffer = &mut self.tables[idx];
                let mut cascade_dropped = 0u64;
                let mut kept = Vec::with_capacity(buffer.rows.len());
                for row in buffer.rows.drain(..) {
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
                buffer.rows = kept;
                if cascade_dropped > 0 {
                    items.push(LoadItem::Discarded {
                        table: buffer.table.clone(),
                        rows: cascade_dropped,
                        values: 0,
                    });
                }
            }

            let has_rows = !self.tables[idx].rows.is_empty();
            let observed = nest::resolve_schema(&mut self.tables[idx]);
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
                        // of the violating batch is written (spec FR-010).
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
                self.enforce_discards(idx, registry, &discard, &mut discarded_ids, &mut items)?;
                // Re-resolve after rollback + filtering; everything left is approved.
                let observed = nest::resolve_schema(&mut self.tables[idx]);
                let changes = registry.diff(&observed);
                (observed, changes)
            };

            if let Some(delta) = registry.apply(observed, kept) {
                let current = registry
                    .get(&delta.table)
                    .expect("apply() just stored this schema")
                    .clone();
                items.push(LoadItem::Delta {
                    schema: current,
                    delta,
                    mode: mode.clone(),
                });
            }

            let buffer = &mut self.tables[idx];
            if !buffer.rows.is_empty() {
                let schema = registry
                    .get(&buffer.table)
                    .expect("schema registered before building")
                    .clone();
                let names = buffer.name_map().to_vec();
                let batch = build::build_batch(&schema, &names, &buffer.rows, load_id)
                    .map_err(|e| RdltError::config(format!("arrow build: {e}")))?;
                buffer.rows.clear();
                items.push(LoadItem::Batch {
                    table: buffer.table.clone(),
                    batch,
                });
            }
        }
        Ok(items)
    }

    /// Apply Discard* actions for one table: roll offending columns back to their
    /// pre-batch states, then drop rows / null values that required the refused
    /// changes. Counts are emitted as a `LoadItem::Discarded` (never silent).
    fn enforce_discards(
        &mut self,
        idx: usize,
        registry: &SchemaRegistry,
        discard: &[(SchemaChange, PolicyAction)],
        discarded_ids: &mut BTreeSet<RowId>,
        items: &mut Vec<LoadItem>,
    ) -> Result<(), RdltError> {
        let snapshot = self.pre_batch.get(idx).map(Vec::as_slice);
        let table_name = self.tables[idx].table.clone();

        // Resolve, per offending change: (source key, offense test input).
        struct Offense {
            source_key: String,
            /// `None` = new column (any non-null value offends);
            /// `Some(ty)` = must fit this (the pre-change) type.
            must_fit: Option<rdlt_core::ColumnType>,
            action: PolicyAction,
        }
        let mut offenses: Vec<Offense> = Vec::new();
        {
            let buffer = &mut self.tables[idx];
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
        }
        let _ = registry; // registry types already captured in `must_fit`

        let mut dropped_rows = 0u64;
        let mut nulled_values = 0u64;
        let buffer = &mut self.tables[idx];
        buffer.rows.retain_mut(|row| {
            let mut keep = true;
            for offense in &offenses {
                let value = row.value.get(&offense.source_key);
                let offends = match (&value, &offense.must_fit) {
                    (None, _) => false,
                    (Some(v), None) => !v.is_null(),
                    (Some(v), Some(ty)) => !value_fits(v, ty),
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
                        if let Some(slot) = row.value.get_mut(&offense.source_key) {
                            *slot = serde_json::Value::Null;
                            nulled_values += 1;
                        }
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
        Ok(())
    }
}
