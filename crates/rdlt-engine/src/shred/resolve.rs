//! The shared back half of both shred inputs: cascade filtering, schema
//! resolution, policy enforcement, registry diff/apply, Arrow building. The
//! JSON path lowers its buffered rows into [`Row`]s and runs
//! [`resolve_tables`]; the Arrow path shares [`resolve_policy`], the ONE
//! policy loop, parameterized by [`Input`] where the two inputs genuinely
//! differ.

use std::collections::{BTreeMap, BTreeSet};

use rdlt_core::commit::WriteMode;
use rdlt_core::error::Error;
use rdlt_core::id::LoadId;
use rdlt_core::schema::{self, TableSchema};

use super::infer::ColumnState;
use super::table::TableBuffer;
use super::view::JsonView;
use super::{build, limits, table};
use crate::identity::RowId;
use crate::load::LoadItem;
use crate::policy::{PolicyAction, SchemaPolicy};
use crate::schema::contract::{change_column, inherited_action, value_fits, violation_for};
use crate::schema::registry::SchemaRegistry;

/// The per-batch shred context: the mutable schema registry plus the run-scoped
/// load id, write mode, schema policy, and the batch-assembly cell budget
/// (`config::Config::with_max_batch_cells`). One bundle, one field order —
/// shared by the JSON path (`json::Shredder::push_and_resolve`) and the Arrow
/// path (`arrow::items`).
pub(crate) struct ShredContext<'a> {
    pub(crate) registry: &'a mut SchemaRegistry,
    pub(crate) load_id: &'a LoadId,
    pub(crate) mode: &'a WriteMode,
    pub(crate) policy: &'a SchemaPolicy,
    pub(crate) max_batch_cells: usize,
}

/// One row inside the resolve: a view value + lineage + the DiscardValue
/// overlay. The JSON path lowers its buffered rows into this before resolving.
pub(crate) struct Row<V> {
    pub(crate) value: V,
    pub(crate) id: RowId,
    pub(crate) parent_id: Option<RowId>,
    pub(crate) root_id: Option<RowId>,
    pub(crate) pos: Option<u64>,
    /// Source keys nulled by `DiscardValue` policy enforcement. An overlay
    /// instead of value mutation, so borrowed (arena) rows work identically.
    ///
    /// A SET, because every use of it is a membership question: the
    /// overlay is consulted once per key per row at enforcement and
    /// again per column per row at build, so a linear scan here is
    /// paid a second time for every key the policy discarded — the
    /// table's width squared, over a width the wire chooses. Nothing
    /// iterates it in order, so a set costs the ordering nobody wanted.
    pub(crate) nulled: BTreeSet<String>,
}

impl<V: Copy> Row<V> {
    /// Top-level field extraction honoring the DiscardValue overlay.
    pub(crate) fn top_level<'a>(&self, key: &str) -> Option<V>
    where
        V: JsonView<'a>,
    {
        if self.nulled.contains(key) {
            return None;
        }
        self.value.obj_get(key)
    }
}

/// Which producer is resolving — the one axis on which the policy loop
/// differs. The JSON path establishes a stream's whole initial shape in one
/// bootstrap resolve, however many tables that takes, and polices any table
/// created LATER through its ancestry; the Arrow path maps one batch onto
/// one table, so a creation there is always that table's first version, never
/// evolution.
#[derive(Clone, Copy)]
pub(crate) enum Input {
    Json { bootstrapping: bool },
    Arrow,
}

/// THE policy loop both inputs run: resolve the action for every change in
/// order — Evolve keeps it, Freeze fails the whole batch typed before any row
/// of it is written, and a Discard* action is handed to `on_discard`, the one
/// step each input answers in its own way. Returns the kept changes.
pub(crate) fn resolve_policy(
    input: Input,
    policy: &SchemaPolicy,
    registry: &SchemaRegistry,
    observed: &TableSchema,
    changes: Vec<schema::Change>,
    mut on_discard: impl FnMut(schema::Change, PolicyAction) -> Result<(), Error>,
) -> Result<Vec<schema::Change>, Error> {
    let mut kept: Vec<schema::Change> = Vec::new();
    for change in changes {
        let action = match (&change, input) {
            (
                schema::Change::CreateTable { .. },
                Input::Arrow
                | Input::Json {
                    bootstrapping: true,
                },
            ) => PolicyAction::Evolve,
            (
                schema::Change::CreateTable { .. },
                Input::Json {
                    bootstrapping: false,
                },
            ) => inherited_action(policy, registry, observed, None),
            _ => inherited_action(policy, registry, observed, change_column(&change)),
        };
        match action {
            PolicyAction::Evolve => kept.push(change),
            PolicyAction::Freeze => {
                return Err(Error::Schema(violation_for(&observed.table, &change)));
            }
            PolicyAction::DiscardRow | PolicyAction::DiscardValue => on_discard(change, action)?,
        }
    }
    Ok(kept)
}

/// One table's slice of a resolve: its buffer, its buffered rows, and its
/// pre-batch column snapshot — bound together so an index can never pair the
/// wrong snapshot (or row vector) with a buffer. Built once by zipping the
/// previously-parallel slices; the loop then only ever touches one bundle.
struct TableSlice<'a, V> {
    buffer: &'a mut TableBuffer,
    rows: &'a mut Vec<Row<V>>,
    /// Column snapshot to roll back to on Discard* — taken OUT of the buffer
    /// (the resolve mutates the columns the snapshot describes, so it cannot
    /// borrow it back). `None` for a table not mutated this push — which is
    /// exactly a table that did not exist before this batch or went
    /// unobserved in it (nothing to revert to).
    rollback_snapshot: Option<Vec<(String, ColumnState)>>,
}

/// The shared resolve: cascade filtering, schema resolution, policy
/// enforcement, registry diff/apply, Arrow building.
pub(crate) fn resolve_tables<'v, V: JsonView<'v>>(
    tables: &mut [TableBuffer],
    rows: &mut [Vec<Row<V>>],
    ctx: ShredContext,
) -> Result<Vec<LoadItem>, Error> {
    let ShredContext {
        registry,
        load_id,
        mode,
        policy,
        max_batch_cells,
    } = ctx;
    let mut items = Vec::new();
    // Rows discarded in earlier (parent) tables cascade into their descendants.
    let mut discarded_ids: BTreeSet<RowId> = BTreeSet::new();

    // Pair the index-aligned inputs once, taking each table's pre-push
    // snapshot out of its buffer (see the field's doc for why owned).
    let mut slices: Vec<TableSlice<V>> = tables
        .iter_mut()
        .zip(rows.iter_mut())
        .map(|(buffer, rows)| {
            let rollback_snapshot = buffer.take_rollback_snapshot();
            TableSlice {
                buffer,
                rows,
                rollback_snapshot,
            }
        })
        .collect();

    // Captured BEFORE any table is applied: everything this resolve establishes
    // for a stream that has none yet is its initial shape, however many tables
    // that needs. A table appearing after that is a change TO the shape, not
    // the shape itself.
    let bootstrapping = registry.is_empty();

    for d in &mut slices {
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
        // A row-less table whose observation state is UNCHANGED since its
        // schema last reached the registry has nothing to resolve, diff, or
        // emit — skipping before `resolve_schema` is what keeps an idle push
        // from paying O(total table state) of pure CPU on a maximal stream.
        // (A dirty table with no rows still resolves: its delta must reach the
        // registry even when this push carries none of its rows.)
        if !has_rows && !d.buffer.is_dirty() {
            continue;
        }
        let observed = table::resolve_schema(d.buffer);
        let table = observed.table.clone();
        if !has_rows && registry.get(&table).is_none() {
            // Never registered and nothing to write — nothing reached the
            // registry, so the table is clean from the registry's point of
            // view; the next observation re-dirties it.
            d.buffer.mark_clean();
            continue;
        }

        let changes = registry.diff(&observed);

        // ---- Policy resolution per change ----
        let mut discard: Vec<(schema::Change, PolicyAction)> = Vec::new();
        let kept = resolve_policy(
            Input::Json { bootstrapping },
            policy,
            registry,
            &observed,
            changes,
            |change, action| {
                if matches!(change, schema::Change::CreateTable { .. }) {
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
                    // `dropped == 0` is unreachable here: the CreateTable
                    // change EXISTS because this resolve observed rows for a
                    // table that did not exist; had the cascade above emptied
                    // them there would be no observation and no change to
                    // refuse. Kept as a defensive guard — a discard report is
                    // only ever emitted for a real discard.
                    if dropped > 0 {
                        items.push(LoadItem::Discarded {
                            table: table.clone(),
                            rows: dropped,
                            values: 0,
                        });
                    }
                    return Ok(());
                }
                discard.push((change, action));
                Ok(())
            },
        )?;

        let (observed, kept) = if discard.is_empty() {
            (observed, kept)
        } else {
            enforce_discards(
                d.buffer,
                d.rows,
                d.rollback_snapshot.as_deref(),
                &discard,
                &mut discarded_ids,
                &mut items,
            );
            // Re-resolve after rollback + filtering; everything left is approved.
            let observed = table::resolve_schema(d.buffer);
            let changes = registry.diff(&observed);
            (observed, changes)
        };

        // The cell budget fires BEFORE the registry apply: a refused push must
        // not leave its schema mutation behind — the registry would desync
        // from the destination's DDL the moment an error path ever learned to
        // continue past it. The builder materializes every schema column for
        // every row — nulls where a row lacks the field — so the columns ×
        // rows product is refused before any array is built, not metered
        // after the gigabytes are resident. `observed`'s width IS the
        // post-apply registry width.
        if !d.rows.is_empty() {
            limits::refuse_over_cell_budget(
                &d.buffer.table,
                observed.columns.len(),
                d.rows.len(),
                max_batch_cells,
            )?;
        }

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
                d.buffer.normalized_to_source(),
                d.rows.as_slice(),
                load_id,
            )
            .map_err(|e| Error::internal(format!("arrow build: {e}")))?;
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
            items.push(LoadItem::batch(d.buffer.table.clone(), batch));
        }
        // The table's resolved state has reached the registry (or there was
        // nothing to change): clean until the next observation. Error exits
        // above deliberately skip this — an un-applied mutation must be
        // re-resolved by the next push.
        d.buffer.mark_clean();
    }
    Ok(items)
}

/// Apply Discard* actions for one table: roll offending columns back to their
/// pre-batch states, then drop rows / null values (via the overlay) that
/// required the refused changes. Counts are emitted as a `LoadItem::Discarded`
/// (never silent).
fn enforce_discards<'v, V: JsonView<'v>>(
    buffer: &mut TableBuffer,
    rows: &mut Vec<Row<V>>,
    rollback_snapshot: Option<&[(String, ColumnState)]>,
    discard: &[(schema::Change, PolicyAction)],
    discarded_ids: &mut BTreeSet<RowId>,
    items: &mut Vec<LoadItem>,
) {
    let table_name = buffer.table.clone();

    // Resolve, per offending change: (source key, offense test input).
    struct Offense {
        source_key: String,
        /// `None` = new column (any non-null value offends);
        /// `Some(ty)` = must fit this (the pre-change) type.
        must_fit: Option<rdlt_core::schema::ColumnType>,
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
        let must_fit = match change {
            schema::Change::WidenColumn { from, .. } => Some(from.clone()),
            _ => None,
        };
        offenses.push(Offense {
            source_key,
            must_fit,
            action: *action,
        });
    }

    // ONE rollback for all of them: reverting per change would re-derive
    // the buffer's whole column index once per discarded column, which is
    // quadratic in a width the wire chooses.
    let reverted: Vec<String> = offenses
        .iter()
        .map(|offense| offense.source_key.clone())
        .collect();
    buffer.revert_columns(&reverted, rollback_snapshot);

    // Keyed, so each row is walked ONCE: its own entries, each asked
    // whether it is an offense. The alternative — every offense asked of
    // every row — is rows × offenses, and both are the wire's to choose:
    // a million one-key rows beside one row adding four thousand columns
    // is legal under every byte and value budget, yet multiplies to
    // billions of consultations inside one blocking call, repeatable
    // every push because the rollback reverts and the identical push
    // re-offends. A row's entries already yield one value per key (the
    // last occurrence), so walking them asks exactly what the lookup did.
    let offenses_by_key: BTreeMap<&str, &Offense> = offenses
        .iter()
        .map(|offense| (offense.source_key.as_str(), offense))
        .collect();

    let mut dropped_rows = 0u64;
    let mut nulled_values = 0u64;
    rows.retain_mut(|row| {
        let mut keep = true;
        for (key, value) in row.value.obj_entries() {
            let Some(offense) = offenses_by_key.get(key) else {
                continue;
            };
            if row.nulled.contains(key) {
                continue;
            }
            let offends = match &offense.must_fit {
                None => !value.is_null(),
                Some(ty) => !value_fits(value, ty),
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
                    row.nulled.insert(offense.source_key.clone());
                    nulled_values += 1;
                }
                PolicyAction::Evolve | PolicyAction::Freeze => unreachable!("filtered above"),
            }
        }
        keep
    });

    // Both comparisons are structurally redundant: `enforce_discards` is only
    // CALLED when the discard set is non-empty — a real violation was found —
    // and acting on one drops at least one row or nulls at least one value.
    // Kept defensive: a zero-valued `Discarded` would make the event useless
    // to a consumer watching it to detect data loss.
    if dropped_rows > 0 || nulled_values > 0 {
        items.push(LoadItem::Discarded {
            table: table_name,
            rows: dropped_rows,
            values: nulled_values,
        });
    }
}

#[cfg(test)]
mod overlay_tests {
    use super::*;

    /// The DiscardValue overlay is consulted once per key per row while
    /// the policy is enforced and again per column per row while the
    /// batch builds — so what one consultation costs is paid over the
    /// table's width squared, across a width the wire chooses, inside
    /// one blocking call and repeatable every push. That is why it is a
    /// SET: membership is the only question asked of it, and a set
    /// answers in logarithmic time by its own contract rather than by
    /// anything a test here could measure.
    ///
    /// What this holds is the BEHAVIOUR the change must not alter — a
    /// nulled key reads as absent, an untouched one reads through, and
    /// nulling the same key twice is nulling it once, which the scanned
    /// list counted and re-scanned twice over.
    #[test]
    fn the_overlay_hides_exactly_what_was_nulled() {
        // The real view, so the overlay is tested over the thing it
        // actually overlays.
        let mut arena = crate::shred::arena::Arena::default();
        let rows = arena
            .parse_rows(
                br#"{"kept":1,"gone":2}"#,
                rdlt_connector::channel::MAX_RECORD_BATCH_ROWS,
                rdlt_connector::channel::MAX_JSON_VALUES_PER_PUSH,
            )
            .expect("the fixture parses");
        let mut row = Row {
            value: arena.node(rows[0]),
            id: RowId::from_bytes([0u8; 32]),
            parent_id: None,
            root_id: None,
            pos: None,
            nulled: BTreeSet::new(),
        };
        assert!(row.top_level("gone").is_some(), "untouched, so it reads");

        row.nulled.insert("gone".to_string());
        row.nulled.insert("gone".to_string());
        assert_eq!(row.nulled.len(), 1, "nulling twice nulls once");
        assert!(row.top_level("gone").is_none(), "nulled, so it is absent");
        assert!(
            row.top_level("kept").is_some(),
            "its neighbour is untouched"
        );
    }
}
