//! Shared shredding state: per-table naming, shape observation, lineage
//! identity, and schema resolution — everything the tape traversal (`tape.rs`)
//! and the drain (`mod.rs`) build on.
//!
//! History: until feature 003 this file also held the original owned-`Value`
//! "tree" shredder. The tape path replaced it (equivalence proven by
//! construction over the shared [`super::view::JsonView`] core, then by a
//! proptest, both retired with the tree path once it shipped); its behavioral
//! invariants stay pinned by `tests/shred_property.rs` and the hazard cases in
//! `arena.rs`/`tests/passthrough.rs`.

use rdlt_core::identity::{FieldValue, RowIdBuilder};
use rdlt_core::naming::{IdentRules, UniqueNamer};
use rdlt_core::schema::system_columns;
use rdlt_core::{ColumnDef, ParentLink, Provenance, RowId, TableName, TableSchema};

use super::canon::{canonical_json_bytes, render_scalar};
use super::infer::ColState;
use super::view::JsonView;

/// One table's persistent shredding state: naming, shape observation, lineage —
/// everything EXCEPT the buffered rows (those are per-batch and path-specific).
#[derive(Debug)]
pub(crate) struct TableBuffer {
    pub(crate) table: TableName,
    pub(crate) parent: Option<ParentLink>,
    /// Column observation states in first-seen order; source key → state.
    pub(crate) columns: Vec<(String, ColState)>,
    /// Source key → normalized column/child name mapping (collision-safe).
    namer: UniqueNamer,
    names: Vec<(String, String)>,
}

impl TableBuffer {
    pub(crate) fn new(table: TableName, parent: Option<ParentLink>, rules: IdentRules) -> Self {
        let mut namer = UniqueNamer::new(rules);
        // System columns RESERVE their names: a source field literally named
        // `_rdlt_id` gets suffixed rather than aliasing the lineage column.
        for sys in [
            system_columns::LOAD_ID,
            system_columns::ID,
            system_columns::PARENT_ID,
            system_columns::POS,
            system_columns::ROOT_ID,
        ] {
            namer.reserve(sys);
        }
        Self {
            table,
            parent,
            columns: Vec::new(),
            namer,
            names: Vec::new(),
        }
    }

    /// Source key → normalized column name pairs accumulated so far.
    pub(crate) fn name_map(&self) -> &[(String, String)] {
        &self.names
    }

    /// Reverse lookup: normalized column name → source key.
    pub(crate) fn source_key_for(&self, normalized: &str) -> Option<&str> {
        self.names
            .iter()
            .find(|(_, n)| n == normalized)
            .map(|(source, _)| source.as_str())
    }

    /// Policy enforcement: revert one column's observation state to its pre-batch
    /// snapshot (or remove it if the column first appeared this batch).
    pub(crate) fn revert_column(
        &mut self,
        source_key: &str,
        snapshot: Option<&[(String, ColState)]>,
    ) {
        let prior = snapshot.and_then(|columns| {
            columns
                .iter()
                .find(|(key, _)| key == source_key)
                .map(|(_, state)| state.clone())
        });
        match prior {
            Some(state) => {
                if let Some(idx) = self.columns.iter().position(|(k, _)| k == source_key) {
                    self.columns[idx].1 = state;
                }
            }
            None => self.columns.retain(|(k, _)| k != source_key),
        }
    }

    pub(crate) fn column_name(&mut self, source_key: &str) -> String {
        if let Some((_, normalized)) = self.names.iter().find(|(k, _)| k == source_key) {
            return normalized.clone();
        }
        let normalized = self.namer.name_for(source_key);
        self.names.push((source_key.to_owned(), normalized.clone()));
        normalized
    }

    pub(crate) fn state_mut(&mut self, source_key: &str) -> &mut ColState {
        if let Some(idx) = self.columns.iter().position(|(k, _)| k == source_key) {
            &mut self.columns[idx].1
        } else {
            self.columns
                .push((source_key.to_owned(), ColState::Unknown));
            &mut self.columns.last_mut().expect("just pushed").1
        }
    }
}

/// `_rdlt_id` for a root row: key hash when the stream declares a primary key,
/// content hash otherwise (design doc §5.4).
pub(crate) fn row_identity<'a, V: JsonView<'a>>(primary_key: Option<&[String]>, row: V) -> RowId {
    match primary_key {
        Some(key_fields) if !key_fields.is_empty() => {
            let mut builder = RowIdBuilder::keyed();
            for field in key_fields {
                let rendered = row.obj_get(field).and_then(render_scalar);
                match &rendered {
                    Some(text) => builder.field(field, FieldValue::Bytes(text.as_bytes())),
                    None => builder.field(field, FieldValue::Null),
                };
            }
            builder.finish()
        }
        _ => content_hash(row),
    }
}

pub(crate) fn content_hash<'a, V: JsonView<'a>>(row: V) -> RowId {
    let mut scratch = Vec::new();
    content_hash_with(row, &mut scratch)
}

/// `content_hash` with a caller-owned scratch buffer — traversals hash every
/// row and child, and per-call Vec churn was visible in the shred profile.
pub(crate) fn content_hash_with<'a, V: JsonView<'a>>(row: V, scratch: &mut Vec<u8>) -> RowId {
    scratch.clear();
    canonical_json_bytes(row, scratch);
    let mut builder = RowIdBuilder::keyless();
    builder.field("", FieldValue::Bytes(scratch));
    builder.finish()
}

/// Resolve one table's observation state into schema columns: system/lineage
/// columns first, then source columns in first-seen order.
pub(crate) fn resolve_schema(buffer: &mut TableBuffer) -> TableSchema {
    let mut columns: Vec<ColumnDef> = Vec::new();
    let system = |name: &str, ty| ColumnDef {
        name: name.to_owned(),
        ty: rdlt_core::ColumnType::scalar(ty),
        nullable: false,
        provenance: Provenance::System,
    };
    columns.push(system(
        system_columns::LOAD_ID,
        rdlt_core::LogicalType::Utf8,
    ));
    columns.push(system(system_columns::ID, rdlt_core::LogicalType::Utf8));
    if buffer.parent.is_some() {
        columns.push(system(
            system_columns::PARENT_ID,
            rdlt_core::LogicalType::Utf8,
        ));
        columns.push(system(system_columns::POS, rdlt_core::LogicalType::Int64));
        columns.push(system(
            system_columns::ROOT_ID,
            rdlt_core::LogicalType::Utf8,
        ));
    }

    let sources: Vec<(String, Option<rdlt_core::ColumnType>)> = buffer
        .columns
        .iter()
        .map(|(key, state)| (key.clone(), state.resolve()))
        .collect();
    for (source_key, resolved) in sources {
        if let Some(ty) = resolved {
            let name = buffer.column_name(&source_key);
            columns.push(ColumnDef {
                name,
                ty,
                nullable: true,
                provenance: Provenance::Inferred,
            });
        }
    }

    TableSchema {
        table: buffer.table.clone(),
        parent: buffer.parent.clone(),
        columns,
    }
}
