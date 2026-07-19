//! Schema registry: current version per table, evolution as `SchemaDelta`s
//! (data-model.md §3). Deltas are the ONLY way schemas change, and a delta is always
//! emitted before the first batch at its `to` version (crash-replay invariant 3).

use std::collections::BTreeMap;

use rdlt_core::{ColumnType, SchemaChange, SchemaDelta, TableName, TableSchema, schema};

#[derive(Debug, Default)]
pub(crate) struct SchemaRegistry {
    tables: BTreeMap<TableName, TableSchema>,
}

impl SchemaRegistry {
    pub(crate) fn get(&self, table: &TableName) -> Option<&TableSchema> {
        self.tables.get(table)
    }

    /// Store `observed` as current, described by pre-computed `changes` (usually from
    /// [`Self::diff`], possibly policy-filtered). No-op when nothing changed.
    pub(crate) fn apply(
        &mut self,
        observed: TableSchema,
        changes: Vec<SchemaChange>,
    ) -> Option<SchemaDelta> {
        if changes.is_empty() {
            return None;
        }
        let from = self
            .tables
            .get(&observed.table)
            .map(TableSchema::content_hash);
        let delta = SchemaDelta {
            table: observed.table.clone(),
            from,
            to: observed.content_hash(),
            changes,
        };
        self.tables.insert(observed.table.clone(), observed);
        Some(delta)
    }

    /// Non-mutating: what would change if `observed` became current?
    /// Columns only append and types only widen — the shredder's observation states
    /// guarantee it; debug assertions verify (a shrink here is an engine bug).
    pub(crate) fn diff(&self, observed: &TableSchema) -> Vec<SchemaChange> {
        let table = &observed.table;
        let Some(current) = self.tables.get(table) else {
            return vec![SchemaChange::CreateTable {
                schema: observed.clone(),
            }];
        };

        let mut changes = Vec::new();
        for column in &observed.columns {
            match current.column(&column.name) {
                None => changes.push(SchemaChange::AddColumn {
                    column: column.clone(),
                }),
                Some(existing) if existing.ty != column.ty => {
                    debug_assert!(
                        is_widening(&existing.ty, &column.ty),
                        "schema regression on {}.{}: {:?} -> {:?}",
                        table,
                        column.name,
                        existing.ty,
                        column.ty
                    );
                    changes.push(SchemaChange::WidenColumn {
                        name: column.name.clone(),
                        from: existing.ty.clone(),
                        to: column.ty.clone(),
                    });
                }
                Some(_) => {}
            }
        }
        for existing in &current.columns {
            if schema::system_columns::is_system(&existing.name) {
                continue;
            }
            debug_assert!(
                observed.column(&existing.name).is_some(),
                "column {}.{} vanished from observation",
                table,
                existing.name
            );
        }
        changes
    }
}

/// Structural widening check (used in debug assertions): scalar-lattice order,
/// struct fields append/widen, anything → Json.
fn is_widening(from: &ColumnType, to: &ColumnType) -> bool {
    use rdlt_core::types::is_widening_of;
    match (from, to) {
        (
            _,
            ColumnType::Scalar {
                scalar: rdlt_core::LogicalType::Json,
            },
        ) => true,
        (ColumnType::Scalar { scalar: a }, ColumnType::Scalar { scalar: b }) => {
            is_widening_of(*a, *b)
        }
        (ColumnType::ScalarList { item: a }, ColumnType::ScalarList { item: b }) => {
            is_widening_of(*a, *b)
        }
        (
            ColumnType::Struct {
                fields: from_fields,
            },
            ColumnType::Struct { fields: to_fields },
        ) => from_fields.iter().all(|f| {
            to_fields
                .iter()
                .find(|t| t.name == f.name)
                .is_some_and(|t| f.ty == t.ty || is_widening(&f.ty, &t.ty))
        }),
        _ => false,
    }
}
