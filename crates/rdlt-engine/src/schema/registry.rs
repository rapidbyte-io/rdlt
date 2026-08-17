//! Schema registry: current version per table, evolution as `schema::Delta`s.
//! Deltas are the ONLY way schemas change, and a delta is always
//! emitted before the first batch at its `to` version.

use std::collections::BTreeMap;

use rdlt_core::id::TableName;
use rdlt_core::schema::{self, ColumnType, TableSchema};

#[derive(Debug, Default)]
pub(crate) struct SchemaRegistry {
    tables: BTreeMap<TableName, TableSchema>,
}

impl SchemaRegistry {
    /// Has this stream established any table yet? Distinguishes the drain that
    /// creates a stream's initial shape from every later one, which is what makes
    /// a mid-run table creation policeable without treating the bootstrap as
    /// drift.
    pub(crate) fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    pub(crate) fn get(&self, table: &TableName) -> Option<&TableSchema> {
        self.tables.get(table)
    }

    /// Store `observed` as current, described by pre-computed `changes` (usually from
    /// [`Self::diff`], possibly policy-filtered). No-op when nothing changed. Returns
    /// the delta paired with the now-current schema, so callers emitting a
    /// `Delta` load item need not look the schema back up.
    pub(crate) fn apply(
        &mut self,
        observed: TableSchema,
        changes: Vec<schema::Change>,
    ) -> Option<(schema::Delta, TableSchema)> {
        if changes.is_empty() {
            return None;
        }
        let from = self
            .tables
            .get(&observed.table)
            .map(TableSchema::content_hash);
        let delta = schema::Delta {
            table: observed.table.clone(),
            from,
            to: observed.content_hash(),
            changes,
        };
        let current = observed.clone();
        self.tables.insert(observed.table.clone(), observed);
        Some((delta, current))
    }

    /// Non-mutating: what would change if `observed` became current?
    /// Columns only append and types only widen — the shredder's observation states
    /// guarantee it; debug assertions verify (a shrink here is an engine bug).
    pub(crate) fn diff(&self, observed: &TableSchema) -> Vec<schema::Change> {
        let table = &observed.table;
        let Some(current) = self.tables.get(table) else {
            return vec![schema::Change::CreateTable {
                schema: observed.clone(),
            }];
        };

        let mut changes = Vec::new();
        for column in &observed.columns {
            match current.column(&column.name) {
                None => changes.push(schema::Change::AddColumn {
                    column: column.clone(),
                }),
                Some(existing) if existing.column_type != column.column_type => {
                    debug_assert!(
                        is_widening(&existing.column_type, &column.column_type),
                        "schema regression on {}.{}: {:?} -> {:?}",
                        table,
                        column.name,
                        existing.column_type,
                        column.column_type
                    );
                    changes.push(schema::Change::WidenColumn {
                        name: column.name.clone(),
                        from: existing.column_type.clone(),
                        to: column.column_type.clone(),
                    });
                }
                Some(_) => {}
            }
        }
        for existing in &current.columns {
            if schema::system::is_system(&existing.name) {
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
    use crate::shred::is_widening_of;
    match (from, to) {
        (
            _,
            ColumnType::Scalar {
                scalar: rdlt_core::types::LogicalType::Json,
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
                .is_some_and(|t| {
                    f.column_type == t.column_type || is_widening(&f.column_type, &t.column_type)
                })
        }),
        _ => false,
    }
}

#[cfg(test)]
mod widening_tests {
    //! `is_widening` guards a `debug_assert!`, which is precisely why nothing
    //! caught it drifting: an assertion that always passes fails no test. The
    //! function is pure, so it is pinned DIRECTLY rather than through the
    //! assertion it feeds — the only way to notice a weakened invariant check
    //! is to check the checker.
    use super::*;
    use rdlt_core::schema::{Column, Provenance};
    use rdlt_core::types::LogicalType;

    fn scalar(t: LogicalType) -> ColumnType {
        ColumnType::scalar(t)
    }

    fn field(name: &str, ty: ColumnType) -> Column {
        Column {
            name: name.into(),
            column_type: ty,
            nullable: true,
            provenance: Provenance::Inferred,
        }
    }

    #[test]
    fn widening_is_directional_and_json_is_the_top() {
        use LogicalType::*;

        // Up the lattice is widening; DOWN it is not. The second half is what a
        // `-> true` mutation erases, and a schema regression is exactly what the
        // assertion exists to catch.
        assert!(is_widening(&scalar(Int64), &scalar(Utf8)));
        assert!(is_widening(&scalar(Int64), &scalar(Float64)));
        assert!(!is_widening(&scalar(Utf8), &scalar(Int64)), "no narrowing");
        assert!(
            !is_widening(&scalar(Float64), &scalar(Int64)),
            "no narrowing"
        );

        // Json is the top: everything reaches it, from every shape.
        assert!(is_widening(&scalar(Utf8), &scalar(Json)));
        assert!(is_widening(&scalar(Binary), &scalar(Json)));
        assert!(
            is_widening(&ColumnType::Struct { fields: vec![] }, &scalar(Json)),
            "a struct widens to Json too — the arm is (_, Json), not (Scalar, Json)"
        );
        assert!(
            !is_widening(&scalar(Json), &scalar(Utf8)),
            "nothing leaves the top"
        );

        // Lists widen by their item type, on the same lattice.
        assert!(is_widening(
            &ColumnType::ScalarList { item: Int64 },
            &ColumnType::ScalarList { item: Utf8 }
        ));
        assert!(!is_widening(
            &ColumnType::ScalarList { item: Utf8 },
            &ColumnType::ScalarList { item: Int64 }
        ));

        // A struct widens when every existing field is still present and each
        // is IDENTICAL or itself widened. Comparing those field types by
        // equality is load-bearing: invert it and an unchanged field stops
        // counting as compatible.
        let before = ColumnType::Struct {
            fields: vec![field("a", scalar(Int64)), field("b", scalar(Utf8))],
        };
        let unchanged = before.clone();
        assert!(
            is_widening(&before, &unchanged),
            "identical fields are compatible — the == arm"
        );
        let widened = ColumnType::Struct {
            fields: vec![
                field("a", scalar(Utf8)),
                field("b", scalar(Utf8)),
                field("c", scalar(Int64)),
            ],
        };
        assert!(
            is_widening(&before, &widened),
            "widened field plus a new one"
        );
        let narrowed = ColumnType::Struct {
            fields: vec![field("a", scalar(Int64)), field("b", scalar(Int64))],
        };
        assert!(
            !is_widening(&before, &narrowed),
            "a narrowed field is a regression"
        );
        let dropped = ColumnType::Struct {
            fields: vec![field("a", scalar(Int64))],
        };
        assert!(
            !is_widening(&before, &dropped),
            "a dropped field is a regression"
        );

        // Shape changes are not widening (except to Json, covered above).
        assert!(!is_widening(
            &scalar(Int64),
            &ColumnType::Struct { fields: vec![] }
        ));
    }
}
