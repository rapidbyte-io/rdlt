//! Schema registry: current version per table, evolution as `schema::Delta`s.
//! Deltas are the ONLY way schemas change, and a delta is always
//! emitted before the first batch at its `to` version.

use std::collections::BTreeMap;

use rdlt_core::id::TableName;
use rdlt_core::schema::{self, ColumnType, TableSchema};

use crate::shred::infer;

#[derive(Debug, Default)]
pub(crate) struct SchemaRegistry {
    tables: BTreeMap<TableName, TableSchema>,
}

impl SchemaRegistry {
    /// Has this stream established any table yet? Distinguishes the resolve that
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

/// Structural widening check (used in debug assertions): the shared scalar
/// lattice order at every scalar position, struct fields append/widen by
/// name, anything → Json. Deliberately a predicate of its own beside the
/// cross-batch join in `shred::types`: the join is order-preserving where this
/// check is order-blind over struct fields, so the two agree everywhere except
/// a struct whose fields were merely reordered — pinned below.
fn is_widening(from: &ColumnType, to: &ColumnType) -> bool {
    match (from, to) {
        (
            _,
            ColumnType::Scalar {
                scalar: rdlt_core::types::LogicalType::Json,
            },
        ) => true,
        (ColumnType::Scalar { scalar: a }, ColumnType::Scalar { scalar: b }) => {
            infer::is_widening_of(*a, *b)
        }
        (ColumnType::ScalarList { item: a }, ColumnType::ScalarList { item: b }) => {
            infer::is_widening_of(*a, *b)
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
    //! `is_widening` guards a `debug_assert!`, and an assertion that always
    //! passes fails no test. The function is pure, so it is pinned DIRECTLY
    //! rather than through the assertion it feeds — the only way to notice a
    //! weakened invariant check is to check the checker.
    use super::*;
    use rdlt_core::schema::{Column, Provenance};
    use rdlt_core::types::LogicalType;

    use crate::shred::types::join_column_types;

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

    /// The predicate and the cross-batch join answer the SAME lattice: over
    /// every scalar pair, every list pair, and every struct pair whose fields
    /// keep their order, `is_widening(a, b)` holds exactly when the join of
    /// `a` and `b` is `b`. The one recorded difference is a struct whose
    /// fields were merely REORDERED — the predicate is order-blind and
    /// accepts it, the order-preserving join does not — which is why the two
    /// stay separate functions.
    #[test]
    fn the_predicate_agrees_with_the_join_except_on_reordered_struct_fields() {
        use LogicalType::*;
        let scalars = [
            Bool,
            Int64,
            Float64,
            Utf8,
            Uuid,
            Json,
            Binary,
            TimestampTz,
            TimestampNaive,
            Date,
            Time,
            Decimal {
                precision: 10,
                scale: 2,
            },
            Decimal {
                precision: 38,
                scale: 0,
            },
        ];
        let mut shapes: Vec<ColumnType> = Vec::new();
        for s in scalars {
            shapes.push(scalar(s));
            shapes.push(ColumnType::ScalarList { item: s });
        }
        for a in [Int64, Utf8, Json] {
            for b in [Int64, Float64, Utf8] {
                shapes.push(ColumnType::Struct {
                    fields: vec![field("a", scalar(a)), field("b", scalar(b))],
                });
                shapes.push(ColumnType::Struct {
                    fields: vec![field("a", scalar(a))],
                });
                shapes.push(ColumnType::Struct {
                    fields: vec![
                        field("a", scalar(a)),
                        field("b", scalar(b)),
                        field("c", scalar(Bool)),
                    ],
                });
            }
        }
        // Every generated struct lists its fields in one fixed order, so no
        // pair below is a mere reordering.
        let mut compared = 0usize;
        for from in &shapes {
            for to in &shapes {
                let joined =
                    join_column_types(from, to).expect("shallow shapes never hit the depth cap");
                assert_eq!(
                    is_widening(from, to),
                    &joined == to,
                    "predicate vs join disagree on {from:?} -> {to:?} (join gives {joined:?})"
                );
                compared += 1;
            }
        }
        assert!(compared > 1000, "the oracle compared {compared} pairs");

        // The recorded difference, stated: a reordered struct passes the
        // order-blind predicate and fails the order-preserving join.
        let before = ColumnType::Struct {
            fields: vec![field("a", scalar(Int64)), field("b", scalar(Utf8))],
        };
        let reordered = ColumnType::Struct {
            fields: vec![field("b", scalar(Utf8)), field("a", scalar(Int64))],
        };
        assert!(is_widening(&before, &reordered));
        assert_ne!(
            join_column_types(&before, &reordered).expect("shallow"),
            reordered
        );
    }
}
