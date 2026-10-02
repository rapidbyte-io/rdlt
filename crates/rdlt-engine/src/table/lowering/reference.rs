//! The reference for the lowering differential (spec §20.4): each value lowered on its own, from
//! the table's model alone, sharing none of the routes and conversions lowering plans make once.
//!
//! Every conversion is exact, so a value's column holds the value itself, whatever type the
//! column has and however the destination stores it. The reference says what each value means as
//! a [`Canon`]; the differential reads every stored cell back to one and compares.

use rdlt_connector::{ColumnKey, ColumnPath, LogicalType};
use rdlt_testkit::canon::{Canon, canonical_into, holds};
use rdlt_testkit::drawn::Scalar;

use crate::policy::SchemaPolicy;
use crate::table::TableView;

/// What lowering a batch into a view should give.
#[derive(Debug, Default, PartialEq)]
pub(super) struct Expected {
    /// Each kept row's value in each of the view's columns.
    pub(super) rows: Vec<Vec<Canon>>,
    /// For each of the view's columns, the type of the batch's values it holds, if any.
    pub(super) sources: Vec<Option<LogicalType>>,
    pub(super) discarded_rows: u64,
    pub(super) discarded_values: u64,
    /// Whether a kept value is beyond what its column's type holds, which refuses the batch: a
    /// time in a unit whose `i64` cannot hold it.
    pub(super) refused: bool,
}

/// `rows` of a batch whose columns are `columns`, names and types, lowered into `view` value by
/// value; a value no column of the view holds is a change `policy` discards.
pub(super) fn lower(
    view: &TableView,
    policy: SchemaPolicy,
    columns: &[(String, LogicalType)],
    rows: &[Vec<Scalar>],
) -> Expected {
    let targets: Vec<Option<usize>> = columns
        .iter()
        .enumerate()
        .map(|(index, (name, logical))| holding(view, name, logical, exact(rows, index)))
        .collect();
    let mut expected = Expected {
        sources: vec![None; view.model.columns.len()],
        ..Expected::default()
    };
    for (target, (_, logical)) in targets.iter().zip(columns) {
        if let Some(column) = target {
            expected.sources[*column] = Some(logical.clone());
        }
    }
    for row in rows {
        let changes = row
            .iter()
            .zip(&targets)
            .filter(|(value, target)| target.is_none() && **value != Scalar::Null)
            .count() as u64;
        if changes > 0 && policy == SchemaPolicy::DiscardRow {
            expected.discarded_rows += 1;
            continue;
        }
        expected.discarded_values += changes;
        let mut cells = vec![Canon::Null; view.model.columns.len()];
        for ((value, target), (_, logical)) in row.iter().zip(&targets).zip(columns) {
            if let Some(column) = target {
                let to = view.model.columns[*column].logical_type();
                expected.refused |= !holds(value, logical, to);
                cells[*column] = canonical_into(value, logical, to);
            }
        }
        expected.rows.push(cells);
    }
    expected
}

/// Whether every value of the batch's column `column` in `rows` is an integer a 64-bit float holds
/// exactly.
fn exact(rows: &[Vec<Scalar>], column: usize) -> bool {
    rows.iter().all(|row| match &row[column] {
        Scalar::Int(value) => value.unsigned_abs() <= crate::table::EXACT_IN_FLOAT,
        _ => true,
    })
}

/// The view's column holding values of `logical` for the source column `name`: its own column,
/// else its variants in the order of their kinds, whichever first holds every value of the type.
///
/// The own column also holds 64-bit integers cast where it is one of 64-bit floats and every one
/// is `exact`.
fn holding(view: &TableView, name: &str, logical: &LogicalType, exact: bool) -> Option<usize> {
    let path = ColumnPath::from(name);
    let mut variants: Vec<(rdlt_connector::TypeKind, usize)> = view
        .model
        .columns
        .iter()
        .enumerate()
        .filter_map(
            |(index, column)| match view.model.names.owner(column.name()) {
                Some(ColumnKey::Variant { column, kind }) if *column == path => {
                    Some((*kind, index))
                }
                _ => None,
            },
        )
        .collect();
    variants.sort_unstable();
    let own = view
        .model
        .names
        .get(&ColumnKey::Source(path.clone()))
        .and_then(|name| {
            view.model
                .columns
                .iter()
                .position(|field| field.name() == name)
        });
    let cast = |index: usize| {
        exact
            && *logical == LogicalType::Int64
            && *view.model.columns[index].logical_type() == LogicalType::Float64
    };
    let fits = |index: &usize| {
        let column = view.model.columns[*index].logical_type();
        column.join(logical) == *column
    };
    own.filter(|index| fits(index) || cast(*index))
        .or_else(|| variants.into_iter().map(|(_, index)| index).find(fits))
}
