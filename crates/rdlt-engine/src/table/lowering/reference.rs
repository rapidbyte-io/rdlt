//! The reference for the lowering differential: each value lowered on its own, from the table's
//! model alone, sharing none of the routes and conversions lowering plans make once.
//!
//! Every conversion is exact, so a value's column holds the value itself, whatever type the
//! column has and however the destination stores it. The reference says what each value means as
//! a [`Canon`]; the differential reads every stored cell back to one and compares.

use std::collections::BTreeSet;

use rdlt_connector::{ChangeOp, ColumnKey, ColumnPath, LogicalType};
use rdlt_testkit::canon::{Canon, canonical, holds};
use rdlt_testkit::drawn::Scalar;

use crate::policy::SchemaPolicy;
use crate::table::TableView;

/// What lowering a batch into a view should give.
#[derive(Debug, Default, PartialEq)]
pub(super) struct Expected {
    /// Each kept row's value in each of the view's columns.
    pub(super) rows: Vec<Vec<Canon>>,
    /// The position in the batch of each kept row, in order.
    pub(super) kept: Vec<usize>,
    /// For each of the view's columns, the type of the batch's values it holds, if any.
    pub(super) sources: Vec<Option<LogicalType>>,
    /// For each of the batch's columns, the view's column its values go to as they are, where
    /// one does: where its unchanged flag is kept.
    pub(super) written: Vec<Option<usize>>,
    pub(super) discarded_rows: u64,
    pub(super) discarded_values: u64,
    /// Whether a kept value is beyond what its column's type holds where the column's policy
    /// refuses it, which refuses the batch: a time in a unit whose `i64` cannot hold it.
    pub(super) refused: bool,
}

/// `rows` of a batch whose columns are `columns`, names and types, lowered into `view` value by
/// value, each column following its policy in `policies`: a value no column of the view holds is
/// a change the policy discards, and a value its column's type cannot hold is refused, dropped
/// with its row or nulled.
pub(super) fn lower(
    view: &TableView,
    policies: &[SchemaPolicy],
    columns: &[(String, LogicalType)],
    rows: &[Vec<Scalar>],
) -> Expected {
    let targets: Vec<Option<usize>> = columns
        .iter()
        .enumerate()
        .map(|(index, (name, logical))| holding(view, name, logical, exact(rows, index)))
        .collect();
    let splits = splits(view, columns);
    let mut expected = outline(view, columns, &targets, &splits);
    let held = |value: &Scalar, split: &Option<usize>| held(view, value, *split);
    for (position, row) in rows.iter().enumerate() {
        let (mut drops, mut discarded, mut refused) = (false, 0, false);
        let mut nulled = vec![false; columns.len()];
        for (index, value) in row.iter().enumerate() {
            if *value == Scalar::Null || held(value, &splits[index]).is_some() {
                continue;
            }
            let unheld = match targets[index] {
                // A change: no column of the view holds the value.
                None => true,
                Some(column) => {
                    let to = view.model.columns[column].logical_type();
                    !holds(value, &columns[index].1, to)
                }
            };
            if !unheld {
                continue;
            }
            match policies[index] {
                SchemaPolicy::DiscardRow => drops = true,
                SchemaPolicy::DiscardValue => {
                    discarded += 1;
                    nulled[index] = true;
                }
                // A change under another policy is applied or refused before lowering; a value
                // its column cannot hold refuses the batch.
                _ if targets[index].is_none() => discarded += 1,
                _ => refused = true,
            }
        }
        if drops {
            expected.discarded_rows += 1;
            continue;
        }
        expected.discarded_values += discarded;
        expected.refused |= refused;
        let mut cells = vec![Canon::Null; view.model.columns.len()];
        for (index, ((value, target), (_, logical))) in
            row.iter().zip(&targets).zip(columns).enumerate()
        {
            if let Some((own, (scalar, from))) = held(value, &splits[index]) {
                cells[own] = canonical(&scalar, &from);
            } else if let Some(column) = target
                && !nulled[index]
            {
                cells[*column] = canonical(value, logical);
            }
        }
        expected.rows.push(cells);
        expected.kept.push(position);
    }
    expected
}

/// What lowering a batch whose columns are `columns` into `view` gives before any row: where each
/// column's values go, `targets` the columns holding them as they are and `splits` the own
/// columns of those of JSON that read them.
fn outline(
    view: &TableView,
    columns: &[(String, LogicalType)],
    targets: &[Option<usize>],
    splits: &[Option<usize>],
) -> Expected {
    let mut expected = Expected {
        sources: vec![None; view.model.columns.len()],
        written: targets
            .iter()
            .zip(splits)
            .map(|(target, split)| if split.is_none() { *target } else { None })
            .collect(),
        ..Expected::default()
    };
    for (target, (_, logical)) in targets.iter().zip(columns) {
        if let Some(column) = target {
            expected.sources[*column] = Some(logical.clone());
        }
    }
    for own in splits.iter().flatten() {
        expected.sources[*own] = Some(view.model.columns[*own].logical_type().clone());
    }
    expected
}

/// The column `value`, of a column of JSON whose own column is `split`, goes to and what it is
/// there, where its own column holds it.
fn held(
    view: &TableView,
    value: &Scalar,
    split: Option<usize>,
) -> Option<(usize, (Scalar, LogicalType))> {
    let (Scalar::Json(value), Some(own)) = (value, split) else {
        return None;
    };
    let to = view.model.columns[own].logical_type();
    rdlt_testkit::held::held(value, to).map(|scalar| (own, scalar))
}

/// For each of `columns`, its own column where it is JSON and its own column is of another type,
/// which then takes each value it holds.
fn splits(view: &TableView, columns: &[(String, LogicalType)]) -> Vec<Option<usize>> {
    let positions = view.model.positions();
    columns
        .iter()
        .map(|(name, logical)| {
            let key = ColumnKey::Source(ColumnPath::from(name.as_str()));
            let own = *positions.get(view.model.names.get(&key)?)?;
            let json = *logical == LogicalType::Json;
            let typed = *view.model.columns[own].logical_type() != LogicalType::Json;
            (json && typed).then_some(own)
        })
        .collect()
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

/// Of `kept`, positions of a batch's rows, those a merge by key keeps: the last of each key in
/// `keys`, in their order.
pub(super) fn merged(kept: &[usize], keys: &[i64]) -> Vec<usize> {
    kept.iter()
        .enumerate()
        .filter(|(at, row)| {
            !kept[at + 1..]
                .iter()
                .any(|later| keys[*later] == keys[**row])
        })
        .map(|(_, row)| *row)
        .collect()
}

/// The view's columns that pushed `flags` flag unchanged, where the pushed batch has `columns`
/// data columns after its key and `written[i]` is where data column `i` goes as it is; `None`
/// where the row flags nothing.
///
/// Pushed ordinal 1 is the key, data column 0; pushed ordinal `3 + i` is data column `1 + i`;
/// the op, sequence and unchanged columns and every ordinal past the fields flag nothing.
pub(super) fn written(
    flags: Option<u16>,
    columns: usize,
    written: &[Option<usize>],
) -> Option<BTreeSet<usize>> {
    let flags = flags?;
    let data = |pushed: usize| match pushed {
        1 => Some(0),
        pushed if (3..3 + columns).contains(&pushed) => Some(pushed - 2),
        _ => None,
    };
    Some(
        (0..16)
            .filter(|pushed| flags & (1 << pushed) != 0)
            .filter_map(|pushed| written[data(pushed)?])
            .collect(),
    )
}

/// Of `kept`, positions of a change batch's rows, those a merge with hard deletes keeps: each
/// row no later row of its key supersedes.
///
/// An insert or update that flags no stored column unchanged supersedes, as a delete does;
/// `flags[i]` are those row `i` flags.
pub(super) fn unsuperseded(
    kept: &[usize],
    keys: &[i64],
    ops: &[ChangeOp],
    flags: &[Option<BTreeSet<usize>>],
) -> Vec<usize> {
    let supersedes = |row: usize| match ops[row] {
        ChangeOp::Insert | ChangeOp::Update => flags[row].as_ref().is_none_or(BTreeSet::is_empty),
        ChangeOp::Delete => true,
        ChangeOp::Truncate => false,
    };
    kept.iter()
        .copied()
        .filter(|row| {
            !kept
                .iter()
                .any(|later| *later > *row && keys[*later] == keys[*row] && supersedes(*later))
        })
        .collect()
}
