//! Rows rendered canonically, so the tables of two loads compare whatever their batches' shapes
//! and whatever the metadata the engine stamped on them.

use std::cmp::Ordering;

use arrow_array::RecordBatch;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use rdlt_connector::META_PREFIX;

use crate::protocol::Violation;

/// Every row of `batches`, each rendered as its columns by name, without the engine's metadata
/// columns, which differ between loads; sorted.
pub(crate) fn rendered(batches: &[RecordBatch]) -> Result<Vec<String>, Violation> {
    let options = FormatOptions::default().with_null("null");
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let mut columns = Vec::new();
        for (field, column) in schema.fields().iter().zip(batch.columns()) {
            if field.name().starts_with(META_PREFIX) {
                continue;
            }
            let formatter = ArrayFormatter::try_new(column.as_ref(), &options)
                .map_err(|error| format!("column `{}` does not render: {error}", field.name()))?;
            columns.push((field.name().as_str(), formatter));
        }
        columns.sort_by(|left, right| left.0.cmp(right.0));
        for row in 0..batch.num_rows() {
            let values: Vec<String> = columns
                .iter()
                .map(|(name, formatter)| format!("{name}={}", formatter.value(row)))
                .collect();
            rows.push(values.join(", "));
        }
    }
    rows.sort_unstable();
    Ok(rows)
}

/// The first row, in order, that one of two tables holds and the other lacks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Parted<'a> {
    /// A row the table never killed holds, and the table once killed lacks.
    Missing(&'a str),
    /// A row the table once killed holds, and the table never killed lacks.
    Extra(&'a str),
}

/// Where `killed`'s rows first part from `clean`'s, both [`rendered`]; `None` when they are the
/// same.
pub(crate) fn parted<'a>(clean: &'a [String], killed: &'a [String]) -> Option<Parted<'a>> {
    // Both are sorted: the first place they part says which row is missing or extra.
    let at = clean
        .iter()
        .zip(killed)
        .position(|(clean, killed)| clean != killed)
        .unwrap_or(clean.len().min(killed.len()));
    match (clean.get(at), killed.get(at)) {
        (None, None) => None,
        (Some(row), None) => Some(Parted::Missing(row)),
        (None, Some(row)) => Some(Parted::Extra(row)),
        (Some(row), Some(other)) => Some(match row.cmp(other) {
            Ordering::Less => Parted::Missing(row),
            _ => Parted::Extra(other),
        }),
    }
}
