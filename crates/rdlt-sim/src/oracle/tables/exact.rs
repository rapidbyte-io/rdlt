//! Columns state records exact: a 64-bit float holds every integer they store exactly.

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_schema::DataType;

use crate::destination::Published;

/// Integers whose magnitude is at most this, 2⁵³, are exact as a 64-bit float.
const EXACT_IN_FLOAT: u64 = 1 << 53;

/// What in `published` breaks what state records: a column recorded exact holding an integer a
/// 64-bit float would round.
pub(super) fn findings(published: &Published) -> Vec<String> {
    let mut findings = Vec::new();
    for name in &published.exact {
        for stored in &published.rows {
            let Some(column) = stored.row.column_by_name(name) else {
                continue;
            };
            let Ok(integers) = arrow_cast::cast(column, &DataType::Int64) else {
                findings.push(format!(
                    "{name}, recorded exact, holds {}",
                    column.data_type()
                ));
                continue;
            };
            let integers = integers.as_primitive::<Int64Type>();
            findings.extend(
                integers
                    .iter()
                    .flatten()
                    .filter(|value| value.unsigned_abs() > EXACT_IN_FLOAT)
                    .map(|value| format!("{name}, recorded exact, holds {value}")),
            );
        }
    }
    findings
}
