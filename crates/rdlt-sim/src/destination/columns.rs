//! The columns the destination holds for each table, as the schema changes applied to it
//! declare them, checked the way the destination contract says.

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use rdlt_connector::{ConnectorError, LogicalType, Result, TableChange, TableSchema};

/// A table's columns: identifier to stored type.
pub(crate) type Columns = BTreeMap<String, LogicalType>;

/// Applies `change` to `columns`; a widened column becomes the join of its type and the new one,
/// and a column declared at a type it does not hold is a `schema_conflict` that changes nothing.
pub(crate) fn apply(columns: &mut Columns, change: &TableChange) -> Result<()> {
    let mut next = columns.clone();
    let declared: Vec<(&str, &LogicalType, bool)> = match change {
        TableChange::Create { schema, .. } => schema
            .fields()
            .iter()
            .map(|field| (field.name(), field.logical_type(), false))
            .collect(),
        TableChange::AddColumn { field, .. } => vec![(field.name(), field.logical_type(), false)],
        TableChange::Widen { column, to, .. } => {
            if !columns.contains_key(column.as_ref()) {
                return Err(ConnectorError::data(format!("no column {column}")));
            }
            vec![(column.as_ref(), to, true)]
        }
    };
    for (name, to, widens) in declared {
        match next.get(name) {
            Some(held) if held.join(to) == *held => {}
            Some(held) if widens => {
                let joined = held.join(to);
                next.insert(name.to_owned(), joined);
            }
            Some(held) => {
                return Err(ConnectorError::data(format!(
                    "table {} already has column {name} as {held:?}, not {to:?}: {change:?}",
                    change.table().name
                ))
                .with_code("schema_conflict"));
            }
            None => {
                next.insert(name.to_owned(), to.clone());
            }
        }
    }
    *columns = next;
    Ok(())
}

/// What in `batch` its table's `columns` cannot take: a column the table lacks, or a type its
/// column does not hold.
pub(crate) fn unfit(columns: &Columns, batch: &RecordBatch) -> Vec<String> {
    let schema = match TableSchema::from_arrow(&batch.schema()) {
        Ok(schema) => schema,
        Err(error) => return vec![format!("unreadable batch schema: {error}")],
    };
    schema
        .fields()
        .iter()
        .filter_map(|field| {
            let name = field.name();
            let written = field.logical_type();
            match columns.get(name) {
                None => Some(format!("wrote column {name}, which the table lacks")),
                Some(held) if held.join(written) != *held => Some(format!(
                    "wrote {written:?} to column {name}, which holds {held:?}"
                )),
                Some(_) => None,
            }
        })
        .collect()
}
