//! How a reference destination keeps a table's columns as schema changes arrive, and holds
//! what its table writers stage to them.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use rdlt_connector::merge;
use rdlt_connector::{
    COLUMN_MISSING, ConnectorError, Field, LogicalType, MergeKey, Result, SCHEMA_CONFLICT,
    TableChange, TableSchema,
};

/// Refuses `batch`, written for the table `name` whose columns are `stored`, unless each of its
/// columns is one of the table's at a type the column holds, the op and unchanged flags of a
/// change stream merged by `key` apart.
///
/// A column the table lacks is a `Data` error coded `column_missing`, and a value its column's
/// type cannot hold one under the merge's codes.
pub(crate) fn written(
    name: &str,
    stored: &SchemaRef,
    key: Option<&MergeKey>,
    batch: &RecordBatch,
) -> Result<()> {
    let changes = key.and_then(|key| key.changes.as_ref());
    let flag = |column: &str| {
        changes.is_some_and(|changes| {
            *changes.op == *column || changes.unchanged.as_deref() == Some(column)
        })
    };
    let fields = batch.schema_ref().fields();
    let lacked = fields
        .iter()
        .map(|field| field.name())
        .find(|column| stored.column_with_name(column).is_none() && !flag(column));
    if let Some(column) = lacked {
        let message = format!("table {name} has no column {column}, which a batch holds");
        return Err(ConnectorError::data(message).with_code(COLUMN_MISSING));
    }
    merge::holds([batch], stored).map_err(|error| merge::failed("staging rows", error))
}

/// The columns of a table that has `schema` once `change` applies to it; `None` is a table that
/// does not exist yet.
///
/// A change the columns already reflect changes nothing. A column declared at a type the table's
/// column does not hold is a `Data` error coded `schema_conflict`; a widen makes the column the
/// join of its type and the new one. A type not in canonical form, which the engine declares
/// every type in, is a `Data` error, so the table holds only canonical types, which hold
/// exactly what the join says.
pub(crate) fn changed(schema: Option<&TableSchema>, change: &TableChange) -> Result<TableSchema> {
    canonical(change)?;
    let schema = match (schema, change) {
        (Some(schema), _) => schema,
        (None, TableChange::Create { schema, .. }) => return Ok(schema.clone()),
        (None, _) => {
            return Err(ConnectorError::data(format!(
                "table {} does not exist",
                change.table().name
            )));
        }
    };
    let mut fields: Vec<Field> = schema.fields().iter().cloned().collect();
    match change {
        TableChange::Create {
            schema: declared, ..
        } => {
            for field in declared.fields().iter() {
                add(&mut fields, field, change)?;
            }
        }
        TableChange::AddColumn { field, .. } => add(&mut fields, field, change)?,
        TableChange::Widen { column, to, .. } => {
            let Some(field) = fields
                .iter_mut()
                .find(|field| field.name() == column.as_ref())
            else {
                return Err(ConnectorError::data(format!(
                    "table {} has no column {column}",
                    change.table().name
                )));
            };
            if !holds(field, to) {
                let joined = field.logical_type().join(to);
                *field = Field::new(field.name(), joined, field.is_nullable());
            }
        }
    }
    TableSchema::new(fields)
        .map_err(|error| ConnectorError::internal(format!("changing a table: {error}")))
}

/// Adds `field` to `fields` as a nullable column unless a column of its name holding its type is
/// there.
fn add(fields: &mut Vec<Field>, field: &Field, change: &TableChange) -> Result<()> {
    match fields
        .iter()
        .find(|existing| existing.name() == field.name())
    {
        Some(existing) if holds(existing, field.logical_type()) => Ok(()),
        Some(existing) => Err(conflict(change, existing)),
        None => {
            fields.push(Field::new(field.name(), field.logical_type().clone(), true));
            Ok(())
        }
    }
}

/// Refuses `change` where it declares a type not in canonical form.
fn canonical(change: &TableChange) -> Result<()> {
    let declared: Vec<(&str, &LogicalType)> = match change {
        TableChange::Create { schema, .. } => schema
            .fields()
            .iter()
            .map(|field| (field.name(), field.logical_type()))
            .collect(),
        TableChange::AddColumn { field, .. } => vec![(field.name(), field.logical_type())],
        TableChange::Widen { column, to, .. } => vec![(column.as_ref(), to)],
    };
    match declared
        .into_iter()
        .find(|(_, logical)| **logical != logical.canonical())
    {
        Some((name, logical)) => Err(ConnectorError::data(format!(
            "table {} declares column {name} as {logical}, not in canonical form",
            change.table().name
        ))),
        None => Ok(()),
    }
}

/// Whether `column` holds every value of `logical`.
fn holds(column: &Field, logical: &LogicalType) -> bool {
    column.logical_type().join(logical) == *column.logical_type()
}

/// The error for `change` declaring `existing` at another type.
fn conflict(change: &TableChange, existing: &Field) -> ConnectorError {
    ConnectorError::data(format!(
        "table {} already has column {} as {:?}",
        change.table().name,
        existing.name(),
        existing.logical_type()
    ))
    .with_code(SCHEMA_CONFLICT)
}

#[cfg(test)]
mod tests;
