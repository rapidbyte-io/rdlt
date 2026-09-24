//! How a reference destination keeps a table's columns as schema changes arrive.

use rdlt_connector::{ConnectorError, Field, LogicalType, Result, TableChange, TableSchema};

/// The columns of a table that has `schema` once `change` applies to it; `None` is a table that
/// does not exist yet.
///
/// A change the columns already reflect changes nothing. A column declared at a type the table's
/// column does not hold is a `Data` error coded `schema_conflict`; a widen makes the column the
/// join of its type and the new one.
pub(crate) fn changed(schema: Option<&TableSchema>, change: &TableChange) -> Result<TableSchema> {
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
    .with_code("schema_conflict")
}
