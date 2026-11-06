//! Declarative ops: renaming top-level columns, masking values and adding constant columns, each
//! applied with Arrow kernels.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, RecordBatchOptions};
use arrow_buffer::BooleanBuffer;
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};

use super::{Bound, Transform, TransformError};

/// Bytes an output array takes beyond its values: its buffers' rounding and its first offset.
pub(super) const ARRAY_ROOM: u64 = 256;

/// A declarative op: data the engine validates and bounds, safe in a tenant's configuration.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    /// Renames top-level columns, each from its first name to its second.
    Rename(Vec<(String, String)>),
    /// Adds top-level columns, each holding its literal in every row.
    Constant(Vec<(String, Literal)>),
}

/// The value a constant column holds.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Literal {
    /// Text, as `Utf8`.
    Text(String),
    /// A 64-bit integer.
    Integer(i64),
    /// A boolean.
    Boolean(bool),
}

impl Op {
    /// Renames each column from its first name to its second.
    pub fn rename<F: Into<String>, T: Into<String>>(
        pairs: impl IntoIterator<Item = (F, T)>,
    ) -> Self {
        Self::Rename(
            pairs
                .into_iter()
                .map(|(from, to)| (from.into(), to.into()))
                .collect(),
        )
    }

    /// Adds each column, holding its literal.
    pub fn constant<C: Into<String>>(columns: impl IntoIterator<Item = (C, Literal)>) -> Self {
        Self::Constant(
            columns
                .into_iter()
                .map(|(name, value)| (name.into(), value))
                .collect(),
        )
    }
}

impl Literal {
    fn data_type(&self) -> DataType {
        match self {
            Self::Text(_) => DataType::Utf8,
            Self::Integer(_) => DataType::Int64,
            Self::Boolean(_) => DataType::Boolean,
        }
    }

    /// Bytes: the literal's value, as its column holds it a row.
    fn bytes(&self) -> usize {
        match self {
            Self::Text(text) => text.len(),
            Self::Integer(_) => 8,
            Self::Boolean(_) => 1,
        }
    }
}

impl Transform for Op {
    fn name(&self) -> &'static str {
        match self {
            Self::Rename(_) => "rename",
            Self::Constant(_) => "constant",
        }
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        match self {
            Self::Rename(pairs) => renamed(input, pairs),
            Self::Constant(columns) => constants(input, columns),
        }
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        let schema = self.output(batch.schema_ref())?;
        let rows = batch.num_rows();
        let columns = match self {
            Self::Rename(_) => batch.columns().to_vec(),
            Self::Constant(constants) => {
                let mut columns = batch.columns().to_vec();
                columns.extend(constants.iter().map(|(_, value)| repeated(value, rows)));
                columns
            }
        };
        built(self.name(), schema, columns, rows)
    }

    fn lineage(&self, input: &Schema) -> Option<Vec<Vec<usize>>> {
        let mut lineage: Vec<Vec<usize>> = (0..input.fields().len())
            .map(|column| vec![column])
            .collect();
        if let Self::Constant(columns) = self {
            lineage.extend(columns.iter().map(|_| Vec::new()));
        }
        Some(lineage)
    }

    fn bound(&self, _input: &Schema) -> Option<Bound> {
        Some(match self {
            Self::Rename(_) => Bound::new(0),
            Self::Constant(columns) => columns.iter().fold(Bound::new(0), |bound, (_, value)| {
                bound.plus(&literal_bound(value))
            }),
        })
    }
}

/// The batch of `schema` holding `columns` of `rows` rows, which op `name` made.
fn built(
    name: &str,
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
    rows: usize,
) -> Result<RecordBatch, TransformError> {
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    RecordBatch::try_new_with_options(schema, columns, &options)
        .map_err(|_| TransformError::new(format!("{name} made columns unlike its schema")))
}

/// `input`'s fields as an op passes them on.
fn fields(input: &Schema) -> Vec<FieldRef> {
    input.fields().iter().cloned().collect()
}

fn renamed(input: &Schema, pairs: &[(String, String)]) -> Result<SchemaRef, TransformError> {
    let mut fields = fields(input);
    for (from, to) in pairs {
        let Some(at) = fields.iter().position(|field| field.name() == from) else {
            continue;
        };
        if fields.iter().any(|field| field.name() == to) {
            let message = format!("renaming {from} to {to} meets a column of that name");
            return Err(TransformError::new(message).with_column(to.clone()));
        }
        fields[at] = Arc::new(fields[at].as_ref().clone().with_name(to.clone()));
    }
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        input.metadata().clone(),
    )))
}

fn constants(input: &Schema, columns: &[(String, Literal)]) -> Result<SchemaRef, TransformError> {
    let mut fields = fields(input);
    for (name, value) in columns {
        if fields.iter().any(|field| field.name() == name) {
            let message = format!("constant {name} meets a column of that name");
            return Err(TransformError::new(message).with_column(name.clone()));
        }
        fields.push(Arc::new(Field::new(name, value.data_type(), false)));
    }
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        input.metadata().clone(),
    )))
}

/// A column of `rows` rows, each holding `value`, its buffers taken at their final sizes.
fn repeated(value: &Literal, rows: usize) -> ArrayRef {
    match value {
        Literal::Text(text) => {
            let mut builder = StringBuilder::with_capacity(rows, rows.saturating_mul(text.len()));
            for _ in 0..rows {
                builder.append_value(text);
            }
            Arc::new(builder.finish())
        }
        Literal::Integer(value) => Arc::new(Int64Array::from_value(*value, rows)),
        Literal::Boolean(true) => Arc::new(BooleanArray::new(BooleanBuffer::new_set(rows), None)),
        Literal::Boolean(false) => {
            Arc::new(BooleanArray::new(BooleanBuffer::new_unset(rows), None))
        }
    }
}

fn literal_bound(value: &Literal) -> Bound {
    let bytes = u64::try_from(value.bytes()).unwrap_or(u64::MAX);
    match value {
        Literal::Text(_) => Bound::new(ARRAY_ROOM).with_rows(bytes.saturating_add(4)),
        Literal::Integer(_) | Literal::Boolean(_) => Bound::new(ARRAY_ROOM).with_rows(bytes),
    }
}
