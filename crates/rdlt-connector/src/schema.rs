//! Table schemas and column paths.

#[cfg(test)]
mod tests;

use std::fmt;
use std::sync::Arc;

use arrow_schema::Schema;
use serde::{Deserialize, Serialize};

use crate::limits::MAX_NESTING_DEPTH;
use crate::types::{Field, Fields, LogicalType, TypeError, TypeKind, UnsupportedType};

/// The ordered, uniquely named fields of a table, nested no deeper than
/// [`MAX_NESTING_DEPTH`] levels, counting a top-level column as the first.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "StoredSchema", into = "StoredSchema")]
pub struct TableSchema {
    fields: Fields,
}

/// A schema as it is stored.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredSchema {
    fields: Vec<Field>,
}

/// Why an Arrow schema has no table schema equivalent.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
    /// A field's Arrow type has no logical equivalent.
    #[error(transparent)]
    Unsupported(#[from] UnsupportedType),
    /// The fields are not a valid set.
    #[error(transparent)]
    Type(#[from] TypeError),
}

impl TableSchema {
    /// A schema of `fields`, which must have distinct names and nest no deeper than the limit.
    pub fn new(fields: Vec<Field>) -> Result<Self, TypeError> {
        let depth = depth(&fields);
        if depth > MAX_NESTING_DEPTH {
            return Err(TypeError::TooDeep {
                depth,
                limit: MAX_NESTING_DEPTH,
            });
        }
        Ok(Self {
            fields: Fields::new(fields)?,
        })
    }

    /// The fields, in order.
    pub fn fields(&self) -> &Fields {
        &self.fields
    }

    /// The field called `name`.
    pub fn field(&self, name: &str) -> Option<&Field> {
        self.fields.get(name)
    }

    /// The Arrow schema for this table.
    pub fn to_arrow(&self) -> Schema {
        Schema::new(self.fields.iter().map(Field::to_arrow).collect::<Vec<_>>())
    }

    /// The table schema for an Arrow schema; see [`Field::from_arrow`].
    pub fn from_arrow(schema: &Schema) -> Result<Self, SchemaError> {
        let fields = schema
            .fields()
            .iter()
            .map(|field| Field::from_arrow(field))
            .collect::<Result<_, _>>()?;
        Ok(Self::new(fields)?)
    }
}

impl TryFrom<StoredSchema> for TableSchema {
    type Error = TypeError;

    fn try_from(stored: StoredSchema) -> Result<Self, TypeError> {
        Self::new(stored.fields)
    }
}

impl From<TableSchema> for StoredSchema {
    fn from(schema: TableSchema) -> Self {
        Self {
            fields: schema.fields.into(),
        }
    }
}

/// How deep `fields` nest, counting a top-level field as the first level; found without
/// recursion, so a type of any depth is measured.
fn depth(fields: &[Field]) -> u64 {
    let mut deepest = 0;
    let mut pending: Vec<(&Field, u64)> = fields.iter().map(|field| (field, 1)).collect();
    while let Some((field, depth)) = pending.pop() {
        deepest = deepest.max(depth);
        let below = depth.saturating_add(1);
        match field.logical_type() {
            LogicalType::Struct(nested) => {
                pending.extend(nested.iter().map(|field| (field, below)));
            }
            LogicalType::List(item) => pending.push((item, below)),
            _ => {}
        }
    }
    deepest
}

/// A column addressed from a table's root: one segment per nesting level.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "Vec<String>", into = "Vec<String>")]
pub struct ColumnPath(Vec<Arc<str>>);

/// A column path had no segments.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("a column path needs at least one segment")]
pub struct EmptyColumnPath;

impl ColumnPath {
    /// A path of `segments`, root first.
    pub fn new<S: Into<Arc<str>>>(
        segments: impl IntoIterator<Item = S>,
    ) -> Result<Self, EmptyColumnPath> {
        let segments: Vec<Arc<str>> = segments.into_iter().map(Into::into).collect();
        if segments.is_empty() {
            return Err(EmptyColumnPath);
        }
        Ok(Self(segments))
    }

    /// The path's segments, root first.
    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(AsRef::as_ref)
    }
}

impl From<&str> for ColumnPath {
    fn from(column: &str) -> Self {
        Self(vec![Arc::from(column)])
    }
}

impl fmt::Display for ColumnPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let joined: Vec<&str> = self.segments().collect();
        f.write_str(&joined.join("."))
    }
}

impl TryFrom<Vec<String>> for ColumnPath {
    type Error = EmptyColumnPath;

    fn try_from(segments: Vec<String>) -> Result<Self, Self::Error> {
        Self::new(segments)
    }
}

impl From<ColumnPath> for Vec<String> {
    fn from(path: ColumnPath) -> Self {
        path.0.iter().map(ToString::to_string).collect()
    }
}

/// A table column, as a name map knows it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnKey {
    /// A source column, by its path from the table's root.
    Source(ColumnPath),
    /// A variant column: a sibling of a source column that holds its values of another type, for
    /// a destination that cannot change the source column's type.
    Variant {
        /// The source column.
        column: ColumnPath,
        /// The kind of values the variant holds.
        kind: TypeKind,
    },
}

impl ColumnKey {
    /// The source column this key belongs to.
    pub fn column(&self) -> &ColumnPath {
        match self {
            Self::Source(column) | Self::Variant { column, .. } => column,
        }
    }
}

impl From<ColumnPath> for ColumnKey {
    fn from(column: ColumnPath) -> Self {
        Self::Source(column)
    }
}

impl fmt::Display for ColumnKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(column) => write!(f, "{column}"),
            Self::Variant { column, kind } => write!(f, "{column} ({kind:?} variant)"),
        }
    }
}
