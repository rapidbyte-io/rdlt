//! A table as the engine models it: its columns by identifier with their logical types, and the
//! source column each one holds.

use rdlt_connector::{ColumnKey, Field, NameMap, SchemaVersion, TableSchema, TableState};

use crate::error::{Error, ErrorKind};

/// A table's columns and names; version 0 is a table not created yet.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Model {
    pub(crate) version: u32,
    /// The columns in order, metadata columns aside: identifiers, logical types and nullability.
    pub(crate) columns: Vec<Field>,
    pub(crate) names: NameMap,
}

impl Model {
    /// The committed model of a table, or an empty one when nothing is committed.
    pub(crate) fn from_state(state: Option<&TableState>) -> Result<Self, Error> {
        let Some(state) = state else {
            return Ok(Self::default());
        };
        let names = state.names.clone();
        let Some((SchemaVersion(version), schema)) = &state.schema else {
            return Ok(Self {
                names,
                ..Self::default()
            });
        };
        let columns: Vec<Field> = schema.fields().iter().cloned().collect();
        if let Some(orphan) = columns
            .iter()
            .find(|field| names.owner(field.name()).is_none())
        {
            return Err(Error::new(
                ErrorKind::Destination,
                format!("state names no source column for column {}", orphan.name()),
            )
            .with_code("state_invalid"));
        }
        Ok(Self {
            version: *version,
            columns,
            names,
        })
    }

    /// Whether the destination has the table.
    pub(crate) fn created(&self) -> bool {
        self.version > 0
    }

    /// The column holding `key`, with its position.
    pub(crate) fn column(&self, key: &ColumnKey) -> Option<(usize, &Field)> {
        let name = self.names.get(key)?;
        self.columns
            .iter()
            .enumerate()
            .find(|(_, field)| field.name() == name)
    }

    /// The model's columns as a schema.
    pub(crate) fn schema(&self) -> TableSchema {
        TableSchema::new(self.columns.clone())
            .expect("identifiers are distinct, since the name map is injective")
    }
}
