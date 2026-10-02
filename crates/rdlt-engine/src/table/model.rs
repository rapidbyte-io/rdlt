//! A table as the engine models it: its columns by identifier with their logical types, and the
//! source column each one holds.

use std::collections::BTreeSet;
use std::sync::Arc;

use rdlt_connector::{
    ColumnKey, Field, LogicalType, NameMap, SchemaVersion, TableSchema, TableState,
};

use crate::error::{Error, ErrorKind};

/// A table's columns and names; version 0 is a table not created yet.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Model {
    /// The schema's version, which each change to the destination's table advances.
    pub(crate) version: u32,
    /// How many changes this attempt made to the model, of the schema or of which columns are
    /// exact, which state has recorded when it records the model.
    pub(crate) revision: u32,
    /// The columns in order, metadata columns aside: identifiers, logical types and nullability.
    pub(crate) columns: Vec<Field>,
    pub(crate) names: NameMap,
    /// The columns of 64-bit integers every stored value of which a 64-bit float holds exactly,
    /// which floats may join without loss.
    pub(crate) exact: BTreeSet<Arc<str>>,
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
        // Version 0 is a table not created, which records no schema.
        if *version == 0 {
            return Err(Error::new(
                ErrorKind::Destination,
                "state records a table's schema at version 0",
            )
            .with_code("state_invalid"));
        }
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
        // Only columns of 64-bit integers are exact: a widened column is no longer one.
        let exact = columns
            .iter()
            .filter(|field| *field.logical_type() == LogicalType::Int64)
            .filter_map(|field| state.exact.get(field.name()).cloned())
            .collect();
        Ok(Self {
            version: *version,
            revision: 0,
            columns,
            names,
            exact,
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
