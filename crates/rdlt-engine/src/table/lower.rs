//! Lowering: how a destination stores each logical type, and the physical columns of a table.

use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use rdlt_connector::{Capabilities, Field, LogicalType, TimeUnit, TypeKind};

use super::model::Model;
use crate::policy::Nested;

/// The identifiers of a table's metadata columns under the destination's rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MetaNames {
    pub(crate) load_id: Arc<str>,
    pub(crate) loaded_at: Arc<str>,
    /// The sequence column, for merge tables.
    pub(crate) seq: Option<Arc<str>>,
}

impl MetaNames {
    /// Every metadata identifier, which source columns may not take.
    pub(crate) fn all(&self) -> Vec<&str> {
        let mut names = vec![self.load_id.as_ref(), self.loaded_at.as_ref()];
        names.extend(self.seq.as_deref());
        names
    }
}

/// The type of the load id column: the load's UUID.
pub(crate) const LOAD_ID_TYPE: LogicalType = LogicalType::Uuid;

/// The type of the loaded-at column.
pub(crate) fn loaded_at_type() -> LogicalType {
    LogicalType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

/// How the destination stores a column of `logical` under `nested`.
///
/// A type the destination stores natively stays. Nested values stay native only when every value
/// inside them is native too and the policy asks for it; otherwise they, like `Json`, become
/// `Json`, or text where the destination has no JSON type. Any other type becomes its text.
pub(crate) fn lower(
    logical: &LogicalType,
    nested: Nested,
    capabilities: &Capabilities,
) -> LogicalType {
    match logical {
        LogicalType::Struct(_) | LogicalType::List(_) => {
            if nested == Nested::Native && native(logical, capabilities) {
                logical.clone()
            } else {
                json(capabilities)
            }
        }
        LogicalType::Json => json(capabilities),
        other if capabilities.types.contains(&other.kind()) => other.clone(),
        _ => LogicalType::Utf8,
    }
}

/// `Json`, or text where the destination has no JSON type.
fn json(capabilities: &Capabilities) -> LogicalType {
    if capabilities.types.contains(&TypeKind::Json) || capabilities.nested.json {
        LogicalType::Json
    } else {
        LogicalType::Utf8
    }
}

/// Whether the destination stores `logical` and everything inside it natively.
fn native(logical: &LogicalType, capabilities: &Capabilities) -> bool {
    match logical {
        LogicalType::Null => true,
        LogicalType::Struct(fields) => {
            capabilities.nested.structs
                && fields
                    .iter()
                    .all(|field| native(field.logical_type(), capabilities))
        }
        LogicalType::List(item) => {
            capabilities.nested.lists && native(item.logical_type(), capabilities)
        }
        LogicalType::Json => capabilities.types.contains(&TypeKind::Json),
        other => capabilities.types.contains(&other.kind()),
    }
}

/// The columns a table has in the destination: the model's columns lowered, then the metadata
/// columns.
pub(crate) fn physical_fields(
    model: &Model,
    nested: &[Nested],
    meta: &MetaNames,
    capabilities: &Capabilities,
) -> Vec<Field> {
    let mut fields: Vec<Field> = model
        .columns
        .iter()
        .zip(nested)
        .map(|(column, nested)| {
            Field::new(
                column.name(),
                lower(column.logical_type(), *nested, capabilities),
                column.is_nullable(),
            )
        })
        .collect();
    let native = Nested::Native;
    fields.push(Field::new(
        Arc::clone(&meta.load_id),
        lower(&LOAD_ID_TYPE, native, capabilities),
        false,
    ));
    fields.push(Field::new(
        Arc::clone(&meta.loaded_at),
        lower(&loaded_at_type(), native, capabilities),
        false,
    ));
    if let Some(seq) = &meta.seq {
        fields.push(Field::new(
            Arc::clone(seq),
            lower(&LogicalType::Binary, native, capabilities),
            false,
        ));
    }
    fields
}

/// The Arrow schema of `fields`.
pub(crate) fn arrow_schema(fields: &[Field]) -> SchemaRef {
    Arc::new(Schema::new(
        fields.iter().map(Field::to_arrow).collect::<Vec<_>>(),
    ))
}
