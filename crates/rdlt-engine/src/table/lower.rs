//! Lowering: how a destination stores each logical type, and the physical columns of a table.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_schema::{DataType, Field as ArrowField, Schema, SchemaRef};
use rdlt_connector::{
    Capabilities, Field, ID_COLUMN, IDX_COLUMN, LOAD_ID_COLUMN, LOADED_AT_COLUMN, LogicalType,
    PARENT_ID_COLUMN, ROOT_ID_COLUMN, SEQ_COLUMN, TimeUnit, TypeKind,
};

use super::model::Model;
use crate::error::Error;
use crate::naming::Naming;
use crate::policy::Nested;

/// The identifiers of a table's metadata columns under the destination's rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MetaNames {
    pub(crate) load_id: Arc<str>,
    pub(crate) loaded_at: Arc<str>,
    /// The sequence column, for merge tables.
    pub(crate) seq: Option<Arc<str>>,
    /// Each row's id, for the tables of normalized streams.
    pub(crate) id: Option<Arc<str>>,
    /// The parent's id, the root's id and the position in the parent's array, for child tables.
    pub(crate) parent: Option<[Arc<str>; 3]>,
}

/// Which lineage columns a table has.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LineageColumns {
    /// None: the stream does not normalize.
    None,
    /// The row's id: a normalized stream's own table.
    Root,
    /// The row's id and its parent's, root's and position: a child table.
    Child,
}

impl MetaNames {
    /// The metadata identifiers of a table under `naming`'s rules: a sequence column for a merge
    /// table, and the lineage columns it has.
    pub(crate) fn assign(
        naming: &Naming,
        merge: bool,
        lineage: LineageColumns,
    ) -> Result<Self, Error> {
        let mut taken = BTreeSet::new();
        let mut name = |column: &str| -> Result<Arc<str>, Error> {
            let name = naming.metadata(column, &taken)?;
            taken.insert(name.clone());
            Ok(name.into())
        };
        Ok(Self {
            load_id: name(LOAD_ID_COLUMN)?,
            loaded_at: name(LOADED_AT_COLUMN)?,
            seq: merge.then(|| name(SEQ_COLUMN)).transpose()?,
            id: (lineage != LineageColumns::None)
                .then(|| name(ID_COLUMN))
                .transpose()?,
            parent: match lineage {
                LineageColumns::Child => Some([
                    name(PARENT_ID_COLUMN)?,
                    name(ROOT_ID_COLUMN)?,
                    name(IDX_COLUMN)?,
                ]),
                _ => None,
            },
        })
    }

    /// Every metadata identifier, which source columns may not take.
    pub(crate) fn all(&self) -> Vec<&str> {
        let mut names = vec![self.load_id.as_ref(), self.loaded_at.as_ref()];
        names.extend(self.seq.as_deref());
        names.extend(self.id.as_deref());
        names.extend(self.parent.iter().flatten().map(AsRef::as_ref));
        names
    }
}

/// The type of the lineage id columns: 16 bytes of xxh3-128.
pub(crate) const ID_TYPE: LogicalType = LogicalType::Binary;

/// The type of the position column.
pub(crate) const IDX_TYPE: LogicalType = LogicalType::Int64;

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
/// `Json`, or text where the destination has no JSON type. Nested values a normalized stream
/// leaves are deeper than it normalizes, and are `Json` too. Any other type becomes its text.
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
    // Lineage columns are nullable: rows loaded before their stream normalized have none.
    if let Some(id) = &meta.id {
        fields.push(Field::new(
            Arc::clone(id),
            lower(&ID_TYPE, native, capabilities),
            true,
        ));
    }
    if let Some([parent, root, idx]) = &meta.parent {
        for (name, logical) in [(parent, ID_TYPE), (root, ID_TYPE), (idx, IDX_TYPE)] {
            fields.push(Field::new(
                Arc::clone(name),
                lower(&logical, native, capabilities),
                true,
            ));
        }
    }
    fields
}

/// The Arrow schema of prepared batches of a table whose columns are `fields`, the columns of the
/// `model`, of these types, first: each stored as another type names its own, and the load id and
/// load start, which hold one value per batch, are dictionaries of it (spec §8.5).
pub(crate) fn prepared_schema(fields: &[Field], model: &[LogicalType]) -> SchemaRef {
    let columns = model.len();
    let fields: Vec<ArrowField> = fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let arrow = named(field.to_arrow(), field.logical_type(), model.get(index));
            if index == columns || index == columns + 1 {
                let encoded = DataType::Dictionary(
                    Box::new(DataType::Int8),
                    Box::new(arrow.data_type().clone()),
                );
                arrow.with_data_type(encoded)
            } else {
                arrow
            }
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// `arrow`, a column stored as `stored`, naming `logical`, the model's type for it, where the
/// destination stores it as another type.
#[expect(clippy::disallowed_types, reason = "Arrow field metadata is a HashMap")]
fn named(arrow: ArrowField, stored: &LogicalType, logical: Option<&LogicalType>) -> ArrowField {
    match logical {
        Some(logical) if logical != stored => {
            let mut metadata: std::collections::HashMap<String, String> = arrow.metadata().clone();
            let json = serde_json::to_string(logical).expect("a logical type serializes");
            metadata.insert(rdlt_connector::LOGICAL_TYPE_KEY.to_owned(), json);
            arrow.with_metadata(metadata)
        }
        _ => arrow,
    }
}
