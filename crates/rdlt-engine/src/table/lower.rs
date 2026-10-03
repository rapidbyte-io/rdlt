//! Lowering: how a destination stores each logical type, and the physical columns of a table.

use std::sync::Arc;

use arrow_schema::{DataType, Field as ArrowField, Schema, SchemaRef};
use rdlt_connector::{
    Capabilities, DELETED_AT_COLUMN, Field, ID_COLUMN, IDX_COLUMN, IS_CURRENT_COLUMN,
    LOAD_ID_COLUMN, LOADED_AT_COLUMN, LogicalType, OP_COLUMN, PARENT_ID_COLUMN, ROOT_ID_COLUMN,
    ROW_HASH_COLUMN, SEQ_COLUMN, TimeUnit, TypeKind, UNCHANGED_COLUMN, VALID_FROM_COLUMN,
    VALID_TO_COLUMN,
};

use super::model::Model;
use crate::naming::Naming;
use crate::policy::Nested;

/// The identifiers of a table's metadata columns under the destination's rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MetaNames {
    pub(crate) load_id: Arc<str>,
    pub(crate) loaded_at: Arc<str>,
    /// The sequence column, for merge tables and change streams' tables.
    pub(crate) seq: Option<Arc<str>>,
    /// A change stream's op, unchanged and deleted-at columns.
    pub(crate) changes: Option<ChangeNames>,
    /// Each row's id, for the tables of normalized streams.
    pub(crate) id: Option<Arc<str>>,
    /// The parent's id, the root's id and the position in the parent's array, for child tables.
    pub(crate) parent: Option<[Arc<str>; 3]>,
    /// A history table's columns.
    pub(crate) history: Option<HistoryNames>,
}

/// The identifiers of a history table's columns, and where its versions' beginnings
/// come from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HistoryNames {
    pub(crate) valid_from: Arc<str>,
    pub(crate) valid_to: Arc<str>,
    pub(crate) is_current: Arc<str>,
    pub(crate) row_hash: Arc<str>,
    /// The incoming column naming when each change happened, the stream's change time; `None`
    /// begins each version when its batch arrived.
    pub(crate) change_time: Option<Arc<str>>,
}

/// How a change stream's table holds its changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChangeLayout {
    /// As a log: every change is a row, its op and unchanged columns stored.
    Log,
    /// Merged by key: the op and unchanged columns only direct the merge, and a soft delete
    /// records when it removed a row.
    Merge {
        /// Whether deletes keep their rows, recording when they removed them.
        soft: bool,
    },
}

/// The identifiers of a change stream's op, unchanged and deleted-at columns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChangeNames {
    pub(crate) op: Arc<str>,
    pub(crate) unchanged: Arc<str>,
    /// The column recording soft deletes, for a merge table whose deletes are soft.
    pub(crate) deleted_at: Option<Arc<str>>,
    /// Whether the op and unchanged columns are stored, as a log stores them; a merge table's
    /// only direct the merge, written after its stored columns.
    pub(crate) stored: bool,
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
    pub(crate) fn assign(naming: &Naming, merge: bool, lineage: LineageColumns) -> Self {
        Self::assign_changes(naming, merge, lineage, None)
    }

    /// [`MetaNames::assign`], and for a change stream's table the columns its `layout` has.
    pub(crate) fn assign_changes(
        naming: &Naming,
        merge: bool,
        lineage: LineageColumns,
        layout: Option<ChangeLayout>,
    ) -> Self {
        let name = |column: &str| naming.metadata(column);
        Self {
            load_id: name(LOAD_ID_COLUMN),
            loaded_at: name(LOADED_AT_COLUMN),
            seq: (merge || layout.is_some()).then(|| name(SEQ_COLUMN)),
            changes: layout.map(|layout| ChangeNames {
                op: name(OP_COLUMN),
                unchanged: name(UNCHANGED_COLUMN),
                deleted_at: (layout == ChangeLayout::Merge { soft: true })
                    .then(|| name(DELETED_AT_COLUMN)),
                stored: layout == ChangeLayout::Log,
            }),
            id: (lineage != LineageColumns::None).then(|| name(ID_COLUMN)),
            parent: (lineage == LineageColumns::Child).then(|| {
                [
                    name(PARENT_ID_COLUMN),
                    name(ROOT_ID_COLUMN),
                    name(IDX_COLUMN),
                ]
            }),
            history: None,
        }
    }

    /// These names, with a history table's columns under `naming`'s rules, whose versions begin
    /// at the incoming `change_time` column, or when their batch arrived.
    pub(crate) fn with_history(mut self, naming: &Naming, change_time: Option<Arc<str>>) -> Self {
        self.history = Some(HistoryNames {
            valid_from: naming.metadata(VALID_FROM_COLUMN),
            valid_to: naming.metadata(VALID_TO_COLUMN),
            is_current: naming.metadata(IS_CURRENT_COLUMN),
            row_hash: naming.metadata(ROW_HASH_COLUMN),
            change_time,
        });
        self
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

/// The columns a table has in the destination, at their logical types: the model's columns, then
/// the metadata columns.
pub(crate) fn logical_fields(model: &Model, meta: &MetaNames) -> Vec<Field> {
    let mut fields = model.columns.clone();
    let column = |name: &Arc<str>, logical: LogicalType, nullable| {
        Field::new(Arc::clone(name), logical, nullable)
    };
    fields.push(column(&meta.load_id, LOAD_ID_TYPE, false));
    fields.push(column(&meta.loaded_at, loaded_at_type(), false));
    if let Some(seq) = &meta.seq {
        fields.push(column(seq, LogicalType::Binary, false));
    }
    if let Some(changes) = &meta.changes {
        if changes.stored {
            fields.push(column(&changes.op, LogicalType::Int8, false));
            fields.push(column(&changes.unchanged, LogicalType::Binary, true));
        }
        if let Some(deleted_at) = &changes.deleted_at {
            fields.push(column(deleted_at, loaded_at_type(), true));
        }
    }
    if let Some(history) = &meta.history {
        fields.push(column(&history.valid_from, loaded_at_type(), false));
        fields.push(column(&history.valid_to, loaded_at_type(), true));
        fields.push(column(&history.is_current, LogicalType::Bool, false));
        // A delete's row carries no hash, only the key whose version it closes.
        fields.push(column(&history.row_hash, LogicalType::Binary, true));
    }
    // Lineage columns are nullable: rows loaded before their stream normalized have none.
    if let Some(id) = &meta.id {
        fields.push(column(id, ID_TYPE, true));
    }
    if let Some([parent, root, idx]) = &meta.parent {
        for (name, logical) in [(parent, ID_TYPE), (root, ID_TYPE), (idx, IDX_TYPE)] {
            fields.push(column(name, logical, true));
        }
    }
    fields
}

/// The columns a table has in the destination: [`logical_fields`], each lowered as the
/// destination stores it, the model's under their columns' `nested` settings.
pub(crate) fn physical_fields(
    logical: &[Field],
    nested: &[Nested],
    capabilities: &Capabilities,
) -> Vec<Field> {
    logical
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let nested = nested.get(index).copied().unwrap_or(Nested::Native);
            Field::new(
                field.name(),
                lower(field.logical_type(), nested, capabilities),
                field.is_nullable(),
            )
        })
        .collect()
}

/// The Arrow schema of prepared batches of a table whose columns are `fields`, of the types
/// `logical` gives, the model's `columns` first: each stored as another type names its own, and
/// the load id and load start, which hold one value per batch, are dictionaries of it (spec
/// §8.5).
pub(crate) fn prepared_schema(fields: &[Field], logical: &[Field], columns: usize) -> SchemaRef {
    let fields: Vec<ArrowField> = fields
        .iter()
        .zip(logical)
        .enumerate()
        .map(|(index, (field, logical))| {
            let arrow = named(
                field.to_arrow(),
                field.logical_type(),
                logical.logical_type(),
            );
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

/// The fields of a merge table's written batches that only direct the merge, after its stored
/// columns: a change stream's op, and its unchanged columns but in a history table, whose hashes
/// need whole rows.
pub(crate) fn directive_fields(meta: &MetaNames) -> Vec<ArrowField> {
    match &meta.changes {
        Some(changes) if !changes.stored => {
            let mut fields = vec![ArrowField::new(changes.op.as_ref(), DataType::Int8, false)];
            if meta.history.is_none() {
                fields.push(ArrowField::new(
                    changes.unchanged.as_ref(),
                    DataType::Binary,
                    true,
                ));
            }
            fields
        }
        _ => Vec::new(),
    }
}

/// `arrow`, a column stored as `stored`, naming `logical`, its type, where the destination stores
/// it as another type.
#[expect(clippy::disallowed_types, reason = "Arrow field metadata is a HashMap")]
fn named(arrow: ArrowField, stored: &LogicalType, logical: &LogicalType) -> ArrowField {
    match logical {
        logical if logical != stored => {
            let mut metadata: std::collections::HashMap<String, String> = arrow.metadata().clone();
            let json = serde_json::to_string(logical).expect("a logical type serializes");
            metadata.insert(rdlt_connector::LOGICAL_TYPE_KEY.to_owned(), json);
            arrow.with_metadata(metadata)
        }
        _ => arrow,
    }
}
