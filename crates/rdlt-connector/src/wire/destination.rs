//! Tables, schema changes, commits and receipts on the wire.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use super::types::{instant, system_time};
use super::{Invalid, required, v1};
use crate::commit::{ChildTable, CommitMeta, DroppedTable, Receipt, SegmentRange, SegmentSet};
use crate::destination::{
    ChangeColumns, Deletion, HistoryColumns, MergeKey, RootKey, TableChange, TableRef, WriteStats,
};
use crate::id::{CommitSeq, Epoch, GenerationId, LoadId, SchemaVersion, SegmentId, TablePath};
use crate::schema::TableSchema;
use crate::state::StateChange;
use crate::types::{Field, LogicalType};

/// The load id whose 16 bytes `bytes` holds.
pub(crate) fn load_id(bytes: &[u8]) -> Result<LoadId, Invalid> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| Invalid::OutOfRange("load id"))?;
    Ok(LoadId::from_bytes(bytes))
}

impl From<&MergeKey> for v1::MergeKey {
    fn from(key: &MergeKey) -> Self {
        Self {
            columns: key.columns.iter().map(ToString::to_string).collect(),
            seq: key.seq.to_string(),
            root: key.root.as_ref().map(|root| v1::RootKey {
                table: root.table.to_string(),
                id: root.id.to_string(),
                seq: root.seq.to_string(),
            }),
            changes: key.changes.as_ref().map(|changes| v1::ChangeColumns {
                op: changes.op.to_string(),
                unchanged: changes.unchanged.as_ref().map(ToString::to_string),
                deleted_at: match &changes.deletion {
                    Deletion::Hard => None,
                    Deletion::Soft { at } => Some(at.to_string()),
                },
            }),
            history: key.history.as_ref().map(|history| v1::HistoryColumns {
                valid_from: history.valid_from.to_string(),
                valid_to: history.valid_to.to_string(),
                is_current: history.is_current.to_string(),
                row_hash: history.row_hash.to_string(),
            }),
        }
    }
}

/// `name` as a destination's identifier: not empty, within the longest identifier a
/// destination may declare, and nothing in it hides or reorders what is around it.
fn identifier(what: &'static str, name: String) -> Result<Arc<str>, Invalid> {
    crate::id::validate(what, &name, usize::from(u16::MAX), crate::id::printable)
        .map_err(|error| Invalid::rejected(what, error))?;
    Ok(Arc::from(name))
}

impl TryFrom<v1::MergeKey> for MergeKey {
    type Error = Invalid;

    fn try_from(key: v1::MergeKey) -> Result<Self, Invalid> {
        let column = |name| identifier("merge key column", name);
        Ok(Self {
            columns: key
                .columns
                .into_iter()
                .map(column)
                .collect::<Result<_, _>>()?,
            seq: column(key.seq)?,
            root: key
                .root
                .map(|root| {
                    Ok::<_, Invalid>(RootKey {
                        table: identifier("root table", root.table)?,
                        id: column(root.id)?,
                        seq: column(root.seq)?,
                    })
                })
                .transpose()?,
            changes: key
                .changes
                .map(|changes| {
                    Ok::<_, Invalid>(ChangeColumns {
                        op: column(changes.op)?,
                        unchanged: changes.unchanged.map(column).transpose()?,
                        deletion: match changes.deleted_at {
                            None => Deletion::Hard,
                            Some(at) => Deletion::Soft { at: column(at)? },
                        },
                    })
                })
                .transpose()?,
            history: key
                .history
                .map(|history| {
                    Ok::<_, Invalid>(HistoryColumns {
                        valid_from: column(history.valid_from)?,
                        valid_to: column(history.valid_to)?,
                        is_current: column(history.is_current)?,
                        row_hash: column(history.row_hash)?,
                    })
                })
                .transpose()?,
        })
    }
}

impl From<&TableRef> for v1::TableRef {
    fn from(table: &TableRef) -> Self {
        Self {
            path: Some(v1::TablePath::from(&table.path)),
            name: table.name.to_string(),
            version: table.version.0,
            generation: table.generation.map(|generation| generation.0),
            merge: table.merge.as_ref().map(v1::MergeKey::from),
        }
    }
}

impl TryFrom<v1::TableRef> for TableRef {
    type Error = Invalid;

    fn try_from(table: v1::TableRef) -> Result<Self, Invalid> {
        Ok(Self {
            path: TablePath::try_from(required("table path", table.path)?)?,
            name: identifier("table identifier", table.name)?,
            // A table's first schema is version 1.
            version: Some(SchemaVersion(table.version))
                .filter(|version| version.0 > 0)
                .ok_or(Invalid::OutOfRange("schema version"))?,
            generation: table.generation.map(GenerationId),
            merge: table.merge.map(MergeKey::try_from).transpose()?,
        })
    }
}

impl From<&TableChange> for v1::TableChange {
    fn from(change: &TableChange) -> Self {
        use v1::table_change::Change;
        let change = match change {
            TableChange::Create { table, schema } => Change::Create(v1::CreateTable {
                table: Some(v1::TableRef::from(table)),
                schema: Some(v1::TableSchema::from(schema)),
            }),
            TableChange::AddColumn { table, field } => Change::AddColumn(v1::AddColumn {
                table: Some(v1::TableRef::from(table)),
                field: Some(v1::Field::from(field)),
            }),
            TableChange::Widen {
                table,
                column,
                from,
                to,
            } => Change::Widen(v1::WidenColumn {
                table: Some(v1::TableRef::from(table)),
                column: column.to_string(),
                from: Some(v1::LogicalType::from(from)),
                to: Some(v1::LogicalType::from(to)),
            }),
        };
        Self {
            change: Some(change),
        }
    }
}

impl TryFrom<v1::TableChange> for TableChange {
    type Error = Invalid;

    fn try_from(change: v1::TableChange) -> Result<Self, Invalid> {
        use v1::table_change::Change;
        let table = |table: Option<v1::TableRef>| TableRef::try_from(required("table", table)?);
        Ok(match required("table change", change.change)? {
            Change::Create(create) => Self::Create {
                table: table(create.table)?,
                schema: TableSchema::try_from(required("table schema", create.schema)?)?,
            },
            Change::AddColumn(add) => Self::AddColumn {
                table: table(add.table)?,
                field: Field::try_from(required("column", add.field)?)?,
            },
            Change::Widen(widen) => Self::Widen {
                table: table(widen.table)?,
                column: Arc::from(widen.column),
                from: LogicalType::try_from(required("column type", widen.from)?)?,
                to: LogicalType::try_from(required("widened type", widen.to)?)?,
            },
        })
    }
}

impl From<WriteStats> for v1::WriteStats {
    fn from(stats: WriteStats) -> Self {
        Self {
            rows: stats.rows,
            bytes: stats.bytes,
        }
    }
}

impl From<v1::WriteStats> for WriteStats {
    fn from(stats: v1::WriteStats) -> Self {
        Self {
            rows: stats.rows,
            bytes: stats.bytes,
        }
    }
}

impl From<&CommitMeta> for v1::CommitMeta {
    fn from(meta: &CommitMeta) -> Self {
        Self {
            load_id: meta.load_id.as_bytes().to_vec().into(),
            commit_seq: meta.commit_seq.get(),
            epoch: meta.epoch.0,
            segments: meta
                .segments
                .ranges()
                .iter()
                .map(|range| v1::SegmentRange {
                    first: range.first.0,
                    last: range.last.0,
                })
                .collect(),
            state_delta: meta.state_delta.iter().map(v1::StateChange::from).collect(),
            finish_generations: meta
                .finish_generations
                .iter()
                .map(|(table, generation)| v1::FinishGeneration {
                    table: Some(v1::TablePath::from(table)),
                    generation: generation.0,
                })
                .collect(),
            child_tables: meta
                .child_tables
                .iter()
                .map(|child| v1::ChildTable {
                    table: child.table.to_string(),
                    merge: Some(v1::MergeKey::from(&child.merge)),
                })
                .collect(),
            drop_tables: meta
                .drop_tables
                .iter()
                .map(|dropped| v1::DroppedTable {
                    path: Some(v1::TablePath::from(&dropped.path)),
                    name: dropped.name.to_string(),
                })
                .collect(),
        }
    }
}

impl TryFrom<v1::CommitMeta> for CommitMeta {
    type Error = Invalid;

    fn try_from(meta: v1::CommitMeta) -> Result<Self, Invalid> {
        let ranges = meta
            .segments
            .into_iter()
            .map(|range| SegmentRange {
                first: SegmentId(range.first),
                last: SegmentId(range.last),
            })
            .collect::<Vec<_>>();
        let finish_generations = meta
            .finish_generations
            .into_iter()
            .map(|finish| {
                let table = TablePath::try_from(required("generation's table", finish.table)?)?;
                Ok((table, GenerationId(finish.generation)))
            })
            .collect::<Result<_, Invalid>>()?;
        let child_tables = meta
            .child_tables
            .into_iter()
            .map(|child| {
                let merge = MergeKey::try_from(required("child table's key", child.merge)?)?;
                Ok(ChildTable {
                    table: identifier("child table identifier", child.table)?,
                    merge,
                })
            })
            .collect::<Result<_, Invalid>>()?;
        let drop_tables = meta
            .drop_tables
            .into_iter()
            .map(|dropped| {
                let path = TablePath::try_from(required("dropped table's path", dropped.path)?)?;
                Ok(DroppedTable {
                    path,
                    name: identifier("dropped table identifier", dropped.name)?,
                })
            })
            .collect::<Result<_, Invalid>>()?;
        Ok(Self {
            load_id: load_id(&meta.load_id)?,
            commit_seq: CommitSeq::new(meta.commit_seq).ok_or(Invalid::OutOfRange("commit seq"))?,
            epoch: Epoch(meta.epoch),
            segments: SegmentSet::try_from(ranges)
                .map_err(|error| Invalid::rejected("segments", error))?,
            state_delta: meta
                .state_delta
                .into_iter()
                .map(StateChange::try_from)
                .collect::<Result<_, _>>()?,
            finish_generations,
            child_tables,
            drop_tables,
        })
    }
}

impl From<&Receipt> for v1::Receipt {
    fn from(receipt: &Receipt) -> Self {
        Self {
            load_id: receipt.load_id.as_bytes().to_vec().into(),
            commit_seq: receipt.commit_seq.get(),
            committed_at: Some(instant(receipt.committed_at)),
            rows: receipt.rows,
            bytes: receipt.bytes,
        }
    }
}

impl TryFrom<v1::Receipt> for Receipt {
    type Error = Invalid;

    fn try_from(receipt: v1::Receipt) -> Result<Self, Invalid> {
        Ok(Self {
            load_id: load_id(&receipt.load_id)?,
            commit_seq: CommitSeq::new(receipt.commit_seq)
                .ok_or(Invalid::OutOfRange("commit seq"))?,
            committed_at: system_time(required("commit time", receipt.committed_at)?)?,
            rows: receipt.rows,
            bytes: receipt.bytes,
        })
    }
}
