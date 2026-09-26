//! Tables, schema changes, commits and receipts on the wire.

use std::sync::Arc;

use super::types::{instant, system_time};
use super::{Invalid, required, v1};
use crate::commit::{ChildTable, CommitMeta, Receipt, SegmentRange, SegmentSet};
use crate::destination::{MergeKey, RootKey, TableChange, TableRef, WriteStats};
use crate::id::{CommitSeq, Epoch, GenerationId, LoadId, SchemaVersion, SegmentId, TablePath};
use crate::schema::TableSchema;
use crate::state::StateChange;
use crate::types::{Field, LogicalType};

/// The load id whose 16 bytes `bytes` holds.
pub(super) fn load_id(bytes: &[u8]) -> Result<LoadId, Invalid> {
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
        }
    }
}

impl From<v1::MergeKey> for MergeKey {
    fn from(key: v1::MergeKey) -> Self {
        Self {
            columns: key.columns.into_iter().map(Arc::from).collect(),
            seq: Arc::from(key.seq),
            root: key.root.map(|root| RootKey {
                table: Arc::from(root.table),
                id: Arc::from(root.id),
                seq: Arc::from(root.seq),
            }),
        }
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
            name: Arc::from(table.name),
            version: SchemaVersion(table.version),
            generation: table.generation.map(GenerationId),
            merge: table.merge.map(MergeKey::from),
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
                let merge = MergeKey::from(required("child table's key", child.merge)?);
                Ok(ChildTable {
                    table: Arc::from(child.table),
                    merge,
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
