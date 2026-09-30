//! The rows a history table of `D-HIST` is written, and what it stores.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Int8Array, Int64Array, RecordBatch, StringArray,
};

use crate::change::{ChangeOp, OP_COLUMN, SEQ_COLUMN};
use crate::meta::{
    DELETED_AT_COLUMN, IS_CURRENT_COLUMN, ROW_HASH_COLUMN, VALID_FROM_COLUMN, VALID_TO_COLUMN,
};
use crate::schema::TableSchema;
use crate::types::{Field, LogicalType};

/// One row a history stream writes: its op, key, name, sequence and when it takes effect.
#[derive(Clone, Copy)]
pub(super) struct Row {
    op: ChangeOp,
    id: Option<i64>,
    name: Option<&'static str>,
    seq: u8,
    at: i64,
}

pub(super) fn upsert(id: i64, name: &'static str, seq: u8, at: i64) -> Row {
    Row {
        op: ChangeOp::Update,
        id: Some(id),
        name: Some(name),
        seq,
        at,
    }
}

pub(super) fn delete(id: i64, seq: u8, at: i64) -> Row {
    Row {
        op: ChangeOp::Delete,
        name: None,
        ..upsert(id, "", seq, at)
    }
}

pub(super) fn truncate(seq: u8, at: i64) -> Row {
    Row {
        op: ChangeOp::Truncate,
        id: None,
        ..delete(0, seq, at)
    }
}

/// A version a history table publishes, as the clause compares them.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Version {
    pub(super) id: i64,
    /// When it began.
    pub(super) from: i64,
    pub(super) name: Option<String>,
    /// When a soft delete opened it, if one did.
    pub(super) deleted: Option<i64>,
    /// When a later change closed it.
    pub(super) to: Option<i64>,
    pub(super) current: bool,
    /// The last byte of the sequence of the row that opened it.
    pub(super) seq: u8,
    /// Whether its hash is the one its data's writer gave.
    pub(super) hashed: bool,
}

/// The current version of `id` holding `name` since `from`, opened at `seq`.
pub(super) fn current(id: i64, name: &str, from: i64, seq: u8) -> Version {
    Version {
        id,
        from,
        name: Some(name.to_owned()),
        deleted: None,
        to: None,
        current: true,
        seq,
        hashed: true,
    }
}

/// A version of `id` holding `name` from `from` until `to`, opened at `seq`.
pub(super) fn closed(id: i64, name: &str, from: i64, to: i64, seq: u8) -> Version {
    Version {
        to: Some(to),
        current: false,
        ..current(id, name, from, seq)
    }
}

/// A version of `id` keeping `name`, which a soft delete at `seq` opened at `from`, current or
/// closed at `to`.
pub(super) fn deleted(id: i64, name: &str, from: i64, to: Option<i64>, seq: u8) -> Version {
    Version {
        deleted: Some(from),
        to,
        current: to.is_none(),
        ..current(id, name, from, seq)
    }
}

/// The hash a writer gives `name`: its bytes, zero-padded to 16.
pub(super) fn hash(name: &str) -> [u8; 16] {
    let mut hash = [0_u8; 16];
    for (byte, source) in hash.iter_mut().zip(name.bytes()) {
        *byte = source;
    }
    hash
}

/// How a table of this clause is written: as a change stream's, with `soft` or hard deletes, or
/// as a table whose rows are all upserts.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Plain,
    Changes { soft: bool },
}

impl Kind {
    fn soft(self) -> bool {
        self == Self::Changes { soft: true }
    }
}

/// `rows` as a history stream writes them: the key, name and sequence, the deletion time where
/// deletes are soft, the history columns, then a change stream's op.
pub(super) fn written(rows: &[Row], kind: Kind) -> RecordBatch {
    let seqs = rows.iter().map(|row| {
        let mut seq = [0_u8; 16];
        seq[15] = row.seq;
        seq
    });
    let mut columns: Vec<(&str, ArrayRef)> = vec![
        (
            "id",
            Arc::new(rows.iter().map(|r| r.id).collect::<Int64Array>()),
        ),
        (
            "name",
            Arc::new(rows.iter().map(|r| r.name).collect::<StringArray>()),
        ),
        (SEQ_COLUMN, Arc::new(BinaryArray::from_iter_values(seqs))),
    ];
    if kind.soft() {
        let at: Int64Array = rows
            .iter()
            .map(|row| (row.op != ChangeOp::Update).then_some(row.at))
            .collect();
        columns.push((DELETED_AT_COLUMN, Arc::new(at)));
    }
    let hashes: BinaryArray = rows
        .iter()
        .map(|row| row.name.map(hash))
        .map(|hash| hash.map(|hash| hash.to_vec()))
        .collect();
    columns.extend([
        (
            VALID_FROM_COLUMN,
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.at))) as ArrayRef,
        ),
        (VALID_TO_COLUMN, Arc::new(Int64Array::new_null(rows.len()))),
        (
            IS_CURRENT_COLUMN,
            Arc::new(BooleanArray::from(vec![true; rows.len()])),
        ),
        (ROW_HASH_COLUMN, Arc::new(hashes)),
    ]);
    if kind != Kind::Plain {
        let ops = Int8Array::from_iter_values(rows.iter().map(|row| row.op.code()));
        columns.push((OP_COLUMN, Arc::new(ops)));
    }
    RecordBatch::try_from_iter(columns).expect("the certification batch is valid")
}

/// The stored columns of a history table of `kind`.
pub(super) fn stored(kind: Kind) -> TableSchema {
    let mut fields = vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("name", LogicalType::Utf8, true),
        Field::new(SEQ_COLUMN, LogicalType::Binary, false),
    ];
    if kind.soft() {
        fields.push(Field::new(DELETED_AT_COLUMN, LogicalType::Int64, true));
    }
    fields.extend([
        Field::new(VALID_FROM_COLUMN, LogicalType::Int64, false),
        Field::new(VALID_TO_COLUMN, LogicalType::Int64, true),
        Field::new(IS_CURRENT_COLUMN, LogicalType::Bool, false),
        Field::new(ROW_HASH_COLUMN, LogicalType::Binary, true),
    ]);
    TableSchema::new(fields).expect("the certification schema is valid")
}
