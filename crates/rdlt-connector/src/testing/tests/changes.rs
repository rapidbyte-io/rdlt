//! The vault's change merge: each row applies in sequence order, only past the row its key holds
//! and the table's tombstones, unless a flaw breaks one of those behaviors.

use std::collections::BTreeMap;

use arrow_array::cast::AsArray;
use arrow_array::types::Int8Type;
use arrow_array::{Array, ArrayRef, RecordBatch, new_null_array};
use arrow_schema::{DataType, Schema};

use crate::change::ChangeOp;
use crate::destination::{ChangeColumns, Deletion, MergeKey};

/// The behaviors of a change merge a vault flag breaks.
#[derive(Clone, Copy, Default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each flaw breaks one behavior"
)]
pub(super) struct Flaws {
    /// Applies a change whatever the sequence of the row its key holds.
    pub(super) ignore_seq_guard: bool,
    /// Keeps no tombstones, so a change sent again brings a removed row back.
    pub(super) forget_tombstones: bool,
    /// Truncates every row, those sequenced after the truncate too.
    pub(super) truncate_everything: bool,
    /// Removes the rows soft deletes and truncates should mark.
    pub(super) hard_on_soft: bool,
    /// Stores null in the columns an update flags unchanged.
    pub(super) drop_unchanged: bool,
}

/// What a table remembers of the rows its change stream removed outright.
#[derive(Default)]
pub(super) struct Tombstones {
    by_key: BTreeMap<String, Vec<u8>>,
    bound: Option<Vec<u8>>,
}

impl Tombstones {
    fn admits(&self, key: Option<&str>, seq: &[u8]) -> bool {
        let bounded = self.bound.as_deref().is_some_and(|bound| seq < bound);
        let buried = key
            .and_then(|key| self.by_key.get(key))
            .is_some_and(|stone| stone.as_slice() >= seq);
        !bounded && !buried
    }

    fn raise(&mut self, seq: &[u8]) {
        if self.bound.as_deref().is_none_or(|bound| bound < seq) {
            self.by_key.retain(|_, stone| stone.as_slice() >= seq);
            self.bound = Some(seq.to_vec());
        }
    }
}

/// One change table's merge: its key and change columns, and the flaws it has.
struct Merging<'a> {
    key: &'a MergeKey,
    changes: &'a ChangeColumns,
    flaws: Flaws,
}

/// Merges `incoming`, a change stream's written batches, into `published` and `tombstones` by
/// `key`, as `changes` directs and `flaws` break.
pub(super) fn merge_changes(
    published: &mut Vec<RecordBatch>,
    tombstones: &mut Tombstones,
    incoming: &[RecordBatch],
    (key, changes): (&MergeKey, &ChangeColumns),
    flaws: Flaws,
) {
    let merging = Merging {
        key,
        changes,
        flaws,
    };
    let mut rows: Vec<RecordBatch> = incoming
        .iter()
        .flat_map(|batch| (0..batch.num_rows()).map(|row| batch.slice(row, 1)))
        .collect();
    rows.sort_by_key(|row| merging.seq(row));
    let mut stored: Vec<RecordBatch> = published
        .iter()
        .flat_map(|batch| (0..batch.num_rows()).map(|row| batch.slice(row, 1)))
        .collect();
    for row in rows {
        merging.apply(&mut stored, tombstones, &row);
    }
    *published = stored;
}

impl Merging<'_> {
    fn seq(&self, row: &RecordBatch) -> Vec<u8> {
        let seq = row
            .column_by_name(&self.key.seq)
            .expect("change rows carry a sequence");
        let seq = arrow_cast::cast(seq, &DataType::Binary).expect("sequences are bytes");
        seq.as_binary::<i32>().value(0).to_vec()
    }

    fn key_of(&self, row: &RecordBatch) -> String {
        let values: Vec<String> = self
            .key
            .columns
            .iter()
            .map(|column| {
                let values = row
                    .column_by_name(column)
                    .expect("change rows carry their key");
                arrow_cast::display::array_value_to_string(values, 0).expect("keys display")
            })
            .collect();
        values.join("\u{1}")
    }

    fn op(&self, row: &RecordBatch) -> Option<ChangeOp> {
        let ops = row.column_by_name(&self.changes.op)?;
        let ops = arrow_cast::cast(ops, &DataType::Int8).ok()?;
        ChangeOp::from_code(ops.as_primitive::<Int8Type>().value(0))
    }

    /// The column rows are marked deleted in, where deletes are soft and no flaw removes them.
    fn at(&self) -> Option<&str> {
        match &self.changes.deletion {
            Deletion::Soft { at } if !self.flaws.hard_on_soft => Some(at),
            _ => None,
        }
    }

    fn apply(&self, stored: &mut Vec<RecordBatch>, tombstones: &mut Tombstones, row: &RecordBatch) {
        let seq = self.seq(row);
        let op = self.op(row);
        let admitted =
            |key: Option<&str>| self.flaws.forget_tombstones || tombstones.admits(key, &seq);
        if op == Some(ChangeOp::Truncate) {
            if !admitted(None) {
                return;
            }
            let before =
                |kept: &RecordBatch| self.flaws.truncate_everything || self.seq(kept) < seq;
            if let Some(at) = self.at() {
                for kept in stored.iter_mut().filter(|kept| before(kept)) {
                    *kept = self.marked(kept, row, at);
                }
            } else {
                stored.retain(|kept| !before(kept));
                tombstones.raise(&seq);
            }
            return;
        }
        let key = self.key_of(row);
        if !admitted(Some(&key)) {
            return;
        }
        let current = stored.iter().position(|kept| self.key_of(kept) == key);
        let newer = current.is_some_and(|index| self.seq(&stored[index]) >= seq);
        if newer && !self.flaws.ignore_seq_guard {
            return;
        }
        match (op, self.at(), current) {
            (Some(ChangeOp::Delete), None, current) => {
                if let Some(index) = current {
                    stored.remove(index);
                }
                tombstones.by_key.insert(key, seq);
            }
            (Some(ChangeOp::Delete), Some(at), Some(index)) => {
                stored[index] = self.marked(&stored[index], row, at);
            }
            (Some(ChangeOp::Delete), Some(_), None) => {}
            (_, _, current) => {
                let merged = self.upserted(current.map(|index| &stored[index]), row);
                match current {
                    Some(index) => stored[index] = merged,
                    None => stored.push(merged),
                }
                tombstones.by_key.remove(&key);
            }
        }
    }

    /// The stored columns of `row`: all but those that direct the merge.
    fn stored_schema(&self, row: &RecordBatch) -> Schema {
        let fields: Vec<_> = row
            .schema()
            .fields()
            .iter()
            .filter(|field| {
                *field.name() != *self.changes.op
                    && self.changes.unchanged.as_deref() != Some(field.name().as_str())
            })
            .cloned()
            .collect();
        Schema::new(fields)
    }

    /// `row`, an insert or update, as it replaces `current`: its columns, but those it flags
    /// unchanged, which keep `current`'s value, or are null.
    fn upserted(&self, current: Option<&RecordBatch>, row: &RecordBatch) -> RecordBatch {
        let flags = self
            .changes
            .unchanged
            .as_deref()
            .and_then(|name| row.column_by_name(name))
            .filter(|flags| flags.is_valid(0))
            .map(|flags| flags.as_binary::<i32>().value(0).to_vec())
            .unwrap_or_default();
        let schema = self.stored_schema(row);
        let columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|field| {
                let ordinal = row
                    .schema()
                    .index_of(field.name())
                    .expect("a stored column");
                let flagged = flags
                    .get(ordinal / 8)
                    .is_some_and(|byte| byte & (1 << (ordinal % 8)) != 0);
                let kept = current
                    .filter(|_| flagged && !self.flaws.drop_unchanged)
                    .and_then(|current| current.column_by_name(field.name()));
                match (flagged, kept) {
                    (true, Some(kept)) => kept.clone(),
                    (true, None) => new_null_array(field.data_type(), 1),
                    (false, _) => row.column(ordinal).clone(),
                }
            })
            .collect();
        RecordBatch::try_new(schema.into(), columns).expect("a stored row")
    }

    /// `kept` marked deleted by `row`: it takes the row's sequence, and its deletion time in `at`
    /// unless it was deleted already.
    fn marked(&self, kept: &RecordBatch, row: &RecordBatch, at: &str) -> RecordBatch {
        let deleted = kept.column_by_name(at).is_some_and(|at| at.is_valid(0));
        let columns: Vec<ArrayRef> = kept
            .schema()
            .fields()
            .iter()
            .zip(kept.columns())
            .map(|(field, column)| {
                let takes = *field.name() == *self.key.seq || (*field.name() == *at && !deleted);
                match row.column_by_name(field.name()).filter(|_| takes) {
                    Some(value) => value.clone(),
                    None => column.clone(),
                }
            })
            .collect();
        RecordBatch::try_new(kept.schema(), columns).expect("a marked row")
    }
}
