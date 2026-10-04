//! The vault's history merge: each row applies in sequence order as `HistoryColumns` says, unless
//! a flaw breaks one of its behaviors.

use arrow_array::cast::AsArray;
use arrow_array::types::Int8Type;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Schema};

use super::changes::Tombstones;
use crate::change::ChangeOp;
use crate::destination::{Deletion, HistoryColumns, MergeKey};

/// The behaviors of a history merge a vault flag breaks.
#[derive(Clone, Copy, Default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each flaw breaks one behavior"
)]
pub(super) struct Flaws {
    /// Replaces a key's current version instead of closing it.
    pub(super) overwrite: bool,
    /// Opens a version for a change equal to the current one.
    pub(super) duplicate: bool,
    /// Closes a version without clearing its current flag.
    pub(super) stay_current: bool,
    /// Applies a change whatever the sequence of its key's newest version.
    pub(super) ignore_seq: bool,
    /// Leaves the version a hard delete should close current.
    pub(super) keep_deleted: bool,
    /// Closes nothing on a truncate.
    pub(super) ignore_truncates: bool,
    /// Keeps the closed version's sequence in the version a soft delete opens.
    pub(super) soft_keeps_seq: bool,
    /// Spares, on a truncate, the versions its own commit opened.
    pub(super) spare_commit: bool,
    /// Stores versions without their hash.
    pub(super) drop_hash: bool,
    /// Begins a version when its change says, though its key held a later instant.
    pub(super) trust_times: bool,
    /// Begins a version no earlier than its key's versions began, whenever they ended.
    pub(super) clamp_by_starts: bool,
    /// Begins a version no earlier than its key's versions ended, whenever they began.
    pub(super) clamp_by_ends: bool,
}

/// One history table's merge: its key and history columns, and the flaws it has.
struct Versioning<'a> {
    key: &'a MergeKey,
    history: &'a HistoryColumns,
    flaws: Flaws,
    /// How many versions the table held before the commit.
    earlier: usize,
}

/// Merges `incoming`, a history stream's written batches, into `published` and `tombstones`, as
/// `key` directs and `flaws` break.
pub(super) fn merge_history(
    published: &mut Vec<RecordBatch>,
    tombstones: &mut Tombstones,
    incoming: &[RecordBatch],
    (key, history): (&MergeKey, &HistoryColumns),
    flaws: Flaws,
) {
    let versioning = Versioning {
        key,
        history,
        flaws,
        earlier: published.iter().map(RecordBatch::num_rows).sum(),
    };
    let mut rows: Vec<RecordBatch> = incoming
        .iter()
        .flat_map(|batch| (0..batch.num_rows()).map(|row| batch.slice(row, 1)))
        .collect();
    rows.sort_by_key(|row| versioning.seq(row));
    let mut stored: Vec<RecordBatch> = published
        .iter()
        .flat_map(|batch| (0..batch.num_rows()).map(|row| batch.slice(row, 1)))
        .collect();
    for row in rows {
        versioning.apply(&mut stored, tombstones, &row);
    }
    *published = stored;
}

impl Versioning<'_> {
    fn bytes(row: &RecordBatch, column: &str) -> Option<Vec<u8>> {
        let values = row.column_by_name(column)?;
        let values = arrow_cast::cast(values, &DataType::Binary).ok()?;
        let values = values.as_binary::<i32>();
        values.is_valid(0).then(|| values.value(0).to_vec())
    }

    fn seq(&self, row: &RecordBatch) -> Vec<u8> {
        Self::bytes(row, &self.key.seq).expect("history rows carry a sequence")
    }

    fn key_of(&self, row: &RecordBatch) -> String {
        let values: Vec<String> = self
            .key
            .columns
            .iter()
            .map(|column| {
                let values = row.column_by_name(column).expect("rows carry their key");
                arrow_cast::display::array_value_to_string(values, 0).expect("keys display")
            })
            .collect();
        values.join("\u{1}")
    }

    fn op(&self, row: &RecordBatch) -> ChangeOp {
        let op = self
            .key
            .changes
            .as_ref()
            .and_then(|changes| row.column_by_name(&changes.op))
            .and_then(|ops| arrow_cast::cast(ops, &DataType::Int8).ok());
        op.and_then(|ops| ChangeOp::from_code(ops.as_primitive::<Int8Type>().value(0)))
            .unwrap_or(ChangeOp::Update)
    }

    /// The column versions are marked deleted in, where deletes are soft.
    fn at(&self) -> Option<&str> {
        match self.key.changes.as_ref().map(|changes| &changes.deletion) {
            Some(Deletion::Soft { at }) => Some(at),
            _ => None,
        }
    }

    fn current(&self, row: &RecordBatch) -> bool {
        let current = row
            .column_by_name(&self.history.is_current)
            .expect("versions say whether they are current");
        current.as_boolean().value(0)
    }

    fn deleted(&self, row: &RecordBatch) -> bool {
        self.at()
            .and_then(|at| row.column_by_name(at))
            .is_some_and(|at| at.is_valid(0))
    }

    fn apply(&self, stored: &mut Vec<RecordBatch>, tombstones: &mut Tombstones, row: &RecordBatch) {
        let seq = self.seq(row);
        let changes = self.key.changes.is_some();
        if self.op(row) == ChangeOp::Truncate {
            if !tombstones.admits(None, &seq) {
                return;
            }
            let closing: Vec<usize> = (0..stored.len())
                .filter(|index| self.current(&stored[*index]) && self.seq(&stored[*index]) < seq)
                .filter(|index| !self.flaws.spare_commit || *index < self.earlier)
                .filter(|_| !self.flaws.ignore_truncates)
                .collect();
            for index in closing {
                self.remove(stored, index, row);
            }
            if self.at().is_none() {
                tombstones.raise(&seq);
            }
            return;
        }
        let key = self.key_of(row);
        let versions = || (0..stored.len()).filter(|index| self.key_of(&stored[*index]) == key);
        let newest = versions().map(|index| self.seq(&stored[index])).max();
        let past = !changes
            || self.flaws.ignore_seq
            || (tombstones.admits(Some(&key), &seq) && newest.is_none_or(|newest| newest < seq));
        if !past {
            return;
        }
        let current = versions().find(|index| self.current(&stored[*index]));
        if self.op(row) == ChangeOp::Delete {
            if let Some(index) = current {
                self.remove(stored, index, row);
            }
            if self.at().is_none() {
                tombstones.bury(&key, &seq);
            }
            return;
        }
        if let Some(index) = current {
            let live = &stored[index];
            let equal = !self.deleted(live)
                && Self::bytes(live, &self.history.row_hash)
                    == Self::bytes(row, &self.history.row_hash);
            if equal && !self.flaws.duplicate {
                return;
            }
        }
        let began = self.began(stored, &key, row);
        if let Some(index) = current {
            if self.flaws.overwrite {
                stored.remove(index);
            } else {
                stored[index] = self.closed(&stored[index], &began);
            }
        }
        stored.push(self.stored(row, None, &began));
    }

    /// The instant at `column` of `version`, where it holds one, as the integer it counts.
    fn instant(version: &RecordBatch, column: &str) -> Option<(i64, ArrayRef)> {
        let values = version.column_by_name(column)?;
        let counted = arrow_cast::cast(values, &DataType::Int64).ok()?;
        let counted = counted.as_primitive::<arrow_array::types::Int64Type>();
        counted
            .is_valid(0)
            .then(|| (counted.value(0), ArrayRef::clone(values)))
    }

    /// When `row`, a change of the key `key` that acts, begins: when it says, or the latest
    /// instant the key's versions in `stored` hold, if that is later.
    fn began(&self, stored: &[RecordBatch], key: &str, row: &RecordBatch) -> ArrayRef {
        let (from, to) = (&*self.history.valid_from, &*self.history.valid_to);
        let says = Self::instant(row, from).expect("rows say when they begin");
        if self.flaws.trust_times {
            return says.1;
        }
        let (starts, ends) = (!self.flaws.clamp_by_ends, !self.flaws.clamp_by_starts);
        let held = stored
            .iter()
            .filter(|version| self.key_of(version) == key)
            .flat_map(|version| {
                let start = Self::instant(version, from).filter(|_| starts);
                [start, Self::instant(version, to).filter(|_| ends)]
            })
            .flatten();
        held.fold(says, |latest, instant| {
            if instant.0 > latest.0 {
                instant
            } else {
                latest
            }
        })
        .1
    }

    /// Removes the version at `index` as `row`, a delete or truncate, says: closes it, and where
    /// deletes are soft opens a deleted version keeping its data, unless it is deleted already.
    fn remove(&self, stored: &mut Vec<RecordBatch>, index: usize, row: &RecordBatch) {
        let version = stored[index].clone();
        let began = || self.began(stored, &self.key_of(&version), row);
        match self.at() {
            Some(_) if self.deleted(&version) => {}
            Some(at) => {
                let began = began();
                stored[index] = self.closed(&version, &began);
                let kept = self.stored(&version, Some((row, at)), &began);
                stored.push(kept);
            }
            None if self.flaws.keep_deleted => {}
            None => stored[index] = self.closed(&version, &began()),
        }
    }

    /// `version` closed at `from`, when the change closing it begins.
    fn closed(&self, version: &RecordBatch, from: &ArrayRef) -> RecordBatch {
        let columns: Vec<ArrayRef> = version
            .schema()
            .fields()
            .iter()
            .zip(version.columns())
            .map(|(field, column)| {
                if *field.name() == *self.history.valid_to {
                    arrow_cast::cast(from, field.data_type()).expect("validity casts")
                } else if *field.name() == *self.history.is_current && !self.flaws.stay_current {
                    std::sync::Arc::new(BooleanArray::from(vec![false])) as ArrayRef
                } else {
                    column.clone()
                }
            })
            .collect();
        RecordBatch::try_new(version.schema(), columns).expect("a closed version")
    }

    /// `row`'s stored columns as a current version beginning at `began`; with `deleting`, `row`
    /// is the version kept, taking the deleting row's sequence and deletion time.
    fn stored(
        &self,
        row: &RecordBatch,
        deleting: Option<(&RecordBatch, &str)>,
        began: &ArrayRef,
    ) -> RecordBatch {
        let op = self.key.changes.as_ref().map(|changes| &*changes.op);
        let fields: Vec<_> = row
            .schema()
            .fields()
            .iter()
            .filter(|field| Some(field.name().as_str()) != op)
            .map(|field| std::sync::Arc::new(field.as_ref().clone().with_nullable(true)))
            .collect();
        let columns: Vec<ArrayRef> = fields
            .iter()
            .map(|field| {
                let name = field.name().as_str();
                let opening = [&*self.key.seq, &*self.history.valid_from];
                let opening = if self.flaws.soft_keeps_seq {
                    &opening[1..]
                } else {
                    &opening[..]
                };
                let taken = deleting
                    .filter(|(_, at)| opening.contains(&name) || name == *at)
                    .and_then(|(deleting, _)| deleting.column_by_name(name));
                let dropped = self.flaws.drop_hash && name == &*self.history.row_hash;
                if name == &*self.history.valid_to || dropped {
                    arrow_array::new_null_array(field.data_type(), 1)
                } else if name == &*self.history.valid_from {
                    arrow_cast::cast(began, field.data_type()).expect("validity casts")
                } else if name == &*self.history.is_current {
                    std::sync::Arc::new(BooleanArray::from(vec![true])) as ArrayRef
                } else {
                    let column =
                        taken.unwrap_or_else(|| row.column_by_name(name).expect("a stored column"));
                    arrow_cast::cast(column, field.data_type()).expect("a stored value")
                }
            })
            .collect();
        RecordBatch::try_new(Schema::new(fields).into(), columns).expect("a version")
    }
}
