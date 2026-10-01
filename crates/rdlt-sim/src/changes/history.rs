//! History change streams: which merge streams keep history, their whole-row events,
//! and the versions their changes leave.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch, TimestampMicrosecondArray};
use rdlt_connector::{ChangeOp, Field, LogicalType, SEQ_COLUMN, StreamSpec, TableSchema, TimeUnit};
use rdlt_engine::{DeleteMode, OnTruncate};

use super::workload::{ChangeStream, Event};
use crate::rng::SplitMix64;

/// One version of a key in a history table: its key, when it began (its change's position, as
/// its change time holds it in microseconds), value, counter, when a later change closed it,
/// whether it is current, and whether a soft delete opened it.
pub(crate) type Version = (i64, u64, Option<String>, i64, Option<u64>, bool, bool);

/// `events` of a history stream over `keys` keys made whole, as a history table needs whole
/// rows: an update leaving the value unchanged sets it instead, and now and then, as `rng` draws,
/// an update sends its key's live data again, which changes no version.
pub(crate) fn whole(events: &mut [Event], keys: u64, rng: &mut SplitMix64) {
    let mut live: BTreeMap<i64, (Option<String>, Option<i64>)> = (0..keys)
        .map(|key| {
            let key = i64::try_from(key).unwrap_or(i64::MAX);
            (key, (Some(format!("s{key}")), Some(0)))
        })
        .collect();
    for (index, event) in events.iter_mut().enumerate() {
        let position = i64::try_from(index + 1).unwrap_or(i64::MAX);
        if event.partial {
            event.partial = false;
            event.value = Some(format!("v{position}"));
        }
        let echo = rng.chance(200);
        match (event.op, event.key) {
            (ChangeOp::Truncate, _) => live.clear(),
            (ChangeOp::Delete, Some(key)) => {
                live.remove(&key);
            }
            (_, Some(key)) => {
                if let Some((value, n)) = live.get(&key).filter(|_| echo) {
                    event.value.clone_from(value);
                    event.n = *n;
                }
                live.insert(key, (event.value.clone(), event.n));
            }
            _ => {}
        }
    }
}

impl ChangeStream {
    /// Every version a history table holds once the changes of `round` apply, sorted: the
    /// snapshot's rows at the captured position, then each change read after it.
    pub(crate) fn history(&self, round: usize) -> Vec<Version> {
        let captured = u64::try_from(self.captured).unwrap_or(u64::MAX);
        let mut chain = Chain::default();
        for (key, row) in self.snapshot() {
            chain.open(key, captured, row.value, row.n, false);
        }
        let read = &self.events[self.captured..self.rounds[round]];
        for (index, event) in read.iter().enumerate() {
            let position = captured + 1 + u64::try_from(index).unwrap_or(u64::MAX);
            let soft = self.deletes == DeleteMode::Soft;
            match (event.op, event.key) {
                (ChangeOp::Truncate, _) if self.truncates == OnTruncate::Ignore => {}
                (ChangeOp::Truncate, _) => {
                    let keys: Vec<i64> = chain.current.keys().copied().collect();
                    for key in keys {
                        chain.remove(key, position, soft);
                    }
                }
                (ChangeOp::Delete, _) if self.deletes == DeleteMode::Ignore => {}
                (ChangeOp::Delete, Some(key)) => chain.remove(key, position, soft),
                (_, Some(key)) => {
                    let (value, n) = (event.value.clone(), event.n.unwrap_or(0));
                    chain.upsert(key, position, value, n);
                }
                _ => {}
            }
        }
        let mut versions = chain.versions;
        versions.sort();
        versions
    }
}

/// Versions as they chain, with each key's live one.
#[derive(Default)]
struct Chain {
    versions: Vec<Version>,
    current: BTreeMap<i64, usize>,
}

impl Chain {
    fn open(&mut self, key: i64, from: u64, value: Option<String>, n: i64, deleted: bool) {
        self.current.insert(key, self.versions.len());
        self.versions
            .push((key, from, value, n, None, true, deleted));
    }

    fn close(&mut self, key: i64, at: u64) -> Option<Version> {
        let index = self.current.remove(&key)?;
        let version = &mut self.versions[index];
        version.4 = Some(at);
        version.5 = false;
        Some(version.clone())
    }

    fn upsert(&mut self, key: i64, at: u64, value: Option<String>, n: i64) {
        let live = self.current.get(&key).map(|index| &self.versions[*index]);
        if live.is_some_and(|live| !live.6 && live.2 == value && live.3 == n) {
            return;
        }
        self.close(key, at);
        self.open(key, at, value, n, false);
    }

    /// Closes `key`'s live version at `at`, and where deletes are `soft` opens a deleted one
    /// keeping its data, unless it is deleted already.
    fn remove(&mut self, key: i64, at: u64, soft: bool) {
        let deleted = self
            .current
            .get(&key)
            .is_none_or(|index| self.versions[*index].6);
        if deleted {
            return;
        }
        if let Some((_, _, value, n, ..)) = self.close(key, at)
            && soft
        {
            self.open(key, at, value, n, true);
        }
    }
}

/// The column a history stream's rows carry their change time in.
const AT: &str = "at";

/// `spec` of a history stream: its rows also carry their change time, which begins their
/// versions.
pub(super) fn timed_spec(spec: StreamSpec, schema: &TableSchema) -> StreamSpec {
    let mut fields: Vec<Field> = schema.fields().iter().cloned().collect();
    let micros = LogicalType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    fields.push(Field::new(AT, micros, true));
    let schema = TableSchema::new(fields).expect("the change schema has distinct names");
    spec.with_schema(schema).with_change_time(AT)
}

/// `batch`, a change batch, with each row's change time after its data: its position, from its
/// sequence's last eight bytes, in microseconds.
pub(super) fn timed(batch: &RecordBatch) -> RecordBatch {
    let seqs = batch
        .column_by_name(SEQ_COLUMN)
        .expect("change rows carry a sequence")
        .as_fixed_size_binary();
    let at = seqs.iter().map(|seq| {
        let position = seq.map_or(0, |seq| {
            u64::from_be_bytes(seq[8..].try_into().expect("sequences are 16 bytes"))
        });
        i64::try_from(position).unwrap_or(i64::MAX)
    });
    let at = TimestampMicrosecondArray::from_iter_values(at).with_timezone("UTC");
    let data = batch
        .schema()
        .index_of("n")
        .expect("change rows carry a counter")
        + 1;
    let mut fields: Vec<_> = batch.schema().fields().iter().cloned().collect();
    let mut columns = batch.columns().to_vec();
    fields.insert(
        data,
        Arc::new(arrow_schema::Field::new(AT, at.data_type().clone(), true)),
    );
    columns.insert(data, Arc::new(at));
    RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns)
        .expect("a valid change batch")
}
