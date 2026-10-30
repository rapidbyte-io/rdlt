//! What lowering a batch as a plan says holds: how the table stores each column, and what each
//! row takes in the columns the batch does not hold.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::DataType;
use parking_lot::Mutex;
use rdlt_connector::LogicalType;
use rdlt_connector::cost::{Stored, arrays};
use rdlt_wire::limits::count;

use super::{LoweringPlan, Source, aligned, changes};
use crate::cost::flags_bytes;
use crate::table::TableView;

/// How a plan's table stores each column, worked out once: by the incoming columns, and by the
/// columns of the change batches it cut last.
#[derive(Debug)]
pub(super) struct Storing {
    /// By the incoming columns, which every unit the plan cuts shares.
    columns: Arc<[Option<Stored>]>,
    /// By the columns of the change batches cut last, for the batches whose change columns sit
    /// where theirs did.
    aligned: Mutex<Option<Aligned>>,
}

impl Storing {
    /// How `view`'s table stores each of `incoming` incoming columns, as [`LoweringPlan::stored`]
    /// says, where `sources` says where each column of the table takes its values from.
    pub(super) fn new(view: &TableView, sources: &[Source], incoming: usize) -> Self {
        let mut stored = vec![None; incoming];
        let columns = view.model.columns.iter().zip(&view.lowered);
        for ((column, lowered), source) in columns.zip(sources) {
            let (index, read) = match source {
                Source::Incoming(index, from) if *from != LogicalType::Null => (*index, false),
                // A column of JSON its own column holds in part: what reading it takes covers
                // the values copied to their variant too.
                Source::Read(index) => (*index, true),
                _ => continue,
            };
            stored[index] = Some(Stored {
                column: column.logical_type().clone(),
                text: lowered != column.logical_type(),
                read,
            });
        }
        Self {
            columns: stored.into(),
            aligned: Mutex::new(None),
        }
    }
}

/// How a table stores each column of a change batch, by the batch's own columns.
#[derive(Debug)]
struct Aligned {
    /// Where the batch's change columns sit among its fields.
    places: Vec<usize>,
    stored: Arc<[Option<Stored>]>,
}

/// Bytes: what a row takes in a metadata column of bytes the destination stores: at most 64
/// bytes, which a positional id's 37, a hash's 32 and a sequence's 16 are within, their offset
/// and their validity.
const META_BYTES: u64 = 72;

/// Bytes: what a row takes in a metadata column of bytes the destination stores as text: those
/// bytes as lowering builds them and their hex beside them, each with its offset, and validity.
const META_TEXT: u64 = 120;

impl LoweringPlan {
    /// The bytes the columns a batch of `rows` rows holds nothing in take once lowered: a column
    /// of nulls of each type the destination stores them as, which those of the type share.
    pub(crate) fn null_fill(&self, rows: usize) -> u64 {
        let types: BTreeSet<DataType> = self
            .sources
            .iter()
            .zip(&self.view.lowered)
            .filter(|(source, _)| source.is_null())
            .map(|(_, lowered)| lowered.to_arrow())
            .collect();
        types
            .iter()
            .map(|data_type| rdlt_connector::cost::nulls(data_type, rows))
            .fold(0, u64::saturating_add)
    }

    /// How the plan's table stores each incoming column, in the batch's order: the type of its
    /// column, whether the destination stores it as text, and whether the column reads it in
    /// part; nothing for a column the plan sends nowhere, or sends typed null, which
    /// [`LoweringPlan::null_fill`] counts.
    pub(crate) fn stored(&self) -> Arc<[Option<Stored>]> {
        Arc::clone(&self.storing.columns)
    }

    /// How the plan's table stores each column of `batch`, a change batch, by the batch's own
    /// columns: nothing for its change columns.
    pub(crate) fn aligned(&self, batch: &RecordBatch) -> Arc<[Option<Stored>]> {
        let places = changes::places(batch);
        let mut last = self.storing.aligned.lock();
        if let Some(kept) = last.as_ref()
            && kept.places == places
        {
            return Arc::clone(&kept.stored);
        }
        let stored: Arc<[Option<Stored>]> = aligned(batch, &self.storing.columns).into();
        let kept = Aligned {
            places,
            stored: Arc::clone(&stored),
        };
        *last = Some(kept);
        stored
    }

    /// The bytes each row takes beside the columns its batch holds: a null of each type among
    /// the table's columns the batch holds nothing in, and a value in every metadata column
    /// lowering builds, the two a load shares between its rows left out, a change stream's
    /// unchanged flags among them, whether its table stores them or merges by them.
    pub(crate) fn row_bytes(&self) -> u64 {
        let model = self.view.model.columns.len();
        let changes = self.view.meta.changes.as_ref();
        let meta = self.view.physical.iter().skip(model + 2).map(|field| {
            let lowered = field.logical_type();
            if changes.is_some_and(|changes| field.name() == &*changes.unchanged) {
                return self.flags(*lowered != LogicalType::Binary);
            }
            match lowered {
                LogicalType::Binary => META_BYTES,
                LogicalType::Utf8 | LogicalType::Json => META_TEXT,
                _ => rdlt_connector::cost::nulls(&lowered.to_arrow(), 1),
            }
        });
        // A merge table's flags direct its merge, as bytes, beside its stored columns.
        let directs = changes.filter(|changes| !changes.stored && self.view.meta.history.is_none());
        let directed = directs.map_or(0, |_| self.flags(false));
        meta.fold(
            self.null_fill(1).saturating_add(directed),
            u64::saturating_add,
        )
    }

    /// The bytes lowering a piece takes whatever its rows: the arrays of the batch it becomes.
    pub(crate) fn piece_bytes(&self) -> u64 {
        arrays(self.view.schema.fields())
    }

    /// The bytes a row's unchanged flags take once remapped over the table's columns as far as
    /// the furthest the plan writes, and their text beside them where `text`.
    fn flags(&self, text: bool) -> u64 {
        let columns = self
            .written
            .iter()
            .flatten()
            .max()
            .map_or(0, |furthest| furthest + 1);
        flags_bytes(count(columns), text)
    }
}
