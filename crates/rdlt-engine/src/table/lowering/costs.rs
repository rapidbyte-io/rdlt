//! What lowering a batch as a plan says holds: how the table stores each column, and what each
//! row takes in the columns the batch does not hold.

use rdlt_connector::LogicalType;
use rdlt_connector::cost::Stored;
use rdlt_wire::limits::count;

use super::{LoweringPlan, Source};
use crate::cost::{arrays, flags_bytes};

/// Bytes: what a row takes in a metadata column of bytes the destination stores: at most 64
/// bytes, which a positional id's 37, a hash's 32 and a sequence's 16 are within, their offset
/// and their validity.
const META_BYTES: u64 = 72;

/// Bytes: what a row takes in a metadata column of bytes the destination stores as text: those
/// bytes as lowering builds them and their hex beside them, each with its offset, and validity.
const META_TEXT: u64 = 120;

impl LoweringPlan {
    /// The bytes the columns a batch of `rows` rows holds nothing in take once lowered: each a
    /// column of nulls as the destination stores it.
    pub(crate) fn null_fill(&self, rows: usize) -> u64 {
        self.sources
            .iter()
            .zip(&self.view.lowered)
            .filter(|(source, _)| source.is_null())
            .map(|(_, lowered)| rdlt_connector::cost::nulls(&lowered.to_arrow(), rows))
            .fold(0, u64::saturating_add)
    }

    /// How the plan's table stores each incoming column, in the batch's order: the type of its
    /// column, whether the destination stores it as text, and whether the column reads it in
    /// part; nothing for a column the plan sends nowhere, or sends typed null, which
    /// [`LoweringPlan::null_fill`] counts.
    pub(crate) fn stored(&self) -> Vec<Option<Stored>> {
        let mut stored = vec![None; self.routes.len()];
        let columns = self.view.model.columns.iter().zip(&self.view.lowered);
        for ((column, lowered), source) in columns.zip(&self.sources) {
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
        stored
    }

    /// The bytes each row takes beside the columns its batch holds: a null in every column of
    /// the table the batch holds nothing in, and a value in every metadata column lowering
    /// builds, the two a load shares between its rows left out, a change stream's unchanged
    /// flags among them, whether its table stores them or merges by them.
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
