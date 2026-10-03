//! What lowering a batch as a plan says holds: how the table stores each column, and what each
//! row takes in the columns the batch does not hold.

use rdlt_connector::LogicalType;
use rdlt_connector::cost::Stored;

use super::{LoweringPlan, Source};

/// Bytes: what a row takes in a metadata column of text or bytes, as an id's sixteen bytes, its
/// offset and its validity.
const META_BYTES: u64 = 40;

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
    /// builds, the two a load shares between its rows left out.
    pub(crate) fn row_bytes(&self) -> u64 {
        let model = self.view.model.columns.len();
        let meta = self.view.physical.iter().skip(model + 2).map(|field| {
            let slot = rdlt_connector::cost::nulls(&field.logical_type().to_arrow(), 1);
            match field.logical_type() {
                LogicalType::Utf8 | LogicalType::Binary | LogicalType::Json => META_BYTES,
                _ => slot,
            }
        });
        meta.fold(self.null_fill(1), u64::saturating_add)
    }
}
