//! Whether lowering a batch hands its columns over as they arrive, reading and writing none of
//! their values: what decides that it costs too little to leave the task that asks for it.

use arrow_array::RecordBatch;
use rdlt_connector::LogicalType;

use super::super::resolve::Route;
use super::{LoweringPlan, Source, Stamp};
use crate::policy::SchemaPolicy;

impl LoweringPlan {
    /// Whether the plan hands every incoming column to its table's column as it arrives: each
    /// goes to a column of its own type, stored in that type, whose values need no check, every
    /// row is kept, and the only columns lowering adds are the load's constant ones.
    pub(crate) fn hands_over(&self) -> bool {
        let view = &self.view;
        let meta = &view.meta;
        // A merge table, and a change stream's, has a sequence column.
        let plain = meta.seq.is_none()
            && meta.changes.is_none()
            && meta.id.is_none()
            && meta.history.is_none();
        let routed = self
            .routes
            .iter()
            .all(|route| matches!(route, Route::Column(_) | Route::Skip));
        let checked = self.policies.iter().any(|policy| {
            matches!(
                policy,
                SchemaPolicy::DiscardRow | SchemaPolicy::DiscardValue
            )
        });
        let columns = view.model.columns.iter().zip(&view.lowered);
        let sources = columns
            .zip(&self.sources)
            .all(|((column, lowered), source)| match source {
                Source::Incoming(_, from) => {
                    from == column.logical_type() && from == lowered && unchecked(from)
                }
                Source::Read(_) | Source::Rest(_) | Source::Nulls => false,
            });
        plain && routed && !checked && sources
    }

    /// Whether lowering `batch`, of the load `stamp` names, copies no value and reads none:
    /// where the plan hands every column over, each of the batch's columns already is of the
    /// type its table stores, and the load's constant columns are built for as many rows.
    pub(crate) fn copies_nothing(&self, batch: &RecordBatch, stamp: &Stamp) -> bool {
        let fields = self.view.schema.fields();
        let stored = self
            .sources
            .iter()
            .zip(fields)
            .all(|(source, field)| match source {
                Source::Incoming(index, _) => batch
                    .columns()
                    .get(*index)
                    .is_some_and(|column| column.data_type() == field.data_type()),
                Source::Read(_) | Source::Rest(_) | Source::Nulls => false,
            });
        let built = self.constants.lock();
        let built = built
            .as_ref()
            .is_some_and(|built| built.fits(stamp, batch.num_rows()));
        self.handed && stored && built
    }
}

/// Whether a column of `logical`, as it arrives, is lowered without a look at its values: a
/// decimal's precision is checked value by value, a typed null is built in its column's type, and
/// a nested value is walked.
fn unchecked(logical: &LogicalType) -> bool {
    !matches!(
        logical,
        LogicalType::Null | LogicalType::Decimal(_) | LogicalType::Struct(_) | LogicalType::List(_)
    )
}
