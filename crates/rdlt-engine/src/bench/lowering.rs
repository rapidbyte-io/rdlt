//! The lowering workload: a batch of each kind a lowering plan prepares, prepared as a partition
//! prepares it, one batch at a time on the calling thread.

#[cfg(test)]
mod tests;

use std::sync::OnceLock;

use crate::fixtures::lowering::{Case, LoweringCase};

/// One kind of batch, and the plan that prepares it, made the first time it is prepared.
#[derive(Debug)]
pub struct Lowering {
    case: LoweringCase,
    rows: u32,
    made: OnceLock<Case>,
}

impl Lowering {
    /// Rows of each batch: what a partition prepares at once from a batch of a few megabytes.
    pub const ROWS: u32 = 65_536;

    /// A batch of `rows` rows of each kind, in the order the bench reports them: appends stored
    /// natively and as text, merges of unique and of duplicated keys, a history table, a change
    /// stream with unchanged flags, a split column of JSON, a value the stream discards, and
    /// instants widened into a finer column.
    pub fn all(rows: u32) -> Vec<Self> {
        LoweringCase::ALL
            .into_iter()
            .map(|case| Self {
                case,
                rows,
                made: OnceLock::new(),
            })
            .collect()
    }

    /// The kind of batch, as benchmark ids name it.
    pub fn name(&self) -> &'static str {
        self.case.name()
    }

    /// The rows of the batch.
    pub fn rows(&self) -> u64 {
        u64::from(self.rows)
    }

    /// Prepares the batch as a partition does; the rows of its table it made.
    ///
    /// # Panics
    ///
    /// Panics where the batch holds fewer than two rows, or preparing it fails, keeps other rows
    /// than the kind of batch keeps, or discards other values.
    pub fn prepare(&self) -> u64 {
        let case = self.made.get_or_init(|| Case::new(self.case, self.rows));
        let prepared = case.prepare().expect("the batch prepares");
        let kept = case.kept();
        let name = self.name();
        assert_eq!(prepared.batch.num_rows(), kept.rows, "{name}");
        assert_eq!(prepared.discarded_values, kept.discarded_values, "{name}");
        u64::try_from(kept.rows).expect("a batch's rows fit in 64 bits")
    }
}
