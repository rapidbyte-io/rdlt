//! The lowering workload: a batch of each kind a lowering plan prepares, prepared as a partition
//! prepares it, one batch at a time on the calling thread.

#[cfg(test)]
mod tests;

use crate::fixtures::lowering::{Case, LoweringCase};

/// One kind of batch and the plan that prepares it.
#[derive(Debug)]
pub struct Lowering {
    case: Case,
    name: &'static str,
}

impl Lowering {
    /// Rows of each batch: what a partition prepares at once from a batch of a few megabytes.
    pub const ROWS: u32 = 65_536;

    /// A batch of `rows` rows of each kind, in the order the bench reports them: appends stored
    /// natively and as text, merges of unique and of duplicated keys, a history table, a change
    /// stream with unchanged flags, a split column of JSON, a value the stream discards, and
    /// instants widened into a finer column.
    ///
    /// # Panics
    ///
    /// Panics where `rows` is below two.
    pub fn all(rows: u32) -> Vec<Self> {
        LoweringCase::ALL
            .into_iter()
            .map(|case| Self {
                case: Case::new(case, rows),
                name: case.name(),
            })
            .collect()
    }

    /// The kind of batch, as benchmark ids name it.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// The rows of the batch.
    ///
    /// # Panics
    ///
    /// Panics where the count does not fit in 64 bits, which happens on no platform Rust supports.
    pub fn rows(&self) -> u64 {
        u64::try_from(self.case.rows()).expect("a batch's rows fit in 64 bits")
    }

    /// Prepares the batch as a partition does; the rows of its table it made.
    ///
    /// # Panics
    ///
    /// Panics where preparing fails, keeps other rows than the kind of batch keeps, or discards
    /// other values.
    pub fn prepare(&self) -> u64 {
        let prepared = self.case.prepare().expect("the batch prepares");
        let kept = self.case.kept();
        assert_eq!(prepared.batch.num_rows(), kept.rows, "{}", self.name);
        assert_eq!(
            prepared.discarded_values, kept.discarded_values,
            "{}",
            self.name
        );
        u64::try_from(kept.rows).expect("a batch's rows fit in 64 bits")
    }
}
