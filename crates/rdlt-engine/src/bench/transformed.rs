//! A stream's transform stage run on one batch, as the instruction counts and the fuzz targets
//! drive it.

use arrow_array::RecordBatch;
use rdlt_connector::{ColumnPath, StreamName};

use crate::error::Error;
use crate::plan::StreamPlan;
use crate::transform::stage::{Chain, Ran, Stage, refused};

/// A stream's stage and the chain it makes of one batch's schema, run on that batch as often as
/// asked.
#[derive(Debug)]
pub struct Transformed {
    stream: StreamName,
    stage: Stage,
    chain: Chain,
    batch: RecordBatch,
}

/// Why a batch cannot be run through a stream's stage.
#[derive(Debug, thiserror::Error)]
pub enum Untransformed {
    /// The plan includes every column and transforms nothing, so its stream has no stage.
    #[error("the plan includes every column and transforms nothing")]
    NoStage,
    /// The stage refused the batch's schema or the batch, as its stream would fail.
    #[error(transparent)]
    Refused(#[from] Error),
}

impl Transformed {
    /// The stage of `plan`'s stream, keyed by `key`, for batches like `batch`.
    ///
    /// # Errors
    ///
    /// [`Untransformed::NoStage`] where the plan includes every column and transforms nothing,
    /// and [`Untransformed::Refused`] where its stage refuses `batch`'s schema.
    pub fn try_new(
        plan: &StreamPlan,
        key: &[&str],
        batch: RecordBatch,
    ) -> Result<Self, Untransformed> {
        let key: Vec<ColumnPath> = key.iter().copied().map(ColumnPath::from).collect();
        let stage = Stage::new(plan, &key, None).ok_or(Untransformed::NoStage)?;
        let stream = plan.name().clone();
        let chain = stage
            .chain(batch.schema_ref())
            .map_err(|refusal| refused(&stream, refusal))?;
        Ok(Self {
            stream,
            stage,
            chain,
            batch,
        })
    }

    /// Runs the stage on the batch, holding nothing back; returns the rows it leaves.
    ///
    /// # Errors
    ///
    /// The error the stage's refusal of the batch fails its stream with.
    pub fn run(&self) -> Result<usize, Untransformed> {
        let ran = self.ran(u64::MAX)?;
        Ok(ran.batches.iter().map(RecordBatch::num_rows).sum())
    }

    /// Runs the stage on the batch, holding nothing back; returns the batches it leaves.
    ///
    /// # Errors
    ///
    /// The error the stage's refusal of the batch fails its stream with.
    pub fn batches(&self) -> Result<Vec<RecordBatch>, Untransformed> {
        self.batches_within(u64::MAX)
    }

    /// Runs the stage on the batch, its transforms holding at most `free` bytes beyond it, a
    /// part that does not fit cut in halves; returns the batches it leaves.
    ///
    /// # Errors
    ///
    /// The error the stage's refusal of the batch fails its stream with.
    pub fn batches_within(&self, free: u64) -> Result<Vec<RecordBatch>, Untransformed> {
        Ok(self.ran(free)?.batches)
    }

    fn ran(&self, free: u64) -> Result<Ran, Untransformed> {
        let ran = self.stage.run(&self.chain, self.batch.clone(), free);
        ran.map_err(|refusal| Untransformed::Refused(refused(&self.stream, refusal)))
    }
}
