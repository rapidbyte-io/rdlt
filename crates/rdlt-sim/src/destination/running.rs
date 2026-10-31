//! Tries of commits the destination answered transiently while their transactions still ran:
//! each lands later, as its pipeline's next commit call begins or once one or two of its calls
//! have ended, so before the try's retry, after it, or after the commit that follows it, answered
//! by the key that landed last as any commit is.

use rdlt_connector::{COMMIT_STALE, CommitMeta, Epoch, PipelineId};

use super::land;
use super::store::Store;
use crate::reached::Reach;
use crate::world::World;

/// A try of a commit still running at the destination.
#[derive(Debug)]
pub(super) struct Running {
    pipeline: PipelineId,
    /// The epoch of the session that sent it.
    epoch: Epoch,
    meta: CommitMeta,
    /// How many of its pipeline's commit calls end before it lands; with none, it lands as the
    /// next begins.
    outlasts: u8,
}

impl Store {
    /// Leaves the try of `meta` by `pipeline`'s session of `epoch` running, to land once
    /// `outlasts` of the pipeline's commit calls have ended, or as the next begins where none.
    pub(super) fn leave_running(
        &mut self,
        pipeline: &PipelineId,
        epoch: Epoch,
        meta: &CommitMeta,
        outlasts: u8,
    ) {
        self.running.push(Running {
            pipeline: pipeline.clone(),
            epoch,
            meta: meta.clone(),
            outlasts,
        });
    }
}

/// Lands the tries of `pipeline` left running that wait for no commit call to end, as one of its
/// calls begins.
pub(super) fn begun(world: &World, pipeline: &PipelineId) {
    let mut store = world.store.lock();
    let due = taken(&mut store, pipeline);
    for running in &due {
        landed(world, &mut store, running);
    }
}

/// Counts one of `pipeline`'s commit calls ended, and lands the tries left running that waited
/// for it last.
pub(super) fn ended(world: &World, pipeline: &PipelineId) {
    let mut store = world.store.lock();
    let mine = store
        .running
        .iter_mut()
        .filter(|running| running.pipeline == *pipeline);
    for running in mine {
        running.outlasts = running.outlasts.saturating_sub(1);
    }
    let due = taken(&mut store, pipeline);
    for running in &due {
        landed(world, &mut store, running);
    }
}

/// The tries of `pipeline` left running that wait for no commit call to end, taken from `store`.
fn taken(store: &mut Store, pipeline: &PipelineId) -> Vec<Running> {
    let (due, left) = std::mem::take(&mut store.running)
        .into_iter()
        .partition(|running| running.pipeline == *pipeline && running.outlasts == 0);
    store.running = left;
    due
}

/// Lands `running` in `store` as a commit lands; a try a later commit overtook is refused as
/// stale.
fn landed(world: &World, store: &mut Store, running: &Running) {
    let answered = land(
        world,
        store,
        &running.pipeline,
        running.epoch,
        &running.meta,
    );
    if answered.is_err_and(|error| error.code() == Some(COMMIT_STALE)) {
        world.reach(Reach::DelayedCopyRefused);
    }
}
