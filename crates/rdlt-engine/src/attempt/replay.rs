//! Replaying the write-ahead logs earlier loads left: each commit a log holds without its receipt
//! is committed again, in a session of its own, before the attempt opens.
//!
//! A log is replayed once a fence at its next chunk keeps its load, if it still runs, from
//! publishing more. Each commit stages again only the segments of partitions the destination
//! still holds where the load left them, so a newer load that committed the same rows meanwhile
//! never sees them twice.

mod checked;
mod decide;
mod staged;

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use rdlt_connector::cost::Allocations;
use rdlt_connector::{Epoch, LoadId, PipelineId, SegmentSet, StreamName, TableChange};

use parking_lot::Mutex;

use super::{RunContext, open};
use crate::budget::MemoryBudget;
use crate::crash::crash_point;
use crate::error::{Error, Side};
use crate::report::AttemptLog;
use crate::table::SharedSession;
use crate::wal::frame;
use crate::wal::scan::{self, Logged, Scanned};
use crate::wal::taken::{self, Taken};
use crate::wal::{Positions, WalStore};
use decide::decide;
use staged::Staged;

/// The session replay commits through, and where the destination stands as it goes.
struct Replaying {
    session: Arc<SharedSession>,
    epoch: Epoch,
    positions: Positions,
    /// The epoch of each stream's last reset.
    resets: BTreeMap<StreamName, Epoch>,
    /// The last commit the destination received.
    last: Option<(LoadId, u64)>,
    /// The first load whose commit reached the pipeline at the destination, once one did.
    origin: Option<LoadId>,
    /// The store the replayed logs are kept in.
    store: Option<LoadId>,
    /// The most destination writers held open at once.
    writers: NonZeroUsize,
    /// The run's memory budget, which what replay stages is charged to.
    budget: MemoryBudget,
}

/// Replays every log of the pipeline whose load is gone, then removes it; a session opens only
/// where a commit needs one, under `load_id`, the attempt's.
///
/// A commit that lands segments again is progress of the attempt, recorded in `log` as it
/// lands: a log holds each commit once, so an attempt progresses this way no more often than
/// an earlier one logged rows.
pub(super) async fn replay(
    context: &RunContext,
    load_id: LoadId,
    log: &Mutex<AttemptLog>,
) -> Result<(), Error> {
    let Some(store) = context.env.wal() else {
        return Ok(());
    };
    let mut replaying: Option<Replaying> = None;
    let replayed = replay_into(context, store.as_ref(), load_id, log, &mut replaying).await;
    let Some(replaying) = replaying else {
        return replayed;
    };
    if replayed.is_err() {
        // A failed replay's failure matters more than any error from closing.
        super::closed_within(context, replaying.session.close()).await;
        return replayed;
    }
    replaying.session.close().await
}

/// Replays as [`replay`] does, opening the session into `replaying` the first commit needs.
async fn replay_into(
    context: &RunContext,
    store: &dyn WalStore,
    load_id: LoadId,
    log: &Mutex<AttemptLog>,
    replaying: &mut Option<Replaying>,
) -> Result<(), Error> {
    let pipeline = context.plan.pipeline();
    let limits = frame::limits(context.config.memory().get());
    // What removals a crash interrupted left is never read, only removed.
    for load in store.leftovers(pipeline).await.map_err(Error::from_wal)? {
        store
            .remove_log(pipeline, load)
            .await
            .map_err(Error::from_wal)?;
    }
    let loads = store.loads(pipeline).await.map_err(Error::from_wal)?;
    for load in loads.into_iter().filter(|load| *load != load_id) {
        let number = match taken::take(store, pipeline, load, limits.frame_bytes).await? {
            Taken::Finished => None,
            Taken::Fenced { number } => Some(number),
            // A load still running holds the pipeline: the attempt waits for it to end, as its
            // session would fence the load at the destination while its log takes rows on.
            Taken::Running => return Err(Error::wal_running(load)),
            // Another replay removed the log since it was listed, having replayed it.
            Taken::Gone => continue,
        };
        if let Some(number) = number {
            let Some(scanned) = taken::scanned(store, pipeline, load, limits.frame_bytes).await?
            else {
                continue;
            };
            for logged in scanned.pending() {
                if replaying.is_none() {
                    let store = log.lock().store;
                    *replaying = Some(begin(context, load_id, store).await?);
                }
                let Some(replaying) = replaying.as_mut() else {
                    return Err(Error::internal("a replay's session is gone"));
                };
                let landed = replaying
                    .commit(store, pipeline, &scanned, logged, limits)
                    .await?;
                log.lock().progressed |= landed;
            }
            // A replay that took the log over since is left to delete it.
            if !taken::release(store, pipeline, load, number).await? {
                continue;
            }
            crash_point!("engine.replay.released");
        }
        store
            .remove_log(pipeline, load)
            .await
            .map_err(Error::from_wal)?;
    }
    Ok(())
}

async fn begin(
    context: &RunContext,
    load_id: LoadId,
    store: Option<LoadId>,
) -> Result<Replaying, Error> {
    let opened = open(context, load_id).await?;
    // Nothing of another store's pipeline lands here: its logs may hold what this one does not.
    if let Err(error) = taken::one_store(store, opened.state.log_store) {
        super::closed_within(context, opened.session.close()).await;
        return Err(error);
    }
    Ok(Replaying {
        store,
        positions: Positions::of(&opened.state),
        resets: opened.state.resets.clone(),
        last: opened
            .state
            .last_receipt
            .as_ref()
            .map(|receipt| (receipt.load_id, receipt.commit_seq.get())),
        origin: opened.state.origin,
        session: opened.session,
        epoch: opened.epoch,
        writers: context.config.growth().writers(),
        budget: context.budget.clone(),
    })
}

impl Replaying {
    /// Commits `logged` again, from `scanned`, its load's log; whether it landed segments.
    async fn commit(
        &mut self,
        store: &dyn WalStore,
        pipeline: &PipelineId,
        scanned: &Scanned,
        logged: &Logged,
        limits: rdlt_wire::Limits,
    ) -> Result<bool, Error> {
        let meta = &logged.meta;
        checked::bound(scanned.header.as_ref(), self.origin, meta.load_id, pipeline)?;
        checked::checked(logged, self.epoch, self.store, pipeline)?;
        let opened = scanned.header.as_ref().and_then(|header| header.opened);
        let decision = decide(&self.positions, &self.resets, self.last, opened, logged);
        self.stage(store, pipeline, scanned, &decision.staged, limits)
            .await?;
        let whole = decision.whole;
        let landed = !decision.staged.is_empty();
        let replayed = decision.replayed(meta, self.epoch);
        crash_point!("engine.replay.before");
        self.session
            .commit(&replayed)
            .await?
            .map_err(|error| failed("replaying a commit", error))?;
        crash_point!("engine.replay.after");
        self.positions.apply(&replayed.state_delta);
        if whole {
            self.last = Some((meta.load_id, meta.commit_seq.get()));
        }
        Ok(landed)
    }

    /// Stages `segments`' batch frames again, in the order they were logged, each table created
    /// first as its schema frame says, through at most as many writers at once as an attempt
    /// holds open.
    async fn stage(
        &self,
        store: &dyn WalStore,
        pipeline: &PipelineId,
        scanned: &Scanned,
        segments: &SegmentSet,
        limits: rdlt_wire::Limits,
    ) -> Result<(), Error> {
        let mut staged = Staged::new(self.writers, self.budget.clone());
        let mut created = BTreeSet::new();
        for segment in segments.iter() {
            for located in scanned.batches.get(&segment).into_iter().flatten() {
                let table = &table(scanned, located.table)?.table;
                if created.insert(located.table) {
                    self.create(scanned, located.table).await?;
                }
                // The frame is reserved before it is read, and what decoding its batch allocates
                // for its buffers before it is decoded; the frame's bytes go once it is, and what
                // else the batch holds, beside its buffers, is reserved then.
                let frame = staged.reserve(located.len).await?;
                let read = scan::batch(store, pipeline, *located, limits).await?;
                let buffers = read.held();
                let decoding = staged.reserve(buffers).await?;
                let batch = read.decode()?;
                drop(frame);
                let rest = Allocations::of(&batch).bytes().saturating_sub(buffers);
                let held = [decoding, staged.reserve(rest).await?];
                let open = || async {
                    self.session
                        .writer(table)
                        .await?
                        .map_err(|error| failed("opening a replayed table's writer", error))
                };
                staged.write(table, open, (segment, batch), held).await?;
            }
        }
        staged.flush().await
    }

    /// Creates the log's table `index` where it is missing.
    async fn create(&self, scanned: &Scanned, index: u32) -> Result<(), Error> {
        let table = table(scanned, index)?;
        let create = TableChange::Create {
            table: table.table.clone(),
            schema: table.schema.clone(),
        };
        self.session
            .apply_schema(&[create])
            .await?
            .map_err(|error| failed("creating a replayed table", error))
    }
}

/// The log's table `index`, as its schema frame describes it.
fn table(scanned: &Scanned, index: u32) -> Result<&frame::Table, Error> {
    scanned.tables.get(&index).ok_or_else(|| {
        Error::wal(format!(
            "a logged batch names table {index}, which the log never describes"
        ))
        .with_code(crate::limits::WAL_UNREADABLE)
    })
}

fn failed(what: &str, error: rdlt_connector::ConnectorError) -> Error {
    Error::connector(Side::Destination, what, error)
}
