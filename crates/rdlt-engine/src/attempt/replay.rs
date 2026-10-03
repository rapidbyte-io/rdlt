//! Replaying the write-ahead logs earlier loads left: each commit a log holds without its receipt
//! is committed again, in a session of its own, before the attempt opens (spec §15.6).
//!
//! A log is replayed only once its load is gone, which its claim shows. Each commit stages again
//! only the segments of partitions the destination still holds where the load left them, so a
//! newer load that committed the same rows meanwhile never sees them twice.

mod decide;
mod staged;

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use rdlt_connector::{Epoch, LoadId, PipelineId, SegmentSet, StreamName, TableChange};

use parking_lot::Mutex;

use super::{RunContext, open};
use crate::crash::crash_point;
use crate::error::{Error, Side};
use crate::report::AttemptLog;
use crate::table::SharedSession;
use crate::wal::scan::{self, Logged, Scanned};
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
    /// The most destination writers held open at once.
    writers: NonZeroUsize,
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
    for load in store.loads(pipeline).await.map_err(Error::from_wal)? {
        // A load still running holds its log; it commits it itself, or leaves it to a later
        // replay once it is gone.
        let Some(claim) = store.claim(pipeline, load).await.map_err(Error::from_wal)? else {
            continue;
        };
        let scanned = scan::scan(store, pipeline, load).await?;
        for logged in scanned.pending() {
            let replaying = match replaying {
                Some(replaying) => replaying,
                None => replaying.insert(begin(context, load_id).await?),
            };
            let landed = replaying.commit(store, pipeline, &scanned, logged).await?;
            log.lock().progressed |= landed;
        }
        store
            .remove_log(pipeline, load)
            .await
            .map_err(Error::from_wal)?;
        drop(claim);
    }
    Ok(())
}

async fn begin(context: &RunContext, load_id: LoadId) -> Result<Replaying, Error> {
    let opened = open(context, load_id).await?;
    Ok(Replaying {
        positions: Positions::of(&opened.state),
        resets: opened.state.resets.clone(),
        last: opened
            .state
            .last_receipt
            .as_ref()
            .map(|receipt| (receipt.load_id, receipt.commit_seq.get())),
        session: opened.session,
        epoch: opened.epoch,
        writers: context.config.growth().writers(),
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
    ) -> Result<bool, Error> {
        let meta = &logged.meta;
        let opened = scanned.header.as_ref().and_then(|header| header.opened);
        let decision = decide(&self.positions, &self.resets, self.last, opened, logged);
        self.stage(store, pipeline, scanned, &decision.staged)
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
    ) -> Result<(), Error> {
        let mut staged = Staged::new(self.writers);
        let mut created = BTreeSet::new();
        for segment in segments.iter() {
            for located in scanned.batches.get(&segment).into_iter().flatten() {
                let table = &table(scanned, located.table)?.table;
                if created.insert(located.table) {
                    self.create(scanned, located.table).await?;
                }
                let batch = scan::batch(store, pipeline, *located).await?;
                let open = || async {
                    self.session
                        .writer(table)
                        .await?
                        .map_err(|error| failed("opening a replayed table's writer", error))
                };
                staged.write(table, open, segment, batch).await?;
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
fn table(scanned: &Scanned, index: u32) -> Result<&crate::wal::frame::Table, Error> {
    scanned.tables.get(&index).ok_or_else(|| {
        Error::wal(format!(
            "a logged batch names table {index}, which the log never describes"
        ))
        .with_code("wal_unreadable")
    })
}

fn failed(what: &str, error: rdlt_connector::ConnectorError) -> Error {
    Error::connector(Side::Destination, what, error)
}
