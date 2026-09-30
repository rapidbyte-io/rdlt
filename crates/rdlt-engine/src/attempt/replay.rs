//! Replaying the write-ahead logs earlier loads left: each commit a log holds without its receipt
//! is committed again, in a session of its own, before the attempt opens (spec §15.6).
//!
//! A log is replayed only once its load is gone, which its claim shows. Each commit stages again
//! only the segments of partitions the destination still holds where the load left them, so a
//! newer load that committed the same rows meanwhile never sees them twice.

mod decide;

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::sync::Arc;

use rdlt_connector::{CommitMeta, DestinationWriter, LoadId, PipelineId, SegmentSet, TableChange};

use super::{RunContext, open};
use crate::error::{Error, Side};
use crate::table::SharedSession;
use crate::wal::scan::{self, Logged, Scanned};
use crate::wal::{Positions, WalStore};
use decide::{Decision, decide};

/// The session replay commits through, and where the destination stands as it goes.
struct Replaying {
    session: Arc<SharedSession>,
    epoch: rdlt_connector::Epoch,
    positions: Positions,
    /// The last commit the destination received.
    last: Option<(LoadId, u64)>,
}

/// Replays every log of the pipeline whose load is gone, then removes it; a session opens only
/// where a commit needs one, under `load_id`, the attempt's.
pub(super) async fn replay(context: &RunContext, load_id: LoadId) -> Result<(), Error> {
    let Some(store) = context.env.wal() else {
        return Ok(());
    };
    let pipeline = context.plan.pipeline();
    let mut replaying: Option<Replaying> = None;
    for load in store.loads(pipeline).await.map_err(Error::from_wal)? {
        // A load still running holds its log; it commits it itself, or leaves it to a later
        // replay once it is gone.
        let Some(claim) = store.claim(pipeline, load).await.map_err(Error::from_wal)? else {
            continue;
        };
        let scanned = scan::scan(store.as_ref(), pipeline, load).await?;
        for logged in scanned.pending() {
            let replaying = match &mut replaying {
                Some(replaying) => replaying,
                None => replaying.insert(begin(context, load_id).await?),
            };
            replaying
                .commit(store.as_ref(), pipeline, &scanned, logged)
                .await?;
        }
        store
            .remove_log(pipeline, load)
            .await
            .map_err(Error::from_wal)?;
        drop(claim);
    }
    if let Some(replaying) = replaying {
        replaying.session.close().await?;
    }
    Ok(())
}

async fn begin(context: &RunContext, load_id: LoadId) -> Result<Replaying, Error> {
    let opened = open(context, load_id).await?;
    Ok(Replaying {
        positions: Positions::of(&opened.state),
        last: opened
            .state
            .last_receipt
            .as_ref()
            .map(|receipt| (receipt.load_id, receipt.commit_seq.get())),
        session: opened.session,
        epoch: opened.epoch,
    })
}

impl Replaying {
    /// Commits `logged` again, from `scanned`, its load's log.
    async fn commit(
        &mut self,
        store: &dyn WalStore,
        pipeline: &PipelineId,
        scanned: &Scanned,
        logged: &Logged,
    ) -> Result<(), Error> {
        let meta = &logged.meta;
        let opened = scanned.header.as_ref().and_then(|header| header.opened);
        let Decision {
            staged,
            moved,
            whole,
        } = decide(&self.positions, self.last, opened, logged);
        self.stage(store, pipeline, scanned, &staged).await?;
        let replayed = CommitMeta {
            epoch: self.epoch,
            segments: staged,
            // Where another load committed since, only the partitions this commit still moves
            // change: the rest of its state is older than the destination's.
            state_delta: if whole {
                meta.state_delta.clone()
            } else {
                moved
            },
            finish_generations: if whole {
                meta.finish_generations.clone()
            } else {
                Vec::new()
            },
            ..meta.clone()
        };
        self.session
            .commit(&replayed)
            .await?
            .map_err(|error| failed("replaying a commit", error))?;
        self.positions.apply(&replayed.state_delta);
        if whole {
            self.last = Some((meta.load_id, meta.commit_seq.get()));
        }
        Ok(())
    }

    /// Stages `segments`' batch frames again, each table created first, as its schema frame says.
    async fn stage(
        &self,
        store: &dyn WalStore,
        pipeline: &PipelineId,
        scanned: &Scanned,
        segments: &SegmentSet,
    ) -> Result<(), Error> {
        let mut writers: BTreeMap<u32, Box<dyn DestinationWriter>> = BTreeMap::new();
        for segment in segments.iter() {
            for located in scanned.batches.get(&segment).into_iter().flatten() {
                let writer = match writers.entry(located.table) {
                    Entry::Occupied(writer) => writer.into_mut(),
                    Entry::Vacant(vacant) => {
                        vacant.insert(self.writer(scanned, located.table).await?)
                    }
                };
                let batch = scan::batch(store, pipeline, *located).await?;
                writer
                    .write(segment, batch)
                    .await
                    .map_err(|error| failed("staging a replayed batch", error))?;
            }
        }
        for writer in writers.values_mut() {
            writer
                .flush()
                .await
                .map_err(|error| failed("staging replayed batches", error))?;
        }
        Ok(())
    }

    /// A writer of the log's table `index`, created first where it is missing.
    async fn writer(
        &self,
        scanned: &Scanned,
        index: u32,
    ) -> Result<Box<dyn DestinationWriter>, Error> {
        let table = scanned.tables.get(&index).ok_or_else(|| {
            Error::wal(format!(
                "a logged batch names table {index}, which the log never describes"
            ))
            .with_code("wal_unreadable")
        })?;
        let create = TableChange::Create {
            table: table.table.clone(),
            schema: table.schema.clone(),
        };
        self.session
            .apply_schema(&[create])
            .await?
            .map_err(|error| failed("creating a replayed table", error))?;
        self.session
            .writer(&table.table)
            .await?
            .map_err(|error| failed("opening a replayed table's writer", error))
    }
}

fn failed(what: &str, error: rdlt_connector::ConnectorError) -> Error {
    Error::connector(Side::Destination, what, error)
}
