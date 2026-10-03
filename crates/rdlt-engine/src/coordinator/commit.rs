//! A commit: every sealed segment with the state that goes with it, then the committed cursors
//! acknowledged to the source.

use std::collections::BTreeMap;

use rdlt_connector::{CommitMeta, StateChange, StateEntry, StreamName};

use super::Coordinator;
use crate::crash::crash_point;
use crate::error::Error;
use crate::partition::CursorHold;
use crate::report::{CommitRecord, StreamReport};

impl Coordinator {
    /// Commits every sealed segment with the state that goes with it, then acknowledges the
    /// committed cursors to the source.
    ///
    /// Commits nothing when there is nothing to publish or record.
    pub(super) async fn commit(&mut self) -> Result<(), Error> {
        // A new phase's stale entries go before its partitions' positions, which may reuse ids;
        // the log records its partitions' seals from where the phase starts them.
        let begun = self.phase_delta();
        let mut delta: Vec<StateChange> = begun
            .iter()
            .flat_map(|begun| begun.changes.iter().cloned())
            .collect();
        let mut collected = self.collect(&delta);
        // The seals' cursors stay charged until the commit that takes them has landed.
        let _held: Vec<CursorHold> = std::mem::take(&mut collected.held);
        let completing: Vec<usize> = (0..self.parts.streams.len())
            .filter(|index| self.parts.streams[*index].completes())
            .collect();
        let mut tables = self.parts.tables.delta();
        delta.extend(self.sequences_delta());
        delta.extend(self.state_delta(&collected.positions, &completing));
        delta.append(&mut tables.changes);
        let finish_generations = self.finish_generations(&completing);
        if collected.segments.is_empty() && delta.is_empty() {
            return Ok(());
        }
        let progressed = self.progresses(&collected, &delta);
        let streams = self.stream_reports(collected.streams, &completing);
        self.receipted(&streams, &mut delta);
        crash_point!("engine.flush.before");
        self.parts.lanes.flush().await?;
        crash_point!("engine.flush.after");
        let horizon = self.horizon().await?;
        let mut meta = CommitMeta {
            load_id: self.parts.load_id,
            commit_seq: self.seq,
            epoch: self.parts.epoch,
            segments: collected.segments,
            state_delta: delta,
            finish_generations,
            child_tables: self.parts.tables.child_tables(),
            drop_tables: Vec::new(),
            horizon: Some(horizon),
        };
        let forgotten = self.relieve(&mut meta, &tables.born);
        if self
            .log_commit(&meta, &tables, collected.sealed, begun)
            .await?
        {
            self.acknowledge(&collected.reported, false).await?;
        }
        // The commit completing a stream publishes it: a replace swaps its generation in.
        crash_point!("engine.complete.before", !completing.is_empty());
        let receipt = self.committed(&meta).await?;
        crash_point!("engine.complete.after", !completing.is_empty());
        self.parts.tables.recorded(&tables.revisions);
        self.record(receipt, streams, &completing)?;
        self.forgot(forgotten);
        self.record_positions(&collected.positions);
        self.landed(&collected.positions, progressed);
        crash_point!("engine.ack.before");
        self.acknowledge(&collected.reported, true).await?;
        crash_point!("engine.ack.after");
        Ok(())
    }

    /// Records the commit's own receipt in `delta` and as pending, so an attempt that loses the
    /// response can still be credited with it once a later attempt reads it back.
    fn receipted(
        &self,
        streams: &BTreeMap<StreamName, StreamReport>,
        delta: &mut Vec<StateChange>,
    ) {
        // A destination no commit of the pipeline reached yet learns the load whose commit
        // reaches it first, which names it to the logs of the loads after.
        let log = self.parts.log.lock();
        if self.seq == rdlt_connector::CommitSeq::FIRST && log.origin.is_none() {
            let origin = StateEntry::Origin(self.parts.load_id);
            delta.push(StateChange::Put(origin.to_record()));
        }
        // And a destination that names no store for the pipeline's logs learns this load's.
        if let (rdlt_connector::CommitSeq::FIRST, None, Some(store)) =
            (self.seq, log.log_store, log.store)
        {
            delta.push(StateChange::Put(StateEntry::LogStore(store).to_record()));
        }
        drop(log);
        let marker = self.marker(streams);
        delta.push(StateChange::Put(
            StateEntry::Receipt(marker.clone()).to_record(),
        ));
        self.parts.log.lock().pending = Some(CommitRecord {
            receipt: marker,
            streams: streams.clone(),
        });
    }
}
