//! A commit: every sealed segment with the state that goes with it, then the committed cursors
//! acknowledged to the source.

use rdlt_connector::{CommitMeta, StateChange, StateEntry};

use super::Coordinator;
use crate::crash::crash_point;
use crate::error::Error;
use crate::partition::CursorHold;
use crate::report::CommitRecord;

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
        let tables = self.parts.tables.delta();
        delta.extend(self.sequences_delta());
        delta.extend(self.state_delta(&collected.positions, &completing));
        delta.extend(tables.changes);
        let finish_generations = self.finish_generations(&completing);
        if collected.segments.is_empty() && delta.is_empty() {
            return Ok(());
        }
        let progressed = self.progresses(&collected, &delta);
        let streams = self.stream_reports(collected.streams, &completing);
        // The commit records its own receipt, so an attempt that loses the response can still be
        // credited with it once a later attempt reads it back.
        let marker = self.marker(&streams);
        delta.push(StateChange::Put(
            StateEntry::Receipt(marker.clone()).to_record(),
        ));
        self.parts.log.lock().pending = Some(CommitRecord {
            receipt: marker,
            streams: streams.clone(),
        });
        crash_point!("engine.flush.before");
        self.parts.lanes.flush().await?;
        crash_point!("engine.flush.after");
        let meta = CommitMeta {
            load_id: self.parts.load_id,
            commit_seq: self.seq,
            epoch: self.parts.epoch,
            segments: collected.segments,
            state_delta: delta,
            finish_generations,
            child_tables: self.parts.tables.child_tables(),
            drop_tables: Vec::new(),
        };
        if self
            .log_commit(&meta, tables.prepaid, collected.sealed, begun)
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
        self.record_positions(&collected.positions);
        self.landed(&collected.positions, progressed);
        crash_point!("engine.ack.before");
        self.acknowledge(&collected.reported, true).await?;
        crash_point!("engine.ack.after");
        Ok(())
    }
}
