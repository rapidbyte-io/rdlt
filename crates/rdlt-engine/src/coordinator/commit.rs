//! A commit: every sealed segment with the state that goes with it, then the committed cursors
//! acknowledged to the source.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rdlt_connector::{
    CommitMeta, GenerationId, Horizon, SegmentSet, StateChange, StateEntry, StreamName, TablePath,
};

use super::Coordinator;
use crate::crash::crash_point;
use crate::env::Env;
use crate::error::Error;
use crate::partition::CursorHold;
use crate::report::{CommitPhases, CommitRecord, StreamReport, Trigger};

impl Coordinator {
    /// Commits every sealed segment with the state that goes with it, then acknowledges the
    /// committed cursors to the source.
    ///
    /// Commits nothing when there is nothing to publish or record; a commit that lands is
    /// counted as `trigger` made it due, with the time its phases took.
    pub(super) async fn commit(&mut self, trigger: Trigger) -> Result<(), Error> {
        // A new phase's stale entries go before its partitions' positions, which may reuse ids.
        let mut delta = self.phase_delta();
        let mut collected = self.collect();
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
        let env = Arc::clone(&self.parts.env);
        let (mut laps, mut phases) = (Laps::new(env.as_ref()), CommitPhases::default());
        crash_point!("engine.flush.before");
        self.parts.lanes.flush().await?;
        crash_point!("engine.flush.after");
        phases.flush = laps.lap();
        let mut meta = self.meta(collected.segments, delta, finish_generations);
        let (measured, forgotten) = self.relieve(&mut meta, &tables.born)?;
        // The commit completing a stream publishes it: a replace swaps its generation in.
        crash_point!("engine.complete.before", !completing.is_empty());
        let receipt = self
            .committed(&meta, measured, &collected.positions)
            .await?;
        crash_point!("engine.complete.after", !completing.is_empty());
        phases.commit = laps.lap();
        self.parts.tables.recorded(&tables.versions);
        self.record(receipt, streams, &completing)?;
        self.forgot(forgotten);
        self.record_positions(&collected.positions);
        self.landed(&collected.positions, progressed);
        crash_point!("engine.ack.before");
        self.acknowledge(&collected.reported).await?;
        crash_point!("engine.ack.after");
        phases.ack = laps.lap();
        self.parts.tally.add(|counters| {
            counters.commits.add(trigger);
            counters.phases.add(&phases);
        });
        Ok(())
    }

    /// The commit of `segments` with `delta`, finishing `finish_generations`; it removes the
    /// staging of the segments abandoned since the last.
    ///
    /// Nothing commits it again once it lands, so it is its own horizon: the destination may
    /// forget the receipts of every commit before it.
    fn meta(
        &mut self,
        segments: SegmentSet,
        delta: Vec<StateChange>,
        finish_generations: Vec<(TablePath, GenerationId)>,
    ) -> CommitMeta {
        let horizon = Horizon {
            load_id: self.parts.load_id,
            commit_seq: self.seq,
        };
        CommitMeta {
            load_id: self.parts.load_id,
            commit_seq: self.seq,
            epoch: self.parts.epoch,
            segments,
            abandoned: std::mem::take(&mut self.abandoned),
            state_delta: delta,
            finish_generations,
            child_tables: self.parts.tables.child_tables(),
            drop_tables: Vec::new(),
            horizon: Some(horizon),
        }
    }

    /// Records the commit's own receipt in `delta` and as pending, so an attempt that loses the
    /// response can still be credited with it once a later attempt reads it back.
    fn receipted(
        &self,
        streams: &BTreeMap<StreamName, StreamReport>,
        delta: &mut Vec<StateChange>,
    ) {
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

/// Times the phases of a commit one after another on a clock.
struct Laps<'a> {
    env: &'a dyn Env,
    last: Instant,
}

impl<'a> Laps<'a> {
    /// Laps of `env`'s clock from now.
    fn new(env: &'a dyn Env) -> Self {
        Self {
            env,
            last: env.instant(),
        }
    }

    /// The time since the last lap ended, or since the first began.
    fn lap(&mut self) -> Duration {
        let now = self.env.instant();
        let lap = now.saturating_duration_since(self.last);
        self.last = now;
        lap
    }
}
