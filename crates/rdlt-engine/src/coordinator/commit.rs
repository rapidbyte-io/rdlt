//! A commit: every sealed segment with the state that goes with it, then the committed cursors
//! acknowledged to the source, with those owed to it.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rdlt_connector::{
    CommitMeta, Cursor, GenerationId, PartitionState, Receipt, SegmentSet, StateChange, StreamName,
    TablePath,
};

use super::Coordinator;
use crate::crash::crash_point;
use crate::env::Env;
use crate::error::{Error, shown};
use crate::event::Unreported;
use crate::partition::CursorHold;
use crate::report::{CommitPhases, CommitRecord, Pending, StreamReport, Trigger};
use crate::status::Step;

impl Coordinator {
    /// Commits every sealed segment with the state that goes with it, then acknowledges the
    /// committed cursors to the source, with those owed to it; the commit `began` with its
    /// barrier, where it raised one.
    ///
    /// Commits nothing when there is nothing to publish or record, and still tells the source
    /// the cursors owed to it; a commit that lands is counted as `trigger` made it due, with the
    /// time its phases took, and told of once its report ends.
    pub(super) async fn commit(&mut self, trigger: Trigger, began: Instant) -> Result<(), Error> {
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
            let turn = self.turn(trigger);
            self.step(Some(Step::Ack), self.parts.env.instant());
            let (_, acknowledged) = self
                .acknowledge(collected.reported, &collected.positions, turn)
                .await;
            self.step(None, self.parts.env.instant());
            return acknowledged;
        }
        let progressed = self.progresses(&collected, &delta);
        let streams = self.stream_reports(collected.streams, &completing);
        self.pending(&streams);
        let env = Arc::clone(&self.parts.env);
        let (mut laps, mut phases) = (Laps::new(env.as_ref()), CommitPhases::default());
        self.step(Some(Step::Flush), laps.last);
        crash_point!("engine.flush.before");
        self.parts.lanes.flush().await?;
        crash_point!("engine.flush.after");
        phases.flush = laps.lap();
        self.step(Some(Step::Commit), laps.last);
        let mut meta = self.meta(collected.segments, delta, finish_generations);
        let (measured, forgotten) = self.relieve(&mut meta, &tables.born)?;
        // The commit completing a stream publishes it: a replace swaps its generation in.
        crash_point!("engine.complete.before", !completing.is_empty());
        let receipt = self
            .committed(&meta, measured, &collected.positions)
            .await?;
        crash_point!("engine.complete.after", !completing.is_empty());
        phases.commit = laps.lap();
        let landed = self.landed_streams(&collected.positions);
        self.parts.live.landed(landed, laps.last);
        let unreported = self
            .parts
            .announcer
            .observes()
            .then(|| self.unreported((trigger, began), (&receipt, &streams), phases));
        self.parts.tables.recorded(&tables.versions);
        self.record(receipt, streams, &completing, unreported)?;
        self.forgot(forgotten);
        self.record_positions(&collected.positions);
        self.landed(&collected.positions, progressed);
        self.parts.tally.add(|counters| {
            counters.commits.add(trigger);
            counters.phases.add(&phases);
        });
        let reported = (collected.reported, &collected.positions);
        self.report(reported, trigger, &mut laps).await
    }

    /// Reports the cursors `reported` of the commit that landed with `positions`, which `trigger`
    /// made due, timed by `laps`, then tells of the commit with what became of its report.
    ///
    /// # Errors
    ///
    /// As [`acknowledge`](Self::acknowledge) fails.
    async fn report(
        &mut self,
        (reported, positions): (BTreeMap<usize, Cursor>, &BTreeMap<usize, PartitionState>),
        trigger: Trigger,
        laps: &mut Laps<'_>,
    ) -> Result<(), Error> {
        self.parts.live.reporting(laps.last);
        crash_point!("engine.ack.before");
        let turn = self.turn(trigger);
        let (acks, acknowledged) = self.acknowledge(reported, positions, turn).await;
        let ack = CommitPhases {
            ack: laps.lap(),
            ..CommitPhases::default()
        };
        self.parts.tally.add(|counters| counters.phases.add(&ack));
        // An attempt that ended first took the commit from the log, and its run told of it.
        let unreported = self.parts.log.lock().unreported.take();
        if let Some(unreported) = unreported {
            let told = || unreported.reported(acks, ack.ack, laps.last);
            self.parts.announcer.announce(told);
        }
        self.step(None, laps.last);
        acknowledged?;
        crash_point!("engine.ack.after");
        Ok(())
    }

    /// The commit about to be recorded, made due by `trigger`, begun at `began`, landed with
    /// `receipt`, taking `streams`' counts, its phases having taken `phases`; its report is next.
    fn unreported(
        &self,
        (trigger, began): (Trigger, Instant),
        (receipt, streams): (&Receipt, &BTreeMap<StreamName, StreamReport>),
        phases: CommitPhases,
    ) -> Unreported {
        let streams = streams
            .iter()
            .map(|(name, counts)| (shown(name), counts.clone()))
            .collect();
        Unreported {
            seq: self.key.seq,
            trigger,
            rows: receipt.rows,
            bytes: receipt.bytes,
            began,
            phases,
            streams,
            reporting: self.parts.env.instant(),
        }
    }

    /// Advances past a landed commit: what state now records, and the commit to tell of once its
    /// report ends, where anything is told.
    ///
    /// # Errors
    ///
    /// `receipt_overflow` where the receipt counts more than the attempt's totals hold.
    fn record(
        &mut self,
        receipt: Receipt,
        streams: BTreeMap<StreamName, StreamReport>,
        completing: &[usize],
        unreported: Option<Unreported>,
    ) -> Result<(), Error> {
        self.after = Some(receipt.key);
        self.key.seq = self
            .key
            .seq
            .checked_add(1)
            .expect("a session commits fewer than u64::MAX times");
        for stream in &mut self.parts.streams {
            if let Some(cycle) = &mut stream.cycle {
                cycle.recorded = true;
                cycle.stale.clear();
            }
        }
        for index in completing {
            if let Some(cycle) = &mut self.parts.streams[*index].cycle {
                cycle.finished = true;
            }
        }
        let mut log = self.parts.log.lock();
        log.pending = None;
        log.committed.add(CommitRecord { receipt, streams })?;
        // Under the lock the commit's counts take, so a run that takes them tells of it.
        log.unreported = unreported;
        Ok(())
    }

    /// The streams of the partitions whose positions a commit landing `positions` records, each
    /// named once for each of its partitions there: every seal stands its partition there, so
    /// each stream whose rows land is among them.
    fn landed_streams<'a>(
        &'a self,
        positions: &'a BTreeMap<usize, PartitionState>,
    ) -> impl Iterator<Item = StreamName> + 'a {
        positions.keys().map(|partition| {
            let stream = self.parts.partitions[*partition].stream;
            self.parts.streams[stream].name.clone()
        })
    }

    /// The commit of `segments` with `delta`, finishing `finish_generations`, under the next key
    /// and expecting the last commit that landed; it removes the staging of the segments
    /// abandoned since the last.
    fn meta(
        &mut self,
        segments: SegmentSet,
        delta: Vec<StateChange>,
        finish_generations: Vec<(TablePath, GenerationId)>,
    ) -> CommitMeta {
        CommitMeta {
            key: self.key,
            after: self.after,
            segments,
            abandoned: std::mem::take(&mut self.abandoned),
            state_delta: delta,
            finish_generations,
            child_tables: self.parts.tables.child_tables(),
            drop_tables: Vec::new(),
        }
    }

    /// Records the commit as in flight, so an attempt that loses its answer is still credited
    /// with it once a later attempt's open finds its key landed.
    fn pending(&self, streams: &BTreeMap<StreamName, StreamReport>) {
        self.parts.log.lock().pending = Some(Pending {
            key: self.key,
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
