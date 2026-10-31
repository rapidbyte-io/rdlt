//! One run's live state: what it is in the middle of, the attempts it ended, the attempt in
//! progress, and where its partitions and lanes stand.
//!
//! The run writes it at attempt and commit-step boundaries; a status reads it.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime};

use parking_lot::{Mutex, MutexGuard};
use rdlt_connector::{PartitionId, PipelineId, StreamName};

use super::clock::{stamp, unstamp};
use super::{Call, LaneCall, Partitions, Phase, Quiet, RunClock, Status, Step, StreamStatus};
use crate::budget::MemoryBudget;
use crate::env::Env;
use crate::error::{Error, ErrorReport};
use crate::event::Unreported;
use crate::report::{AttemptLog, AttemptRecord, Credit, Ledger, Report, RunStatus, Tally};

/// One run's live state, shared by the run and every status of it.
pub(crate) struct Live {
    env: Arc<dyn Env>,
    pipeline: PipelineId,
    budget: MemoryBudget,
    tally: Arc<Tally>,
    state: Mutex<State>,
}

/// What the run writes as it goes.
struct State {
    /// The run's clock, from when it began.
    clock: RunClock,
    phase: Phase,
    /// When the phase began.
    since: Instant,
    /// The attempts that ended, folded.
    ledger: Ledger,
    /// The attempt in progress, where one is.
    attempt: Option<Running>,
    /// The attempt's partitions, by their places.
    places: Vec<Place>,
    /// The attempt's lanes' calls, once its lanes run.
    lanes: Option<Arc<LaneCalls>>,
    /// When the last commit that landed each stream's rows or positions landed.
    landed: BTreeMap<StreamName, Instant>,
    /// The run's report, once it ended.
    ended: Option<Report>,
}

/// What an attempt that ended left.
pub(crate) struct Ended {
    /// Whether a commit of it moved the load on.
    pub(crate) progressed: bool,
    /// What a commit of an earlier attempt it found landed published, where it found one.
    pub(crate) credited: Option<Credit>,
    /// Its commit that landed whose report it ended before, which the run tells of.
    pub(crate) unreported: Option<Unreported>,
}

/// The attempt in progress.
struct Running {
    log: Arc<Mutex<AttemptLog>>,
    started_at: SystemTime,
}

/// A partition's place in an attempt: the partition that holds it, and its stamp, which the
/// coordinator stores as it hears from the partition.
#[derive(Clone, Debug)]
pub(crate) struct Place {
    pub(crate) stream: StreamName,
    pub(crate) id: PartitionId,
    pub(crate) stamp: Arc<AtomicU64>,
}

impl Place {
    /// The stamp of a partition waiting for a slot.
    pub(crate) const WAITING: u64 = 0;
    /// The stamp of a partition whose read ended.
    pub(crate) const ENDED: u64 = u64::MAX;
}

/// Each lane's call into the destination in flight: the stamp of when it was made, from the
/// lanes' start, shifted two bits left, and the call in the low two; zero between calls.
#[derive(Debug)]
pub(crate) struct LaneCalls {
    origin: Instant,
    calls: Box<[AtomicU64]>,
}

impl LaneCalls {
    /// The calls of `lanes` lanes starting at `origin`, none in flight.
    pub(crate) fn new(origin: Instant, lanes: usize) -> Self {
        Self {
            origin,
            calls: (0..lanes).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    /// Notes that lane `lane` makes `call`, at `at`.
    pub(crate) fn calling(&self, lane: usize, call: Call, at: Instant) {
        let code = match call {
            Call::Open => 1,
            Call::Write => 2,
            Call::Flush => 3,
        };
        if let Some(calls) = self.calls.get(lane) {
            calls.store((stamp(self.origin, at) << 2) + code, Ordering::Relaxed);
        }
    }

    /// Notes that lane `lane`'s call returned.
    pub(crate) fn returned(&self, lane: usize) {
        if let Some(calls) = self.calls.get(lane) {
            calls.store(0, Ordering::Relaxed);
        }
    }

    /// Each lane's call in flight, its time on `clock`.
    pub(super) fn read(&self, clock: &RunClock) -> Vec<Option<LaneCall>> {
        let call = |stored: u64| {
            let call = match stored & 0b11 {
                1 => Call::Open,
                2 => Call::Write,
                3 => Call::Flush,
                _ => return None,
            };
            let since = clock.wall(unstamp(self.origin, stored >> 2));
            Some(LaneCall { call, since })
        };
        let calls = self.calls.iter();
        calls
            .map(|stored| call(stored.load(Ordering::Relaxed)))
            .collect()
    }
}

impl Live {
    /// The live state of a run of `pipeline` beginning now on `env`, whose memory is `budget`'s
    /// and whose counters `tally` holds.
    pub(crate) fn new(
        env: Arc<dyn Env>,
        pipeline: PipelineId,
        budget: MemoryBudget,
        tally: Arc<Tally>,
    ) -> Self {
        let clock = RunClock::new(env.as_ref());
        let state = State {
            clock,
            phase: Phase::Opening,
            since: clock.began(),
            ledger: Ledger::new(pipeline.clone()),
            attempt: None,
            places: Vec::new(),
            lanes: None,
            landed: BTreeMap::new(),
            ended: None,
        };
        Self {
            env,
            pipeline,
            budget,
            tally,
            state: Mutex::new(state),
        }
    }

    /// Notes that the run begins now: its clock starts again, since a run starts when it is
    /// first polled.
    pub(crate) fn begin(&self) {
        let clock = RunClock::new(self.env.as_ref());
        let mut state = self.state.lock();
        (state.clock, state.since) = (clock, clock.began());
    }

    /// The run's clock.
    pub(crate) fn clock(&self) -> RunClock {
        self.state.lock().clock
    }

    /// How many attempts ended.
    pub(crate) fn attempted(&self) -> u64 {
        self.state.lock().ledger.report().attempted
    }

    /// Notes that an attempt, which `log` records, started at `started_at`: it opens the
    /// destination now.
    pub(crate) fn started(&self, log: Arc<Mutex<AttemptLog>>, started_at: SystemTime) {
        let now = self.env.instant();
        let mut state = self.state.lock();
        let running = Running { log, started_at };
        state.attempt = Some(running);
        (state.phase, state.since) = (Phase::Opening, now);
    }

    /// Folds in the attempt in progress, which ended `failed` or not with `error`.
    ///
    /// # Errors
    ///
    /// As [`Ledger::record`] fails.
    pub(crate) fn ended(&self, error: Option<ErrorReport>, failed: bool) -> Result<Ended, Error> {
        let ended_at = self.env.now();
        let mut state = self.state.lock();
        let running = state
            .attempt
            .take()
            .expect("an attempt ends after it started");
        let mut log = std::mem::take(&mut *running.log.lock());
        let unreported = log.unreported.take();
        state.places.clear();
        state.lanes = None;
        let record = AttemptRecord {
            started_at: running.started_at,
            ended_at,
            log,
            error,
        };
        let (progressed, credited) = state.ledger.record(record, failed)?;
        Ok(Ended {
            progressed,
            credited,
            unreported,
        })
    }

    /// The state to note what the run does in, unless the run ended: a task its end left running
    /// changes nothing a status then shows.
    fn running(&self) -> Option<MutexGuard<'_, State>> {
        let state = self.state.lock();
        state.ended.is_none().then_some(state)
    }

    /// Notes that the run is in `phase` from `at`.
    pub(crate) fn phase(&self, phase: Phase, at: Instant) {
        if let Some(mut state) = self.running() {
            (state.phase, state.since) = (phase, at);
        }
    }

    /// Notes that the commit at its commit step landed and reports its cursors from `at`, under
    /// its sequence and with the tries it took.
    pub(crate) fn reporting(&self, at: Instant) {
        let Some(mut state) = self.running() else {
            return;
        };
        if let Phase::Committing { seq, tries, .. } = state.phase {
            let step = Step::Ack;
            (state.phase, state.since) = (Phase::Committing { seq, step, tries }, at);
        }
    }

    /// Notes that the run waits to start its next attempt at `until`, from `at`.
    pub(crate) fn backing_off(&self, until: Instant, at: Instant) {
        let Some(mut state) = self.running() else {
            return;
        };
        let until = state.clock.wall(until);
        (state.phase, state.since) = (Phase::BackingOff { until }, at);
    }

    /// Notes the attempt's partitions, by their places.
    pub(crate) fn places(&self, places: Vec<Place>) {
        if let Some(mut state) = self.running() {
            state.places = places;
        }
    }

    /// Notes that the partition at `index` holds `place`.
    pub(crate) fn place(&self, index: usize, place: Place) {
        let Some(mut state) = self.running() else {
            return;
        };
        match state.places.get_mut(index) {
            Some(held) => *held = place,
            None => state.places.push(place),
        }
    }

    /// Notes the attempt's lanes' calls.
    pub(crate) fn lanes(&self, calls: Arc<LaneCalls>) {
        if let Some(mut state) = self.running() {
            state.lanes = Some(calls);
        }
    }

    /// Notes that a commit landing `streams`' rows or positions landed at `at`.
    pub(crate) fn landed(&self, streams: impl IntoIterator<Item = StreamName>, at: Instant) {
        let Some(mut state) = self.running() else {
            return;
        };
        for stream in streams {
            state.landed.insert(stream, at);
        }
    }

    /// Ends the run as `status`: its report, which every status from now on carries.
    pub(crate) fn end(&self, status: RunStatus) -> Report {
        let now = self.env.instant();
        let mut state = self.state.lock();
        // No attempt is folded in once the run ends, so its ledger's report is taken, not copied.
        let ledger = std::mem::replace(&mut state.ledger, Ledger::new(self.pipeline.clone()));
        let report = self.assemble(ledger.into_report(), &state, status, now);
        (state.phase, state.since) = (Phase::Ended { status }, now);
        state.ended = Some(report.clone());
        report
    }

    /// What the run is doing now.
    pub(crate) fn status(&self) -> Status {
        let now = self.env.instant();
        let state = self.state.lock();
        let report = match &state.ended {
            Some(report) => report.clone(),
            None => self.assemble(
                state.ledger.report().clone(),
                &state,
                RunStatus::Running,
                now,
            ),
        };
        let clock = state.clock;
        let (partitions, quietest) = partitions(&state.places, &clock);
        let lanes = state.lanes.as_ref().map(|lanes| lanes.read(&clock));
        let streams = streams(&report, &state.landed, &clock);
        let (phase, since) = (state.phase, clock.wall(state.since));
        drop(state);
        Status {
            phase,
            since,
            partitions,
            quietest,
            waiting: self.budget.open_waits(|at| clock.wall(at)),
            lanes: lanes.unwrap_or_default(),
            streams,
            report,
        }
    }

    /// The run's report as `status`, at `now`: `report`, what its ledger folded, with the attempt
    /// in progress listed as ending now, and what it counted.
    fn assemble(
        &self,
        mut report: Report,
        state: &State,
        status: RunStatus,
        now: Instant,
    ) -> Report {
        if let Some(running) = &state.attempt {
            let record = AttemptRecord {
                started_at: running.started_at,
                ended_at: state.clock.wall(now),
                log: running.log.lock().clone(),
                error: None,
            };
            // Totals that would pass what they hold leave the report as it was.
            report.absorb(record).ok();
        }
        report.status = status;
        report.elapsed = now.saturating_duration_since(state.clock.began());
        report.peak_memory = self.budget.peak();
        report.counters = self.tally.counters();
        report.counters.waits = self.budget.waited();
        report.counters.signals = self.budget.signals();
        report.counters.kept_peak = self.budget.kept_peak();
        report
    }
}

/// How many of `places` read, wait for a slot and ended, and which reading has been quiet
/// longest, on `clock`.
fn partitions(places: &[Place], clock: &RunClock) -> (Partitions, Option<Quiet>) {
    let mut partitions = Partitions::default();
    let mut quietest: Option<(&Place, u64)> = None;
    for place in places {
        match place.stamp.load(Ordering::Relaxed) {
            Place::WAITING => partitions.waiting += 1,
            Place::ENDED => partitions.ended += 1,
            stamp => {
                partitions.reading += 1;
                if quietest.is_none_or(|(_, quiet)| stamp < quiet) {
                    quietest = Some((place, stamp));
                }
            }
        }
    }
    let quietest = quietest.map(|(place, stamp)| Quiet {
        stream: place.stream.to_string(),
        partition: place.id.to_string(),
        since: clock.wall(unstamp(clock.began(), stamp)),
    });
    (partitions, quietest)
}

/// Each stream of `report` with when `landed` says its last commit landed, on `clock`.
fn streams(
    report: &Report,
    landed: &BTreeMap<StreamName, Instant>,
    clock: &RunClock,
) -> BTreeMap<String, StreamStatus> {
    let mut streams: BTreeMap<String, StreamStatus> = report
        .streams
        .iter()
        .map(|(name, stream)| {
            let status = StreamStatus {
                landed: None,
                behind: stream.behind,
                headroom: stream.headroom,
            };
            (name.clone(), status)
        })
        .collect();
    for (stream, at) in landed {
        let stream = streams.entry(stream.to_string()).or_default();
        stream.landed = Some(clock.wall(*at));
    }
    streams
}
