//! Resetting a stream once every phase converged, while runs of every pipeline load: the reset
//! fences them, and the tables must converge again on what the model says.

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{Destination, PipelineId, ReadMode, Source, StreamName};
use rdlt_engine::{Engine, ResetReport, ResetScope, WriteMode};

use super::scenario::{Scenario, in_process};
use super::{DISRUPTED, PIPELINES, Simulation};
use crate::World;
use crate::canary::Canary;
use crate::destination::records;
use crate::placing::Placing;
use crate::reached::Reach;
use crate::rng::SplitMix64;
use crate::seed::Seed;
use crate::swarm::RESET;
use crate::workload::SimStream;

/// How many times a reset that faults is tried before faults are turned off for it.
const TRIES: usize = 8;

/// What a reset that faults kept from committing is tried again with.
type Unfinished = (Engine, Arc<dyn Source>, Arc<dyn Destination>);

/// A drawn reset: which pipeline's stream, how, and how long after the runs start.
#[derive(Clone)]
struct Drawn {
    pipeline: PipelineId,
    streams: [StreamName; 1],
    scope: ResetScope,
    delay: Duration,
}

/// The simulated source and destination of the world registered as `world`: placed by `placing`
/// where there is one, never on the peer that stalls, and through the host's registry in this
/// process otherwise.
async fn connected(
    world: &str,
    placing: Option<&Placing>,
) -> (Arc<dyn Source>, Arc<dyn Destination>) {
    let Some(placing) = placing else {
        return in_process(world).await;
    };
    let config = Canary::of(world).config();
    (
        placing.source(&config, false).await,
        placing.destination(&config, false).await,
    )
}

/// Tries `racing` after its delay, up to [`TRIES`] times while faults fail it; the connectors a
/// reset that faults kept from committing, or none once it answered.
async fn resetting(
    (engine, world): (Engine, Arc<World>),
    (name, placing): (String, Option<Placing>),
    seed: Seed,
    racing: Drawn,
) -> Option<Unfinished> {
    tokio::time::sleep(racing.delay).await;
    let (source, destination) = connected(&name, placing.as_ref()).await;
    for _ in 0..TRIES {
        let recorded = records(&world, &racing.pipeline, &racing.streams[0]);
        match tried(
            &engine,
            &racing,
            &source,
            &destination,
            (&*world, name.as_str()),
        )
        .await
        {
            Ok(_) => return None,
            Err(error) if refused(&error) => {
                world.reach(Reach::ResetRefused);
                assert!(
                    !recorded,
                    "seed {seed}: resetting {:?} as {:?} was refused, though the \
                     pipeline recorded the stream before the reset began: {error}",
                    racing.streams, racing.scope
                );
                return None;
            }
            Err(_) => {}
        }
    }
    Some((engine, source, destination))
}

impl Simulation {
    /// Resets a stream the source can read again, drawn with its scope apart from the seed's
    /// generator, while a run of every pipeline loads in `phase`; whether it drew one.
    ///
    /// Only resets after which the model's rows still hold are drawn, as [`scopes`] says. A reset
    /// of a stream the pipeline's state records nothing of when the reset opens the destination
    /// must be refused as `stream_not_found`, which changes nothing the model holds. A run raced
    /// may record the stream as the reset begins, so a reset raced is wrong only where it refuses
    /// a stream the state recorded before it began; once the runs have ended, the state alone
    /// says which answer is right.
    ///
    /// On the simulated network with faults, the network is disrupted while the reset and the
    /// runs it races last, and its retry without faults places its connectors again.
    pub(super) async fn reset(&mut self, seed: Seed, phase: usize) -> bool {
        let Some(drawn) = self.draw(seed) else {
            return false;
        };
        self.world
            .reset
            .lock()
            .insert(drawn.streams[0].name().to_owned());
        let resetting = resetting(
            (self.engine.clone(), Arc::clone(&self.world)),
            (self.name.clone(), self.placing.clone()),
            seed,
            drawn.clone(),
        );
        let features = self.world.workload.features;
        self.world.set_faulty(features.faults);
        // Drawn apart from every other draw, and only where the network is.
        let network = (features.faults && self.placing.is_some())
            .then(|| SplitMix64::new(seed.value() ^ RESET ^ DISRUPTED));
        let placing = self.placing.clone();
        let mut reports = Vec::new();
        // The reset fences the runs it races: they may fail, faults or not.
        let running = self.attempt(phase, Scenario::Plain, false, None, &mut reports);
        let raced = Box::pin(async { tokio::join!(running, resetting) });
        let (_, unfinished) = match (&placing, network) {
            (Some(placing), Some(rng)) => Box::pin(placing.disrupting(rng, raced)).await,
            _ => raced.await,
        };
        // The raced runs have ended: the stream's positions only move forward from here on.
        self.world.reset.lock().remove(drawn.streams[0].name());
        if let Some(unfinished) = unfinished {
            self.settle(seed, &drawn, unfinished).await;
        }
        true
    }

    /// Resets again, without faults, what faults kept from committing: it must now, or refuse a
    /// stream the pipeline recorded nothing of.
    async fn settle(&self, seed: Seed, drawn: &Drawn, unfinished: Unfinished) {
        let (engine, mut source, mut destination) = unfinished;
        self.world.set_faulty(false);
        if self.placing.is_some() {
            // A network that healed may have cut the connections the faulty tries held.
            (source, destination) = connected(&self.name, self.placing.as_ref()).await;
        }
        let recorded = records(&self.world, &drawn.pipeline, &drawn.streams[0]);
        let watch = (&*self.world, self.name.as_str());
        let answer = tried(&engine, drawn, &source, &destination, watch).await;
        if answer.as_ref().is_err_and(refused) {
            self.world.reach(Reach::ResetRefused);
        }
        let right = match &answer {
            Ok(_) => recorded,
            Err(error) => !recorded && refused(error),
        };
        if !right {
            let (streams, scope) = (&drawn.streams, drawn.scope);
            panic!(
                "seed {seed}: resetting {streams:?} as {scope:?}, recorded: {recorded}, \
                 answered {answer:?}"
            );
        }
    }

    /// The reset `seed` draws apart from its generator, where the feature is on and a stream
    /// can be reset.
    fn draw(&self, seed: Seed) -> Option<Drawn> {
        let workload = &self.world.workload;
        if !workload.features.reset {
            return None;
        }
        let mut rng = SplitMix64::new(seed.value() ^ RESET);
        let resettable: Vec<(usize, Vec<ResetScope>)> = workload
            .streams
            .iter()
            .enumerate()
            .map(|(index, stream)| (index, scopes(stream)))
            .filter(|(_, scopes)| !scopes.is_empty())
            .collect();
        let (index, scopes) = pick(&mut rng, &resettable)?;
        let scope = pick(&mut rng, scopes)
            .copied()
            .unwrap_or(ResetScope::Tables);
        let pipeline =
            PipelineId::parse(PIPELINES[*index % workload.pipelines]).expect("valid pipeline id");
        let stream = StreamName::new(&workload.streams[*index].name).expect("valid stream name");
        Some(Drawn {
            pipeline,
            streams: [stream],
            scope,
            delay: Duration::from_millis(rng.below(40)),
        })
    }
}

/// Tries `drawn` once, with `engine`.
async fn tried(
    engine: &Engine,
    drawn: &Drawn,
    source: &Arc<dyn Source>,
    destination: &Arc<dyn Destination>,
    (world, name): (&World, &str),
) -> Result<ResetReport, rdlt_engine::Error> {
    let answer = engine
        .reset(
            &drawn.pipeline,
            &drawn.streams,
            drawn.scope,
            Arc::clone(source),
            Arc::clone(destination),
        )
        .await;
    if let Err(error) = &answer {
        Canary::of(name).answered(world, "reset", error);
    }
    answer
}

/// The resets after which the model's rows for `stream` still hold.
///
/// A stream whose columns or key drift is never reset: read again, its earlier phases' rows
/// would meet the types its columns drifted to, and a table created anew would meet its drift
/// columns afresh, where the policies that discarded or refused them before may keep them. Nor is
/// one whose partitions share keys: read again at once, their merges of a key race, where the
/// model has the last phase's row win. An appended stream is never read again into its table,
/// which keeps what it held, a full read's partial cycle too, beside the rows read again; and a
/// full read's table is dropped only where the read replaces it.
fn scopes(stream: &SimStream) -> Vec<ResetScope> {
    let steady_keys = stream
        .key_types
        .iter()
        .all(|types| types.iter().all(|kind| *kind == types[0]));
    let drifts = !stream.drift.is_empty() || !steady_keys;
    if !stream.replayable || stream.read == ReadMode::Cdc || drifts || stream.shared_keys {
        return Vec::new();
    }
    let appended = stream.write == WriteMode::Append;
    let mut scopes = Vec::new();
    // A full read serves the rows its phase holds: a table it replaces holds just those, but one
    // it merges or appends to keeps earlier phases' too, which a table created anew would lack.
    if stream.read == ReadMode::Incremental || stream.write == WriteMode::Replace {
        scopes.push(ResetScope::Tables);
    }
    if !appended {
        scopes.push(ResetScope::Positions);
    }
    scopes
}

/// Whether `error` refuses a reset of a stream the pipeline recorded nothing of.
fn refused(error: &rdlt_engine::Error) -> bool {
    error.code() == Some("stream_not_found")
}

/// One of `items`, drawn from `rng`.
fn pick<'a, T>(rng: &mut SplitMix64, items: &'a [T]) -> Option<&'a T> {
    if items.is_empty() {
        return None;
    }
    let index = usize::try_from(rng.below(items.len() as u64)).unwrap_or(0);
    items.get(index)
}
