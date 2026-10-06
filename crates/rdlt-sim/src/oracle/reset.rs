//! Resetting a stream once every phase converged, while runs of every pipeline load: the reset
//! fences them, and the tables must converge again on what the model says.

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{
    ConnectContext, Destination, PipelineId, ReadMode, Source, StreamName, destination_factory,
    source_factory,
};
use rdlt_engine::{Engine, ResetReport, ResetScope, WriteMode};
use serde_json::json;

use super::scenario::Scenario;
use super::{PIPELINES, Simulation};
use crate::destination::{SimDestination, records};
use crate::rng::SplitMix64;
use crate::seed::Seed;
use crate::source::SimSource;
use crate::swarm::RESET;
use crate::workload::SimStream;

/// How many times a reset that faults is tried before faults are turned off for it.
const TRIES: usize = 8;

/// A drawn reset: which pipeline's stream, how, and how long after the runs start.
#[derive(Clone)]
struct Drawn {
    pipeline: PipelineId,
    streams: [StreamName; 1],
    scope: ResetScope,
    delay: Duration,
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
    pub(super) async fn reset(&mut self, seed: Seed, phase: usize) -> bool {
        let Some(drawn) = self.draw(seed) else {
            return false;
        };
        self.world
            .reset
            .lock()
            .insert(drawn.streams[0].name().to_owned());
        let (engine, world, name) = (
            self.engine.clone(),
            Arc::clone(&self.world),
            self.name.clone(),
        );
        let racing = drawn.clone();
        let resetting = async move {
            tokio::time::sleep(racing.delay).await;
            let (source, destination) = connected(&name).await;
            for _ in 0..TRIES {
                let recorded = records(&world, &racing.pipeline, &racing.streams[0]);
                match tried(&engine, &racing, &source, &destination).await {
                    Ok(_) => return None,
                    Err(error) if refused(&error) => {
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
        };
        self.world.set_faulty(self.world.workload.features.faults);
        let mut reports = Vec::new();
        // The reset fences the runs it races: they may fail, faults or not.
        let running = self.attempt(phase, Scenario::Plain, false, None, &mut reports);
        let (_, unfinished) = tokio::join!(running, resetting);
        // The raced runs have ended: no acknowledgement of a commit from before the reset is
        // left to reach the source, so it is checked again from here on.
        self.world.reset.lock().remove(drawn.streams[0].name());
        if let Some((engine, source, destination)) = unfinished {
            // Faults kept it from committing: without them, it must, or refuse a stream the
            // pipeline recorded nothing of.
            self.world.set_faulty(false);
            let recorded = records(&self.world, &drawn.pipeline, &drawn.streams[0]);
            let answer = tried(&engine, &drawn, &source, &destination).await;
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
        true
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

/// The simulated source and destination of the world `world`.
async fn connected(world: &str) -> (Arc<dyn Source>, Arc<dyn Destination>) {
    let config = json!({ "world": world });
    let destination = destination_factory::<SimDestination>()
        .connect(config.clone(), ConnectContext::new())
        .await
        .expect("the simulated destination connects");
    let source = source_factory::<SimSource>()
        .connect(config, ConnectContext::new())
        .await
        .expect("the simulated source connects");
    (Arc::from(source), Arc::from(destination))
}

/// Tries `drawn` once, with `engine`.
async fn tried(
    engine: &Engine,
    drawn: &Drawn,
    source: &Arc<dyn Source>,
    destination: &Arc<dyn Destination>,
) -> Result<ResetReport, rdlt_engine::Error> {
    engine
        .reset(
            &drawn.pipeline,
            &drawn.streams,
            drawn.scope,
            Arc::clone(source),
            Arc::clone(destination),
        )
        .await
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
