//! Deterministic simulation harness for the rdlt engine.
//!
//! A simulation runs on one thread with a paused clock and a seeded [`SimEnv`], so a failing run
//! replays exactly from its [`Seed`]. Some seeds place their connectors on hosts of their own on
//! a simulated network, turmoil's, whose hosts' paused clocks step together.
//!
//! ```
//! use std::time::Duration;
//!
//! use rdlt_engine::Env;
//! use rdlt_sim::{Seed, run};
//!
//! let slept = run(Seed::new(1), |env| async move {
//!     let start = env.instant();
//!     env.sleep(Duration::from_secs(60)).await;
//!     env.instant() - start
//! });
//! assert!(slept >= Duration::from_secs(60));
//! ```

#![forbid(unsafe_code)]

mod destination;
mod env;
mod network;
mod oracle;
mod rng;
mod seed;
mod source;
mod swarm;
mod workload;
mod world;

pub use destination::{
    Cells, Digest, SimDestination, SimDestinationConfig, SimSession, SimWriter, Stored, completions,
};
pub use env::{InlinePool, SimEnv};
pub use oracle::{check_exactly_once, stress};
pub use rng::SplitMix64;
pub use seed::{SEED_VAR, SEEDS_VAR, Seed, SeedVarError, run, run_threaded, seeds};
pub use source::{SimCursor, SimSource, SimSourceConfig, schema};
pub use swarm::Features;
pub use workload::{Drift, Level, PHASES, Relaxed, Resolved, Row, SimStream, Workload};
pub use world::World;
