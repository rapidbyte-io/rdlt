//! Deterministic simulation harness for the rdlt engine.
//!
//! A simulation runs on one thread with a paused clock and a seeded [`SimEnv`], so a failing run
//! replays exactly from its [`Seed`].
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

mod destination;
mod env;
mod oracle;
mod rng;
mod seed;
mod source;
mod workload;
mod world;

pub use destination::{
    Cells, SimDestination, SimDestinationConfig, SimSession, SimWriter, completions, published,
};
pub use env::{InlinePool, SimEnv};
pub use oracle::check_exactly_once;
pub use rng::SplitMix64;
pub use seed::{SEED_VAR, SEEDS_VAR, Seed, SeedVarError, run, seeds};
pub use source::{SimCursor, SimSource, SimSourceConfig, schema};
pub use workload::{Drift, Extra, PHASES, Row, Shape, SimStream, Workload};
pub use world::World;
