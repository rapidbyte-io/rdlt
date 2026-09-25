//! The rdlt data-movement engine.
//!
//! An [`Engine`] runs a [`PipelinePlan`] from a source to a destination exactly once: every row
//! the source emits before a committed checkpoint is published exactly once, whatever fails,
//! retries or runs concurrently. Every source of nondeterminism the engine uses (clocks,
//! randomness, CPU scheduling) comes from an [`Env`], so the whole engine runs under
//! deterministic simulation.
//!
//! ```
//! use std::num::NonZeroUsize;
//!
//! use rdlt_engine::{Env, RayonPool, SystemEnv};
//!
//! let threads = NonZeroUsize::new(2).expect("2 is non-zero");
//! let env = SystemEnv::new(RayonPool::new(threads)?);
//! assert!(env.now() > std::time::UNIX_EPOCH);
//! # Ok::<(), rdlt_engine::ComputePoolError>(())
//! ```

mod attempt;
#[cfg(feature = "bench")]
#[doc(hidden)]
pub mod bench;
mod budget;
mod compute;
mod config;
mod coordinator;
#[cfg(test)]
mod drawn;
mod env;
mod error;
mod lane;
mod naming;
mod normalize;
mod partition;
mod plan;
mod policy;
mod report;
mod run;
mod scope;
mod shred;
mod table;

pub use compute::{ComputePool, ComputePoolError, Job, RayonPool};
pub use config::{BatchPolicy, CommitPolicy, EngineConfig, EngineConfigBuilder, RetryPolicy};
pub use env::{Env, Sleep, SystemEnv};
pub use error::{Error, ErrorKind, ErrorReport};
pub use plan::{PipelinePlan, StreamPlan, WriteMode};
pub use policy::{Nested, OnUnsupported, SchemaPolicy, SchemaSettings};
pub use report::{AttemptReport, Report, RunStatus, StreamReport};
pub use run::{Engine, RunControl, RunHandle, RunOutcome, StopMode};
