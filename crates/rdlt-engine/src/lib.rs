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

#![cfg_attr(
    not(test),
    expect(dead_code, reason = "the partition pipeline uses these from Task 7 on")
)]

mod attempt;
mod budget;
mod compute;
mod config;
mod coordinator;
mod env;
mod error;
mod lane;
mod naming;
mod partition;
mod plan;
mod report;
mod run;
mod scope;

pub use compute::{ComputePool, ComputePoolError, Job, RayonPool};
pub use config::{CommitPolicy, EngineConfig, EngineConfigBuilder, RetryPolicy};
pub use env::{Env, Sleep, SystemEnv};
pub use error::{Error, ErrorKind, ErrorReport};
pub use plan::{PipelinePlan, StreamPlan, WriteMode};
pub use report::{AttemptReport, Report, RunStatus, StreamReport};
pub use run::{Engine, RunControl, RunHandle, RunOutcome, StopMode};
