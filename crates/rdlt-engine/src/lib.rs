//! The rdlt data-movement engine.
//!
//! An [`Engine`] runs a [`PipelinePlan`] from a source to a destination exactly once: every row
//! the source emits before a committed checkpoint is published exactly once, whatever fails,
//! retries or runs concurrently. Every source of nondeterminism the engine uses (clocks,
//! randomness, CPU scheduling) comes from an [`Env`], so the whole engine runs under
//! deterministic simulation.
//!
//! A process gives its tokio runtime the workers [`Cores::try_from_host`] suggests for the cores
//! it may run on, and the engine's compute pool the rest; an embedder that fixes its own layout
//! passes a [`Cores`] to [`SystemEnv::try_new`] instead.
//!
//! ```
//! use rdlt_engine::{Cores, Env, SystemEnv};
//!
//! let cores = Cores::try_from_host()?;
//! let runtime = tokio::runtime::Builder::new_multi_thread()
//!     .worker_threads(cores.workers().get())
//!     .enable_all()
//!     .build()?;
//! let env = SystemEnv::try_from_runtime(runtime.handle())?;
//! assert_eq!(env.cores(), cores.count());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Building
//!
//! The figures in the repository's `docs/perf` assume its release profile: `lto = "fat"` and
//! `codegen-units = 1`, without which the shredder runs measurably slower (`docs/perf/shred.md`
//! records how much). Cargo applies only the root package's profiles, so an embedder sets the
//! same in its own `[profile.release]`, and keeps `panic = "unwind"`: the engine contains a
//! panicking job by unwinding, and does not build without it.

#![forbid(unsafe_code)]

#[cfg(panic = "abort")]
compile_error!(
    "rdlt-engine fails a run whose task or job panics by unwinding: build with panic = \"unwind\""
);

mod attempt;
#[cfg(feature = "bench")]
#[doc(hidden)]
pub mod bench;
mod budget;
mod compute;
mod config;
#[cfg(any(test, feature = "conformance"))]
pub mod conformance;
mod coordinator;
mod cost;
mod crash;
mod deadline;
mod env;
mod error;
#[cfg(any(test, feature = "bench"))]
mod fixtures;
mod json;
mod lane;
mod limits;
mod named;
mod naming;
mod normalize;
mod partition;
mod plan;
mod policy;
mod report;
mod run;
mod scope;
mod shred;
mod stored;
mod table;
mod wal;
mod watch;

pub use compute::{ComputePool, ComputePoolError, Cores, Job, RayonPool};
pub use config::{
    BatchPolicy, CommitPolicy, EngineConfig, EngineConfigBuilder, GrowthLimits, RetryPolicy,
};
pub use env::{Clock, Env, Sleep, SystemClock, SystemEnv};
pub use error::{Error, ErrorKind, ErrorReport};
pub use plan::{DeleteMode, OnTruncate, PipelinePlan, RetentionLoss, StreamPlan, Until, WriteMode};
pub use policy::{Nested, OnUnsupported, SchemaPolicy, SchemaSettings};
pub use report::{
    AttemptReport, CommitPhases, Commits, Counters, Forgotten, LaneCounters, LogCounters,
    PoolCounters, REPORTED_ATTEMPTS, REPORTED_FORGOTTEN, Report, RunStatus, ShredCounts,
    StoreRequests, StreamReport, Waited, Waits,
};
pub use run::{Engine, ResetReport, ResetScope, RunControl, RunHandle, RunOutcome, StopMode};
pub use wal::{Chunk, LocalWal, StagedChunk, WalStore};
#[cfg(feature = "object-store")]
pub use wal::{ObjectStoreOptions, ObjectStoreWal, StoreRefusal, WalObjects};
