//! The rdlt data-movement engine.
//!
//! Every source of nondeterminism the engine uses (clocks, randomness, CPU scheduling) comes
//! from an [`Env`], so the whole engine can run under deterministic simulation.
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

mod compute;
mod env;

pub use compute::{ComputePool, ComputePoolError, Job, RayonPool};
pub use env::{Env, Sleep, SystemEnv};
