//! Run policies: what the engine does when a stream's schema changes,
//! and how it retries transient failures.

pub use rdlt_engine::config::{Jitter, RetryPolicy};
pub use rdlt_engine::policy::{PolicyAction, SchemaPolicy};
