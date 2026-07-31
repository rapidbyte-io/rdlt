//! Declarative Postgres source configuration: connection, stream selection,
//! per-table cursor + key configuration, batching knobs. One YAML document a
//! platform can render and validate; unknown fields are errors.
//!
//! There is deliberately NO retry configuration — retry policy is engine-owned;
//! this source only classifies errors.

mod validate;
mod vocabulary;

pub use vocabulary::*;
