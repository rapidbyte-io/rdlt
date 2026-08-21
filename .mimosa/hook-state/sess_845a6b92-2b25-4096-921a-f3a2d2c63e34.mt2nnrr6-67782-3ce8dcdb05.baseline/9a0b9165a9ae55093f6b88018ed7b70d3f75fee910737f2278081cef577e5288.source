//! Glob-import target for pipeline authors: the vocabulary a pipeline
//! definition touches, without reaching for the nouns.
//!
//! Outcome types stay behind their nouns (`report::Run`,
//! `metrics::Snapshot`) because bare `Run`/`Table`/`Snapshot` in a glob
//! would collide with an author's own names — and so does `Error`: an
//! author's crate usually has one, so it is spelled `rdlt::error::Error`.
//!
//! ```compile_fail
//! use rdlt::prelude::*;
//! fn f(_: Error) {} // the prelude does NOT export `Error`
//! ```

pub use crate::{
    commit::{BatchPolicy, CommitPolicy, WriteMode},
    cursor::Cursor,
    event::{PartCloseReason, PipelineEvent},
    id::{StreamName, TableName},
    metrics::Metrics,
    pipeline::Pipeline,
    policy::{PolicyAction, SchemaPolicy},
    report::ResumedFrom,
};
