//! The in-memory connector pair: certified by this crate's own suites,
//! and the "runs anywhere" source and destination an embedder wires a
//! pipeline through in examples and tests.
//!
//! - [`Source`] (with [`Stream`] and [`Batch`]) — scripted streams of
//!   JSON rows with checkpoints.
//! - [`Destination`] — a warehouse under one mutex, with read-back
//!   oracles ([`Row`] is its row shape).

mod destination;
mod source;

pub use destination::{Destination, Row};
pub use source::{Batch, Source, Stream};
