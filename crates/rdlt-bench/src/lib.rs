//! # rdlt-bench — the declarative benchmark harness
//!
//! The harness measures the product against its competitors from
//! declarative cells and records the result. Cells, bars, fixtures and
//! competitor variants are DATA under `benches/`; this crate is the one
//! protocol that runs them, judges the bars, and splices the record. It is
//! a development harness and ships nothing a pipeline runs.
//!
//! The modules are the map: [`cell`], [`bar`], [`fixture`] and
//! [`competitor`] load the registry (each refusing at load with the
//! offender named); [`measure`] is the protocol — quiet guard, warmups, N
//! counted runs, medians; [`product`] the rdlt arm, [`competitor`] the
//! baseline arms, [`sample`] the process-tree resource sampler and
//! [`template`] the `{{key}}` substitution they share; [`artifact`] the
//! frozen per-cell record, [`history`] the append-only feed, [`report`]
//! the RESULTS.md tables; [`matrix`] runs the four commands over the
//! loaded registry, [`paths`] anchors everything to the repo, [`error`]
//! is the one error type. Every name is reached by its module path.

pub mod artifact;
pub mod bar;
pub mod cell;
pub mod competitor;
pub mod error;
pub mod fixture;
pub mod history;
pub mod matrix;
pub mod measure;
pub mod paths;
pub mod product;
pub mod report;
pub mod sample;
pub mod template;
