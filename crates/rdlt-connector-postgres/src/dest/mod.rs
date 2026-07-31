//! # PostgreSQL destination
//!
//! Binary-protocol COPY into unlogged staging tables; publication is one transaction
//! moving stage → target, upserting the state document, and recording the commit
//! receipt. Receives FLATTENED schemas — `structs: false` makes the
//! engine lower nested objects at the seam. Depends on the SPI only.
//!
//! Module layout (source-mirroring, all crate-private): `config` the
//! handle/builder, `ddl` type mapping + table DDL, `encode` the binary-COPY
//! wire encoding, `commit` the load-session protocol.
//!
//! # What a unit transaction costs
//!
//! Append and Replace rows COPY straight into their target inside one
//! transaction per commit unit, instead of landing in a stage table and being
//! moved by `INSERT … SELECT` at publish. Every row is written once rather
//! than twice. Merge is unchanged: its arms join delivered rows against the
//! target, so it genuinely needs the stage.
//!
//! That trade has four consequences worth knowing before running rdlt against
//! a busy database. None of them blocks a load; all of them are about what
//! ELSE the database can do while one is running.
//!
//! - **A Replace target is locked for the whole load, not just the publish.**
//!   `TRUNCATE` takes ACCESS EXCLUSIVE and holds it until the unit commits.
//!   Under the old staged publish the target was locked for the publish alone
//!   — on a 1M-row load, roughly 740 ms. It is now locked from the first batch
//!   to the commit. Readers of that table block for that whole window.
//! - **Vacuum falls behind while a unit is open.** The transaction holds its
//!   `xmin` for its lifetime, and that pins the oldest row version the whole
//!   DATABASE may reclaim — not just this table's. A long load therefore
//!   delays cleanup everywhere.
//! - **A stalled load holds both at once.** A load blocked on a slow source
//!   keeps the target's ACCESS EXCLUSIVE lock and the vacuum horizon, having
//!   written nothing recently. Commit cadence is the control: more frequent
//!   commit units mean shorter transactions and shorter locks.
//! - **Constraint violations surface at `write`, not at publish.** The server
//!   enforces the target's constraints during the COPY, so a bad row fails at
//!   the batch that carried it and names the row. Under staging the row landed
//!   in a permissive stage first and failed later, at `INSERT … SELECT`.

mod classify;
mod commit;
mod config;
mod connector;
mod ddl;
mod dialect;
mod encode;
#[cfg(feature = "failpoints")]
mod fail_points;
#[doc(hidden)]
pub mod sqlgen;
#[doc(hidden)]
pub mod testhook;

pub use config::{
    AbsentPolicy, DedupSort, DestinationOptions, MergeStrategy, Postgres, Scd2Options, SortOrder,
    TableOptions,
};

#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub use fail_points::FAIL_POINTS;

pub(crate) use classify::{classify_stmt, describe, fatal, transient};
pub(crate) use dialect::quote;
