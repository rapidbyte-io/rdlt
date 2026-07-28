//! # rdlt-connector-snowflake — Snowflake connector
//!
//! Destination-only; the module layout mirrors the connector-family
//! convention (`rdlt-connector-postgres`, `rdlt-connector-duckdb`): the
//! destination lives in [`dest`], and this root stays a thin façade so a
//! future source slots in beside it.
//!
//! ## Where Snowflake differs from the other SQL destinations
//!
//! Three service facts shape the whole design, and each is proven rather than
//! assumed (the feature's research records the probes):
//!
//! - **DDL auto-commits an open transaction.** A schema statement issued
//!   inside a unit silently commits everything before it, which would turn a
//!   partial unit into a published one. The unit is therefore pure DML, and
//!   the executor that runs it refuses DDL rather than trusting callers.
//! - **No enforced unique constraints.** Primary keys are informational, so
//!   key identity is delivered by the merge statement's own dedup rather than
//!   by an arbiter index the database polices.
//! - **Unquoted identifiers fold to upper case**, and a quoted lower-case name
//!   is a *different* object. Every identifier this crate emits is quoted
//!   upper case, which is what an unquoted user query resolves to anyway.

pub mod dest;
