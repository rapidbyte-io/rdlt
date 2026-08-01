//! # rdlt-connector-sdk — connector authoring scaffolding
//!
//! The OPTIONAL layer of the connector SDK: what two or more connectors
//! were measured to implement identically, extracted once. The protocol
//! lives in `rdlt-connector`; the conformance suites live in
//! `rdlt-testkit`; a connector may use this crate or ignore it — hosts
//! never depend on it.
//!
//! One seam ships today: [`config::Document`], the validated
//! config-document contract (every text entry point parses THEN validates,
//! by construction), with [`config::schema_of`] beside it behind the
//! `schema` feature. The seam deliberately renders NO text — error types
//! and every message stay in the connector, which is what lets frozen,
//! connector-specific spellings survive adoption byte-identically.
//!
//! This crate has no rdlt dependencies at all (pure serde), so it is
//! consumable by an out-of-tree connector exactly as by an in-tree one.

// Warn, not deny: an undocumented public item is a gap to fill, not a
// reason to fail a contributor's build. `make docs` is where the
// published surface is held to -D warnings.
#![warn(missing_docs)]

pub mod config;

pub use config::Document;
