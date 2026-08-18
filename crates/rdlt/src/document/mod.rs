//! The pipeline document: the ONE YAML document every consumer of the
//! engine — the `rdlt` CLI, the bench harness, library embedders —
//! parses, and its construction into a runnable [`Pipeline`].
//!
//! ONE document describes a whole pipeline: pipeline-wide settings, the
//! source, and the destination. EVERY arm names an OUT-OF-PROCESS
//! connector, and carries no connector vocabulary in type: the rich
//! spellings (`postgres:`, `file:`, `duckdb:`, …) are sugar for the
//! explicit `connector:` form, each parsing to the same
//! [`connector::Connector`] through the one role-aware table
//! ([`connector::id`] reads its ids), and every requirement — rich or
//! explicit — is resolved through a [`crate::runtime::provider::Provider`]
//! at build time into a spawned connector binary.
//!
//! The config document inside any arm is OPAQUE here: it crosses the
//! wire in the handshake and the CONNECTOR's own config gate validates
//! it — the facade and CLI never learn connector vocabularies, so a
//! refusal arrives in the connector's own wording, never a facade
//! paraphrase.
//!
//! Here: the document's typed nodes and its parse ([`Document`],
//! [`WriteMode`], [`Config`], [`parse`], [`read`]); the requirement every
//! arm names ([`connector`]); the construction into a [`Pipeline`]
//! ([`build`], [`build_with`]) and the typed refusal it can end in
//! ([`Error`]).
//!
//! [`Pipeline`]: crate::pipeline::Pipeline

mod build;
pub mod connector;
mod model;

pub use build::{Error, build, build_with};
pub use model::{Config, Document, MAX_DOCUMENT_BYTES, WriteMode, parse, read};
