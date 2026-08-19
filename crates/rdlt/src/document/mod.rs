//! The pipeline document: the ONE YAML document every consumer of the
//! engine — the `rdlt` CLI, the bench harness, library embedders —
//! parses; its construction into a runnable [`Pipeline`] lives on the
//! noun ([`Pipeline::from_file`], [`Pipeline::from_text`],
//! [`Pipeline::from_document`], [`Pipeline::from_document_with`]).
//!
//! ONE document describes a whole pipeline: pipeline-wide settings, the
//! source, and the destination. EVERY arm names an OUT-OF-PROCESS
//! connector in the ONE form `connector: {id, version?, path?, config}`
//! — a [`connector::Connector`] — and the facade knows no connector by
//! name: every requirement is resolved through a
//! [`crate::runtime::provider::Provider`] at build time into a spawned
//! binary, discovered from the id or named by `path`.
//!
//! The config document inside any arm is OPAQUE here: it crosses the
//! wire in the handshake and the CONNECTOR's own config gate validates
//! it — the facade and CLI never learn connector vocabularies, so a
//! refusal arrives in the connector's own wording, never a facade
//! paraphrase.
//!
//! Here: the document's typed nodes and its parse ([`Document`],
//! [`WriteMode`], [`SchemaPolicy`], [`Resources`], [`Config`],
//! [`parse`], [`read`]) and the requirement
//! every arm names ([`connector`]). Every document problem construction
//! can end in is an [`Error::Config`](crate::error::Error::Config).
//!
//! [`Pipeline`]: crate::pipeline::Pipeline
//! [`Pipeline::from_file`]: crate::pipeline::Pipeline::from_file
//! [`Pipeline::from_text`]: crate::pipeline::Pipeline::from_text
//! [`Pipeline::from_document`]: crate::pipeline::Pipeline::from_document
//! [`Pipeline::from_document_with`]: crate::pipeline::Pipeline::from_document_with

pub mod connector;
mod model;

pub use model::{
    Config, Document, MAX_DOCUMENT_BYTES, Resources, SchemaPolicy, WriteMode, parse, read,
};
