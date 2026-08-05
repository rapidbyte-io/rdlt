//! `serve()`: turning an sdk connector into an out-of-process protocol
//! server (038). Behind the `serve` feature, OFF by default — a
//! connector that never runs out-of-process pays nothing for this
//! module, not even tonic in its dependency tree (`cargo tree -i tonic`
//! against a connector crate at default features stays clean).
//!
//! [`common`] is the plumbing every service shares (the UDS bind, the
//! [`common::ServeError`] taxonomy, the `common::error_frame`
//! builder). [`source`] is the [`crate::source::SourceConnector`] half —
//! [`source::source`] is the entry a spawned connector process actually
//! runs. [`destination`] is the [`crate::destination::DestinationConnector`]
//! half — one bidi stream IS the session; [`destination::destination`]
//! is its entry point.

pub mod common;
pub mod destination;
pub mod source;
