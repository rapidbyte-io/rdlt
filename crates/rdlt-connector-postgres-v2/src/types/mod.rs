//! THE Postgres type rulebook — one closed [`Kind`] vocabulary, dispatched
//! over exhaustively by every face of the crate that touches a value:
//!
//! - [`map`] — catalog type (+ optional hint) → [`Kind`] and its projection
//! - [`binary`] — COPY BINARY wire bytes → values (and the stream decoder)
//! - [`text`] — Postgres TEXT output forms → values (CDC tuples, literals)
//! - [`literal`] — values → injection-safe SQL literals (resume predicates)
//! - [`builder`] — the one Arrow column builder both input faces feed
//!
//! A new kind is therefore a compiler-forced edit in every face: no face
//! carries a `_` arm over [`Kind`], so adding a variant refuses to compile
//! until every conversion says what it does.

pub(crate) mod binary;
pub(crate) mod builder;
#[cfg(feature = "destination")]
pub(crate) mod encode;
pub(crate) mod literal;
pub(crate) mod map;
pub(crate) mod text;
pub(crate) mod vocabulary;

pub(crate) use vocabulary::{Column, Kind, Scalar};
