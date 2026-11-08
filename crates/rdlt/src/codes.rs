//! The facade's codes: what its errors carry beside the engine's and the host's.
//!
//! An embedder reads a failure's code from its [`Error`](crate::Error) or its report, and
//! compares it with these; it makes none.

use rdlt_engine::Code;

/// The document is larger than [`DOCUMENT_BYTES`](crate::limits::DOCUMENT_BYTES).
pub const DOCUMENT_TOO_LARGE: Code = Code::new("document_too_large");
/// The document nests deeper, or holds more nodes, than its limits.
pub const DOCUMENT_TOO_DEEP: Code = Code::new("document_too_deep");
/// The document holds an anchor, an alias, a tag, a merge key or a second document.
pub const DOCUMENT_REFUSED: Code = Code::new("document_refused");
/// The document is not YAML or JSON.
pub const DOCUMENT_SYNTAX: Code = Code::new("document_syntax");
/// The document lacks a key its place needs.
pub const DOCUMENT_MISSING_KEY: Code = Code::new("document_missing_key");
/// A key is given twice in one mapping.
pub const DOCUMENT_DUPLICATE_KEY: Code = Code::new("document_duplicate_key");
/// The document's file cannot be read.
pub const DOCUMENT_UNREADABLE: Code = Code::new("document_unreadable");
/// A value of the document is of the wrong kind, or names no choice.
pub const DOCUMENT_INVALID: Code = Code::new("document_invalid");
/// A key of the document is not one its place takes.
pub const DOCUMENT_UNKNOWN_KEY: Code = Code::new("document_unknown_key");
/// The document's `version` is not 1.
pub const DOCUMENT_VERSION: Code = Code::new("document_version");
/// A connector's id, version requirement or digest, or the pipeline's name, is not one.
pub const REFERENCE_INVALID: Code = Code::new("reference_invalid");
/// A stream's name is not one.
pub const STREAM_INVALID: Code = Code::new("stream_invalid");
/// A stream pattern matches no stream of the source's catalog.
pub const PATTERN_UNMATCHED: Code = Code::new("pattern_unmatched");
/// A `code` transform names nothing the host registered.
pub const TRANSFORM_UNKNOWN: Code = Code::new("transform_unknown");
/// A host file sets something that needs a feature this build of the facade lacks.
pub const HOST_SETTING_UNAVAILABLE: Code = Code::new("host_setting_unavailable");
/// A transform needs a feature this build of the facade lacks.
pub const TRANSFORM_UNAVAILABLE: Code = Code::new("transform_unavailable");
/// An `hmac_sha256` mask has no key.
pub const MASK_KEY_MISSING: Code = Code::new("mask_key_missing");
/// An `hmac_sha256` mask's key is written out, not a secret reference.
pub const MASK_KEY_LITERAL: Code = Code::new("mask_key_literal");
/// A TLS file the host names cannot be read.
pub const TLS_UNREADABLE: Code = Code::new("tls_unreadable");
/// Export of traces and metrics could not be installed.
pub const TELEMETRY_UNAVAILABLE: Code = Code::new("telemetry_unavailable");
/// The compute pool could not start, or the host's cores are unknown.
pub const ENVIRONMENT_UNAVAILABLE: Code = Code::new("environment_unavailable");
/// A source or destination failed a call the facade made of it.
pub const CONNECTOR_FAILED: Code = Code::new("connector_failed");
