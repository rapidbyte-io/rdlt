//! The trace context the host sends in each call's metadata, from the source its embedder
//! installs once: the facade's OpenTelemetry export installs one; without it nothing is sent.

use std::sync::OnceLock;

use rdlt_wire::trace::TRACEPARENT;
pub use rdlt_wire::trace::TraceParent;

static SOURCE: OnceLock<fn() -> Option<TraceParent>> = OnceLock::new();

/// Sends, in each call's metadata, the context `source` answers in the calling task; once a
/// process: answers whether this call installed it.
pub fn propagate(source: fn() -> Option<TraceParent>) -> bool {
    SOURCE.set(source).is_ok()
}

/// The context the host sends from the calling task now, where a source is installed.
pub fn current() -> Option<TraceParent> {
    SOURCE.get().and_then(|source| source())
}

/// Puts the calling task's context, where there is one, in `headers`.
pub(crate) fn stamp(headers: &mut http::HeaderMap) {
    if let Some(parent) = current()
        && let Ok(value) = http::HeaderValue::from_str(&parent.to_string())
    {
        headers.insert(TRACEPARENT, value);
    }
}
