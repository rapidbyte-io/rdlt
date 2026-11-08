//! `serve` continuing its host's trace: each call is answered in a span that names the host's
//! trace and calling span, read only from a well-formed `traceparent`.
//!
//! The span ends with the answer's head; a stream's messages after it are not in it.

#[cfg(test)]
mod tests;

use std::fmt::Write as _;
use std::task::{Context, Poll};

use rdlt_wire::trace::{TRACEPARENT, TraceParent};
use tracing::Instrument as _;
use tracing::instrument::Instrumented;

/// The served calls of `S`, each in a span of its own.
#[derive(Clone)]
pub(super) struct Traced<S>(pub(super) S);

impl<S, B> tower::Service<http::Request<B>> for Traced<S>
where
    S: tower::Service<http::Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Instrumented<S::Future>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.0.poll_ready(context)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let span = tracing::info_span!(
            "rdlt.serve",
            rpc = method(request.uri().path()),
            trace_id = tracing::field::Empty,
            parent_id = tracing::field::Empty,
        );
        let parent = request
            .headers()
            .get(TRACEPARENT)
            .and_then(|value| TraceParent::parse(value.as_bytes()));
        if let Some(parent) = parent
            && !span.is_disabled()
        {
            span.record("trace_id", hex(&parent.trace_id()));
            span.record("parent_id", hex(&parent.parent_id()));
        }
        self.0.call(request).instrument(span)
    }
}

/// `bytes` as lower-case hex.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut text, byte| {
        write!(text, "{byte:02x}").ok();
        text
    })
}

/// The path of a call as the span shows it: the peer's text only where it is short and plain.
fn method(path: &str) -> &str {
    if path.len() <= 128 && path.bytes().all(|byte| byte.is_ascii_graphic()) {
        path
    } else {
        "other"
    }
}
