//! The served connector's calls, each request decoded within its class's limit.
//!
//! A decoder holds what a request's fields become, many times what they took on the wire,
//! before any check of what they say; it also reserves the length a request's prefix declares
//! before its bytes arrive. Each request is held whole before it is decoded, within its class's
//! bound on the wire and, counted by its scan, on what it decodes to; what is still arriving on
//! a connection is held within a window the connection shares.

#[cfg(test)]
mod tests;

use std::convert::Infallible;
use std::task::{Context, Poll};

use rdlt_wire::Limits;
use rdlt_wire::bounded::{Bounded, Bounds, Window};
use rdlt_wire::limits::Class;
use rdlt_wire::tonic::body::Body;
use rdlt_wire::v1::connector_server::ConnectorServer;

use super::service::Service;

/// Frames of the largest message the requests still arriving on one connection may hold
/// together.
pub(crate) const WINDOW_FRAMES: usize = 4;

/// The connector's service, each request held to its bounds before it is decoded.
#[derive(Clone)]
pub(super) struct Classed {
    server: ConnectorServer<Service>,
    limits: Limits,
    window: Window,
}

impl Classed {
    /// `service`, each request held to `limits` for its class, and each answer within the
    /// protocol's largest message.
    pub(super) fn new(service: Service, limits: &Limits) -> Self {
        // Each request is held to its class's bound before tonic sees it: tonic takes the
        // largest of any class, its operator's state limit among them.
        let bytes = limits.largest();
        Self {
            server: ConnectorServer::new(service)
                .max_decoding_message_size(bytes)
                .max_encoding_message_size(bytes),
            limits: *limits,
            window: Window::new(bytes.saturating_mul(WINDOW_FRAMES)),
        }
    }
}

/// The method a request of `path` calls.
fn method(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

impl tower::Service<http::Request<Body>> for Classed {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = <ConnectorServer<Service> as tower::Service<http::Request<Body>>>::Future;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // The server is always ready.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let method = method(request.uri().path());
        let class = Class::of_request(method);
        let bounds = Bounds::of(&self.limits, class, rdlt_wire::scan::request(method));
        let window = Some(self.window.clone());
        let request = request.map(|body| Body::new(Bounded::new(body, bounds, window)));
        tower::Service::call(&mut self.server, request)
    }
}
