//! The served connector's calls, each request held to its class's bounds before it is decoded.
//!
//! A decoder holds what a request's fields become, many times what they took on the wire,
//! before any check of what they say; it also reserves the length a request's prefix declares
//! before its bytes arrive. Each request is held whole before it is decoded, within its class's
//! bound on the wire and, counted by its scan, on what it decodes to; what is still arriving on
//! a connection, its data streams' frames among it, is held within a window the connection
//! shares. A `Data` call's stream is served as a passed one is, every other call by the
//! generated service.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use rdlt_wire::Limits;
use rdlt_wire::bounded::Window;
use rdlt_wire::route::Router;
use rdlt_wire::v1::connector_server::ConnectorServer;

use super::service::Service;

/// Frames of the largest message the requests still arriving on one connection may hold
/// together.
const WINDOW_FRAMES: usize = 4;

/// The connector's calls on one connection.
pub(super) type Classed = Router<ConnectorServer<Service>, Service>;

/// The window of what is still arriving on a connection whose requests are held to `limits`.
pub(super) fn window(limits: &Limits) -> Window {
    Window::new(limits.largest().saturating_mul(WINDOW_FRAMES))
}

/// `service`'s calls, each request held to its connection's limits for its class and to its
/// window, and each answer within the protocol's largest message.
pub(super) fn classed(service: Arc<Service>) -> Classed {
    // Each request is held to its class's bound before tonic sees it: tonic takes the largest of
    // any class, its operator's state limit among them.
    let (limits, window) = (service.limits, service.window.clone());
    let bytes = limits.largest();
    let server = ConnectorServer::from_arc(Arc::clone(&service))
        .max_decoding_message_size(bytes)
        .max_encoding_message_size(bytes);
    Router::new(server, service, &limits, window)
}
