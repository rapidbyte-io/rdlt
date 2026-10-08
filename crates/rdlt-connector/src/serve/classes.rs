//! The served connector's calls, each request held to its class's bounds before it is decoded.
//!
//! A decoder holds what a request's fields become, many times what they took on the wire,
//! before any check of what they say; it also reserves the length a request's prefix declares
//! before its bytes arrive. Each request is held whole before it is decoded, within its class's
//! bound on the wire and, counted by its scan, on what it decodes to; what is still arriving on
//! a connection is held within a window the connection shares. The data plane's calls are then
//! served by hand, every other by the generated service.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use rdlt_wire::Limits;
use rdlt_wire::bounded::Window;
use rdlt_wire::plane::Router;
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

/// `service`'s calls, each request held to `limits` for its class and to `window`, and each
/// answer within the protocol's largest message.
pub(super) fn classed(service: Service, limits: &Limits, window: Window) -> Classed {
    // Each request is held to its class's bound before tonic sees it: tonic takes the largest of
    // any class, its operator's state limit among them.
    let bytes = limits.largest();
    let service = Arc::new(service);
    let server = ConnectorServer::from_arc(Arc::clone(&service))
        .max_decoding_message_size(bytes)
        .max_encoding_message_size(bytes);
    Router::new(server, service, limits, window)
}
