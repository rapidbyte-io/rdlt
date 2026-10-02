//! The served connector's calls, each request decoded within its class's limit.
//!
//! A decoder holds what a request's fields become, many times what they took on the wire,
//! before any check of what they say; it also reserves the length a request's prefix declares
//! before its bytes arrive. One server a class of request, each decoding no more than its class
//! may hold, bounds both by what a call carries rather than by the largest frame.

#[cfg(test)]
mod tests;

use std::convert::Infallible;
use std::sync::Arc;
use std::task::{Context, Poll};

use rdlt_wire::Limits;
use rdlt_wire::limits::Class;
use rdlt_wire::tonic::body::Body;
use rdlt_wire::v1::connector_server::ConnectorServer;

use super::service::Service;

/// The connector's service, served by one server a class of request.
#[derive(Clone)]
pub(super) struct Classed {
    servers: Arc<[ConnectorServer<Service>; 8]>,
}

/// Each class, in the order [`Classed`] holds its servers.
const CLASSES: [Class; 8] = [
    Class::Handshake,
    Class::Control,
    Class::Catalog,
    Class::State,
    Class::Config,
    Class::Schema,
    Class::Cursor,
    Class::Data,
];

impl Classed {
    /// `service`, each request decoded within `limits` for its class, and each answer within
    /// the protocol's largest message.
    pub(super) fn new(service: Service, limits: &Limits) -> Self {
        let service = Arc::new(service);
        let servers = CLASSES.map(|class| {
            ConnectorServer::from_arc(Arc::clone(&service))
                .max_decoding_message_size(limits.decoding(class))
                .max_encoding_message_size(limits.message_bytes())
        });
        Self {
            servers: Arc::new(servers),
        }
    }
}

/// The class of a request to `path`, a call of the protocol's service.
fn class(path: &str) -> Class {
    match path.rsplit('/').next() {
        Some("Handshake") => Class::Handshake,
        Some("Configure") => Class::Config,
        Some("ApplySchema") => Class::Schema,
        Some("Read") => Class::Cursor,
        Some("Plan" | "Committed" | "Commit") => Class::State,
        Some("Write") => Class::Data,
        _ => Class::Control,
    }
}

impl tower::Service<http::Request<Body>> for Classed {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = <ConnectorServer<Service> as tower::Service<http::Request<Body>>>::Future;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Each server is always ready.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let class = class(request.uri().path());
        // Every class is among them.
        let index = CLASSES.iter().position(|each| *each == class).unwrap_or(0);
        let mut server = self.servers[index].clone();
        tower::Service::call(&mut server, request)
    }
}
