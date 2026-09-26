//! Serving a connector over the wire protocol, for a host that runs it out of process.
//!
//! One connection is one session of the protocol: it opens with a handshake that names the role
//! and carries the configuration, and every later call works on the connector that handshake
//! connected. A binary serves every role it has a factory for.

mod handshake;
mod read;
mod service;
mod write;

use std::sync::Arc;

use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use rdlt_wire::Limits;
use rdlt_wire::v1::connector_server::ConnectorServer;
use tokio::io::{AsyncRead, AsyncWrite};
use tower::ServiceExt as _;

use crate::destination::DestinationFactory;
use crate::source::SourceFactory;

/// The roles a connector binary serves, each by its factory.
#[derive(Default)]
pub struct Served {
    source: Option<Box<dyn SourceFactory>>,
    destination: Option<Box<dyn DestinationFactory>>,
}

impl std::fmt::Debug for Served {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Served")
            .field(
                "source",
                &self.source.as_ref().map(|factory| &factory.spec().id),
            )
            .field(
                "destination",
                &self.destination.as_ref().map(|factory| &factory.spec().id),
            )
            .finish()
    }
}

impl Served {
    /// Nothing served yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Serves the source role with `factory`.
    #[must_use]
    pub fn with_source(mut self, factory: Box<dyn SourceFactory>) -> Self {
        self.source = Some(factory);
        self
    }

    /// Serves the destination role with `factory`.
    #[must_use]
    pub fn with_destination(mut self, factory: Box<dyn DestinationFactory>) -> Self {
        self.destination = Some(factory);
        self
    }
}

/// Serving a connection failed in its transport.
#[derive(Debug, thiserror::Error)]
#[error("serving the connection failed")]
pub struct ServeError(#[source] hyper::Error);

/// Serves the protocol on `io`, enforcing `limits` on what it receives, until the host closes the
/// connection.
///
/// # Errors
///
/// A [`ServeError`] when the connection fails in its transport.
pub async fn serve_connection<IO>(
    served: Arc<Served>,
    io: IO,
    limits: Limits,
) -> Result<(), ServeError>
where
    IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let bytes = limits.message_bytes();
    let service = ConnectorServer::new(service::Service::new(served, limits))
        .max_decoding_message_size(bytes)
        .max_encoding_message_size(bytes);
    let service = service.map_request(|request: http::Request<hyper::body::Incoming>| {
        request.map(rdlt_wire::tonic::body::Body::new)
    });
    hyper::server::conn::http2::Builder::new(TokioExecutor::new())
        .timer(TokioTimer::new())
        .initial_connection_window_size(rdlt_wire::limits::CONNECTION_WINDOW)
        .serve_connection(TokioIo::new(io), TowerToHyperService::new(service))
        .await
        .map_err(ServeError)
}
