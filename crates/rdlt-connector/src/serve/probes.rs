//! Certification's probes, as a connection serves them: a destination reading back what it
//! published, and a source telling where it stands.
//!
//! They are served only with the `certify` feature, by a binary that serves a factory which
//! answers them, to a host whose handshake offered them. Without the feature every connection
//! refuses them as unsupported.

use std::sync::Arc;

use rdlt_wire::Limits;
use rdlt_wire::tonic::Status;

use super::handshake::unsupported;
use super::service::Answer;
use crate::destination::{Destination, DestinationFactory, PUBLISHED_CODE};
use crate::error::ConnectorError;
use crate::source::{ACKNOWLEDGED_CODE, Source, SourceFactory};
use crate::spec::ConnectContext;
use crate::wire::{status, v1};

/// The status of a read-back the connection does not serve.
fn no_read_back() -> Status {
    let message = "reading back what was published needs its feature in the handshake";
    status(&unsupported(message, PUBLISHED_CODE))
}

/// The status of a question where the source stands that the connection does not answer.
fn no_standing() -> Status {
    let message = "telling where the source stands needs its feature in the handshake";
    status(&unsupported(message, ACKNOWLEDGED_CODE))
}

/// The probes a connection serves, once its configuration connected them.
#[cfg(feature = "certify")]
#[derive(Default)]
pub(super) struct Probes {
    reader: tokio::sync::OnceCell<Arc<dyn crate::destination::PublishedReader>>,
    acknowledger: tokio::sync::OnceCell<Arc<dyn crate::source::AcknowledgedReader>>,
}

#[cfg(feature = "certify")]
impl Probes {
    /// Whether `request` offers the source's probe to a `factory` that answers it.
    pub(super) fn of_source(request: &v1::HandshakeRequest, factory: &dyn SourceFactory) -> bool {
        offers(request, rdlt_wire::ACKNOWLEDGED) && factory.acknowledges()
    }

    /// Whether `request` offers the destination's probe to a `factory` that answers it.
    pub(super) fn of_destination(
        request: &v1::HandshakeRequest,
        factory: &dyn DestinationFactory,
    ) -> bool {
        offers(request, rdlt_wire::PUBLISHED) && factory.reads_back()
    }

    /// Connects `factory`'s source with `config`, telling where it stands when the handshake
    /// `accepted` that.
    pub(super) async fn connect_source(
        &self,
        factory: &dyn SourceFactory,
        accepted: bool,
        config: serde_json::Value,
    ) -> Result<Arc<dyn Source>, ConnectorError> {
        let context = ConnectContext::new();
        if !accepted {
            return Ok(Arc::from(factory.connect(config, context).await?));
        }
        let (source, acknowledger) = factory.connect_acknowledging(config, context).await?;
        // Set once: a configuration that ran beside this one is refused once connected.
        self.acknowledger.set(acknowledger).ok();
        Ok(source)
    }

    /// Connects `factory`'s destination with `config`, reading back what it published when the
    /// handshake `accepted` that.
    pub(super) async fn connect_destination(
        &self,
        factory: &dyn DestinationFactory,
        accepted: bool,
        config: serde_json::Value,
    ) -> Result<Arc<dyn Destination>, ConnectorError> {
        let context = ConnectContext::new();
        if !accepted {
            return Ok(Arc::from(factory.connect(config, context).await?));
        }
        let (destination, reader) = factory.connect_reading(config, context).await?;
        // Set once: a configuration that ran beside this one is refused once connected.
        self.reader.set(reader).ok();
        Ok(destination)
    }

    /// The rows of `request`'s table, read back as frames within `host`'s limits.
    pub(super) fn read_published(
        &self,
        request: v1::ReadPublishedRequest,
        host: Limits,
    ) -> Result<Answer<v1::ReadFrame>, Status> {
        use crate::wire::Invalid;
        let reader = self.reader.get().cloned().ok_or_else(no_read_back)?;
        let table = request
            .table
            .ok_or(Invalid::Missing("table"))
            .and_then(crate::destination::TableRef::try_from)
            .map_err(|error| super::service::invalid(&error))?;
        Ok(super::published::serve(reader, table, host))
    }

    /// Where the source stands for `request`'s partition.
    pub(super) async fn read_acknowledged(
        &self,
        request: v1::ReadAcknowledgedRequest,
    ) -> Result<v1::ReadAcknowledgedResponse, Status> {
        use super::service::invalid;
        use crate::id::{PartitionId, StreamName};
        use crate::wire::Invalid;
        let acknowledger = self.acknowledger.get().cloned().ok_or_else(no_standing)?;
        let stream = request
            .stream
            .ok_or(Invalid::Missing("stream"))
            .and_then(StreamName::try_from)
            .map_err(|error| invalid(&error))?;
        let partition = PartitionId::parse(request.partition)
            .map_err(|error| invalid(&Invalid::rejected("partition id", error)))?;
        let cursor = acknowledger
            .acknowledged(&stream, &partition)
            .await
            .map_err(|error| status(&error))?;
        Ok(v1::ReadAcknowledgedResponse {
            cursor: cursor.map(|cursor| v1::Cursor::from(&cursor)),
        })
    }
}

/// Whether `request` offers `feature`.
#[cfg(feature = "certify")]
fn offers(request: &v1::HandshakeRequest, feature: &str) -> bool {
    request.features.iter().any(|offered| offered == feature)
}

/// The probes of a build without them: none.
#[cfg(not(feature = "certify"))]
#[derive(Default)]
pub(super) struct Probes;

#[cfg(not(feature = "certify"))]
#[expect(
    clippy::unused_self,
    clippy::unused_async,
    clippy::unnecessary_wraps,
    reason = "the shape of the probes a build with them serves"
)]
impl Probes {
    pub(super) fn of_source(_: &v1::HandshakeRequest, _: &dyn SourceFactory) -> bool {
        false
    }

    pub(super) fn of_destination(_: &v1::HandshakeRequest, _: &dyn DestinationFactory) -> bool {
        false
    }

    pub(super) async fn connect_source(
        &self,
        factory: &dyn SourceFactory,
        _accepted: bool,
        config: serde_json::Value,
    ) -> Result<Arc<dyn Source>, ConnectorError> {
        Ok(Arc::from(
            factory.connect(config, ConnectContext::new()).await?,
        ))
    }

    pub(super) async fn connect_destination(
        &self,
        factory: &dyn DestinationFactory,
        _accepted: bool,
        config: serde_json::Value,
    ) -> Result<Arc<dyn Destination>, ConnectorError> {
        Ok(Arc::from(
            factory.connect(config, ConnectContext::new()).await?,
        ))
    }

    pub(super) fn read_published(
        &self,
        _: v1::ReadPublishedRequest,
        _: Limits,
    ) -> Result<Answer<v1::ReadFrame>, Status> {
        Err(no_read_back())
    }

    pub(super) async fn read_acknowledged(
        &self,
        _: v1::ReadAcknowledgedRequest,
    ) -> Result<v1::ReadAcknowledgedResponse, Status> {
        Err(no_standing())
    }
}
