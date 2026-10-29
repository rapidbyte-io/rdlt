//! Certification's probes, as a connection serves them: a destination reading back what it
//! published, and a source telling where it stands.
//!
//! They are served only with the `certify` feature, by a binary that serves a factory which
//! answers them, to a host whose handshake offered them. Without the feature every connection
//! refuses them as unsupported.

#[cfg(all(test, not(feature = "certify")))]
mod tests;

use std::sync::Arc;

use rdlt_wire::tonic::Status;

use super::handshake::unsupported;
use crate::destination::{
    Destination, DestinationFactory, PUBLISHED_CODE, PublishedReader, TableRef,
};
use crate::error::ConnectorError;
use crate::source::{ACKNOWLEDGED_CODE, Source, SourceFactory};
use crate::spec::ConnectContext;
use crate::wire::{status, v1};

/// The refusal of a read-back the connection does not serve.
fn no_read_back() -> ConnectorError {
    let message = "reading back what was published needs its feature in the handshake";
    unsupported(message, PUBLISHED_CODE)
}

/// The status of a question where the source stands that the connection does not answer.
fn no_standing() -> Status {
    let message = "telling where the source stands needs its feature in the handshake";
    status(&unsupported(message, ACKNOWLEDGED_CODE))
}

/// The probes a connection serves, once its configuration connected them: none in a build
/// without them.
#[derive(Default)]
pub(super) struct Probes {
    #[cfg(feature = "certify")]
    reader: Option<Arc<dyn PublishedReader>>,
    #[cfg(feature = "certify")]
    acknowledger: Option<Arc<dyn crate::source::AcknowledgedReader>>,
}

/// The probes of a connection not yet configured: none.
pub(super) static NONE: Probes = Probes {
    #[cfg(feature = "certify")]
    reader: None,
    #[cfg(feature = "certify")]
    acknowledger: None,
};

/// Whether this build serves the probes.
const PROBED: bool = cfg!(feature = "certify");

/// Whether `request` offers `feature`.
fn offers(request: &v1::HandshakeRequest, feature: &str) -> bool {
    request.features.iter().any(|offered| offered == feature)
}

impl Probes {
    /// Whether `request` offers the source's probe to a `factory` that answers it.
    pub(super) fn of_source(request: &v1::HandshakeRequest, factory: &dyn SourceFactory) -> bool {
        PROBED && offers(request, rdlt_wire::ACKNOWLEDGED) && factory.acknowledges()
    }

    /// Whether `request` offers the destination's probe to a `factory` that answers it.
    pub(super) fn of_destination(
        request: &v1::HandshakeRequest,
        factory: &dyn DestinationFactory,
    ) -> bool {
        PROBED && offers(request, rdlt_wire::PUBLISHED) && factory.reads_back()
    }

    /// Connects `factory`'s source with `config`, and the probes that tell where it stands when
    /// the handshake `accepted` that.
    #[cfg_attr(
        not(feature = "certify"),
        expect(unused_variables, reason = "a build without the probes serves none")
    )]
    pub(super) async fn connect_source(
        factory: &dyn SourceFactory,
        accepted: bool,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> Result<(Arc<dyn Source>, Self), ConnectorError> {
        #[cfg(feature = "certify")]
        if accepted {
            let (source, acknowledger) = factory.connect_acknowledging(config, context).await?;
            let probes = Self {
                acknowledger: Some(acknowledger),
                ..Self::default()
            };
            return Ok((source, probes));
        }
        let source = Arc::from(factory.connect(config, context).await?);
        Ok((source, Self::default()))
    }

    /// Connects `factory`'s destination with `config`, and the probes that read back what it
    /// published when the handshake `accepted` that.
    #[cfg_attr(
        not(feature = "certify"),
        expect(unused_variables, reason = "a build without the probes serves none")
    )]
    pub(super) async fn connect_destination(
        factory: &dyn DestinationFactory,
        accepted: bool,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> Result<(Arc<dyn Destination>, Self), ConnectorError> {
        #[cfg(feature = "certify")]
        if accepted {
            let (destination, reader) = factory.connect_reading(config, context).await?;
            let probes = Self {
                reader: Some(reader),
                ..Self::default()
            };
            return Ok((destination, probes));
        }
        let destination = Arc::from(factory.connect(config, context).await?);
        Ok((destination, Self::default()))
    }

    /// What reads back `request`'s table, and the table.
    ///
    /// # Errors
    ///
    /// An unsupported error where the connection serves no read-back, and an
    /// `invalid_message` error where the request names no table.
    #[cfg_attr(
        not(feature = "certify"),
        expect(
            unused_variables,
            clippy::unused_self,
            clippy::needless_pass_by_value,
            reason = "a build without the probes serves none"
        )
    )]
    pub(super) fn read_published(
        &self,
        request: v1::ReadPublishedRequest,
    ) -> Result<(Arc<dyn PublishedReader>, TableRef), ConnectorError> {
        #[cfg(feature = "certify")]
        if let Some(reader) = self.reader.clone() {
            use crate::wire::Invalid;
            let table = request
                .table
                .ok_or(Invalid::Missing("table"))
                .and_then(TableRef::try_from)
                .map_err(|error| super::service::invalid(&error))?;
            return Ok((reader, table));
        }
        Err(no_read_back())
    }

    /// Where the source stands for `request`'s partition.
    #[cfg_attr(
        not(feature = "certify"),
        expect(
            unused_variables,
            clippy::unused_async,
            reason = "a build without the probes serves none"
        )
    )]
    pub(super) async fn read_acknowledged(
        &self,
        request: v1::ReadAcknowledgedRequest,
    ) -> Result<v1::ReadAcknowledgedResponse, Status> {
        #[cfg(feature = "certify")]
        if let Some(acknowledger) = self.acknowledger.clone() {
            use super::service::invalid;
            use crate::id::{PartitionId, StreamName};
            use crate::wire::Invalid;
            let stream = request
                .stream
                .ok_or(Invalid::Missing("stream"))
                .and_then(StreamName::try_from)
                .map_err(|error| status(&invalid(&error)))?;
            let partition = PartitionId::parse(request.partition)
                .map_err(|error| status(&invalid(&Invalid::rejected("partition id", error))))?;
            let cursor = acknowledger
                .acknowledged(&stream, &partition)
                .await
                .map_err(|error| status(&error))?;
            return Ok(v1::ReadAcknowledgedResponse {
                cursor: cursor.map(|cursor| v1::Cursor::from(&cursor)),
            });
        }
        Err(no_standing())
    }
}
