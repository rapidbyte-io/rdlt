//! The protocol's calls, served for one connection: the handshake agrees a role, the
//! configuration connects the connector for it, and every later call works on it.

#[cfg(test)]
mod tests;

use std::pin::Pin;
use std::sync::Arc;

use rdlt_wire::Limits;
use rdlt_wire::bounded::Window;
use rdlt_wire::route::Streams;
use rdlt_wire::stream::Piped;
use rdlt_wire::tonic::body::Body;
use rdlt_wire::tonic::codegen::tokio_stream::Stream;
use rdlt_wire::tonic::{self, Request, Response, Status, Streaming};
use rdlt_wire::v1::connector_server::Connector;
use tokio::sync::OnceCell;
use tokio_stream::StreamExt as _;
use tokio_util::sync::CancellationToken;

use super::handshake::{Agreed, no_handshake, not_configured, unsupported};
use super::probes::Probes;
use super::sessions::{SessionSlot, Sessions, closed};
use super::until::Until;
use super::{Served, stream};
use crate::destination::{Destination, OpenContext, TableChange, TableRef};
use crate::error::{ConnectorError, ConnectorErrorKind};
use crate::id::{PartitionId, PipelineId, StreamName};
use crate::source::Source;
use crate::state::StreamState;
use crate::wire::{INVALID_MESSAGE, Invalid, status, v1};
use crate::{CommitMeta, Cursor};

/// The connector a handshake connected, for its role.
pub(super) enum Connected {
    Source(Arc<dyn Source>),
    Destination(Arc<dyn Destination>),
}

/// What a configuration connected: the connector for its role, and the probes it serves, set
/// together.
pub(super) struct Configured {
    pub(super) connected: Connected,
    pub(super) probes: Probes,
}

/// The protocol served on one connection.
pub(super) struct Service {
    pub(super) served: Arc<Served>,
    pub(super) limits: Limits,
    /// What the handshake agreed.
    pub(super) agreed: OnceCell<Agreed>,
    /// What the configuration connected.
    pub(super) connected: OnceCell<Configured>,
    /// The host served, where a listening connector accepted it by name.
    pub(super) host_name: Option<Arc<str>>,
    pub(super) sessions: Sessions,
    /// Cancelled once the connection is stopping, which ends every heartbeat stream.
    pub(super) stopping: CancellationToken,
    /// Cancelled once the connection has ended, which ends every data call it still serves.
    pub(super) ended: CancellationToken,
    /// What the connection's requests and data streams still arriving hold together.
    pub(super) window: Window,
}

impl Service {
    pub(super) fn new(
        served: Arc<Served>,
        limits: Limits,
        host_name: Option<Arc<str>>,
        sessions: usize,
        stopping: CancellationToken,
    ) -> Self {
        Self {
            served,
            limits,
            host_name,
            agreed: OnceCell::new(),
            connected: OnceCell::new(),
            sessions: Sessions::holding(sessions),
            stopping,
            ended: CancellationToken::new(),
            window: super::classes::window(&limits),
        }
    }

    /// The limits what this end sends keeps within, once the handshake agreed them; the
    /// protocol's defaults before.
    pub(super) fn host(&self) -> Limits {
        self.agreed
            .get()
            .copied()
            .map(Agreed::host)
            .unwrap_or_default()
    }

    fn connected(&self) -> Result<&Connected, ConnectorError> {
        let Some(configured) = self.connected.get() else {
            return Err(if self.agreed.initialized() {
                not_configured()
            } else {
                no_handshake()
            });
        };
        Ok(&configured.connected)
    }

    /// The probes the configuration connected; none before it.
    pub(super) fn probes(&self) -> &Probes {
        self.connected
            .get()
            .map_or(&super::probes::NONE, |configured| &configured.probes)
    }

    pub(super) fn source(&self) -> Result<Arc<dyn Source>, ConnectorError> {
        match self.connected()? {
            Connected::Source(source) => Ok(Arc::clone(source)),
            Connected::Destination(_) => Err(wrong_role("source")),
        }
    }

    fn destination(&self) -> Result<Arc<dyn Destination>, ConnectorError> {
        match self.connected()? {
            Connected::Destination(destination) => Ok(Arc::clone(destination)),
            Connected::Source(_) => Err(wrong_role("destination")),
        }
    }

    async fn session(&self, id: u64) -> Result<SessionSlot, ConnectorError> {
        self.sessions.session(id).await
    }
}

fn wrong_role(role: &str) -> ConnectorError {
    unsupported(format!("the connection's role is not {role}"), "role")
}

/// `invalid` as the error of a request the connector cannot read.
pub(super) fn invalid(invalid: &Invalid) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, invalid.to_string())
        .with_code(INVALID_MESSAGE)
}

/// `invalid` as the status of a request the connector cannot read.
fn refused(invalid: &Invalid) -> Status {
    status(&self::invalid(invalid))
}

/// The stream the heartbeat answers with.
type Answer<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl Connector for Service {
    async fn handshake(
        &self,
        request: Request<v1::HandshakeRequest>,
    ) -> Result<Response<v1::HandshakeResponse>, Status> {
        let spec = self
            .agree(&request.into_inner())
            .map_err(|error| status(&error))?;
        let accepted_features = self
            .agreed
            .get()
            .and_then(|agreed| agreed.feature())
            .map(|feature| vec![feature.to_owned()])
            .unwrap_or_default();
        Ok(Response::new(v1::HandshakeResponse {
            spec: Some(spec),
            accepted_features,
            limits: Some(self.limits.into()),
            protocol_major: rdlt_wire::PROTOCOL_MAJOR,
        }))
    }

    async fn configure(
        &self,
        request: Request<v1::ConfigureRequest>,
    ) -> Result<Response<v1::ConfigureResponse>, Status> {
        let spec = self
            .connect(request.into_inner())
            .await
            .map_err(|error| status(&error))?;
        Ok(Response::new(v1::ConfigureResponse { spec: Some(spec) }))
    }

    async fn check(
        &self,
        _: Request<v1::CheckRequest>,
    ) -> Result<Response<v1::CheckResponse>, Status> {
        let checked = match self.connected().map_err(|error| status(&error))? {
            Connected::Source(source) => source.check().await,
            Connected::Destination(destination) => destination.check().await,
        };
        checked.map_err(|error| status(&error))?;
        Ok(Response::new(v1::CheckResponse {}))
    }

    async fn discover(
        &self,
        _: Request<v1::DiscoverRequest>,
    ) -> Result<Response<v1::Catalog>, Status> {
        let catalog = self
            .source()
            .map_err(|error| status(&error))?
            .discover()
            .await
            .map_err(|error| status(&error))?;
        Ok(Response::new(v1::Catalog::from(&catalog)))
    }

    async fn plan(
        &self,
        request: Request<v1::PlanRequest>,
    ) -> Result<Response<v1::PlanResponse>, Status> {
        let request = request.into_inner();
        let stream = StreamName::try_from(
            request
                .stream
                .ok_or(Invalid::Missing("stream"))
                .map_err(|e| refused(&e))?,
        )
        .map_err(|e| refused(&e))?;
        let state =
            StreamState::try_from(request.state.unwrap_or_default()).map_err(|e| refused(&e))?;
        let planned = self
            .source()
            .map_err(|error| status(&error))?
            .plan(&stream, &state)
            .await
            .map_err(|error| status(&error))?;
        Ok(Response::new(v1::PlanResponse::from(&planned)))
    }

    async fn read_acknowledged(
        &self,
        request: Request<v1::ReadAcknowledgedRequest>,
    ) -> Result<Response<v1::ReadAcknowledgedResponse>, Status> {
        let answer = self
            .probes()
            .read_acknowledged(request.into_inner())
            .await?;
        Ok(Response::new(answer))
    }

    async fn committed(
        &self,
        request: Request<v1::CommittedRequest>,
    ) -> Result<Response<v1::CommittedResponse>, Status> {
        let request = request.into_inner();
        let stream = StreamName::try_from(
            request
                .stream
                .ok_or(Invalid::Missing("stream"))
                .map_err(|e| refused(&e))?,
        )
        .map_err(|e| refused(&e))?;
        let cursors = request
            .cursors
            .into_iter()
            .map(|committed| {
                let partition = PartitionId::parse(committed.partition)
                    .map_err(|error| Invalid::rejected("partition id", error))?;
                let cursor = Cursor::try_from(committed.cursor.ok_or(Invalid::Missing("cursor"))?)?;
                Ok((partition, cursor))
            })
            .collect::<Result<Vec<_>, Invalid>>()
            .map_err(|e| refused(&e))?;
        // A host is heard for what it was sent or read from, and nothing else of its choosing:
        // a report of anything else is refused before the source hears of it.
        let host = self.host_name.as_deref();
        let sent = &self.served.sent;
        sent.admit(host, &stream, &cursors)
            .map_err(|error| status(&error))?;
        self.source()
            .map_err(|error| status(&error))?
            .committed(&stream, &cursors)
            .await
            .map_err(|error| status(&error))?;
        Ok(Response::new(v1::CommittedResponse {}))
    }

    async fn open(
        &self,
        request: Request<v1::OpenRequest>,
    ) -> Result<Response<v1::OpenResponse>, Status> {
        let request = request.into_inner();
        let pipeline = PipelineId::parse(request.pipeline)
            .map_err(|error| refused(&Invalid::rejected("pipeline id", error)))?;
        let load_id = crate::wire::load_id(&request.load_id).map_err(|e| refused(&e))?;
        let context = OpenContext { pipeline, load_id };
        let destination = self.destination().map_err(|error| status(&error))?;
        // Room is taken before the destination opens the session, so Opens in flight count too;
        // one that fails, or whose call is cancelled, drops its room.
        let opening = self.sessions.open().await;
        let opened = destination
            .open(&context)
            .await
            .map_err(|error| status(&error))?;
        let id = opening.hold(opened.session);
        Ok(Response::new(v1::OpenResponse {
            session: id,
            epoch: opened.epoch.0,
            state: opened.state.iter().map(v1::StateRecord::from).collect(),
        }))
    }

    async fn apply_schema(
        &self,
        request: Request<v1::ApplySchemaRequest>,
    ) -> Result<Response<v1::ApplySchemaResponse>, Status> {
        let request = request.into_inner();
        let change = TableChange::try_from(
            request
                .change
                .ok_or(Invalid::Missing("change"))
                .map_err(|e| refused(&e))?,
        )
        .map_err(|e| refused(&e))?;
        let slot = self
            .session(request.session)
            .await
            .map_err(|error| status(&error))?;
        let mut session = slot.lock().await;
        let session = session
            .as_mut()
            .ok_or_else(|| status(&closed(request.session)))?;
        session
            .apply_schema(&change)
            .await
            .map_err(|error| status(&error))?;
        Ok(Response::new(v1::ApplySchemaResponse {}))
    }

    async fn commit(
        &self,
        request: Request<v1::CommitRequest>,
    ) -> Result<Response<v1::Receipt>, Status> {
        let request = request.into_inner();
        let meta = CommitMeta::try_from(
            request
                .meta
                .ok_or(Invalid::Missing("commit meta"))
                .map_err(|e| refused(&e))?,
        )
        .map_err(|e| refused(&e))?;
        let slot = self
            .session(request.session)
            .await
            .map_err(|error| status(&error))?;
        let mut session = slot.lock().await;
        let session = session
            .as_mut()
            .ok_or_else(|| status(&closed(request.session)))?;
        let receipt = session
            .commit(&meta)
            .await
            .map_err(|error| status(&error))?;
        Ok(Response::new(v1::Receipt::from(&receipt)))
    }

    async fn close(
        &self,
        request: Request<v1::CloseRequest>,
    ) -> Result<Response<v1::CloseResponse>, Status> {
        let id = request.into_inner().session;
        self.sessions
            .close(id)
            .await
            .map_err(|error| status(&error))?;
        Ok(Response::new(v1::CloseResponse {}))
    }

    type HeartbeatStream = Answer<v1::Pong>;

    async fn heartbeat(
        &self,
        request: Request<Streaming<v1::Ping>>,
    ) -> Result<Response<Self::HeartbeatStream>, Status> {
        let pongs = request
            .into_inner()
            .map(|ping| ping.map(|ping| v1::Pong { seq: ping.seq }));
        let stopped = self.stopping.clone().cancelled_owned();
        Ok(Response::new(Box::pin(Until::new(pongs, stopped))))
    }
}

impl Streams for Service {
    fn serve(
        self: Arc<Self>,
        stream: Piped<Body>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        Box::pin(stream::serve(self, stream))
    }
}

impl Service {
    /// The session `id`'s writer for `table`.
    pub(super) async fn writer(
        &self,
        id: u64,
        table: &TableRef,
    ) -> Result<Box<dyn crate::destination::DestinationWriter>, ConnectorError> {
        let slot = self.session(id).await?;
        let mut session = slot.lock().await;
        let session = session.as_mut().ok_or_else(|| closed(id))?;
        session.writer(table).await
    }
}
