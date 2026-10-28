//! The protocol's calls, served for one connection: the handshake agrees a role, the
//! configuration connects the connector for it, and every later call works on it.

#[cfg(test)]
mod tests;

use std::pin::Pin;
use std::sync::Arc;

use rdlt_wire::Limits;
use rdlt_wire::plane::{Answers, Incoming, Plane, Serving};
use rdlt_wire::tonic::codegen::tokio_stream::Stream;
use rdlt_wire::tonic::{self, Request, Response, Status, Streaming};
use rdlt_wire::v1::connector_server::Connector;
use tokio::sync::OnceCell;
use tokio_stream::StreamExt as _;
use tokio_util::sync::CancellationToken;

use super::handshake::{Agreed, no_handshake, not_configured, unsupported};
use super::noted::Noted;
use super::probes::Probes;
use super::sessions::{SessionSlot, Sessions, closed};
use super::until::Until;
use super::{Served, read, write};
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

    fn connected(&self) -> Result<&Connected, Status> {
        let Some(configured) = self.connected.get() else {
            let error = if self.agreed.initialized() {
                not_configured()
            } else {
                no_handshake()
            };
            return Err(status(&error));
        };
        Ok(&configured.connected)
    }

    /// The probes the configuration connected; none before it.
    fn probes(&self) -> &Probes {
        self.connected
            .get()
            .map_or(&super::probes::NONE, |configured| &configured.probes)
    }

    fn source(&self) -> Result<Arc<dyn Source>, Status> {
        match self.connected()? {
            Connected::Source(source) => Ok(Arc::clone(source)),
            Connected::Destination(_) => Err(wrong_role("source")),
        }
    }

    fn destination(&self) -> Result<Arc<dyn Destination>, Status> {
        match self.connected()? {
            Connected::Destination(destination) => Ok(Arc::clone(destination)),
            Connected::Source(_) => Err(wrong_role("destination")),
        }
    }

    async fn session(&self, id: u64) -> Result<SessionSlot, Status> {
        self.sessions.session(id).await
    }
}

fn wrong_role(role: &str) -> Status {
    status(&unsupported(
        format!("the connection's role is not {role}"),
        "role",
    ))
}

/// `invalid` as the status of a request the connector cannot read.
pub(super) fn invalid(invalid: &Invalid) -> Status {
    status(
        &ConnectorError::new(ConnectorErrorKind::Internal, invalid.to_string())
            .with_code(INVALID_MESSAGE),
    )
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
        let checked = match self.connected()? {
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
            .source()?
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
                .map_err(|e| invalid(&e))?,
        )
        .map_err(|e| invalid(&e))?;
        let state =
            StreamState::try_from(request.state.unwrap_or_default()).map_err(|e| invalid(&e))?;
        let planned = self
            .source()?
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
                .map_err(|e| invalid(&e))?,
        )
        .map_err(|e| invalid(&e))?;
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
            .map_err(|e| invalid(&e))?;
        // A host is heard for what it was sent or read from, and nothing else of its choosing:
        // a report of anything else is refused before the source hears of it.
        let host = self.host_name.as_deref();
        let sent = &self.served.sent;
        sent.admit(host, &stream, &cursors)
            .map_err(|error| status(&error))?;
        self.source()?
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
            .map_err(|error| invalid(&Invalid::rejected("pipeline id", error)))?;
        let context = OpenContext { pipeline };
        let destination = self.destination()?;
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
            landed: opened.landed.as_ref().map(v1::Receipt::from),
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
                .map_err(|e| invalid(&e))?,
        )
        .map_err(|e| invalid(&e))?;
        let slot = self.session(request.session).await?;
        let mut session = slot.lock().await;
        let session = session.as_mut().ok_or_else(|| closed(request.session))?;
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
                .map_err(|e| invalid(&e))?,
        )
        .map_err(|e| invalid(&e))?;
        let slot = self.session(request.session).await?;
        let mut session = slot.lock().await;
        let session = session.as_mut().ok_or_else(|| closed(request.session))?;
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
        self.sessions.close(id).await?;
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

impl Plane for Service {
    fn write(&self, frames: Incoming<v1::WriteFrame>) -> Serving<'_, v1::WriteAck> {
        Box::pin(write::serve(self, self.limits, frames))
    }

    fn read(&self, controls: Incoming<v1::ReadControl>) -> Serving<'_, v1::ReadFrame> {
        Box::pin(async move {
            let host = self.host();
            let (frames, read) = read::serve(self.source()?, self.limits, host, controls).await?;
            // Where the read starts, and each checkpoint it sends, the host may report committed.
            let served = Arc::clone(&self.served);
            let noted = Noted::new(frames, served, self.host_name.clone(), read);
            Ok(Box::pin(noted) as Answers<_>)
        })
    }

    fn read_published(&self, request: v1::ReadPublishedRequest) -> Serving<'_, v1::ReadFrame> {
        Box::pin(async move { self.probes().read_published(request, self.host()) })
    }
}

impl Service {
    /// The session `id`'s writer for `table`.
    pub(super) async fn writer(
        &self,
        id: u64,
        table: &TableRef,
    ) -> Result<Box<dyn crate::destination::DestinationWriter>, Status> {
        let slot = self.session(id).await?;
        let mut session = slot.lock().await;
        let session = session.as_mut().ok_or_else(|| closed(id))?;
        session.writer(table).await.map_err(|error| status(&error))
    }
}
