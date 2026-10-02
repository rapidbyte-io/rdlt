//! The protocol's calls, served for one connection: the handshake agrees a role, the
//! configuration connects the connector for it, and every later call works on it.

use std::pin::Pin;
use std::sync::Arc;

use rdlt_wire::Limits;
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
use crate::wire::{Invalid, status, v1};
use crate::{CommitMeta, Cursor};

/// The connector a handshake connected, for its role.
pub(super) enum Connected {
    Source(Arc<dyn Source>),
    Destination(Arc<dyn Destination>),
}

/// The protocol served on one connection.
pub(super) struct Service {
    pub(super) served: Arc<Served>,
    pub(super) limits: Limits,
    /// What the handshake agreed.
    pub(super) agreed: OnceCell<Agreed>,
    pub(super) connected: OnceCell<Connected>,
    /// The host served, where a listening connector accepted it by name.
    pub(super) host_name: Option<Arc<str>>,
    /// Certification's probes, where the handshake accepted them.
    pub(super) probes: Probes,
    /// The host's limits, which what this end sends must keep within.
    pub(super) host: OnceCell<Limits>,
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
            probes: Probes::default(),
            host: OnceCell::new(),
            sessions: Sessions::holding(sessions),
            stopping,
        }
    }

    fn connected(&self) -> Result<&Connected, Status> {
        self.connected.get().ok_or_else(|| {
            let error = if self.agreed.initialized() {
                not_configured()
            } else {
                no_handshake()
            };
            status(&error)
        })
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
            .with_code("invalid_message"),
    )
}

/// The stream a streaming call answers with.
pub(super) type Answer<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

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
            protocol_minor: rdlt_wire::PROTOCOL_MINOR,
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

    type ReadStream = Answer<v1::ReadFrame>;

    async fn read(
        &self,
        request: Request<Streaming<v1::ReadControl>>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let host = self.host.get().copied().unwrap_or_default();
        let (frames, read) =
            read::serve(self.source()?, self.limits, host, request.into_inner()).await?;
        // Where the read starts, and each checkpoint it sends, the host may report committed.
        let served = Arc::clone(&self.served);
        let noted = Noted::new(frames, served, self.host_name.clone(), read);
        Ok(Response::new(Box::pin(noted)))
    }

    type ReadPublishedStream = Answer<v1::ReadFrame>;

    async fn read_published(
        &self,
        request: Request<v1::ReadPublishedRequest>,
    ) -> Result<Response<Self::ReadPublishedStream>, Status> {
        let host = self.host.get().copied().unwrap_or_default();
        let frames = self.probes.read_published(request.into_inner(), host)?;
        Ok(Response::new(frames))
    }

    async fn read_acknowledged(
        &self,
        request: Request<v1::ReadAcknowledgedRequest>,
    ) -> Result<Response<v1::ReadAcknowledgedResponse>, Status> {
        let answer = self.probes.read_acknowledged(request.into_inner()).await?;
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
        let load_id = crate::wire::load_id(&request.load_id).map_err(|e| invalid(&e))?;
        let context = OpenContext { pipeline, load_id };
        let opened = self
            .destination()?
            .open(&context)
            .await
            .map_err(|error| status(&error))?;
        let id = self.sessions.open(opened.session).await;
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

    type WriteStream = Answer<v1::WriteAck>;

    async fn write(
        &self,
        request: Request<Streaming<v1::WriteFrame>>,
    ) -> Result<Response<Self::WriteStream>, Status> {
        let acks = write::serve(self, self.limits, request.into_inner()).await?;
        Ok(Response::new(acks))
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
        let session = self.sessions.take(id).await?;
        session.close().await.map_err(|error| status(&error))?;
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
