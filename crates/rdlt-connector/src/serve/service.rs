//! The protocol's calls, served for one connection: the handshake connects the connector for a
//! role, and every later call works on it.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rdlt_wire::Limits;
use rdlt_wire::tonic::codegen::tokio_stream::Stream;
use rdlt_wire::tonic::{self, Request, Response, Status, Streaming};
use rdlt_wire::v1::connector_server::Connector;
use tokio::sync::{Mutex, OnceCell};
use tokio_stream::StreamExt as _;

use super::handshake::unsupported;
use super::{Served, read, write};
use crate::destination::{Destination, DestinationSession, OpenContext, TableChange};
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

/// A destination session, until its close takes it.
pub(super) type SessionSlot = Arc<Mutex<Option<Box<dyn DestinationSession>>>>;

/// The protocol served on one connection.
pub(super) struct Service {
    pub(super) served: Arc<Served>,
    pub(super) limits: Limits,
    pub(super) connected: OnceCell<Connected>,
    /// The host's limits, which what this end sends must keep within.
    pub(super) host: OnceCell<Limits>,
    pub(super) sessions: Mutex<BTreeMap<u64, SessionSlot>>,
    pub(super) next_session: AtomicU64,
}

impl Service {
    pub(super) fn new(served: Arc<Served>, limits: Limits) -> Self {
        Self {
            served,
            limits,
            connected: OnceCell::new(),
            host: OnceCell::new(),
            sessions: Mutex::new(BTreeMap::new()),
            next_session: AtomicU64::new(1),
        }
    }

    fn connected(&self) -> Result<&Connected, Status> {
        self.connected.get().ok_or_else(|| {
            status(
                &ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "the connection has had no handshake",
                )
                .with_code("no_handshake"),
            )
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
        self.sessions.lock().await.get(&id).cloned().ok_or_else(|| {
            status(
                &ConnectorError::new(ConnectorErrorKind::Internal, format!("no session {id}"))
                    .with_code("no_session"),
            )
        })
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
            .connect(request.into_inner())
            .await
            .map_err(|error| status(&error))?;
        Ok(Response::new(v1::HandshakeResponse {
            spec: Some(spec),
            accepted_features: Vec::new(),
            limits: Some(self.limits.into()),
        }))
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
        let partitions = self
            .source()?
            .plan(&stream, &state)
            .await
            .map_err(|error| status(&error))?;
        Ok(Response::new(v1::PlanResponse {
            partitions: partitions
                .iter()
                .map(|partition| partition.id().as_str().to_owned())
                .collect(),
        }))
    }

    type ReadStream = Answer<v1::ReadFrame>;

    async fn read(
        &self,
        request: Request<Streaming<v1::ReadControl>>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let host = self.host.get().copied().unwrap_or_default();
        let frames = read::serve(self.source()?, host, request.into_inner()).await?;
        Ok(Response::new(frames))
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
        let id = self.next_session.fetch_add(1, Ordering::Relaxed);
        self.sessions
            .lock()
            .await
            .insert(id, Arc::new(Mutex::new(Some(opened.session))));
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
        let slot = self
            .sessions
            .lock()
            .await
            .remove(&id)
            .ok_or_else(|| closed(id))?;
        let session = slot.lock().await.take().ok_or_else(|| closed(id))?;
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
        Ok(Response::new(Box::pin(pongs)))
    }
}

impl Service {
    /// The session `id`'s writer for `table`.
    pub(super) async fn writer(
        &self,
        id: u64,
        table: &crate::destination::TableRef,
    ) -> Result<Box<dyn crate::destination::DestinationWriter>, Status> {
        let slot = self.session(id).await?;
        let mut session = slot.lock().await;
        let session = session.as_mut().ok_or_else(|| closed(id))?;
        session.writer(table).await.map_err(|error| status(&error))
    }
}

fn closed(id: u64) -> Status {
    status(
        &ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!("session {id} is closed"),
        )
        .with_code("no_session"),
    )
}
