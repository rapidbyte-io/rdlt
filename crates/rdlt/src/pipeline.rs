//! Pipelines: built from a document on a host, then checked, discovered, reset and run.

mod selector;
#[cfg(test)]
mod tests;

pub use selector::StreamSelector;

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{
    Catalog, ConnectorError, ConnectorErrorKind, DEADLINE_EXCEEDED, Destination, Role, Source,
    StreamName,
};
use rdlt_engine::{Engine, EngineConfig, Env, PipelinePlan, ResetReport, ResetScope, SystemEnv};
use rdlt_host::{Placed, Provider as _};
use serde::Serialize;
use tokio::sync::broadcast;

use crate::limits::{EVENT_BUFFER, EVENT_BUFFER_MAX};
use crate::lower::{self, Lowered};
use crate::run::{Observed, Run};
use crate::{Commit, Defaults, Document, Error, Host, Resources, Retry, Side, Stream, Until};

/// A pipeline described in code: a builder of its [`Document`].
///
/// ```
/// use rdlt::{Connector, Pipeline, Side, Stream, WriteMode};
///
/// let source = Side::new(Connector::new("io.rapidbyte.memory"))
///     .config(rdlt::json!({ "streams": { "users": [{ "id": 1 }] } }));
/// let destination = Side::new(Connector::new("io.rapidbyte.memory"))
///     .config(rdlt::json!({ "store": "users" }));
/// let builder = Pipeline::builder("users", source, destination)
///     .stream(Stream::new("users").write(WriteMode::merge(["id"])));
/// assert_eq!(builder.document().streams.len(), 1);
/// ```
#[derive(Clone, Debug)]
#[must_use]
pub struct PipelineBuilder {
    document: Document,
}

impl PipelineBuilder {
    /// The pipeline, selecting `stream` too.
    pub fn stream(mut self, stream: Stream) -> Self {
        self.document.streams.push(stream);
        self
    }

    /// The pipeline, every stream taking `defaults` where it says nothing.
    pub fn defaults(mut self, defaults: Defaults) -> Self {
        self.document.defaults = defaults;
        self
    }

    /// The pipeline, committing as `commit` says.
    pub fn commit(mut self, commit: Commit) -> Self {
        self.document.commit = Some(commit);
        self
    }

    /// The pipeline, using `resources`.
    pub fn resources(mut self, resources: Resources) -> Self {
        self.document.resources = resources;
        self
    }

    /// The pipeline, retrying as `retry` says.
    pub fn retry(mut self, retry: Retry) -> Self {
        self.document.retry = Some(retry);
        self
    }

    /// The pipeline, reading as long as `until` says.
    pub fn until(mut self, until: Until) -> Self {
        self.document.until = until;
        self
    }

    /// The pipeline's document as it stands.
    pub fn document(&self) -> &Document {
        &self.document
    }

    /// Builds the pipeline on `host`, checking everything that needs no data.
    ///
    /// First, before a connector is placed: the document's version and names, its connectors'
    /// references, its commit, resources and retry, each stream's modes, keys, columns, hints
    /// and transforms, with mask keys resolved through the host's secrets, SQL parsed and code
    /// transforms found among the host's, each once however many streams it selects; each
    /// stream then has a SQL transform of its own, which plans for its input schemas. Then both
    /// connectors are placed and handshaken, the streams selected from the source's catalog,
    /// and each checked against the catalog and the destination's capabilities, as a run's
    /// start checks it.
    ///
    /// Nothing is read and no session opens. A run of the built pipeline can still refuse its
    /// configuration against what earlier runs committed, as a stream whose key differs from
    /// the key its table was created with.
    ///
    /// # Errors
    ///
    /// The first check that fails, with its code.
    pub async fn build(self, host: &Host) -> Result<Pipeline, Error> {
        let document = self.document;
        let prepared = lower::prepare(&document, host).await?;
        let source = lower::reference(&document.source, Role::Source)?;
        let destination = lower::reference(&document.destination, Role::Destination)?;
        let provider = host.provider();
        let source = provider
            .source(&source, &document.source.config)
            .await
            .map_err(|error| Error::placement(Role::Source, error))?;
        let destination = provider
            .destination(&destination, &document.destination.config)
            .await
            .map_err(|error| Error::placement(Role::Destination, error))?;
        let (source, source_placed) = shared(source);
        let (destination, destination_placed) = shared(destination);
        let catalog = discovered(source.as_ref(), prepared.connector_wait()).await?;
        let Lowered { plan, config } = prepared.bind(&catalog)?;
        let env = host.environment().await?;
        Engine::new(config.clone(), Arc::clone(&env) as Arc<dyn Env>)
            .check(&plan, Arc::clone(&source), Arc::clone(&destination))
            .await
            .map_err(Error::engine)?;
        Ok(Pipeline {
            document,
            plan,
            config,
            source,
            destination,
            placed: (source_placed, destination_placed),
            env,
            event_buffer: EVENTS,
        })
    }
}

/// The events each run of a pipeline keeps, unless it says.
const EVENTS: NonZeroUsize = NonZeroUsize::new(EVENT_BUFFER).expect("a run keeps events");

/// The most events a run keeps.
const MOST_EVENTS: NonZeroUsize = NonZeroUsize::new(EVENT_BUFFER_MAX).expect("a run keeps events");

/// A placed connector, shared by every run of the pipeline, and what it says it is.
fn shared<T: ?Sized>(placed: Placed<Box<T>>) -> (Arc<T>, PlacedConnector) {
    let summary = PlacedConnector {
        id: placed.spec.id.to_string(),
        version: placed.spec.version.clone(),
        placement: Placement::of(placed.placement),
    };
    (Arc::from(placed.connector), summary)
}

/// The catalog `source` offers, answered within `wait`, the engine's wait for a connector's call.
async fn discovered(source: &dyn Source, wait: Duration) -> Result<Catalog, Error> {
    let context = "discovering the catalog";
    let Ok(answered) = tokio::time::timeout(wait, source.discover()).await else {
        let unanswered = format!("no answer within {wait:?}");
        let expired = ConnectorError::new(ConnectorErrorKind::Transient, unanswered)
            .with_code(DEADLINE_EXCEEDED);
        return Err(Error::connector(Role::Source, context, expired));
    };
    answered.map_err(|error| Error::connector(Role::Source, context, error))
}

/// A built pipeline: its connectors placed and handshaken, its plan checked.
///
/// ```no_run
/// # async fn example(host: &rdlt::Host, document: rdlt::Document) -> Result<(), rdlt::Error> {
/// let pipeline = rdlt::Pipeline::from_document(document).build(host).await?;
/// let checked = pipeline.check().await?;
/// println!("{} loads {:?}", checked.pipeline, checked.streams);
/// # Ok(())
/// # }
/// ```
pub struct Pipeline {
    document: Document,
    plan: PipelinePlan,
    config: EngineConfig,
    source: Arc<dyn Source>,
    destination: Arc<dyn Destination>,
    placed: (PlacedConnector, PlacedConnector),
    env: Arc<SystemEnv>,
    event_buffer: NonZeroUsize,
}

impl Pipeline {
    /// A pipeline named `name` from `source` to `destination`, selecting no stream yet.
    pub fn builder(name: impl Into<String>, source: Side, destination: Side) -> PipelineBuilder {
        PipelineBuilder {
            document: Document::new(name, source, destination),
        }
    }

    /// The pipeline `document` describes.
    pub fn from_document(document: Document) -> PipelineBuilder {
        PipelineBuilder { document }
    }

    /// The pipeline as its document: what it was built from, every secret still a reference.
    pub fn document(&self) -> &Document {
        &self.document
    }

    /// The source, placed.
    pub fn source(&self) -> &PlacedConnector {
        &self.placed.0
    }

    /// The destination, placed.
    pub fn destination(&self) -> &PlacedConnector {
        &self.placed.1
    }

    /// The pipeline, each of its runs keeping its last `events` events for subscribers that
    /// fall behind: [`EVENT_BUFFER`] unless set, and at most [`EVENT_BUFFER_MAX`].
    #[must_use]
    pub fn with_event_buffer(mut self, events: NonZeroUsize) -> Self {
        self.event_buffer = events.min(MOST_EVENTS);
        self
    }

    /// Starts a run; nothing happens until it is awaited, and dropping it cancels it.
    ///
    /// Runs of one pipeline share its connectors; each opens the destination, which fences the
    /// runs begun before it.
    pub fn start(&self) -> Run {
        let (events, _) = broadcast::channel(self.event_buffer.get());
        let env = Observed {
            base: Arc::clone(&self.env),
            events: events.clone(),
        };
        let engine = Engine::new(self.config.clone(), Arc::new(env));
        let (source, destination) = (Arc::clone(&self.source), Arc::clone(&self.destination));
        Run::new(engine.run(self.plan.clone(), source, destination), events)
    }

    fn engine(&self) -> Engine {
        Engine::new(self.config.clone(), Arc::clone(&self.env) as Arc<dyn Env>)
    }

    /// Checks the pipeline again as building it did; no session opens, so no run is fenced.
    ///
    /// # Errors
    ///
    /// The first check that fails, as building the pipeline reports it.
    pub async fn check(&self) -> Result<Checked, Error> {
        let (source, destination) = (Arc::clone(&self.source), Arc::clone(&self.destination));
        let engine = self.engine();
        let checking = engine.check(&self.plan, source, destination);
        checking.await.map_err(Error::engine)?;
        Ok(Checked {
            pipeline: self.document.pipeline.clone(),
            streams: self
                .plan
                .streams()
                .iter()
                .map(|stream| stream.name().to_string())
                .collect(),
            source: self.placed.0.clone(),
            destination: self.placed.1.clone(),
        })
    }

    /// The source's catalog.
    ///
    /// # Errors
    ///
    /// The source's failure to discover it, or to answer within the engine's wait for a
    /// connector, as a failure coded `deadline_exceeded`.
    pub async fn discover(&self) -> Result<Catalog, Error> {
        discovered(self.source.as_ref(), self.config.connector_wait()).await
    }

    /// Resets `streams`, each by its name as the catalog shows it or by its identity, as `scope`
    /// says, in one commit.
    ///
    /// The reset opens the destination, which fences every run of the pipeline in progress: a
    /// reset is a write. A stream the pipeline no longer selects is named as the source's catalog
    /// shows it too, and a name two of the catalog's streams show as now is refused, so a reset
    /// never drops the tables of a stream it did not mean. A namespaced stream neither the
    /// pipeline nor the catalog holds now is named by its identity, a [`StreamName`].
    ///
    /// # Errors
    ///
    /// `stream_invalid` for a name that is no stream's, `stream_ambiguous` for one two of the
    /// catalog's streams show as, or the engine's refusal, as `stream_not_found` for a stream the
    /// pipeline never committed.
    pub async fn reset(
        &self,
        streams: impl IntoIterator<Item = impl Into<StreamSelector>>,
        scope: ResetScope,
    ) -> Result<ResetReport, Error> {
        let mut catalog = None;
        let mut names = Vec::new();
        for stream in streams {
            names.push(self.selected(stream.into(), &mut catalog).await?);
        }
        let pipeline = self.plan.pipeline();
        let (source, destination) = (Arc::clone(&self.source), Arc::clone(&self.destination));
        let engine = self.engine();
        let resetting = engine.reset(pipeline, &names, scope, source, destination);
        resetting.await.map_err(Error::engine)
    }

    /// The stream `selector` selects, any catalog needed discovered once into `catalog`.
    async fn selected(
        &self,
        selector: StreamSelector,
        catalog: &mut Option<Catalog>,
    ) -> Result<StreamName, Error> {
        match selector {
            StreamSelector::Shown(name) => self.stream(&name, catalog).await,
            StreamSelector::Identity(identity) => Ok(identity),
        }
    }

    /// The stream `name` names, against the source's catalog, discovered once into `catalog`: a
    /// name two of its streams show as is refused, whatever the pipeline plans; else the
    /// pipeline's stream that shows as it, else the catalog's, else a stream of that name.
    async fn stream(&self, name: &str, catalog: &mut Option<Catalog>) -> Result<StreamName, Error> {
        let offered = match catalog {
            Some(offered) => offered,
            None => catalog.insert(self.discover().await?),
        };
        let shown = lower::resolved(name, offered)?;
        let mut planned = self
            .plan
            .streams()
            .iter()
            .map(rdlt_engine::StreamPlan::name);
        if let Some(stream) = planned.find(|stream| stream.to_string() == name) {
            return Ok(stream.clone());
        }
        if let Some(stream) = shown {
            return Ok(stream);
        }
        lower::written(name)
    }
}

impl std::fmt::Debug for Pipeline {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Pipeline")
            .field("pipeline", &self.document.pipeline)
            .field("source", &self.placed.0)
            .field("destination", &self.placed.1)
            .finish_non_exhaustive()
    }
}

/// What checking a pipeline found: its streams, and where its connectors run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Checked {
    /// The pipeline.
    pub pipeline: String,
    /// The streams it loads, each by its name as the catalog shows it.
    pub streams: Vec<String>,
    /// The source.
    pub source: PlacedConnector,
    /// The destination.
    pub destination: PlacedConnector,
}

/// A connector as it was placed: who it says it is, and where it runs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct PlacedConnector {
    /// Its id, from its handshake.
    pub id: String,
    /// Its version, from its handshake.
    pub version: String,
    /// Where it runs.
    pub placement: Placement,
}

/// Where a connector runs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Placement {
    /// In the host's process.
    InProcess,
    /// In a process of its own, spawned from `path`.
    Process {
        /// Its binary.
        path: PathBuf,
    },
    /// Listening at `endpoint`.
    Remote {
        /// The endpoint's host and port.
        endpoint: String,
    },
}

impl Placement {
    fn of(placement: rdlt_host::Placement) -> Self {
        match placement {
            rdlt_host::Placement::InProcess => Self::InProcess,
            rdlt_host::Placement::Process { path } => Self::Process { path },
            // A reference's text may hold a credential: its host and port alone are shown.
            rdlt_host::Placement::Remote { endpoint } => Self::Remote {
                endpoint: rdlt_host::Endpoint::parse(&endpoint)
                    .map_or_else(|_| String::new(), |at| at.to_string()),
            },
            rdlt_host::Placement::Connected => {
                unreachable!("the facade gives its registry no placement that connects")
            }
        }
    }
}
