//! The engine handle: one pipeline execution, its event stream and its
//! cancellation token.

use std::sync::Arc;

use rdlt_connector::destination::Destination;

use rdlt_connector::source::Source;
use rdlt_core::error::Error;
use rdlt_core::event::PipelineEvent;
use rdlt_core::report;
use tokio_util::sync::CancellationToken;

use crate::check;
use crate::config::Config;
use crate::run::retry;

/// One pipeline execution: consumes itself on [`Engine::run`].
pub struct Engine {
    config: Config,
    source: Arc<dyn Source>,
    destination: Arc<dyn Destination>,
    cancel: CancellationToken,
    events: tokio::sync::broadcast::Sender<PipelineEvent>,
}

/// Ring-buffer depth of the observability broadcast channel. Large enough that a
/// briefly-stalled subscriber rarely lags out; a subscriber slower than this loses
/// oldest events (by design — observability never backpressures data movement).
const EVENT_CHANNEL_CAPACITY: usize = 4096;

/// Typed observability stream: every [`PipelineEvent`] in causal order. Slow
/// consumers lose oldest events rather than backpressuring the pipeline —
/// observability must never slow data movement.
#[derive(Debug)]
pub struct EventStream {
    rx: tokio::sync::broadcast::Receiver<PipelineEvent>,
}

impl EventStream {
    /// `None` once the run finished and all events were consumed.
    pub async fn recv(&mut self) -> Option<PipelineEvent> {
        use tokio::sync::broadcast::error::RecvError;
        loop {
            match self.rx.recv().await {
                Ok(event) => return Some(event),
                // A slow consumer loses events. Skipping silently makes a
                // partial event stream indistinguishable from a complete one,
                // so say how many went — the run report remains the complete
                // record either way.
                Err(RecvError::Lagged(dropped)) => {
                    tracing::warn!(dropped, "event consumer lagged; events were dropped");
                    continue;
                }
                Err(RecvError::Closed) => return None,
            }
        }
    }
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("pipeline", &self.config.pipeline)
            .finish_non_exhaustive()
    }
}

impl Engine {
    pub fn new(config: Config, source: impl Source, destination: impl Destination) -> Self {
        Self {
            config,
            source: Arc::new(source),
            destination: Arc::new(destination),
            cancel: CancellationToken::new(),
            events: tokio::sync::broadcast::channel(EVENT_CHANNEL_CAPACITY).0,
        }
    }

    /// Subscribe to the typed event stream. Call before [`Engine::run`]; multiple
    /// subscribers each see every event.
    pub fn events(&self) -> EventStream {
        EventStream {
            rx: self.events.subscribe(),
        }
    }

    /// Cancelling is safe at any instant and equivalent to a crash: the next run
    /// recovers identically.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Connectivity, discovery and plan checks without a load session:
    /// the source's and destination's [`check`](Source::check) probes,
    /// stream discovery under the stream cap, then the same plan
    /// validation a run performs. The ENGINE creates nothing — no
    /// workdir lock, no WAL, no session, no reads; what a connector's
    /// own probe or construction does is that connector's behavior (a
    /// destination that materializes its target at construction does so
    /// here exactly as a run would). Consumes the engine like
    /// [`Engine::run`]; construct a new one to run after a check.
    pub async fn check(self) -> Result<check::Summary, Error> {
        check::check(&self.config, self.source, self.destination).await
    }

    /// Run to completion (resumable, cancel-safe). Consumes the engine — a run is not
    /// restartable in place; construct a new one to run again.
    pub async fn run(self) -> Result<report::Run, Error> {
        retry::run(
            self.config,
            self.source,
            self.destination,
            self.cancel,
            self.events,
        )
        .await
    }
}
