//! The connectors a hostile seed's network serves: the simulation's own, watched for what a hostile
//! host's connections make them do, and on every other life named after a secret they were told.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, ConnectContext, ConnectorError, ConnectorErrorKind,
    ConnectorSpec, Destination, DestinationFactory, DestinationSession, DestinationWriter,
    OpenContext, OpenedSession, Receipt, Result, Source, SourceFactory, TableChange, TableRef,
    destination_factory, source_factory,
};

use crate::destination::SimDestination;
use crate::network::{Net, Side};
use crate::source::SimSource;
use crate::world::World;

/// What a hostile host's configuration asks of the connector it configures: its connection's
/// number, and how long the connector pauses as it connects and as each session opens.
#[derive(Clone, Copy, Debug)]
pub(super) struct Asked {
    pub(super) connection: u64,
    pause: Duration,
    opening: Duration,
}

impl Asked {
    /// A configuration of `connection` whose connect pauses for `pause`.
    #[expect(
        dead_code,
        reason = "the hostile host's attacks, still to come, build these"
    )]
    pub(super) fn connecting(connection: u64, pause: Duration) -> Self {
        Self {
            connection,
            pause,
            opening: Duration::ZERO,
        }
    }

    /// A configuration of `connection` whose sessions each pause for `opening` as they open.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the hostile host's attacks, still to come, build these"
        )
    )]
    pub(super) fn opening(connection: u64, opening: Duration) -> Self {
        Self {
            connection,
            pause: Duration::ZERO,
            opening,
        }
    }

    /// A configuration that asks nothing, for calls refused before any configuration is read.
    #[expect(
        dead_code,
        reason = "the hostile host's attacks, still to come, build these"
    )]
    pub(super) fn idle() -> Self {
        Self::connecting(0, Duration::ZERO)
    }

    /// What `config` asks, where a hostile host wrote it.
    pub(super) fn of(config: &serde_json::Value) -> Option<Self> {
        let asked = config.get("hostile")?;
        let millis = |key: &str| {
            asked
                .get(key)
                .and_then(serde_json::Value::as_u64)
                .map(Duration::from_millis)
        };
        Some(Self {
            connection: asked.get("connection")?.as_u64()?,
            pause: millis("pause_ms")?,
            opening: millis("opening_ms")?,
        })
    }

    /// The configuration of the world registered as `world` that asks this.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the hostile host's attacks, still to come, build these"
        )
    )]
    pub(super) fn config(self, world: &str) -> String {
        let millis = |pause: Duration| u64::try_from(pause.as_millis()).unwrap_or(u64::MAX);
        serde_json::json!({
            "world": world,
            "hostile": {
                "connection": self.connection,
                "pause_ms": millis(self.pause),
                "opening_ms": millis(self.opening),
            },
        })
        .to_string()
    }
}

/// What a hostile seed's connectors keep across their lives, and see of each hostile connection.
#[derive(Debug, Default)]
pub(crate) struct Watch {
    /// The secret shaped as a version each side was last told, by side.
    pub(super) token: Mutex<[Option<String>; 2]>,
    /// Configurations connecting at once, by hostile connection.
    pub(super) configurations: Mutex<BTreeMap<u64, usize>>,
}

/// One configuration of a hostile connection connecting, counted in its watch until dropped.
pub(super) struct Counted {
    net: Arc<Net>,
    connection: u64,
}

impl Counted {
    /// One more configuration of `connection` connecting in `net`'s watch; beyond `most`, the
    /// world registered as `world` records it.
    pub(super) fn enter(net: &Arc<Net>, connection: u64, most: usize, world: &str) -> Self {
        let now = {
            let mut counts = net.watch.configurations.lock();
            let now = counts.entry(connection).or_default();
            *now += 1;
            *now
        };
        if now > most
            && let Some(world) = World::named(world)
        {
            world.violation(format!(
                "hostile connection {connection} had {now} configurations connecting at once, \
                 beyond {most}"
            ));
        }
        Self {
            net: Arc::clone(net),
            connection,
        }
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        if let Some(now) = self
            .net
            .watch
            .configurations
            .lock()
            .get_mut(&self.connection)
        {
            *now -= 1;
        }
    }
}

/// Where `side`'s token is kept.
fn slot(side: Side) -> usize {
    match side {
        Side::Source => 0,
        Side::Destination => 1,
    }
}

/// The world `config` names.
fn world(config: &serde_json::Value) -> String {
    config
        .get("world")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// The secret shaped as a version `config` told, as its host resolved it.
fn token(config: &serde_json::Value) -> Option<String> {
    config.pointer("/told/token")?.as_str().map(str::to_owned)
}

impl Net {
    /// The source factory a life after `crashes` crashes serves: the simulation's own, watched in a
    /// hostile seed.
    pub(in crate::network) fn source_factory(
        self: &Arc<Self>,
        crashes: u64,
    ) -> Box<dyn SourceFactory> {
        let inner = source_factory::<SimSource>();
        if !self.hostile {
            return inner;
        }
        let spec = self.spec(inner.spec(), Side::Source, crashes);
        Box::new(Watched {
            inner,
            spec,
            net: Arc::clone(self),
            side: Side::Source,
        })
    }

    /// The destination factory a life after `crashes` crashes serves: the simulation's own,
    /// watched in a hostile seed.
    pub(in crate::network) fn destination_factory(
        self: &Arc<Self>,
        crashes: u64,
    ) -> Box<dyn DestinationFactory> {
        let inner = destination_factory::<SimDestination>();
        if !self.hostile {
            return inner;
        }
        let spec = self.spec(inner.spec(), Side::Destination, crashes);
        Box::new(Watched {
            inner,
            spec,
            net: Arc::clone(self),
            side: Side::Destination,
        })
    }

    /// `spec`, its version named after the token `side` was last told on a life after an odd
    /// count of `crashes`: a connector started again that says a secret as its version.
    fn spec(&self, spec: &ConnectorSpec, side: Side, crashes: u64) -> ConnectorSpec {
        let token = self.watch.token.lock()[slot(side)].clone();
        match token.filter(|_| crashes % 2 == 1) {
            Some(token) => ConnectorSpec {
                version: format!("{}-{token}", spec.version),
                ..spec.clone()
            },
            None => spec.clone(),
        }
    }
}

/// A factory of the simulation's connectors, watched for what hostile connections make it do.
struct Watched<F> {
    inner: F,
    spec: ConnectorSpec,
    net: Arc<Net>,
    side: Side,
}

impl<F> Watched<F> {
    /// One of `asked`'s connection's configurations connecting, counted until dropped.
    fn connecting(&self, asked: Option<Asked>, world: &str) -> Option<Counted> {
        asked.map(|asked| Counted::enter(&self.net, asked.connection, 1, world))
    }

    /// Keeps `token`, where a configuration that connected told one.
    fn heard(&self, token: Option<String>) {
        if let Some(token) = token {
            self.net.watch.token.lock()[slot(self.side)] = Some(token);
        }
    }
}

impl SourceFactory for Watched<Box<dyn SourceFactory>> {
    fn spec(&self) -> &ConnectorSpec {
        &self.spec
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, Result<Box<dyn Source>>> {
        Box::pin(async move {
            let (asked, token) = (Asked::of(&config), token(&config));
            let _connecting = self.connecting(asked, &world(&config));
            if let Some(asked) = asked {
                tokio::time::sleep(asked.pause).await;
            }
            let connected = self.inner.connect(config, context).await?;
            self.heard(token);
            Ok(connected)
        })
    }
}

impl DestinationFactory for Watched<Box<dyn DestinationFactory>> {
    fn spec(&self) -> &ConnectorSpec {
        &self.spec
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, Result<Box<dyn Destination>>> {
        Box::pin(async move {
            let (asked, token) = (Asked::of(&config), token(&config));
            let _connecting = self.connecting(asked, &world(&config));
            if let Some(asked) = asked {
                tokio::time::sleep(asked.pause).await;
            }
            let connected = self.inner.connect(config, context).await?;
            self.heard(token);
            Ok(match asked {
                Some(asked) => Box::new(WatchedDestination {
                    inner: connected,
                    opening: asked.opening,
                }) as Box<dyn Destination>,
                None => connected,
            })
        })
    }
}

/// A destination a hostile connection configured, whose sessions pause as the connection asked.
///
/// The simulated destination counts the sessions it holds itself, as one connection holds them.
struct WatchedDestination {
    inner: Box<dyn Destination>,
    opening: Duration,
}

impl Destination for WatchedDestination {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.inner.check()
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            tokio::time::sleep(self.opening).await;
            let opened = self.inner.open(context).await?;
            Ok(OpenedSession {
                session: Box::new(WatchedSession {
                    inner: opened.session,
                    opening: self.opening,
                }),
                ..opened
            })
        })
    }
}

/// A session of a hostile connection, whose call for a writer stays in flight for three of
/// `opening`.
struct WatchedSession {
    inner: Box<dyn DestinationSession>,
    opening: Duration,
}

impl DestinationSession for WatchedSession {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        self.inner.apply_schema(change)
    }

    /// Holds the call in flight for three of the connection's pauses, longer than an open beside
    /// it takes, then refuses it: a hostile connection claims no table.
    fn writer<'a>(
        &'a mut self,
        _: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        let held = self.opening * 3;
        Box::pin(async move {
            tokio::time::sleep(held).await;
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "a hostile connection's session writes no table",
            ))
        })
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        self.inner.commit(meta)
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.inner.close()
    }
}
