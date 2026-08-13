//! [`ManagedSource`]/[`ManagedDestination`] — what a provider hands
//! back — and [`LifecycleGuard`], the owner of the spawned process.
//!
//! The managed types implement the SPI's `Source`/`Destination` by
//! DELEGATION to the client crate's wire adapters, so `Engine::new` takes them
//! unchanged and the guard's lifetime rides the engine's `Arc`: as long
//! as anything can still call the connector, the process it dials is
//! provably alive, and when the last holder drops, the guard kills it
//! and unlinks its socket.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use rdlt_connector::{
    ConnectorSpec, Destination, DestinationCapabilities, DestinationError, LoadSession,
    OpenContext, ReadRequest, Source, SourceError, StreamSpec,
};
use rdlt_connector_client::{HandshakeOutcome, destination, source};
use tokio::process::Child;

/// Owns a spawned connector process and the socket path its handshake
/// line advertised. Dropping the guard is the shutdown: `start_kill`
/// on the child (best-effort, non-blocking — tokio's reaper collects
/// the exit) plus a best-effort unlink of the socket file, closing the
/// two 038-carried items (socket unlink on shutdown; liveness = the
/// guard riding the engine's `Arc`). The provider owns the spawn
/// lifecycle end to end: it creates the guard the moment the handshake
/// line names a socket, so every later failure path kills and unlinks
/// by the same drop.
///
/// The spawning `Command` also sets `kill_on_drop` as a belt beside
/// this guard's own `start_kill` — a child that never reaches a guard
/// (a failure between spawn and line) dies with its `Child`.
#[derive(Debug)]
pub struct LifecycleGuard {
    child: Child,
    socket_path: PathBuf,
}

impl LifecycleGuard {
    /// Guard `child`, unlinking `socket_path` when dropped.
    pub fn new(child: Child, socket_path: impl Into<PathBuf>) -> Self {
        Self {
            child,
            socket_path: socket_path.into(),
        }
    }

    /// The guarded process id — `None` once the child has been reaped.
    /// A test seam as much as telemetry: crash arms kill the process by
    /// this pid to measure what a run does when its connector dies.
    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    /// The socket path the drop will unlink.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

impl Drop for LifecycleGuard {
    fn drop(&mut self) {
        // Best-effort and non-blocking, both halves: `start_kill` only
        // SENDS the signal (no await, no wait — tokio reaps the exit in
        // the background), and a socket that is already gone is not an
        // error worth surfacing from a destructor.
        let _ = self.child.start_kill();
        // Unlink ONLY a socket: the path came verbatim from the child's
        // stdout handshake line, so a connector naming an unrelated
        // file must not commission this host to delete it. The check
        // rides `symlink_metadata` — a symlink AT the path is already
        // not a socket, and following it would judge the wrong inode.
        let is_socket = std::fs::symlink_metadata(&self.socket_path)
            .map(|meta| {
                use std::os::unix::fs::FileTypeExt as _;
                meta.file_type().is_socket()
            })
            .unwrap_or(false);
        if is_socket {
            let _ = std::fs::remove_file(&self.socket_path);
        }
    }
}

/// A provided source: the wire adapter, what the handshake
/// established about the connector, and the process guard. Implements
/// [`Source`] by delegation — hand it to `Engine::new` as-is.
#[derive(Debug)]
pub struct ManagedSource {
    adapter: source::Source,
    identity: String,
    resolved_version: String,
    negotiated_protocol: u32,
    state_format_versions: BTreeMap<String, u32>,
    guard: Option<LifecycleGuard>,
}

impl ManagedSource {
    /// Wrap a connected adapter. `identity` is the requirement's id —
    /// the handshake already verified the connector reported the same
    /// (D-039-2), so recording the requirement's spelling is recording
    /// the truth. The version/protocol/state-format fields come from
    /// `outcome`; `guard` is `None` for a provider whose connector's
    /// lifetime is managed elsewhere (a pool, an embedder's own child).
    pub fn new(
        adapter: source::Source,
        identity: impl Into<String>,
        outcome: &HandshakeOutcome,
        guard: Option<LifecycleGuard>,
    ) -> Self {
        Self {
            adapter,
            identity: identity.into(),
            resolved_version: outcome.connector_version.clone(),
            negotiated_protocol: outcome.negotiated_protocol,
            state_format_versions: outcome.state_format_versions.clone(),
            guard,
        }
    }

    /// The connector id this source was required (and verified) to be.
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// The version the connector reported in its handshake — the
    /// WIRE's value, checked against the requirement only when it
    /// pins a version (D-039-2); an unpinned requirement carries it
    /// as reported. Spec-vs-wire version skew is not refused on this
    /// path — that judgment is the certifier's P3 clause.
    pub fn resolved_version(&self) -> &str {
        &self.resolved_version
    }

    /// The protocol version both sides settled on.
    pub fn negotiated_protocol(&self) -> u32 {
        self.negotiated_protocol
    }

    /// Per-state-kind format versions from the handshake. THREADED
    /// only in 039 (the v0 empty-map hole, recorded): exposed so an
    /// embedder can read them, with negotiation semantics owned by the
    /// feature that adds a second format version.
    pub fn state_format_versions(&self) -> &BTreeMap<String, u32> {
        &self.state_format_versions
    }

    /// The process guard, when this source owns one — crash arms reach
    /// the pid/socket through it.
    pub fn guard(&self) -> Option<&LifecycleGuard> {
        self.guard.as_ref()
    }
}

#[async_trait]
impl Source for ManagedSource {
    fn spec(&self) -> ConnectorSpec {
        self.adapter.spec()
    }

    async fn check(&self) -> Result<(), SourceError> {
        self.adapter.check().await
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        self.adapter.streams().await
    }

    async fn read(&self, request: ReadRequest) -> Result<(), SourceError> {
        self.adapter.read(request).await
    }
}

/// [`ManagedSource`]'s destination mirror: the wire adapter, the
/// handshake's findings, the guard — a [`Destination`] by delegation.
#[derive(Debug)]
pub struct ManagedDestination {
    adapter: destination::Destination,
    identity: String,
    resolved_version: String,
    negotiated_protocol: u32,
    state_format_versions: BTreeMap<String, u32>,
    guard: Option<LifecycleGuard>,
}

impl ManagedDestination {
    /// Wrap a connected adapter — see [`ManagedSource::new`] for the
    /// field provenance.
    pub fn new(
        adapter: destination::Destination,
        identity: impl Into<String>,
        outcome: &HandshakeOutcome,
        guard: Option<LifecycleGuard>,
    ) -> Self {
        Self {
            adapter,
            identity: identity.into(),
            resolved_version: outcome.connector_version.clone(),
            negotiated_protocol: outcome.negotiated_protocol,
            state_format_versions: outcome.state_format_versions.clone(),
            guard,
        }
    }

    /// The connector id this destination was required (and verified)
    /// to be.
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// The version the connector reported in its handshake — the
    /// WIRE's value, checked against the requirement only when it
    /// pins a version (D-039-2); an unpinned requirement carries it
    /// as reported. Spec-vs-wire version skew is not refused on this
    /// path — that judgment is the certifier's P3 clause.
    pub fn resolved_version(&self) -> &str {
        &self.resolved_version
    }

    /// The protocol version both sides settled on.
    pub fn negotiated_protocol(&self) -> u32 {
        self.negotiated_protocol
    }

    /// Per-state-kind format versions from the handshake — threaded
    /// only, see [`ManagedSource::state_format_versions`].
    pub fn state_format_versions(&self) -> &BTreeMap<String, u32> {
        &self.state_format_versions
    }

    /// The process guard, when this destination owns one.
    pub fn guard(&self) -> Option<&LifecycleGuard> {
        self.guard.as_ref()
    }
}

#[async_trait]
impl Destination for ManagedDestination {
    fn spec(&self) -> ConnectorSpec {
        self.adapter.spec()
    }

    async fn check(&self) -> Result<(), DestinationError> {
        self.adapter.check().await
    }

    fn capabilities(&self) -> DestinationCapabilities {
        self.adapter.capabilities()
    }

    async fn open(&self, context: OpenContext) -> Result<Box<dyn LoadSession>, DestinationError> {
        self.adapter.open(context).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The delegation claim, held at compile time: the managed types
    /// ARE the SPI traits, so `Engine::new` takes them unchanged (the
    /// live proof — a run over spawned binaries — is T8's headline).
    #[test]
    fn managed_types_are_the_spi_traits() {
        fn is_source<T: Source>() {}
        fn is_destination<T: Destination>() {}
        is_source::<ManagedSource>();
        is_destination::<ManagedDestination>();
    }
}
