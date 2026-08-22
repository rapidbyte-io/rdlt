//! Plumbing shared by both served roles: the Unix domain socket the
//! handshake line advertises, [`Error`], the handshake choreography,
//! and the two wire shapes a refusal takes.
//!
//! THE STATUS-VS-ERRORFRAME RULE. Which shape a refusal takes tracks
//! whether the thing that went wrong is a violation of the PROTOCOL's
//! own state machine or an outcome the CONNECTOR itself decided. A
//! protocol-state violation — any RPC but `Handshake` and the config-free
//! `Spec` arriving before a handshake has completed, or a second
//! concurrent `OpenSession` on one served process — answers as a raw
//! gRPC `Status` (`Code::FailedPrecondition`) ending the RPC outright:
//! there was no valid session for a payload-shaped outcome to be
//! reported INTO. Everything else — a `Handshake` RPC's OWN refusals
//! (bad role, out-of-range version, undecodable or invalid config), a
//! `Read` request whose documents fail to decode, and every refusal
//! reachable once a destination session is open (write-before-`Open`,
//! write-before-`Ensure`, a second `Open`, an empty frame, a connector's
//! own classified failure) — answers as a [`proto::ErrorFrame`] carried
//! as reply payload inside a stream/RPC that itself completes normally,
//! because those are DATA a caller is meant to inspect uniformly: a bad
//! config is not a protocol bug, and neither is a transient write
//! failure. A caller therefore checks both: `Status` for "you broke the
//! protocol's own sequencing", a typed `ErrorFrame` for "the connector
//! is telling you something".
//!
//! Not here: gRPC client flow-control window sizing. h2's window is what
//! bounds a `Read` stream's in-flight bytes, and that knob belongs to the
//! side that DIALS a connector — a listener built here has only the
//! accept side of the connection.

use std::io;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rdlt_connector::{gate, spec};
use rdlt_connector_protocol::handshake::Line;
use rdlt_connector_protocol::proto;
use rdlt_connector_protocol::{MAX_FRAME_BYTES, PROTOCOL_VERSION};
use tokio::net::UnixListener;

/// Failures standing up or running a served listener itself — never a
/// connector's own classified failure, which rides [`proto::ErrorFrame`]
/// over the wire instead of ending the process.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Creating the private per-process socket directory failed — before
    /// any bind was even attempted.
    #[error("creating the private socket directory at {path}: {source}")]
    SocketDir {
        /// The directory whose creation failed.
        path: PathBuf,
        /// The underlying IO failure.
        #[source]
        source: io::Error,
    },
    /// Binding the Unix domain socket failed (a path collision with a
    /// live listener, a parent directory that does not exist, a
    /// permissions problem).
    #[error("binding the connector socket at {path}: {source}")]
    Bind {
        /// Where the bind was attempted.
        path: PathBuf,
        /// The underlying IO failure.
        #[source]
        source: io::Error,
    },
    /// Restricting the freshly bound socket to its owner failed.
    #[error("setting socket permissions at {path}: {source}")]
    Permissions {
        /// Where the bind was attempted.
        path: PathBuf,
        /// The underlying IO failure.
        #[source]
        source: io::Error,
    },
    /// Writing the handshake line to stdout failed — the one thing a
    /// spawning host is waiting to read.
    #[error("writing the handshake line to stdout: {0}")]
    Stdout(#[source] io::Error),
    /// The gRPC server exited with a transport-level error.
    #[error("connector server: {0}")]
    Serve(#[source] tonic::transport::Error),
    /// The server task itself panicked or was cancelled.
    #[error("connector server task did not complete: {0}")]
    Join(#[source] tokio::task::JoinError),
}

/// A fresh socket path inside a PRIVATE per-process directory under the
/// system temp directory — what the `run` entry points bind to when a
/// spawned connector process has no path of its own to offer.
///
/// A private directory, not a bare pid-derived name in the shared
/// world-writable temp dir: a predictable name there could be
/// pre-created by another local user (the bind fails) or planted as a
/// dangling symlink, which `bind(2)` follows during path resolution,
/// landing the socket at an attacker-chosen location. The directory is
/// created mode `0700` with an unpredictable name BEFORE the socket
/// binds inside it, so both the bind and [`bind`]'s chmod-to-`0600` run
/// in space nothing else can reach.
///
/// A served process dies by SIGKILL, so nothing can reclaim its OWN
/// directory on exit — instead each startup sweeps dead predecessors'
/// directories before minting its own.
pub fn socket_path() -> Result<PathBuf, Error> {
    let base = std::env::temp_dir();
    sweep_dead_serve_dirs(&base);
    Ok(create_private_dir(&base)?.join("connector.sock"))
}

/// Reclaim dead predecessors' socket directories — rmdir-only over the
/// `rdlt-serve-*` entries of `base`, every failure ignored.
///
/// rmdir-only is the WHOLE liveness check, and it is sufficient and
/// race-free — but only because of ITS OTHER HALF, which lives in a
/// different crate: `rdlt_runtime::managed::Guard::drop` unlinks the
/// socket FILE on every cleanup path (a spawned connector's consumer
/// holds the Guard), so a dead predecessor's directory is EMPTY and
/// rmdir succeeds exactly then; a LIVE sibling's directory still holds
/// its socket, so rmdir fails harmlessly; rmdir never follows
/// symlinks; other users' directories fail on permissions. The two
/// halves are pinned together end-to-end by
/// `rdlt-runtime`'s `a_dead_predecessors_serve_dir_is_empty_and_rmdir_able`.
/// Accepted: between a concurrent sibling's mkdir and its bind, its
/// directory is briefly empty and a sweep could rmdir it — that
/// sibling's bind then fails loudly with [`Error::Bind`], never
/// silently, and the window is a few syscalls wide.
fn sweep_dead_serve_dirs(base: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(base) else {
        return 0;
    };
    let mut reclaimed = 0usize;
    for entry in entries.flatten() {
        if entry
            .file_name()
            .as_encoded_bytes()
            .starts_with(b"rdlt-serve-")
            && std::fs::remove_dir(entry.path()).is_ok()
        {
            reclaimed += 1;
        }
    }
    reclaimed
}

/// `sweep_dead_serve_dirs` run on demand — the operator's reclaim
/// verb (`rdlt reclaim`), so clearing a crashed connector's debris does
/// not wait for the next spawn to do it. Same rmdir-only rule, same
/// liveness guarantee: a LIVE sibling's directory still holds its
/// socket and refuses.
pub fn reclaim_dead_serve_dirs() -> usize {
    sweep_dead_serve_dirs(&std::env::temp_dir())
}

/// Create `base/rdlt-serve-<random>` mode `0700`, retrying on a name
/// collision with a fresh token — the std-only shape of `mkdtemp`
/// (delete-on-drop would be useless: a served process dies by SIGKILL,
/// never by unwinding).
fn create_private_dir(base: &Path) -> Result<PathBuf, Error> {
    create_private_dir_with(base, |path| {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
    })
}

/// [`create_private_dir`] over an injectable mode-normalizing step —
/// the seam its failure path is proven through.
fn create_private_dir_with(
    base: &Path,
    normalize_mode: impl Fn(&Path) -> io::Result<()>,
) -> Result<PathBuf, Error> {
    use std::hash::{BuildHasher, Hasher};
    use std::os::unix::fs::DirBuilderExt;

    let mut last_error: Option<(PathBuf, io::Error)> = None;
    for _ in 0..16 {
        // `RandomState` is hashDoS-resistant entropy, not a CSPRNG —
        // which suffices because the SECURITY BOUNDARY is the atomic
        // mkdir-0700-or-fail below, not the name: a pre-created dir or
        // a planted symlink at a predicted name makes `create` fail
        // `AlreadyExists` (mkdir never follows a symlink at its final
        // component) and this loop retry, so guessing tokens buys at
        // most a bounded retry-DoS, never adoption of an attacker's
        // directory.
        let token = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish();
        let path = base.join(format!("rdlt-serve-{token:016x}"));
        let mut builder = std::fs::DirBuilder::new();
        // Mode at mkdir time — atomic, no chmod-after-create window. A
        // umask can only CLEAR bits from 0o700, so the normalization
        // below fixes an over-strict umask without any exposure in
        // between.
        builder.mode(0o700);
        match builder.create(&path) {
            Ok(()) => {
                // A directory whose mode could not be normalized is not
                // handed out AND not left behind: removed best-effort
                // (it is empty — nothing bound yet), the failure
                // reported.
                if let Err(source) = normalize_mode(&path) {
                    let _ = std::fs::remove_dir(&path);
                    return Err(Error::SocketDir { path, source });
                }
                return Ok(path);
            }
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                last_error = Some((path, source));
            }
            Err(source) => return Err(Error::SocketDir { path, source }),
        }
    }
    let (path, source) = last_error.expect("the retry loop ran at least once");
    Err(Error::SocketDir { path, source })
}

/// Bind a Unix domain socket at `path`, then restrict it to the owner
/// (mode `0600`).
///
/// `UnixListener::bind` refuses `AddrInUse` against an existing path
/// unconditionally, and that errno alone cannot tell a real collision
/// (a LIVE listener owns the path) from a stale file an unclean kill
/// left behind. Unlinking unconditionally would silently steal a live
/// listener's path, so on `AddrInUse` the path is probed: a connect
/// that FAILS means nothing usable is listening (stale — unlink and
/// retry); a connect that SUCCEEDS is a real collision, reported as
/// [`Error::Bind`]. The probe → unlink → rebind sequence has a TOCTOU
/// window in which another process could bind the same path; safe in
/// practice because production paths live in a private per-process
/// directory ([`socket_path`]) that no second binder ever targets.
pub fn bind(path: &Path) -> Result<UnixListener, Error> {
    match UnixListener::bind(path) {
        Ok(listener) => finish_bind(path, listener),
        Err(source) if source.kind() == io::ErrorKind::AddrInUse => {
            if std::os::unix::net::UnixStream::connect(path).is_ok() {
                return Err(Error::Bind {
                    path: path.to_path_buf(),
                    source,
                });
            }
            let is_socket = std::fs::symlink_metadata(path)
                .map(|metadata| metadata.file_type().is_socket())
                .unwrap_or(false);
            if !is_socket {
                return Err(Error::Bind {
                    path: path.to_path_buf(),
                    source,
                });
            }
            std::fs::remove_file(path).map_err(|source| Error::Bind {
                path: path.to_path_buf(),
                source,
            })?;
            let listener = UnixListener::bind(path).map_err(|source| Error::Bind {
                path: path.to_path_buf(),
                source,
            })?;
            finish_bind(path, listener)
        }
        Err(source) => Err(Error::Bind {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn finish_bind(path: &Path, listener: UnixListener) -> Result<UnixListener, Error> {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(|source| {
        Error::Permissions {
            path: path.to_path_buf(),
            source,
        }
    })?;
    Ok(listener)
}

/// The frozen pre-handshake refusal: every RPC but `Handshake` and the
/// config-free `Spec`, on either role, answers this as a raw
/// `Status::failed_precondition` until a handshake populates the shell.
pub(super) const HANDSHAKE_NOT_COMPLETED: &str = "handshake has not completed";

/// Every role the protocol defines — tells "asked for the OTHER
/// recognized role" (a real mismatch) apart from "asked for a role that
/// isn't a role at all" (a typo or a version skew).
const KNOWN_ROLES: [&str; 2] = ["source", "destination"];

/// The typed size refusal for an oversized `*_json` document payload
/// against the document ceiling; the tighter cursor ceiling has its own
/// spelling ([`oversized_cursor`]) so the refusal names the bound that
/// actually refused.
pub(super) fn oversized_document(field: &str, len: usize, ceiling: u64) -> String {
    format!(
        "{field} is {len} bytes — larger than the {ceiling}-byte document \
         ceiling; a config or cursor document measures in kilobytes, so a payload this \
         size is a wrong path, refused before it can expand in memory"
    )
}

/// The cursor twin: the bound here is the CURSOR contract's own ceiling,
/// deliberately tighter than the document ceiling — a cursor summarizes
/// resume state (a high-water mark, an offset, a resume token), it never
/// embeds the data.
pub(super) fn oversized_cursor(field: &str, len: usize, ceiling: u64) -> String {
    format!(
        "{field} is {len} bytes — larger than the {ceiling}-byte cursor \
         ceiling; a cursor summarizes resume state and measures in kilobytes, so a \
         payload this size is a wrong path, refused before it can expand in memory"
    )
}

/// Build one [`proto::ErrorFrame`] — every error path converges here
/// rather than constructing the message by hand at each site.
pub(crate) fn error_frame(
    classification: proto::Classification,
    message: impl Into<String>,
    retry_after: Option<Duration>,
) -> proto::ErrorFrame {
    proto::ErrorFrame {
        classification: classification as i32,
        message: message.into(),
        retry_after_ms: retry_after.map(|duration| duration.as_millis() as u64),
    }
}

/// One classified SPI error seen from the serving side. The trait is
/// sdk-local on purpose (no public SPI surface change): it exists so
/// BOTH roles' flatteners share one frame construction and one doc,
/// while each error enum keeps its own arms — they are distinct
/// `#[non_exhaustive]` types, and a wildcard arm per impl is what
/// keeps an unknown FUTURE variant landing FATAL safe-loud.
///
/// `ErrorFrame.message` is the CAUSE text, never the SPI's `Display`
/// frame: the receiving client renders the classification frame exactly
/// once on reconstruction, and a foreign server authoring its own cause
/// text cannot know rdlt's spellings anyway. Nothing is lost — context
/// is already folded into the inner cause. The wildcard arm keeps the
/// full rendered `Display` rather than dropping text — a fallback that
/// may double-frame on reconstruction, preferred over dropping it.
pub(super) trait ClassifiedError: std::fmt::Display {
    /// The classification as the enum, the INNER cause's text as the
    /// message, and the rate-limit hint when there is one.
    fn classified_parts(&self) -> (proto::Classification, String, Option<Duration>);
}

/// Flatten a classified SPI error into the wire's [`proto::ErrorFrame`]
/// — both roles' refusal paths converge here.
pub(super) fn error_frame_of(error: &impl ClassifiedError) -> proto::ErrorFrame {
    let (classification, message, retry_after) = error.classified_parts();
    error_frame(classification, message, retry_after)
}

impl ClassifiedError for rdlt_connector::error::SourceError {
    fn classified_parts(&self) -> (proto::Classification, String, Option<Duration>) {
        use rdlt_connector::error::SourceError as E;
        match self {
            E::Transient(cause) => (proto::Classification::Transient, cause.to_string(), None),
            E::RateLimited {
                retry_after,
                source,
            } => (
                proto::Classification::RateLimited,
                source.to_string(),
                *retry_after,
            ),
            E::Fatal(cause) => (proto::Classification::Fatal, cause.to_string(), None),
            _ => (proto::Classification::Fatal, self.to_string(), None),
        }
    }
}

impl ClassifiedError for rdlt_connector::error::DestinationError {
    fn classified_parts(&self) -> (proto::Classification, String, Option<Duration>) {
        use rdlt_connector::error::DestinationError as E;
        match self {
            E::Transient(cause) => (proto::Classification::Transient, cause.to_string(), None),
            E::RateLimited {
                retry_after,
                source,
            } => (
                proto::Classification::RateLimited,
                source.to_string(),
                *retry_after,
            ),
            E::Fatal(cause) => (proto::Classification::Fatal, cause.to_string(), None),
            _ => (proto::Classification::Fatal, self.to_string(), None),
        }
    }
}

/// A `Check` RPC's reply from a connector outcome — both roles map
/// their classified error through [`error_frame_of`] into the same
/// reply shape.
pub(super) fn check_reply_of<E: ClassifiedError>(outcome: Result<(), E>) -> proto::CheckReply {
    let outcome = match outcome {
        Ok(()) => proto::check_reply::Outcome::Ok(proto::Empty {}),
        Err(error) => proto::check_reply::Outcome::Error(error_frame_of(&error)),
    };
    proto::CheckReply {
        outcome: Some(outcome),
    }
}

/// Run the connector's own code behind one admission permit: what a
/// call costs is the connector's business, and unbounded concurrent
/// ones are the caller's choice, not the connector's. For `Handshake`
/// this stops only a SECOND success — every failed attempt parses its
/// document and runs the connector's validate and assemble, which is
/// where pools are built and keys are read, so an unbounded flood of
/// failing handshakes is an unbounded flood into the connector.
pub(super) async fn admitted<T>(
    admission: &Arc<tokio::sync::Semaphore>,
    probe: impl std::future::Future<Output = Result<T, tonic::Status>>,
) -> Result<T, tonic::Status> {
    // Held for the probe's duration: the permit IS the admission.
    let _admitted = admission
        .clone()
        .try_acquire_owned()
        .map_err(|_| connector_calls_exhausted())?;
    probe.await
}

// ---- the shared listener scaffold ------------------------------------------

/// Frame caps on either generated service server: tonic's 4 MiB default
/// receive cap is below what one legitimate frame may carry, and the
/// encode cap pins the same ceiling on replies. ONE spelling of the
/// pair; every served service carries it.
pub(super) trait FrameCapped: Sized {
    /// Both caps at [`MAX_FRAME_BYTES`].
    fn frame_capped(self) -> Self;
}

macro_rules! frame_capped_for {
    ($proto:ident :: $module:ident :: $server:ident : $service:path) => {
        impl<C: $service> FrameCapped for $proto::$module::$server<C> {
            fn frame_capped(self) -> Self {
                self.max_decoding_message_size(MAX_FRAME_BYTES)
                    .max_encoding_message_size(MAX_FRAME_BYTES)
            }
        }
    };
}

// The generated servers' cap methods are inherent per type, so the
// trait gets one impl per generated family — the caps themselves are
// spelled once above.
frame_capped_for!(
    proto::connector_server::ConnectorServer : proto::connector_server::Connector
);
frame_capped_for!(
    proto::source_service_server::SourceServiceServer
        : proto::source_service_server::SourceService
);
frame_capped_for!(
    proto::destination_service_server::DestinationServiceServer
        : proto::destination_service_server::DestinationService
);

/// Bind at `path`, serve BOTH wire services, and hand back the
/// handshake line a spawning host reads plus the serving task's handle
/// — WITHOUT printing anything. [`run_on`](super) entry points are this
/// plus their role's two services.
pub(super) async fn bind_and_serve<F, Fut>(
    path: &Path,
    build: F,
) -> Result<(Line, tokio::task::JoinHandle<Result<(), Error>>), Error>
where
    F: FnOnce(Admitted<tokio_stream::wrappers::UnixListenerStream>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), tonic::transport::Error>> + Send + 'static,
{
    let listener = bind(path)?;
    let serving = build(admit_connections(
        tokio_stream::wrappers::UnixListenerStream::new(listener),
    ));
    let handle = tokio::spawn(async move { serving.await.map_err(Error::Serve) });
    Ok((handshake_line(path), handle))
}

/// A served connector holds at most this many connections open at once.
///
/// Past it a new connection is accepted and closed at once, unserved.
/// Every connection is a descriptor, a task and an HTTP/2 session
/// before a single request arrives, so with no bound any process that
/// can merely REACH the listener — the TCP binding's weakest adversary,
/// needing neither a handshake nor a frame — parks connections until the
/// descriptor table is full and every honest client is refused. A
/// client has no say in this refusal, which is the point: the bound
/// must hold against one that never speaks.
///
/// Generous against honest use: a host holds one connection per
/// connector process, and a provider fronting several pipelines a
/// handful. The same ceiling guards the private socket — a rogue
/// same-UID client is the standing adversary there.
pub const MAX_CONNECTIONS: usize = 64;

/// Streams open at once on ONE connection — the HTTP/2 setting the
/// peer is told and that the transport enforces with `REFUSED_STREAM`
/// past it. The sum of what the two admission seats admit
/// process-wide: one connection can legitimately carry them all, and
/// a count above it is a stream flood the per-call semaphores never
/// see — `Spec` is admission-exempt by design, and each open stream
/// costs a task and its headers before any gate runs. Refused at the
/// transport, never queued: a queue is memory the peer fills at will.
pub const MAX_REQUESTS_PER_CONNECTION: usize =
    MAX_CONCURRENT_CONNECTOR_CALLS + gate::MAX_DECLARED_STREAM_SPECS;

/// How often a silent connection is pinged, and how long the peer has
/// to answer before the connection is reaped. A peer that vanished —
/// a crashed host, a black-holed route — otherwise holds its connection
/// slot and every stream on it forever. The pings start once the peer
/// has spoken HTTP/2; before that [`PREFACE_DEADLINE`] is the reaper.
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
/// See [`KEEPALIVE_INTERVAL`].
pub const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// How long an admitted connection has to make its first request.
///
/// Until a request arrives nothing else can reap the connection: the
/// HTTP/2 keepalive arms only once the peer has completed the
/// protocol's own handshake, so a peer that connects and sends nothing,
/// one byte, a partial preface, or a preface with no settings — live,
/// or vanished at any of those points — would hold its slot under
/// [`MAX_CONNECTIONS`] until the process restarted, and a handful of
/// them would exclude every honest client for good. A request is the
/// one event that proves the whole stack below it is established, so
/// it is the one event that disarms this deadline; past it the read
/// fails, the connection drops, and the slot returns. An honest client
/// makes its first request (`Spec` or `Handshake`) the moment it
/// connects.
pub const ESTABLISHMENT_DEADLINE: Duration = Duration::from_secs(10);

/// The server builder every served role starts from: the per-connection
/// stream ceiling, the dead-peer keepalive, and the middleware that
/// marks a connection established on its first request — stated once
/// so the private socket and the TCP binding cannot drift apart in
/// what they bound. No TCP-level keepalive: tonic ignores it for a
/// caller-supplied accept stream, and the HTTP/2 pings reap what it
/// would have.
pub(super) fn server_builder()
-> tonic::transport::Server<tower_layer::Stack<EstablishedLayer, tower_layer::Identity>> {
    tonic::transport::Server::builder()
        .max_concurrent_streams(Some(MAX_REQUESTS_PER_CONNECTION as u32))
        .http2_keepalive_interval(Some(KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
        .layer(EstablishedLayer)
}

/// What a [`Connection`] tells the services about itself: the flag a
/// request flips to prove the connection established. Carried as the
/// request's connect-info extension, where [`EstablishedLayer`] finds
/// it.
#[derive(Debug, Clone)]
pub struct Liveness(Arc<std::sync::atomic::AtomicBool>);

impl Liveness {
    fn armed() -> Self {
        Self(Arc::new(std::sync::atomic::AtomicBool::new(false)))
    }
    fn establish(&self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
    fn established(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Acquire)
    }
}

/// The middleware that flips a connection's [`Liveness`] on every
/// request it carries — any request, before routing, so even a call
/// the services refuse counts as the peer having established itself.
#[derive(Debug, Clone, Copy)]
pub struct EstablishedLayer;

impl<S> tower_layer::Layer<S> for EstablishedLayer {
    type Service = Established<S>;
    fn layer(&self, inner: S) -> Self::Service {
        Established { inner }
    }
}

/// See [`EstablishedLayer`].
#[derive(Debug, Clone)]
pub struct Established<S> {
    inner: S,
}

impl<S, B> tonic::codegen::Service<tonic::codegen::http::Request<B>> for Established<S>
where
    S: tonic::codegen::Service<tonic::codegen::http::Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: tonic::codegen::http::Request<B>) -> Self::Future {
        if let Some(liveness) = request.extensions().get::<Liveness>() {
            liveness.establish();
        }
        self.inner.call(request)
    }
}

/// A connection admitted under [`MAX_CONNECTIONS`], holding its slot
/// for as long as it lives — which, until its first request arrives,
/// is at most [`ESTABLISHMENT_DEADLINE`].
#[derive(Debug)]
pub struct Connection<T> {
    io: T,
    liveness: Liveness,
    /// Armed at admission; consulted until the first request lands.
    deadline: std::pin::Pin<Box<tokio::time::Sleep>>,
    _slot: tokio::sync::OwnedSemaphorePermit,
}

impl<T: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for Connection<T> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        if !self.liveness.established()
            && std::future::Future::poll(self.deadline.as_mut(), cx).is_ready()
        {
            return std::task::Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "no request within the {}-second establishment deadline — the \
                     connection is closed and its slot released",
                    ESTABLISHMENT_DEADLINE.as_secs()
                ),
            )));
        }
        std::pin::Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl<T: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for Connection<T> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.io).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_shutdown(cx)
    }
    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
}

impl<T> tonic::transport::server::Connected for Connection<T> {
    type ConnectInfo = Liveness;
    fn connect_info(&self) -> Self::ConnectInfo {
        self.liveness.clone()
    }
}

/// The accept stream under [`MAX_CONNECTIONS`]: each connection the
/// listener yields takes a slot or is dropped on the spot. Dropping is
/// closing — the peer sees a reset, never a served session — and it
/// costs the process one accept, which is what the listener's backlog
/// already charges.
#[derive(Debug)]
pub struct Admitted<S> {
    incoming: S,
    slots: Arc<tokio::sync::Semaphore>,
}

pub(super) fn admit_connections<S>(incoming: S) -> Admitted<S> {
    Admitted {
        incoming,
        slots: Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS)),
    }
}

impl<S, T, E> tokio_stream::Stream for Admitted<S>
where
    S: tokio_stream::Stream<Item = Result<T, E>> + Unpin,
{
    type Item = Result<Connection<T>, E>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            let next = match std::pin::Pin::new(&mut self.incoming).poll_next(cx) {
                std::task::Poll::Ready(next) => next,
                std::task::Poll::Pending => return std::task::Poll::Pending,
            };
            return match next {
                None => std::task::Poll::Ready(None),
                Some(Err(error)) => std::task::Poll::Ready(Some(Err(error))),
                Some(Ok(io)) => match Arc::clone(&self.slots).try_acquire_owned() {
                    Ok(slot) => std::task::Poll::Ready(Some(Ok(Connection {
                        io,
                        liveness: Liveness::armed(),
                        deadline: Box::pin(tokio::time::sleep(ESTABLISHMENT_DEADLINE)),
                        _slot: slot,
                    }))),
                    // Past the ceiling: `io` drops here, closing it, and
                    // the next accept is polled in the same turn.
                    Err(_) => continue,
                },
            };
        }
    }
}

/// The handshake line for an ALREADY-BOUND socket path.
fn handshake_line(path: &Path) -> Line {
    Line {
        socket_path: path.to_path_buf(),
        protocol_min: PROTOCOL_VERSION,
        protocol_max: PROTOCOL_VERSION,
    }
}

/// Print the handshake line to stdout, flushed — the spawning host is
/// reading a pipe, not a TTY. `writeln!`, not `println!`: a spawning
/// host that exits or never reads its child's stdout leaves this write
/// facing a broken pipe, and `println!` panics on an IO error rather
/// than surfacing one.
pub(super) fn announce(line: &Line) -> Result<(), Error> {
    use std::io::Write as _;
    let mut stdout = std::io::stdout();
    writeln!(stdout, "{}", line.render()).map_err(Error::Stdout)?;
    stdout.flush().map_err(Error::Stdout)?;
    Ok(())
}

/// Answer the config-free `Spec` RPC: the connector's static identity
/// serialized as `ConnectorSpec` JSON. Answerable BEFORE any handshake
/// by construction — it reads statics alone and never touches a
/// handshake-populated shell, which is how a provider asks a spawned
/// connector what it IS before deciding what config to hand it.
pub(crate) fn spec_reply(
    name: &'static str,
    version: &'static str,
    config_schema: Option<serde_json::Value>,
) -> tonic::Response<proto::SpecReply> {
    let mut spec = spec::ConnectorSpec::new(name, version);
    spec.config_schema = config_schema;
    tonic::Response::new(proto::SpecReply {
        spec_json: serde_json::to_vec(&spec)
            .expect("a ConnectorSpec serializes to JSON infallibly"),
    })
}

/// Refuse a handshake with a FATAL [`proto::ErrorFrame`] carrying
/// `message`.
pub(crate) fn refuse_handshake(
    message: impl Into<String>,
) -> tonic::Response<proto::HandshakeReply> {
    tonic::Response::new(proto::HandshakeReply {
        outcome: Some(proto::handshake_reply::Outcome::Error(error_frame(
            proto::Classification::Fatal,
            message,
            None,
        ))),
    })
}

/// The most DISTINCT scalar values the redaction will hunt for, and
/// the most bytes they may total. A config document inside the byte
/// ceiling can carry a million tiny scalars; hunting each of them in
/// every spelling through a refusal is work the refusal's author
/// chose, not the connector. Past either bound the refusal is
/// rendered value-free instead — the connector's wording withheld,
/// the fact of the refusal and the reason for the withholding kept.
const MAX_REDACTION_VALUES: usize = 4096;
const MAX_REDACTION_VALUE_BYTES: usize = 1 << 20;

/// What the redaction renders when it cannot run safely.
const VALUE_FREE_REFUSAL: &str = "the connector refused the configuration; its wording is \
                                  withheld because the document carries more values than \
                                  can be redacted from it safely";

/// Every distinct non-null scalar LEAF the config document carries —
/// the values [`redact_values`] shields. Values only, never keys: keys
/// are the document's schema, and refusals legitimately name fields.
/// Computed from the RAW BYTES by re-parsing them, so a SUCCESSFUL
/// handshake pays neither this parse nor the per-leaf clones (the
/// refusal arm no longer holds the parsed document — the failed
/// construction consumed it). The walk is iterative, deduplicates as
/// it goes, and stops at the redaction bounds: `None` means the
/// document is past what can be redacted.
fn config_scalar_values(config_json: &[u8]) -> Option<std::collections::BTreeSet<String>> {
    let Ok(config) = serde_json::from_slice::<serde_json::Value>(config_json) else {
        return Some(Default::default());
    };
    let mut values = std::collections::BTreeSet::new();
    let mut bytes = 0usize;
    let mut pending = vec![&config];
    while let Some(value) = pending.pop() {
        let scalar = match value {
            serde_json::Value::String(text) if !text.is_empty() => text.clone(),
            serde_json::Value::Number(number) => number.to_string(),
            serde_json::Value::Bool(boolean) => boolean.to_string(),
            serde_json::Value::Array(items) => {
                pending.extend(items.iter());
                continue;
            }
            serde_json::Value::Object(fields) => {
                pending.extend(fields.values());
                continue;
            }
            _ => continue,
        };
        if values.contains(&scalar) {
            continue;
        }
        bytes = bytes.saturating_add(scalar.len());
        if values.len() >= MAX_REDACTION_VALUES || bytes > MAX_REDACTION_VALUE_BYTES {
            return None;
        }
        values.insert(scalar);
    }
    Some(values)
}

/// Redact every config scalar value out of a connector-rendered
/// refusal before it crosses the wire: the connector's own wording is
/// the seam's contract, but its serde parse arms quote parsed tokens,
/// so a secret handed to the wrong field would otherwise ride the
/// refusal into host logs. Each value is hunted in every spelling a
/// refusal can carry it — raw, its full `{:?}` Debug form (serde's
/// `Unexpected::Str` renders tokens that way, escaping quotes,
/// backslashes and control bytes), and the Debug body without its
/// surrounding quotes.
///
/// Bounded by construction: the message is capped FIRST, so the
/// matcher walks at most a few kilobytes; the match is ONE
/// left-to-right pass that never rescans what it inserted (an inserted
/// marker cannot match a value, so a value spelled like the marker
/// cannot grow the text without bound); and at each position the
/// longest spelling wins, so a form containing another redacts whole.
/// Wording that quotes no config value crosses back untouched.
fn redact_values(message: String, values: &std::collections::BTreeSet<String>) -> String {
    let message = gate::render_diagnostic(&message, gate::PANIC_TEXT_CAP);
    if values.is_empty() {
        return message;
    }
    let mut spellings: Vec<(String, &'static str)> = Vec::with_capacity(values.len() * 3);
    for value in values {
        spellings.push((format!("{value:?}"), "\"[redacted config value]\""));
        spellings.push((value.escape_debug().to_string(), "[redacted config value]"));
        spellings.push((value.clone(), "[redacted config value]"));
    }
    spellings.sort_by(|a, b| b.0.len().cmp(&a.0.len()).then_with(|| a.0.cmp(&b.0)));
    spellings.dedup_by(|a, b| a.0 == b.0);
    let mut out = String::with_capacity(message.len());
    let mut at = 0;
    while at < message.len() {
        let rest = &message[at..];
        match spellings
            .iter()
            .find(|(needle, _)| rest.starts_with(needle.as_str()))
        {
            Some((needle, marker)) => {
                out.push_str(marker);
                at += needle.len();
            }
            None => {
                let step = rest.chars().next().map_or(1, char::len_utf8);
                out.push_str(&rest[..step]);
                at += step;
            }
        }
    }
    out
}

/// What [`handshake`] needs from a served shell to run the choreography
/// once for either role — implemented for `source::Shell<C>` and
/// `destination::Shell<C>` in their own serve modules, so the ONE
/// function below drives both without either shell type depending on
/// the other's module.
pub(crate) trait HandshakeShell: Sized {
    /// The document gate's own error type — its `Display` is what a
    /// refused handshake carries verbatim (the connector's own wording,
    /// never text this crate invents).
    type Error: std::fmt::Display;

    /// Validate-then-build from an already-parsed config document.
    fn from_config(value: serde_json::Value) -> Result<Self, Self::Error>;
    /// `C::NAME` — the connector's stable identifier.
    fn connector_id(&self) -> &'static str;
    /// `C::VERSION`.
    fn connector_version(&self) -> &'static str;
    /// The shell's `ConnectorSpec`, pre-serialized.
    fn spec_json(&self) -> Vec<u8>;
    /// Destination capabilities, pre-serialized: the wire field carries
    /// the destination's capabilities document and is empty for sources.
    fn capabilities_json(&self) -> Vec<u8>;
    /// The state-format kinds this shell can resume, pre-serialized as
    /// the handshake's ceilinged JSON document. A destination declares
    /// `{"rdlt.state_doc": STATE_FORMAT_VERSION}` — what its SDK build
    /// understands; the client's `ReadState` seat enforces it. A source
    /// declares nothing (its cursors are engine-held, not
    /// connector-persisted), which is the empty document.
    fn state_format_versions_json(&self) -> Vec<u8>;
}

/// The handshake choreography shared by both roles: role check,
/// protocol-version check, document ceiling, config parse, shell
/// construction, spec/capabilities serialization, and the `OnceLock`
/// race. The one thing that differs between roles — the refusal's
/// OTHER-role name — is derived from `this_role` and [`KNOWN_ROLES`], so
/// every frozen refusal spelling lives exactly once.
pub(crate) fn handshake<S: HandshakeShell>(
    slot: &OnceLock<Arc<S>>,
    this_role: &'static str,
    request: proto::HandshakeRequest,
) -> tonic::Response<proto::HandshakeReply> {
    if slot.get().is_some() {
        return refuse_handshake("handshake already completed");
    }

    if request.expected_role != this_role {
        // The role is an inbound identifier like every other: past the
        // identifier ceiling it is refused by LENGTH, never echoed —
        // and a recognizable-length unknown role renders bounded.
        if request.expected_role.len() > gate::MAX_WIRE_IDENTIFIER_BYTES {
            return refuse_handshake(format!(
                "the handshake asked for a role of {} bytes — over the {}-byte wire \
                 identifier ceiling; roles are `source` and `destination`",
                request.expected_role.len(),
                gate::MAX_WIRE_IDENTIFIER_BYTES
            ));
        }
        if !KNOWN_ROLES.contains(&request.expected_role.as_str()) {
            return refuse_handshake(format!(
                "the handshake asked for role `{}`, which this connector does not recognize",
                gate::render_diagnostic(&request.expected_role, gate::MAX_WIRE_IDENTIFIER_BYTES)
            ));
        }
        let other_role = KNOWN_ROLES
            .iter()
            .find(|&&role| role != this_role)
            .expect("KNOWN_ROLES names exactly two roles");
        return refuse_handshake(format!(
            "this connector is a {this_role}; the handshake asked for a {other_role}"
        ));
    }

    // A range check in spirit — one supported version, so the range
    // collapses to `PROTOCOL_VERSION`. Written as `!=` because
    // `PROTOCOL_VERSION` is `u32`'s minimum and a `< min` bound is
    // vacuous; widen to an explicit range the day a second version
    // exists.
    if request.protocol_version != PROTOCOL_VERSION {
        return refuse_handshake(format!(
            "protocol version {} is outside this connector's supported range [{PROTOCOL_VERSION}, {PROTOCOL_VERSION}]",
            request.protocol_version
        ));
    }

    // The size gate runs BEFORE the parse: the frame cap bounds only the
    // bytes on the wire, and parsing a compact document into an untyped
    // `Value` multiplies them many times over in this process.
    let ceiling = gate::MAX_DOCUMENT_BYTES;
    if request.config_json.len() as u64 > ceiling {
        return refuse_handshake(oversized_document(
            "config_json",
            request.config_json.len(),
            ceiling,
        ));
    }

    // Both refusal arms below hold the protocol's secrecy rule (no
    // `*_json` payload is ever echoed verbatim — config documents carry
    // credentials): the parse arm renders the error's KIND and location
    // alone, and the typed arm redacts every config scalar value from
    // the connector's own wording before it crosses back over the wire.
    let config: serde_json::Value = match serde_json::from_slice(&request.config_json) {
        Ok(config) => config,
        Err(error) => {
            return refuse_handshake(format!(
                "invalid config_json: {}",
                gate::describe_parse_error(&error)
            ));
        }
    };

    let shell = match S::from_config(config) {
        Ok(shell) => shell,
        Err(error) => {
            // The redaction sweep lives lexically INSIDE this refusal
            // arm, and only here: it clones every scalar leaf of the
            // document, a whole-document expansion a successful
            // handshake must never pay.
            return refuse_handshake(match config_scalar_values(&request.config_json) {
                Some(values) => redact_values(error.to_string(), &values),
                None => VALUE_FREE_REFUSAL.to_string(),
            });
        }
    };

    let ok = proto::HandshakeOk {
        connector_id: shell.connector_id().to_string(),
        connector_version: shell.connector_version().to_string(),
        spec_json: shell.spec_json(),
        capabilities_json: shell.capabilities_json(),
        // Declared by the shell itself (see the trait method): a
        // destination declares its state-doc ceiling; a source sends
        // the empty document.
        state_format_versions_json: shell.state_format_versions_json(),
    };

    if slot.set(Arc::new(shell)).is_err() {
        // Lost a race against a concurrent handshake on the same
        // session — the same refusal either way.
        return refuse_handshake("handshake already completed");
    }

    tonic::Response::new(proto::HandshakeReply {
        outcome: Some(proto::handshake_reply::Outcome::Ok(ok)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_and_boolean_config_values_are_redacted_too() {
        let values =
            config_scalar_values(br#"{"password": 12345, "enabled": true}"#).expect("in bounds");
        let redacted = redact_values(
            "invalid type: integer `12345`; unexpected boolean `true`".to_string(),
            &values,
        );
        assert_eq!(
            redacted,
            "invalid type: integer `[redacted config value]`; unexpected boolean \
             `[redacted config value]`"
        );
    }

    /// The redaction is bounded by construction: a value spelled like
    /// the marker cannot grow the text (one pass, nothing it inserted is
    /// rescanned), a frame-sized refusal is capped before the matcher
    /// walks it, a document of a million tiny distinct scalars is
    /// refused value-free rather than hunted, and duplicates cost one
    /// needle. The matcher still redacts whole: the Debug spelling of a
    /// value, which contains its raw spelling, is replaced as one.
    #[test]
    fn the_redaction_is_bounded_and_never_rescans_what_it_inserted() {
        let values = config_scalar_values(br#"{"a": "redacted", "b": "config", "c": "["}"#)
            .expect("in bounds");
        let redacted = redact_values("bad value \"redacted\" near [".to_string(), &values);
        assert_eq!(
            redacted,
            "bad value \"[redacted config value]\" near [redacted config value]"
        );
        assert!(
            redacted.len() < 128,
            "a value spelled like the marker does not feed the matcher: {redacted}"
        );

        let flood = format!("x{}", "y".repeat(1 << 20));
        let redacted = redact_values(flood, &values);
        assert!(
            redacted.len() <= gate::PANIC_TEXT_CAP + 64,
            "the refusal is capped before it is walked: {}",
            redacted.len()
        );

        let mut many = String::from("[");
        for n in 0..=MAX_REDACTION_VALUES {
            if n > 0 {
                many.push(',');
            }
            many.push_str(&n.to_string());
        }
        many.push(']');
        assert!(
            config_scalar_values(many.as_bytes()).is_none(),
            "past the value ceiling the redaction declines rather than runs"
        );
        let dup = config_scalar_values(br#"["s","s","s","s"]"#).expect("in bounds");
        assert_eq!(dup.len(), 1, "duplicates cost one needle");
    }

    /// The private socket directory is owner-only from birth and its
    /// name is minted fresh per call — the two properties that close
    /// the predictable-path race (pre-create DoS, dangling-symlink
    /// redirect) at the source.
    #[test]
    fn the_private_socket_dir_is_owner_only_and_unpredictable() {
        let base = tempfile::tempdir().expect("tempdir");

        let first = create_private_dir(base.path()).expect("first private dir");
        let second = create_private_dir(base.path()).expect("second private dir");

        assert_ne!(first, second, "each call mints a fresh name");
        for dir in [&first, &second] {
            let name = dir
                .file_name()
                .and_then(|name| name.to_str())
                .expect("utf8 name");
            assert!(
                name.starts_with("rdlt-serve-"),
                "the sweep's prefix contract: got `{name}`"
            );
            let mode = std::fs::metadata(dir)
                .expect("dir metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o700, "the directory is private to its owner");
        }
    }

    /// A directory whose mode normalization fails is not left behind:
    /// the failure is reported and the directory removed, so a refused
    /// start leaves no `rdlt-serve-*` residue for the next sweep.
    #[test]
    fn a_failed_mode_normalization_leaves_no_directory_behind() {
        let base = tempfile::tempdir().expect("tempdir");
        let error = create_private_dir_with(base.path(), |_| {
            Err(io::Error::other("induced chmod failure"))
        })
        .expect_err("the induced failure refuses");
        let Error::SocketDir { path, .. } = error else {
            panic!("expected SocketDir, got {error:?}");
        };
        assert!(
            !path.exists(),
            "the directory is removed on the failure path"
        );
        assert_eq!(
            std::fs::read_dir(base.path()).expect("read_dir").count(),
            0,
            "nothing is left under the base"
        );
    }

    /// The startup sweep reclaims exactly the dead: an EMPTY
    /// `rdlt-serve-*` sibling (its socket unlinked by the consumer's
    /// cleanup) is removed; a sibling still holding its socket file
    /// survives, and so does anything outside the prefix.
    #[test]
    fn the_sweep_reclaims_empty_serve_dirs_and_nothing_else() {
        let base = tempfile::tempdir().expect("tempdir");
        let dead = base.path().join("rdlt-serve-dead");
        let live = base.path().join("rdlt-serve-live");
        let foreign = base.path().join("someone-elses-dir");
        for dir in [&dead, &live, &foreign] {
            std::fs::create_dir(dir).expect("fixture dir");
        }
        std::fs::write(live.join("connector.sock"), b"").expect("live marker");

        sweep_dead_serve_dirs(base.path());

        assert!(!dead.exists(), "the empty (dead) sibling is reclaimed");
        assert!(live.exists(), "a non-empty (live) sibling survives rmdir");
        assert!(
            foreign.exists(),
            "names outside rdlt-serve-* are never touched"
        );
    }

    /// An unrelated regular file is never treated as stale socket
    /// residue: the failed bind preserves both its bytes and path.
    #[tokio::test]
    async fn a_regular_file_at_the_socket_path_is_refused_and_preserved() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("stale.sock");
        std::fs::write(&path, b"not a socket").expect("write stale file");

        let error = bind(&path).expect_err("a regular file is never stale socket residue");
        assert!(matches!(error, Error::Bind { .. }));
        assert_eq!(
            std::fs::read(&path).expect("file survives"),
            b"not a socket"
        );
    }

    #[tokio::test]
    async fn an_actual_stale_socket_is_reclaimed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("stale.sock");
        drop(std::os::unix::net::UnixListener::bind(&path).expect("fixture socket"));

        let listener = bind(&path).expect("stale socket residue is reclaimable");
        drop(listener);
        assert!(
            std::fs::symlink_metadata(&path)
                .expect("replacement socket")
                .file_type()
                .is_socket()
        );
    }

    /// A path a LIVE listener already owns is a real collision, not a
    /// stale file — the probe-connect must refuse to unlink it out from
    /// under the first listener.
    #[tokio::test]
    async fn a_live_listener_at_the_same_path_is_a_collision_not_a_steal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("live.sock");

        let first = bind(&path).expect("first bind");

        let error = bind(&path).expect_err("a second bind must refuse, not steal the path");
        assert!(
            matches!(error, Error::Bind { .. }),
            "expected a Bind refusal, got {error:?}"
        );

        // The first listener is still the one at `path` — proven by
        // successfully connecting to it, not just by the second bind's
        // refusal.
        drop(tokio::net::UnixStream::connect(&path).await.expect(
            "the first listener is still live and accepting after the second bind refused",
        ));
        drop(first);
    }
}

/// A served connector admits at most this many concurrent calls to the
/// unary RPCs that run its OWN code — `Handshake`, `Check` and
/// `Streams`.
///
/// The other seats reaching a connector's backend have their own
/// bounds: reads have the per-source ceiling, sessions the one-session
/// slot. These three are what remains, and none is a free call. A
/// `Check` opens whatever the connector must touch to answer; a
/// `Streams` asks it to enumerate; and a `Handshake` — every FAILED
/// one, since the success slot only stops the second success — parses
/// a config document and runs the connector's own validate and
/// assemble, which is where pools are built and keys are read. What
/// any of that costs is the connector's business, which is exactly why
/// the count belongs here rather than in each connector: the sdk is
/// the template, and a template that admits unbounded concurrent calls
/// teaches every connector to.
///
/// Generous against honest use by orders of magnitude: a host shakes
/// hands once per connector, probes once per pipeline and enumerates
/// once per source, so the honest concurrent count is one. This bounds
/// a client that decided otherwise.
pub const MAX_CONCURRENT_CONNECTOR_CALLS: usize = 64;

/// The refusal a call past [`MAX_CONCURRENT_CONNECTOR_CALLS`] answers
/// with — a `Status`, like the other admission ceilings, because a full
/// ceiling is a protocol-state answer rather than something the
/// connector said.
pub fn connector_calls_exhausted() -> tonic::Status {
    tonic::Status::resource_exhausted(format!(
        "{MAX_CONCURRENT_CONNECTOR_CALLS} concurrent calls into the connector per process \
         — the ceiling is reached"
    ))
}

#[cfg(test)]
mod connector_admission_tests {
    use super::*;

    /// The probe ceiling refuses, and says which ceiling it was.
    ///
    /// Pinned at the seat rather than over the wire because holding
    /// permits open needs a connector that parks inside `check` or
    /// `streams`, and what matters here is the bound itself: the
    /// semaphore admits exactly its count, the next caller is turned
    /// away, and the refusal is a protocol-state `Status` naming the
    /// number — the same shape the read ceiling answers with, so an
    /// operator reads one story for both.
    #[test]
    fn the_connector_call_ceiling_admits_its_count_and_refuses_the_next() {
        let admission =
            std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_CONNECTOR_CALLS));
        let held: Vec<_> = (0..MAX_CONCURRENT_CONNECTOR_CALLS)
            .map(|_| {
                std::sync::Arc::clone(&admission)
                    .try_acquire_owned()
                    .expect("every permit up to the ceiling is admitted")
            })
            .collect();
        assert!(
            std::sync::Arc::clone(&admission)
                .try_acquire_owned()
                .is_err(),
            "the ceiling admits {MAX_CONCURRENT_CONNECTOR_CALLS} and no more"
        );

        let refusal = connector_calls_exhausted();
        assert_eq!(refusal.code(), tonic::Code::ResourceExhausted);
        assert!(
            refusal
                .message()
                .contains(&MAX_CONCURRENT_CONNECTOR_CALLS.to_string()),
            "the refusal names the ceiling: {}",
            refusal.message()
        );

        drop(held);
        assert!(
            admission.try_acquire_owned().is_ok(),
            "a released permit is admitted again — the ceiling bounds concurrency, \
             not lifetime calls"
        );
    }
}

#[cfg(test)]
mod connection_admission_tests {
    use super::*;
    use tokio_stream::StreamExt as _;

    /// Exactly the ceiling's worth of connections are admitted; the
    /// next is dropped without being yielded, and a slot freed by a
    /// closed connection admits again. The bound is what holds against
    /// a peer that never speaks, so it is exercised with connections
    /// that never do.
    #[tokio::test]
    async fn connections_past_the_ceiling_are_dropped_and_slots_come_back() {
        let peers: Vec<Result<tokio::io::DuplexStream, io::Error>> = (0..MAX_CONNECTIONS + 1)
            .map(|_| Ok(tokio::io::duplex(8).0))
            .collect();
        let mut admitted = admit_connections(tokio_stream::iter(peers));
        let mut held = Vec::new();
        while let Some(connection) = admitted.next().await {
            held.push(connection.expect("the source never errs"));
        }
        assert_eq!(
            held.len(),
            MAX_CONNECTIONS,
            "the one past the ceiling was dropped, not yielded"
        );

        // A closed connection gives its slot back.
        drop(held.pop());
        let mut again = Admitted {
            incoming: tokio_stream::iter(vec![Ok::<_, io::Error>(tokio::io::duplex(8).0)]),
            slots: Arc::clone(&admitted.slots),
        };
        assert!(again.next().await.is_some(), "the freed slot admits");
    }

    /// A connection that makes no request is closed at the
    /// establishment deadline and its slot returns — however many bytes
    /// it sent, since bytes short of a request prove nothing; one whose
    /// request landed keeps its slot past it.
    #[tokio::test(start_paused = true)]
    async fn a_connection_without_a_request_is_reaped_at_the_establishment_deadline() {
        use tokio::io::AsyncReadExt as _;
        let (silent, _silent_peer) = tokio::io::duplex(64);
        let (partial, mut partial_peer) = tokio::io::duplex(64);
        let (requesting, mut requesting_peer) = tokio::io::duplex(64);
        let items: Vec<Result<tokio::io::DuplexStream, io::Error>> =
            vec![Ok(silent), Ok(partial), Ok(requesting)];
        let mut admitted = admit_connections(tokio_stream::iter(items));
        let mut silent = admitted.next().await.expect("first").expect("admitted");
        let mut partial = admitted.next().await.expect("second").expect("admitted");
        let mut requesting = admitted.next().await.expect("third").expect("admitted");
        assert_eq!(admitted.slots.available_permits(), MAX_CONNECTIONS - 3);

        // The partial peer sends a preface prefix — bytes, not a request.
        tokio::io::AsyncWriteExt::write_all(&mut partial_peer, b"PRI * HTTP/2.0")
            .await
            .expect("write");
        let mut scratch = [0u8; 64];
        partial.read(&mut scratch).await.expect("the prefix reads");
        // The requesting peer's request lands: the middleware's mark.
        tokio::io::AsyncWriteExt::write_all(&mut requesting_peer, b"x")
            .await
            .expect("write");
        requesting.read(&mut scratch).await.expect("the byte reads");
        tonic::transport::server::Connected::connect_info(&requesting).establish();

        tokio::time::advance(ESTABLISHMENT_DEADLINE + Duration::from_secs(1)).await;
        for (name, connection) in [("silent", &mut silent), ("partial", &mut partial)] {
            let reaped = connection
                .read(&mut scratch)
                .await
                .expect_err("no request, so reaped");
            assert_eq!(reaped.kind(), io::ErrorKind::TimedOut, "{name}");
        }
        drop(silent);
        drop(partial);
        assert_eq!(admitted.slots.available_permits(), MAX_CONNECTIONS - 1);

        // Established, the other is the keepalive's to judge, not this
        // deadline's: a read past it still waits rather than failing.
        let still_open =
            tokio::time::timeout(Duration::from_secs(1), requesting.read(&mut scratch)).await;
        assert!(
            still_open.is_err(),
            "no deadline fires on an established connection"
        );
    }

    /// A source error passes through rather than costing a slot.
    #[tokio::test]
    async fn an_accept_error_is_yielded_and_takes_no_slot() {
        let items: Vec<Result<tokio::io::DuplexStream, io::Error>> =
            vec![Err(io::Error::other("accept"))];
        let mut admitted = admit_connections(tokio_stream::iter(items));
        assert!(admitted.next().await.expect("yielded").is_err());
        assert_eq!(admitted.slots.available_permits(), MAX_CONNECTIONS);
    }
}
