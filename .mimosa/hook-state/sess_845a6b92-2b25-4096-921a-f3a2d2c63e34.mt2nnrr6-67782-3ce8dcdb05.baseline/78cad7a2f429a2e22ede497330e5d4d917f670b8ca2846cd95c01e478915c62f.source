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
use rdlt_connector_protocol::PROTOCOL_VERSION;
use rdlt_connector_protocol::proto;
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
/// race-free: every consumer of the handshake line unlinks the socket
/// FILE on every cleanup path, so a dead predecessor's directory is
/// EMPTY and rmdir succeeds exactly then; a LIVE sibling's directory
/// still holds its socket, so rmdir fails harmlessly; rmdir never
/// follows symlinks; other users' directories fail on permissions.
/// Accepted: between a concurrent sibling's mkdir and its bind, its
/// directory is briefly empty and a sweep could rmdir it — that
/// sibling's bind then fails loudly with [`Error::Bind`], never
/// silently, and the window is a few syscalls wide.
fn sweep_dead_serve_dirs(base: &Path) {
    let Ok(entries) = std::fs::read_dir(base) else {
        return;
    };
    for entry in entries.flatten() {
        if entry
            .file_name()
            .as_encoded_bytes()
            .starts_with(b"rdlt-serve-")
        {
            let _ = std::fs::remove_dir(entry.path());
        }
    }
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

/// Every non-null scalar LEAF the config document carries, in document
/// order — the values [`redact_values`] shields. Values only, never
/// keys: keys are the document's schema, and refusals legitimately name
/// fields. Computed from the RAW BYTES by re-parsing them, so a
/// SUCCESSFUL handshake pays neither this parse nor the per-leaf clones
/// (the refusal arm no longer holds the parsed document — the failed
/// construction consumed it). The bytes parsed once already, so the
/// re-parse cannot fail; the empty fallback is belt only.
fn config_scalar_values(config_json: &[u8]) -> Vec<String> {
    fn walk(value: &serde_json::Value, into: &mut Vec<String>) {
        match value {
            serde_json::Value::String(text) => {
                if !text.is_empty() {
                    into.push(text.clone());
                }
            }
            serde_json::Value::Number(number) => into.push(number.to_string()),
            serde_json::Value::Bool(boolean) => into.push(boolean.to_string()),
            serde_json::Value::Array(items) => {
                for item in items {
                    walk(item, into);
                }
            }
            serde_json::Value::Object(fields) => {
                for field in fields.values() {
                    walk(field, into);
                }
            }
            _ => {}
        }
    }
    let mut values = Vec::new();
    if let Ok(config) = serde_json::from_slice::<serde_json::Value>(config_json) {
        walk(&config, &mut values);
    }
    values
}

/// Redact every config scalar value out of a connector-rendered
/// refusal before it crosses the wire: the connector's own wording is
/// the seam's contract, but its serde parse arms quote parsed tokens,
/// so a secret handed to the wrong field would otherwise ride the
/// refusal into host logs. Each value is hunted in every spelling a
/// refusal can carry it — raw, its full `{:?}` Debug form (serde's
/// `Unexpected::Str` renders tokens that way, escaping quotes,
/// backslashes and control bytes), and the Debug body without its
/// surrounding quotes — longest first, so a form containing another
/// redacts whole; wording that quotes no config value crosses back
/// untouched.
fn redact_values(message: String, values: &[String]) -> String {
    let mut spellings: Vec<(String, &'static str)> = Vec::new();
    for value in values {
        let debug_form = format!("{value:?}");
        spellings.push((debug_form, "\"[redacted config value]\""));
        spellings.push((value.escape_debug().to_string(), "[redacted config value]"));
        spellings.push((value.clone(), "[redacted config value]"));
    }
    spellings.sort_by_key(|(needle, _)| std::cmp::Reverse(needle.len()));
    let mut redacted = message;
    for (needle, marker) in spellings {
        if redacted.contains(&needle) {
            redacted = redacted.replace(&needle, marker);
        }
    }
    redacted
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
            let values = config_scalar_values(&request.config_json);
            return refuse_handshake(redact_values(error.to_string(), &values));
        }
    };

    let ok = proto::HandshakeOk {
        connector_id: shell.connector_id().to_string(),
        connector_version: shell.connector_version().to_string(),
        spec_json: shell.spec_json(),
        capabilities_json: shell.capabilities_json(),
        // The empty document (= the empty map, the proto field's own
        // convention) by design: with one format version per state kind
        // there is nothing to negotiate; the feature that adds a second
        // format version owns the negotiation semantics.
        state_format_versions_json: Vec::new(),
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
        let values = config_scalar_values(br#"{"password": 12345, "enabled": true}"#);
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
