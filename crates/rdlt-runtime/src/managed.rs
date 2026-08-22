//! [`Managed`] — what a provider hands back — and [`Guard`], the owner
//! of the spawned process.
//!
//! `Managed<A>` implements the SPI's `Source`/`Destination` by
//! DELEGATION to the adapter it wraps, so `Engine::new` takes it
//! unchanged and the guard's lifetime rides the engine's `Arc`: as long
//! as anything can still call the connector, the process it dials is
//! provably alive, and when the last holder drops, the guard kills it
//! and unlinks its socket.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use rdlt_connector::destination::{Capabilities, Destination, LoadSession, OpenContext};
use rdlt_connector::error::{DestinationError, SourceError};
use rdlt_connector::source::{ReadRequest, Source, StreamSpec};
use rdlt_connector::spec::ConnectorSpec;
use rdlt_connector_client::handshake::Outcome;
use tokio::process::Child;

/// Owns a spawned connector process and the socket path its handshake
/// line advertised. Dropping the guard is the shutdown: the process
/// group is killed, the child gets `start_kill` (best-effort,
/// non-blocking — tokio's reaper collects the exit), and the socket
/// file is unlinked. The provider creates the guard the moment the
/// child exists, so every later failure path kills and unlinks by the
/// same drop.
#[derive(Debug)]
pub struct Guard {
    child: Child,
    socket_path: PathBuf,
    /// The advertised socket's identity — device and inode — at the
    /// moment it was accepted: what the drop unlinks, and nothing else.
    socket_inode: Option<(u64, u64)>,
    process_group: Option<u32>,
}

impl Guard {
    /// Guard a provider-spawned child that heads its own process group,
    /// so the drop kills the whole group and forked descendants cannot
    /// outlive teardown. Crate-private because a child sharing the
    /// host's group must never trigger a group-wide signal.
    pub(crate) fn for_process_group(child: Child) -> Self {
        let process_group = child.id();
        Self {
            child,
            socket_path: PathBuf::new(),
            socket_inode: None,
            process_group,
        }
    }

    pub(crate) fn child_mut(&mut self) -> &mut Child {
        &mut self.child
    }

    /// Accept the socket the handshake line advertised — or refuse it.
    ///
    /// The path came verbatim from the child's stdout, and the
    /// connector's configuration (credentials included) is about to be
    /// sent to whatever listens there, so the path is held to the shape
    /// only this user's own served connector produces: a socket (not a
    /// symlink to one), inside a directory owned by this user that
    /// nobody else can write. A path another user could have planted,
    /// or an unrelated socket in a shared directory, is refused before
    /// a byte of config leaves this process. The socket's inode is
    /// recorded so the drop unlinks exactly what was accepted.
    pub(crate) fn accept_socket_path(
        &mut self,
        socket_path: impl Into<PathBuf>,
    ) -> Result<(), String> {
        use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};
        let socket_path = socket_path.into();
        let meta = std::fs::symlink_metadata(&socket_path).map_err(|error| error.to_string())?;
        if !meta.file_type().is_socket() {
            return Err("not a socket (a symlink at the path is not followed)".to_string());
        }
        let parent = socket_path
            .parent()
            .ok_or_else(|| "no parent directory".to_string())?;
        rdlt_connector::core::fs::verify_private_dir(parent).map_err(|error| error.to_string())?;
        self.socket_inode = Some((meta.dev(), meta.ino()));
        self.socket_path = socket_path;
        Ok(())
    }

    /// Signal and disarm the owned process group while the direct
    /// child's pid is still anchored. Call before any explicit reap.
    pub(crate) fn kill_process_group(&mut self) {
        if let Some(pgid) = self
            .process_group
            .take()
            .and_then(|id| i32::try_from(id).ok())
            .and_then(rustix::process::Pid::from_raw)
        {
            let _ = rustix::process::kill_process_group(pgid, rustix::process::Signal::KILL);
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

impl Drop for Guard {
    fn drop(&mut self) {
        // Best-effort and non-blocking, both halves: `start_kill` only
        // SENDS the signal (no await, no wait — tokio reaps the exit in
        // the background), and a socket that is already gone is not an
        // error worth surfacing from a destructor. The direct child is
        // still owned and unreaped here, so its pid/pgid cannot have
        // been recycled between capture and signal.
        self.kill_process_group();
        let _ = self.child.start_kill();
        // Unlink ONLY a socket: the path came verbatim from the child's
        // stdout handshake line, so a connector naming an unrelated
        // file must not commission this host to delete it. The check
        // rides `symlink_metadata` — a symlink AT the path is already
        // not a socket, and following it would judge the wrong inode.
        //
        // THIS UNLINK HAS A DEPENDENT HALF IN ANOTHER CRATE: the sdk's
        // dead-predecessor sweep (`rdlt_connector_sdk::serve::wire::
        // sweep_dead_serve_dirs`) is rmdir-ONLY, and its correctness —
        // "a dead predecessor's directory is EMPTY exactly when the
        // sweep may remove it" — rests on every consumer of the
        // handshake line unlinking the socket FILE here. The two halves
        // are pinned together by rdlt-runtime's
        // `a_dead_predecessors_serve_dir_is_empty_and_rmdir_able`.
        //
        // And only the socket that was ACCEPTED: the entry at the path
        // must still be the inode recorded then — a socket something
        // else bound at the same name since is not this guard's to
        // remove.
        let Some(accepted) = self.socket_inode else {
            return;
        };
        let unchanged = std::fs::symlink_metadata(&self.socket_path)
            .map(|meta| {
                use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};
                meta.file_type().is_socket() && (meta.dev(), meta.ino()) == accepted
            })
            .unwrap_or(false);
        if unchanged {
            let _ = std::fs::remove_file(&self.socket_path);
        }
    }
}

/// A provided connector: the wire adapter, everything the handshake
/// established, and the process guard. Implements [`Source`] when `A`
/// does and [`Destination`] when `A` does — hand it to `Engine::new`
/// as-is.
#[derive(Debug)]
pub struct Managed<A> {
    adapter: A,
    outcome: Outcome,
    guard: Option<Guard>,
}

impl<A> Managed<A> {
    /// The constructor a [`crate::provider::Provider`] implementation
    /// uses to hand back a connected adapter: `outcome` is what the
    /// client's verified `connect` established, and `guard` is `None`
    /// for a connector whose process lifetime is owned elsewhere (a
    /// pool, an embedder's own child).
    pub fn new(adapter: A, outcome: Outcome, guard: Option<Guard>) -> Self {
        Self {
            adapter,
            outcome,
            guard,
        }
    }

    /// What the handshake established: the verified identity, the
    /// reported version, the negotiated protocol, the state-format
    /// versions, the decoded spec.
    pub fn outcome(&self) -> &Outcome {
        &self.outcome
    }

    /// The process guard, when this connector owns one — crash arms
    /// reach the pid and socket through it.
    pub fn guard(&self) -> Option<&Guard> {
        self.guard.as_ref()
    }
}

#[async_trait]
impl<A: Source> Source for Managed<A> {
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

#[async_trait]
impl<A: Destination> Destination for Managed<A> {
    fn spec(&self) -> ConnectorSpec {
        self.adapter.spec()
    }

    async fn check(&self) -> Result<(), DestinationError> {
        self.adapter.check().await
    }

    fn capabilities(&self) -> Capabilities {
        self.adapter.capabilities()
    }

    async fn open(&self, context: OpenContext) -> Result<Box<dyn LoadSession>, DestinationError> {
        self.adapter.open(context).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rdlt_connector_client::{destination, source};

    use super::*;

    /// The delegation claim, held at compile time: the managed wire
    /// adapters ARE the SPI traits, so `Engine::new` takes them
    /// unchanged.
    #[test]
    fn managed_adapters_are_the_spi_traits() {
        fn is_source<T: Source>() {}
        fn is_destination<T: Destination>() {}
        is_source::<Managed<source::Remote>>();
        is_destination::<Managed<destination::Remote>>();
    }

    /// Is `pid` gone? A zombie counts as dead: the guard's `start_kill`
    /// deliberately does not wait (no blocking in Drop) — tokio's
    /// reaper collects the exit — so the honest liveness read is /proc
    /// state, with `Z` (killed, awaiting reap) as dead as absent.
    fn process_dead(pid: u32) -> bool {
        match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Err(_) => true,
            Ok(stat) => match stat.rsplit_once(')') {
                // The state char is the first field after the comm's
                // closing paren.
                Some((_, rest)) => {
                    matches!(rest.trim_start().chars().next(), None | Some('Z' | 'X'))
                }
                None => true,
            },
        }
    }

    /// A `sleep 30` heading its own process group, spawned WITHOUT
    /// `kill_on_drop` so the kill observed by the tests below can only
    /// be the guard's own, never the `Child` destructor's belt.
    fn spawn_sleeper() -> Child {
        tokio::process::Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("sleep spawns")
    }

    /// Dropping the guard kills the child and unlinks the socket file —
    /// with the `Drop` body emptied, this test fails on BOTH asserts
    /// (the sleep survives the bound, the socket file remains).
    #[tokio::test]
    async fn the_guard_drop_kills_the_child_and_unlinks_the_socket() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
            .expect("chmod");
        let socket = dir.path().join("guarded.sock");
        // A REAL socket, not a stand-in file: the guard's unlink is
        // deliberately socket-only, so the unlink half of this pin
        // needs the genuine article on disk.
        let _listener = std::os::unix::net::UnixListener::bind(&socket).expect("the socket binds");

        let mut guard = Guard::for_process_group(spawn_sleeper());
        guard
            .accept_socket_path(&socket)
            .expect("a private socket is accepted");
        let pid = guard.pid().expect("a just-spawned child has a pid");
        assert!(!process_dead(pid), "the child is alive while guarded");
        assert_eq!(guard.socket_path(), socket);

        drop(guard);

        let mut dead = false;
        for _ in 0..100 {
            if process_dead(pid) {
                dead = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(dead, "the guard's drop must kill the child (pid {pid})");
        assert!(
            !socket.exists(),
            "the guard's drop must unlink the socket file"
        );
    }

    /// The advertised path is held to the shape only this user's own
    /// served connector produces, BEFORE any config is sent to it: a
    /// regular file, a symlink to a socket, and a socket in a directory
    /// other users can write are each refused — and nothing refused is
    /// ever unlinked by the drop.
    #[tokio::test]
    async fn an_advertised_path_is_accepted_only_as_a_private_socket() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
            .expect("chmod");
        let important = dir.path().join("important-file");
        std::fs::write(&important, b"operator data").expect("the file writes");
        let socket = dir.path().join("real.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&socket).expect("binds");
        let link = dir.path().join("link.sock");
        std::os::unix::fs::symlink(&socket, &link).expect("link");
        let shared = dir.path().join("shared");
        std::fs::create_dir(&shared).expect("mkdir");
        std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o777)).expect("chmod");
        let planted = shared.join("planted.sock");
        let _planted = std::os::unix::net::UnixListener::bind(&planted).expect("binds");

        for (name, path, expected) in [
            ("a regular file", &important, "not a socket"),
            ("a symlink", &link, "not a socket"),
            ("a shared directory", &planted, "0777"),
        ] {
            let mut guard = Guard::for_process_group(spawn_sleeper());
            let refusal = guard.accept_socket_path(path).expect_err(name);
            assert!(refusal.contains(expected), "{name}: {refusal}");
            drop(guard);
            assert!(path.exists(), "{name}: nothing refused is unlinked");
        }
        assert_eq!(std::fs::read(&important).expect("file"), b"operator data");
    }

    /// The drop unlinks exactly the socket that was accepted: one
    /// re-bound at the same name since — another inode — is left for
    /// whoever bound it.
    #[tokio::test]
    async fn the_guard_drop_leaves_a_socket_rebound_since_acceptance_alone() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
            .expect("chmod");
        let socket = dir.path().join("rebound.sock");
        let first = std::os::unix::net::UnixListener::bind(&socket).expect("binds");
        let mut guard = Guard::for_process_group(spawn_sleeper());
        guard.accept_socket_path(&socket).expect("accepted");
        drop(first);
        std::fs::remove_file(&socket).expect("unbind the first");
        let _second = std::os::unix::net::UnixListener::bind(&socket).expect("rebinds");
        drop(guard);
        assert!(
            socket.exists(),
            "the rebound socket is not this guard's to unlink"
        );
    }
}
