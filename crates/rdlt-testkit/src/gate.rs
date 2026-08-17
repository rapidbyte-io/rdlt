//! The gate on container-backed suites: one runtime probe, the
//! skip-not-fail posture around it, and the reclaim label every admitted
//! container carries. Connector-agnostic by design — a system-specific
//! fixture (a postgres container, a credential convention) lives with its
//! connector and routes through this gate, so the whole workspace keeps
//! ONE posture.
//!
//! Posture rule — one behavior, no environment override: probe for a
//! runtime; a missing one NEVER panics — fixture `start()` implementations
//! return `None` after a visible `SKIP` line and the caller returns early.
//! Panics are reserved for real startup failures WITH the runtime present.
//! The net against a wrongly-skipping suite is COUNT DISCIPLINE, not a
//! knob: the runner prints run/skip counts, every gate of record states
//! the expected ones, and a leg that quietly stopped running surfaces as
//! a moved number.
//!
//! Reclaim convention: every container this workspace starts carries
//! [`RECLAIM_LABEL`], and `make reclaim` removes containers and volumes by
//! it. Testcontainers' own `Drop` reaps a container that finishes
//! normally, but a suite killed mid-run (Ctrl-C, an OOM, a hung fixture)
//! never runs `Drop` and leaves the container AND its anonymous volume
//! behind — which has filled a disk twice. A LABEL rather than a name
//! pattern, because volumes do not inherit names from their container; a
//! new start site MUST carry it, including sites that shell out to the
//! container CLI directly (`--label rdlt-test=1`) instead of going through
//! testcontainers.

/// The label every container started by this workspace carries, so `make
/// reclaim` can remove them (and their volumes) without pattern-matching
/// names and without touching anything else on the machine. See the
/// module doc for why a killed run needs this.
pub const RECLAIM_LABEL: &str = "rdlt-test";

/// Is a container runtime reachable? The single probe for the whole
/// workspace (testcontainers speaks the docker API; podman needs its
/// socket).
pub fn runtime_available() -> bool {
    // 1. An explicitly configured docker endpoint is authoritative.
    if std::env::var_os("DOCKER_HOST").is_some() {
        return true;
    }
    // 2. Rootless podman's user socket.
    if let Some(dir) = std::env::var_os("XDG_RUNTIME_DIR")
        && std::path::Path::new(&dir)
            .join("podman/podman.sock")
            .exists()
    {
        return true;
    }
    // 3. The system docker/podman socket.
    if std::path::Path::new("/var/run/docker.sock").exists() {
        return true;
    }
    // 4. Last resort: ask podman directly (covers hosts where the socket
    //    lives off the default paths but the CLI still works).
    std::process::Command::new("podman")
        .arg("ps")
        .output()
        .is_ok_and(|o| o.status.success())
}
