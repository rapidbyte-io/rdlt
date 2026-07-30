//! Shared container fixtures + the ONE runtime probe every connector suite
//! uses. Behind the `containers` feature so the published lib build carries
//! no testcontainers weight; consumers enable it in their dev-dependency line.
//!
//! Posture rule: a missing container runtime NEVER panics — `start()` returns
//! `None` after a visible `SKIP` line, and the caller returns early. Panics
//! are reserved for real startup failures WITH the runtime present.
//!
//! Reclaim convention: EVERY container this workspace starts carries the label
//! [`RECLAIM_LABEL`], and `make reclaim` removes containers and volumes by it.
//! Testcontainers' own `Drop` reaps a container that finishes normally, but a
//! suite killed mid-run (Ctrl-C, an OOM, a hung fixture) never runs `Drop` and
//! leaves the container AND its anonymous volume behind — twice during feature
//! 017 that filled the disk. The label exists so reclaiming is one command that
//! cannot touch anything else on the machine, and so it is a LABEL rather than
//! a name pattern because volumes do not inherit names from their container.
//! A new start site MUST carry it, including the ones that shell out to the
//! container CLI directly (`--label rdlt-test=1`) instead of going through
//! testcontainers.

use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio_postgres::{Client, NoTls};

/// The postgres image is testcontainers-modules' default repository,
/// `docker.io/library/postgres`; only the tag is ours to pin.
///
/// Pinned: a floating `latest` re-resolves whenever upstream pushes, so a
/// broken upstream build fails our gate without any change on our side. Bump
/// deliberately, with the live cells green, never by drift.
pub const POSTGRES_TAG: &str = "16-alpine";

/// Container-internal postgres port (the module maps it to a random host
/// port, read back at start).
const POSTGRES_PORT: u16 = 5432;

/// The label every container started by this workspace carries, so `make
/// reclaim` can remove them (and their volumes) without pattern-matching names
/// and without touching anything else running on the machine. See the module
/// doc for why a killed run needs this.
pub const RECLAIM_LABEL: &str = "rdlt-test";

/// Env override forcing the skip posture ON even where a runtime IS present —
/// so the skip-not-fail behaviour is verifiable without stopping the runtime.
const FORCE_NO_CONTAINERS: &str = "RDLT_TESTKIT_FORCE_NO_CONTAINERS";

/// Env override DEMANDING a runtime: absence becomes a failure instead of a skip.
///
/// The counterpart to [`FORCE_NO_CONTAINERS`], and the reason both exist. Skip-
/// not-fail is the right default — a contributor without a runtime must still be
/// able to run the gate — but it makes a wrongly-skipping suite indistinguishable
/// from a passing one. Setting this says "I am on a machine where the runtime is
/// present; tell me if a leg did not run."
const REQUIRE_CONTAINERS: &str = "RDLT_TESTKIT_REQUIRE_CONTAINERS";

/// Conn string the fixtures hand out — the module's default credentials over
/// `127.0.0.1` at the mapped port.
fn conn_string(port: u16) -> String {
    format!("host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres")
}

/// Is a container runtime reachable? The single probe for the whole
/// workspace (testcontainers speaks the docker API; podman needs its socket).
pub fn runtime_available() -> bool {
    decide_availability(
        std::env::var_os(REQUIRE_CONTAINERS).is_some(),
        std::env::var_os(FORCE_NO_CONTAINERS).is_some(),
        probe_for_runtime,
    )
}

/// The decision, separated from where the answers come from.
///
/// Split out so the two overrides and their interaction are testable without
/// mutating the process environment — which this workspace cannot do anyway,
/// because `unsafe_code` is denied and `set_var` is unsafe. Taking the probe as a
/// parameter also means the demand-and-fail path can be exercised on a machine
/// that HAS a runtime, which is the machine a maintainer would be on.
pub fn decide_availability(demanded: bool, forced_absent: bool, probe: impl Fn() -> bool) -> bool {
    // Contradictory invocation, not a precedence puzzle: a run that both forces
    // absence and demands presence is a mistake in how it was invoked, and
    // quietly honouring either one would hide it.
    assert!(
        !(demanded && forced_absent),
        "{REQUIRE_CONTAINERS} and {FORCE_NO_CONTAINERS} are both set — one demands \
         a container runtime and the other pretends there is none; pick one"
    );

    if forced_absent {
        return false;
    }

    let found = probe();

    // Every probe said no. If the caller DEMANDED a runtime, that is a failure
    // naming what is missing rather than a skip nobody reads.
    assert!(
        found || !demanded,
        "{REQUIRE_CONTAINERS} is set but no container runtime was found: no \
         DOCKER_HOST, no podman user socket under XDG_RUNTIME_DIR, no \
         /var/run/docker.sock, and `podman ps` did not succeed"
    );
    found
}

/// Ask the machine whether a container runtime is reachable.
fn probe_for_runtime() -> bool {
    // 2. An explicitly configured docker endpoint is authoritative.
    if std::env::var_os("DOCKER_HOST").is_some() {
        return true;
    }
    // 3. Rootless podman's user socket (the standing workspace note).
    if let Some(dir) = std::env::var_os("XDG_RUNTIME_DIR")
        && std::path::Path::new(&dir)
            .join("podman/podman.sock")
            .exists()
    {
        return true;
    }
    // 4. The system docker/podman socket.
    if std::path::Path::new("/var/run/docker.sock").exists() {
        return true;
    }
    // 5. Last resort: ask podman directly (covers hosts where the socket
    //    lives off the default paths but the CLI still works).
    std::process::Command::new("podman")
        .arg("ps")
        .output()
        .is_ok_and(|o| o.status.success())
}

/// A fresh `postgres:16-alpine`, seeded via SQL batches, handing out conn
/// strings/clients. One container per fixture; the container stops on Drop.
pub struct PgFixture {
    // Held for its Drop: the container stops when the fixture drops.
    _container: ContainerAsync<PostgresImage>,
    pub conn: String,
}

// `ContainerAsync` is not `Debug`; expose the useful half by hand.
impl std::fmt::Debug for PgFixture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgFixture")
            .field("conn", &self.conn)
            .finish()
    }
}

impl PgFixture {
    /// Start a fresh postgres, or skip visibly (`None`) without a runtime.
    pub async fn start() -> Option<Self> {
        if !runtime_available() {
            eprintln!("SKIP: no container runtime — postgres fixture not started");
            return None;
        }
        let container = PostgresImage::default()
            .with_tag(POSTGRES_TAG)
            .with_label(RECLAIM_LABEL, "1")
            .start()
            .await
            .expect("start postgres container (runtime present)");
        let port = container
            .get_host_port_ipv4(POSTGRES_PORT)
            .await
            .expect("mapped port");
        Some(Self {
            _container: container,
            conn: conn_string(port),
        })
    }

    /// A raw client for seeding/asserting, independent of the source under test.
    pub async fn client(&self) -> Client {
        let (client, connection) = tokio_postgres::connect(&self.conn, NoTls)
            .await
            .expect("connect to fixture postgres");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    /// Run semicolon-separated DDL/DML (simple batch seeding).
    pub async fn seed(&self, sql: &str) {
        let client = self.client().await;
        client.batch_execute(sql).await.expect("seed SQL");
    }

    /// The source-config `conn:` value for this fixture.
    pub fn conn_url(&self) -> String {
        self.conn.clone()
    }
}

/// A postgres with logical replication enabled — the CDC fixture. Same image,
/// three server flags.
pub struct CdcPgFixture {
    // Held for its Drop: the container stops when the fixture drops.
    _container: ContainerAsync<PostgresImage>,
    conn: String,
}

// `ContainerAsync` is not `Debug`; expose the useful half by hand.
impl std::fmt::Debug for CdcPgFixture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CdcPgFixture")
            .field("conn", &self.conn)
            .finish()
    }
}

impl CdcPgFixture {
    /// Start a fresh logical-replication postgres, or skip visibly (`None`)
    /// without a runtime.
    pub async fn start() -> Option<Self> {
        if !runtime_available() {
            eprintln!("SKIP: no container runtime — CDC postgres fixture not started");
            return None;
        }
        let container = PostgresImage::default()
            .with_tag(POSTGRES_TAG)
            .with_label(RECLAIM_LABEL, "1")
            .with_cmd([
                "postgres",
                "-c",
                "wal_level=logical",
                "-c",
                "max_replication_slots=8",
                "-c",
                "max_wal_senders=8",
            ])
            .start()
            .await
            .expect("start CDC postgres container (runtime present)");
        let port = container
            .get_host_port_ipv4(POSTGRES_PORT)
            .await
            .expect("mapped port");
        Some(Self {
            _container: container,
            conn: conn_string(port),
        })
    }

    pub fn conn_url(&self) -> String {
        self.conn.clone()
    }

    /// A raw client for seeding/mutating/asserting, independent of the source
    /// under test.
    pub async fn client(&self) -> Client {
        let (client, connection) = tokio_postgres::connect(&self.conn, NoTls)
            .await
            .expect("connect to CDC fixture");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    pub async fn seed(&self, sql: &str) {
        self.client()
            .await
            .batch_execute(sql)
            .await
            .expect("seed SQL");
    }
}
