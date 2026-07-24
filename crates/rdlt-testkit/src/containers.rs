//! Shared container fixtures + the ONE runtime probe every connector suite
//! uses. Behind the `containers` feature so the published lib build carries
//! no testcontainers weight; consumers enable it in their dev-dependency line.
//!
//! Posture rule: a missing container runtime NEVER panics — `start()` returns
//! `None` after a visible `SKIP` line, and the caller returns early. Panics
//! are reserved for real startup failures WITH the runtime present.

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

/// Env override forcing the skip posture ON even where a runtime IS present —
/// so the skip-not-fail behaviour is verifiable without stopping the runtime.
const FORCE_NO_CONTAINERS: &str = "RDLT_TESTKIT_FORCE_NO_CONTAINERS";

/// Conn string the fixtures hand out — the module's default credentials over
/// `127.0.0.1` at the mapped port.
fn conn_string(port: u16) -> String {
    format!("host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres")
}

/// Is a container runtime reachable? The single probe for the whole
/// workspace (testcontainers speaks the docker API; podman needs its socket).
pub fn runtime_available() -> bool {
    // 1. Explicit override: any value forces the skip posture, so the
    //    skip-not-fail path is testable on a machine that DOES have a runtime.
    if std::env::var_os(FORCE_NO_CONTAINERS).is_some() {
        return false;
    }
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
