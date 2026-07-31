//! Postgres container fixtures for tests across the workspace, behind the
//! `fixtures` feature. Test support only — no semver guarantees.
//!
//! Lived in rdlt-testkit until the testkit went connector-agnostic; a
//! postgres image, its flags, and its client plumbing belong with the
//! postgres connector. The runtime probe and the reclaim label stay in the
//! testkit (`rdlt_testkit::gate`) — they are container facts, not
//! postgres facts — and the fixtures here route through them so every
//! container-gated suite keeps one skip-not-fail posture and one label
//! `make reclaim` can act on.
//!
//! Posture rule: a missing container runtime NEVER panics — `start()` returns
//! `None` after a visible `SKIP` line, and the caller returns early. Panics
//! are reserved for real startup failures WITH the runtime present.

use rdlt_testkit::gate::{RECLAIM_LABEL, runtime_available};
use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ContainerRequest, ImageExt};
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

/// Conn string the fixtures hand out — the module's default credentials over
/// `127.0.0.1` at the mapped port.
fn conn_string(port: u16) -> String {
    format!("host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres")
}

/// The one start sequence both fixtures share: probe the runtime (skip
/// visibly without one), label for `make reclaim`, start, read the mapped
/// port back into a conn string.
async fn start_labeled(
    request: ContainerRequest<PostgresImage>,
    what: &str,
) -> Option<(ContainerAsync<PostgresImage>, String)> {
    if !runtime_available() {
        eprintln!("SKIP: no container runtime — {what} not started");
        return None;
    }
    let container = request
        .with_label(RECLAIM_LABEL, "1")
        .start()
        .await
        .expect("start postgres container (runtime present)");
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("mapped port");
    let conn = conn_string(port);
    Some((container, conn))
}

/// A raw client on the fixture's conn string, its connection task detached.
async fn raw_client(conn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(conn, NoTls)
        .await
        .expect("connect to fixture postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// A fresh `postgres:16-alpine`, seeded via SQL batches, handing out conn
/// strings/clients. One container per fixture; the container stops on Drop.
pub struct PgFixture {
    // Held for its Drop: the container stops when the fixture drops.
    _container: ContainerAsync<PostgresImage>,
    /// The source-config `conn:` value for this fixture — the ONE spelling.
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
        let request = PostgresImage::default().with_tag(POSTGRES_TAG);
        let (container, conn) = start_labeled(request, "postgres fixture").await?;
        Some(Self {
            _container: container,
            conn,
        })
    }

    /// A raw client for seeding/asserting, independent of the source under test.
    pub async fn client(&self) -> Client {
        raw_client(&self.conn).await
    }

    /// Run semicolon-separated DDL/DML (simple batch seeding).
    pub async fn seed(&self, sql: &str) {
        self.client()
            .await
            .batch_execute(sql)
            .await
            .expect("seed SQL");
    }
}

/// A postgres with logical replication enabled — the CDC fixture. Same image,
/// three server flags.
pub struct CdcPgFixture {
    // Held for its Drop: the container stops when the fixture drops.
    _container: ContainerAsync<PostgresImage>,
    /// The source-config `conn:` value for this fixture — the ONE spelling.
    pub conn: String,
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
        let request = PostgresImage::default().with_tag(POSTGRES_TAG).with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=8",
            "-c",
            "max_wal_senders=8",
        ]);
        let (container, conn) = start_labeled(request, "CDC postgres fixture").await?;
        Some(Self {
            _container: container,
            conn,
        })
    }

    /// A raw client for seeding/mutating/asserting, independent of the source
    /// under test.
    pub async fn client(&self) -> Client {
        raw_client(&self.conn).await
    }

    pub async fn seed(&self, sql: &str) {
        self.client()
            .await
            .batch_execute(sql)
            .await
            .expect("seed SQL");
    }
}
