//! The Oracle Free container fixture: ONE database shared by this
//! binary's cells (it is the slowest fixture the gate has — ~75 s to
//! readiness), skip-not-fail without a runtime.

#![allow(dead_code)] // shared across cells; not every cell uses every helper

use rdlt_connector_oracle::source::{Config, Shell, Stream};
use rdlt_connector_sdk::config::Document;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

pub const PASSWORD: &str = "rdlt-probe-pw";
pub const APP_USER: &str = "rdlt";
pub const SERVICE: &str = "FREEPDB1";
/// Pinned: a floating tag re-resolves on upstream's schedule and
/// would hand their broken day to our gate. Bump deliberately, with
/// the live cells green on the new tag.
pub const IMAGE_TAG: &str = "23.26.2-slim-faststart";

pub struct OracleFixture {
    _container: ContainerAsync<GenericImage>,
    pub host: String,
    pub port: u16,
}

impl OracleFixture {
    /// Start Oracle Free, or skip visibly (None) without a runtime.
    pub async fn start() -> Option<Self> {
        if !rdlt_testkit::gate::runtime_available() {
            eprintln!("SKIP: no container runtime socket — oracle live cells not run");
            return None;
        }
        let container = GenericImage::new("docker.io/gvenzl/oracle-free", IMAGE_TAG)
            .with_exposed_port(1521.tcp())
            // The log line fires BEFORE the PDB registers with the
            // listener; the readiness poll below closes that gap
            // (without it the first connect races ORA-12514).
            .with_wait_for(WaitFor::message_on_stdout("DATABASE IS READY TO USE!"))
            .with_env_var("ORACLE_PASSWORD", PASSWORD)
            .with_env_var("APP_USER", APP_USER)
            .with_env_var("APP_USER_PASSWORD", PASSWORD)
            .with_label(rdlt_testkit::gate::RECLAIM_LABEL, "1")
            .start()
            .await
            .expect("start oracle-free (runtime socket present)");
        let port = container
            .get_host_port_ipv4(1521)
            .await
            .expect("mapped port");
        let fixture = Self {
            _container: container,
            host: "127.0.0.1".to_owned(),
            port,
        };
        fixture.await_service().await;
        Some(fixture)
    }

    /// Ready means a real query answers as the APP user — the log
    /// line alone is not enough.
    async fn await_service(&self) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(180);
        let mut last;
        loop {
            use rdlt_connector_sdk::spi::Source;
            let shell = Shell::new(self.config(&[])).expect("the fixture document is valid");
            match shell.check().await {
                Ok(()) => return,
                Err(e) => last = e.to_string(),
            }
            let _ = &last;
            assert!(
                std::time::Instant::now() < deadline,
                "oracle never accepted a connection within the deadline; last error: {last}"
            );
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }

    /// A source document over this fixture with the given streams.
    pub fn config(&self, streams: &[Stream]) -> Config {
        let value = serde_json::json!({
            "host": self.host,
            "port": self.port,
            "service": SERVICE,
            "user": APP_USER,
            "password": PASSWORD,
            "streams": if streams.is_empty() {
                serde_json::json!([{"name": "probe", "table": "DUAL"}])
            } else {
                serde_json::to_value(streams).expect("streams")
            },
        });
        Config::from_value(value).expect("valid fixture document")
    }

    pub fn shell(&self, streams: &[Stream]) -> Shell {
        Shell::new(self.config(streams)).expect("valid")
    }

    /// Run statements on ONE connection and commit them there.
    ///
    /// Per-statement connections would drop each INSERT's transaction
    /// before any later COMMIT could reach it — probed the hard way:
    /// a seeded table read back empty.
    pub async fn seed(&self, statements: &[&str]) {
        let conn = oracle_rs::connection::Connection::connect(
            &format!("{}:{}/{SERVICE}", self.host, self.port),
            APP_USER,
            PASSWORD,
        )
        .await
        .expect("fixture connect");
        for sql in statements {
            conn.execute(sql, &[])
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        conn.execute("COMMIT", &[]).await.expect("fixture commit");
    }
}

/// A stream document over one table.
pub fn stream(name: &str, table: &str) -> Stream {
    serde_json::from_value(serde_json::json!({"name": name, "table": table}))
        .expect("stream document")
}

/// A stream with a watermark cursor column.
pub fn incremental(name: &str, table: &str, cursor: &str) -> Stream {
    serde_json::from_value(serde_json::json!({
        "name": name, "table": table, "cursor": cursor
    }))
    .expect("stream document")
}
