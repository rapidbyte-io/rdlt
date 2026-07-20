//! Shared testcontainers fixture (T002): one postgres:16-alpine container per
//! test binary, seeded via SQL batches, handing out conn strings/clients.
//! Mirrors the rdlt-dest-postgres test conventions.

use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio_postgres::{Client, NoTls};

pub struct PgFixture {
    // Held for its Drop: the container stops when the fixture drops.
    _container: ContainerAsync<PostgresImage>,
    pub conn: String,
}

#[allow(dead_code)]
impl PgFixture {
    /// Start a fresh postgres:16-alpine (needs docker/podman; the workspace's
    /// existing suites already require this).
    pub async fn start() -> Self {
        let container = PostgresImage::default()
            .with_tag("16-alpine")
            .start()
            .await
            .expect("start postgres container (needs docker/podman)");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("mapped port");
        let conn =
            format!("host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres");
        Self {
            _container: container,
            conn,
        }
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

    /// The source-config YAML `conn:` value for this fixture.
    pub fn conn_url(&self) -> String {
        self.conn.clone()
    }
}

/// Generated PKI for the TLS matrix (T007): a CA, a server cert signed by it
/// (SAN: localhost ONLY — connecting via 127.0.0.1 is the hostname-mismatch
/// case), and an unrelated CA for the wrong-trust-anchor case.
#[allow(dead_code)]
pub struct TlsPki {
    pub ca_pem: String,
    pub wrong_ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
}

#[allow(dead_code)]
impl TlsPki {
    pub fn generate() -> Self {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
        use rcgen::{DistinguishedName, DnType};
        let ca_key = KeyPair::generate().expect("ca key");
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, "rdlt test CA");
        ca_params.distinguished_name = dn;
        let ca = ca_params.self_signed(&ca_key).expect("ca cert");

        let server_key = KeyPair::generate().expect("server key");
        let server_params =
            CertificateParams::new(vec!["localhost".to_string()]).expect("server params");
        let server = server_params
            .signed_by(&server_key, &ca, &ca_key)
            .expect("server cert");

        let wrong_ca_key = KeyPair::generate().expect("wrong ca key");
        let mut wrong_params = CertificateParams::new(Vec::<String>::new()).expect("params");
        wrong_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let mut wrong_dn = DistinguishedName::new();
        wrong_dn.push(DnType::CommonName, "rdlt WRONG CA");
        wrong_params.distinguished_name = wrong_dn;
        let wrong_ca = wrong_params.self_signed(&wrong_ca_key).expect("wrong ca");

        Self {
            ca_pem: ca.pem(),
            wrong_ca_pem: wrong_ca.pem(),
            server_cert_pem: server.pem(),
            server_key_pem: server_key.serialize_pem(),
        }
    }
}

/// A TLS-enabled postgres (T007): certs land via /docker-entrypoint-initdb.d
/// (runs AFTER initdb's temp server, as root — fixes key perms and appends
/// ssl config before the FINAL server start; `hostssl`-only pg_hba makes
/// plaintext connections a protocol-level rejection).
#[allow(dead_code)]
pub struct TlsPgFixture {
    _container: ContainerAsync<PostgresImage>,
    pub port: u16,
    pub pki: TlsPki,
}

#[allow(dead_code)]
impl TlsPgFixture {
    pub async fn start() -> Self {
        let pki = TlsPki::generate();
        let init = r#"#!/bin/bash
set -e
install -m 600 -o postgres -g postgres /tls-in/server.key "$PGDATA/server.key"
install -m 644 -o postgres -g postgres /tls-in/server.crt "$PGDATA/server.crt"
cat >> "$PGDATA/postgresql.conf" <<CONF
ssl = on
ssl_cert_file = 'server.crt'
ssl_key_file = 'server.key'
CONF
cat > "$PGDATA/pg_hba.conf" <<HBA
local   all all             trust
hostssl all all 0.0.0.0/0   trust
hostssl all all ::0/0       trust
HBA
"#;
        let container = PostgresImage::default()
            .with_tag("16-alpine")
            .with_copy_to(
                "/tls-in/server.crt",
                pki.server_cert_pem.clone().into_bytes(),
            )
            .with_copy_to(
                "/tls-in/server.key",
                pki.server_key_pem.clone().into_bytes(),
            )
            .with_copy_to(
                "/docker-entrypoint-initdb.d/zz-tls.sh",
                init.as_bytes().to_vec(),
            )
            .start()
            .await
            .expect("start TLS postgres (needs docker/podman)");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("mapped port");
        Self {
            _container: container,
            port,
            pki,
        }
    }

    /// Conn string via `localhost` (matches the cert SAN).
    pub fn conn_localhost(&self) -> String {
        format!(
            "host=localhost port={} user=postgres password=postgres dbname=postgres",
            self.port
        )
    }

    /// Conn string via `127.0.0.1` (NOT in the cert SAN — the mismatch case).
    pub fn conn_ip(&self) -> String {
        format!(
            "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
            self.port
        )
    }
}
