//! T008: the TLS matrix (contract tls-policy.md) — five modes against a real
//! TLS-only Postgres with a generated CA, positive AND distinguished
//! negative cases, for BOTH connectors through the one shared connect path.

mod common;

use common::{PgFixture, TlsPgFixture};
use rdlt_connector_postgres::source::{PostgresConfig, PostgresSource};
use rdlt_connector_postgres::tls::{PemSource, TlsMode, TlsPolicy};

fn source(conn: &str, tls_yaml: &str) -> PostgresSource {
    PostgresSource::from_yaml(&format!("conn: \"{conn}\"\n{tls_yaml}")).expect("config")
}

/// Drive a real connection through the source (streams() reflects ⇒ connects).
async fn probe_source(conn: &str, tls_yaml: &str) -> Result<(), String> {
    use rdlt_connector::Source as _;
    source(conn, tls_yaml)
        .streams()
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn tls_yaml(mode: &str, root: Option<&str>) -> String {
    match root {
        None => format!("tls:\n  mode: {mode}\n"),
        Some(pem) => {
            let indented = pem.trim().replace('\n', "\n    ");
            format!("tls:\n  mode: {mode}\n  root_cert: |\n    {indented}\n")
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn source_matrix_against_tls_only_server() {
    let Some(fixture) = TlsPgFixture::start().await else {
        return;
    };
    let ca = fixture.pki.ca_pem.clone();
    let wrong_ca = fixture.pki.wrong_ca_pem.clone();
    let localhost = fixture.conn_localhost();
    let ip = fixture.conn_ip();

    // disable → the hostssl-only server rejects plaintext, typed error.
    let err = probe_source(&localhost, "tls:\n  mode: disable\n")
        .await
        .expect_err("plaintext must be rejected by a TLS-only server");
    assert!(err.contains("connect phase"), "{err}");

    // prefer / require → encrypted, no validation, succeed on self-signed.
    probe_source(&localhost, "tls:\n  mode: prefer\n")
        .await
        .expect("prefer connects (encrypted)");
    probe_source(&localhost, "tls:\n  mode: require\n")
        .await
        .expect("require connects without validating (libpq semantics)");

    // verify_ca: our CA passes (even via IP — hostname waived); the wrong
    // CA is a distinguished trust-anchor failure.
    probe_source(&ip, &tls_yaml("verify_ca", Some(&ca)))
        .await
        .expect("verify_ca with the right root (hostname waived)");
    let err = probe_source(&localhost, &tls_yaml("verify_ca", Some(&wrong_ca)))
        .await
        .expect_err("wrong trust anchor must fail");
    assert!(err.contains("TrustAnchor"), "distinguished: {err}");

    // verify_full: succeeds via the SAN'd hostname; the IP is a
    // distinguished hostname failure; missing root is a trust failure.
    probe_source(&localhost, &tls_yaml("verify_full", Some(&ca)))
        .await
        .expect("verify_full via matching hostname");
    let err = probe_source(&ip, &tls_yaml("verify_full", Some(&ca)))
        .await
        .expect_err("IP is not in the cert SAN");
    assert!(err.contains("Hostname"), "distinguished: {err}");
    let err = probe_source(&localhost, &tls_yaml("verify_full", Some(&wrong_ca)))
        .await
        .expect_err("unknown CA under verify_full");
    assert!(err.contains("TrustAnchor"), "distinguished: {err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn prefer_falls_back_on_plaintext_server_and_conn_sslmode_flows() {
    // A plain (non-TLS) postgres: prefer falls back to plaintext — the
    // libpq vocabulary's promise — and conn-string sslmode drives the
    // policy without any tls block.
    let Some(plain) = PgFixture::start().await else {
        return;
    };
    probe_source(&plain.conn.clone(), "")
        .await
        .expect("default prefer falls back to plaintext");
    probe_source(&format!("{} sslmode=disable", plain.conn.clone()), "")
        .await
        .expect("explicit disable on a plaintext server");
    let err = probe_source(&format!("{} sslmode=require", plain.conn.clone()), "")
        .await
        .expect_err("require against a server without TLS must fail");
    assert!(err.contains("connect phase"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_uses_the_same_policy_path() {
    use rdlt_connector::core::{LoadId, PipelineId};
    use rdlt_connector::{Destination as _, OpenCtx};

    let Some(fixture) = TlsPgFixture::start().await else {
        return;
    };
    let pipeline = PipelineId::new("tls");
    let load = LoadId::new("tls-load");

    // verify_full + right root + matching hostname: the destination opens.
    let ok = rdlt_connector_postgres::dest::Postgres::connect(fixture.conn_localhost())
        .dataset("tls_ok")
        .tls(TlsPolicy {
            mode: TlsMode::VerifyFull,
            root_cert: Some(PemSource(fixture.pki.ca_pem.clone())),
            ..TlsPolicy::default()
        });
    assert!(
        ok.open(OpenCtx::new(pipeline.clone(), load.clone()))
            .await
            .is_ok(),
        "destination over verify_full must open"
    );

    // Same policy, wrong trust anchor: typed failure through the SAME path.
    let bad = rdlt_connector_postgres::dest::Postgres::connect(fixture.conn_localhost())
        .dataset("tls_bad")
        .tls(TlsPolicy {
            mode: TlsMode::VerifyFull,
            root_cert: Some(PemSource(fixture.pki.wrong_ca_pem.clone())),
            ..TlsPolicy::default()
        });
    let err = match bad.open(OpenCtx::new(pipeline, load)).await {
        Ok(_) => panic!("wrong CA must fail the destination identically"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("TrustAnchor"), "{err}");
}

#[test]
fn config_validation_matrix() {
    // Contradiction typed at validate; refinement allowed; bad roots typed.
    assert!(
        PostgresConfig::from_yaml("conn: \"host=h sslmode=disable\"\ntls:\n  mode: require\n")
            .is_err()
    );
    assert!(
        PostgresConfig::from_yaml("conn: \"host=h sslmode=require\"\ntls:\n  mode: verify_ca\n")
            .is_ok()
    );
}

// ---- Feature 007 US1: mutual TLS (contract tls-client-auth.md) ----

fn pem_block(name: &str, pem: &str) -> String {
    let indented = pem.trim().replace('\n', "\n    ");
    format!("  {name}: |\n    {indented}\n")
}

fn mtls_yaml(mode: &str, root: &str, client: Option<(&str, &str)>) -> String {
    let mut yaml = format!("tls:\n  mode: {mode}\n");
    yaml.push_str(&pem_block("root_cert", root));
    if let Some((cert, key)) = client {
        yaml.push_str(&pem_block("client_cert", cert));
        yaml.push_str(&pem_block("client_key", key));
    }
    yaml
}

#[tokio::test(flavor = "multi_thread")]
async fn client_cert_matrix_against_cert_auth_server() {
    use rdlt_connector::core::{LoadId, PipelineId};
    use rdlt_connector::{Destination as _, OpenCtx};

    let Some(fixture) = TlsPgFixture::start_cert_auth().await else {
        return;
    };
    let pki = &fixture.pki;
    let localhost = fixture.conn_localhost();

    // Valid credential: the SOURCE syncs…
    let good = mtls_yaml(
        "verify_full",
        &pki.ca_pem,
        Some((&pki.client_cert_pem, &pki.client_key_pem)),
    );
    probe_source(&localhost, &good)
        .await
        .expect("valid client cert + key must connect (source)");

    // …and the DESTINATION opens through the same path.
    let dest = rdlt_connector_postgres::dest::Postgres::connect(fixture.conn_localhost())
        .dataset("mtls_ok")
        .tls(TlsPolicy {
            mode: TlsMode::VerifyFull,
            root_cert: Some(PemSource(pki.ca_pem.clone())),
            client_cert: Some(PemSource(pki.client_cert_pem.clone())),
            client_key: Some(PemSource(pki.client_key_pem.clone())),
        });
    dest.open(OpenCtx::new(
        PipelineId::new("mtls"),
        LoadId::new("mtls-load"),
    ))
    .await
    .expect("destination over mTLS must open");

    // No credential: the server demands one — distinguished ClientCert.
    let err = probe_source(&localhost, &mtls_yaml("verify_full", &pki.ca_pem, None))
        .await
        .expect_err("cert-auth server must reject a credential-less client");
    assert!(err.contains("ClientCert"), "distinguished: {err}");

    // Wrong-CA credential: same distinguished class, whichever layer the
    // server rejects at (TLS alert or auth-phase 28000).
    let wrong = mtls_yaml(
        "verify_full",
        &pki.ca_pem,
        Some((&pki.wrong_client_cert_pem, &pki.wrong_client_key_pem)),
    );
    let err = probe_source(&localhost, &wrong)
        .await
        .expect_err("wrong-CA client cert must be rejected");
    assert!(err.contains("ClientCert"), "distinguished: {err}");

    // Mismatched key: a CONFIG error before any connection.
    let mismatched = mtls_yaml(
        "verify_full",
        &pki.ca_pem,
        Some((&pki.client_cert_pem, &pki.wrong_client_key_pem)),
    );
    let err = probe_source(&localhost, &mismatched)
        .await
        .expect_err("mismatched cert/key must fail as config");
    assert!(err.contains("client credential"), "config-shaped: {err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn credential_offered_but_unused_still_syncs() {
    // C5: against a server that does NOT verify clients, carrying a
    // credential changes nothing.
    let Some(fixture) = TlsPgFixture::start().await else {
        return;
    };
    let yaml = mtls_yaml(
        "verify_full",
        &fixture.pki.ca_pem,
        Some((&fixture.pki.client_cert_pem, &fixture.pki.client_key_pem)),
    );
    probe_source(&fixture.conn_localhost(), &yaml)
        .await
        .expect("credential offered but unused must not break the sync");
}

// ---- Feature 007 US3: conn-string portability (connstring-portability.md) ----

#[tokio::test(flavor = "multi_thread")]
async fn sslrootcert_url_syncs_and_application_name_is_set() {
    use rdlt_connector::core::{LoadId, PipelineId};
    use rdlt_connector::{Destination as _, OpenCtx};

    let Some(fixture) = TlsPgFixture::start().await else {
        return;
    };
    // Write the CA where a real deployment would have it: on disk.
    let dir = tempfile::tempdir().expect("tempdir");
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&ca_path, &fixture.pki.ca_pem).expect("write ca");

    // A production-shaped libpq URL — verify-full + sslrootcert, NO tls block.
    let url = format!(
        "postgresql://postgres:postgres@localhost:{}/postgres?sslmode=verify-full&sslrootcert={}",
        fixture.port,
        ca_path.display()
    );

    // SOURCE: reflect over the URL (connects verified), then check that the
    // live session carries application_name=rdlt (A1 / SC-006).
    use rdlt_connector::Source as _;
    let source = PostgresSource::from_yaml(&format!("conn: \"{url}\"\n")).expect("config");
    source
        .streams()
        .await
        .expect("sslrootcert URL syncs (source)");
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=localhost port={} user=postgres password=postgres dbname=postgres sslmode=require",
            fixture.port
        ),
        {
            let mut roots = rustls::RootCertStore::empty();
            let mut cur = std::io::Cursor::new(fixture.pki.ca_pem.clone().into_bytes());
            for c in rustls_pemfile::certs(&mut cur) {
                roots.add(c.expect("ca cert")).expect("add ca");
            }
            tokio_postgres_rustls::MakeRustlsConnect::new(
                rustls::ClientConfig::builder_with_provider(
                    rustls::crypto::ring::default_provider().into(),
                )
                .with_safe_default_protocol_versions()
                .expect("protocol versions")
                .with_root_certificates(roots)
                .with_no_client_auth(),
            )
        },
    )
    .await
    .expect("probe connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    // Hold a source connection open while probing: reflect() connections are
    // short-lived, so probe our OWN default instead — a second rdlt-path
    // connection via the same gate.
    let held = {
        let parsed = rdlt_connector_postgres::tls::parse_conn(&url, None).expect("gate");
        rdlt_connector_postgres::tls::connect(&parsed.pg, &parsed.policy)
            .await
            .expect("held rdlt connection")
    };
    let names: Vec<String> = client
        .query(
            "SELECT DISTINCT application_name FROM pg_stat_activity WHERE application_name = 'rdlt'",
            &[],
        )
        .await
        .expect("pg_stat_activity")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(names, vec!["rdlt"], "A1: rdlt identifies itself");
    drop(held);

    // DESTINATION: the same URL through the same gate.
    let dest = rdlt_connector_postgres::dest::Postgres::connect(&url).dataset("url_ok");
    dest.open(OpenCtx::new(
        PipelineId::new("url"),
        LoadId::new("url-load"),
    ))
    .await
    .expect("sslrootcert URL opens (destination)");
}

/// Review F3: EVERY connect-phase db error carries the real server message —
/// not just the cert-28000 shape. Unknown database is the everyday case.
#[tokio::test(flavor = "multi_thread")]
async fn common_connect_failures_carry_the_server_message() {
    let Some(plain) = PgFixture::start().await else {
        return;
    };
    let bad_db = plain
        .conn
        .clone()
        .replace("dbname=postgres", "dbname=doesnotexist");
    let err = probe_source(&bad_db, "")
        .await
        .expect_err("unknown database must fail");
    assert!(
        err.contains("doesnotexist") && err.contains("SQLSTATE"),
        "server message + SQLSTATE surfaced, not bare 'db error': {err}"
    );
}
