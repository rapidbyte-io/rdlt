//! T008: the TLS matrix (contract tls-policy.md) — five modes against a real
//! TLS-only Postgres with a generated CA, positive AND distinguished
//! negative cases, for BOTH connectors through the one shared connect path.

mod common;

use common::{PgFixture, TlsPgFixture};
use rdlt_postgres::source::{PostgresConfig, PostgresSource};
use rdlt_postgres::tls::{RootCert, TlsMode, TlsPolicy};

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
    let fixture = TlsPgFixture::start().await;
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
    let plain = PgFixture::start().await;
    probe_source(&plain.conn_url(), "")
        .await
        .expect("default prefer falls back to plaintext");
    probe_source(&format!("{} sslmode=disable", plain.conn_url()), "")
        .await
        .expect("explicit disable on a plaintext server");
    let err = probe_source(&format!("{} sslmode=require", plain.conn_url()), "")
        .await
        .expect_err("require against a server without TLS must fail");
    assert!(err.contains("connect phase"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_uses_the_same_policy_path() {
    use rdlt_connector::core::{LoadId, PipelineId};
    use rdlt_connector::{Destination as _, OpenCtx};

    let fixture = TlsPgFixture::start().await;
    let pipeline = PipelineId::new("tls");
    let load = LoadId::new("tls-load");

    // verify_full + right root + matching hostname: the destination opens.
    let ok = rdlt_postgres::dest::Postgres::connect(fixture.conn_localhost())
        .dataset("tls_ok")
        .tls(TlsPolicy {
            mode: TlsMode::VerifyFull,
            root_cert: Some(RootCert(fixture.pki.ca_pem.clone())),
        });
    assert!(
        ok.open(OpenCtx::new(pipeline.clone(), load.clone()))
            .await
            .is_ok(),
        "destination over verify_full must open"
    );

    // Same policy, wrong trust anchor: typed failure through the SAME path.
    let bad = rdlt_postgres::dest::Postgres::connect(fixture.conn_localhost())
        .dataset("tls_bad")
        .tls(TlsPolicy {
            mode: TlsMode::VerifyFull,
            root_cert: Some(RootCert(fixture.pki.wrong_ca_pem.clone())),
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
