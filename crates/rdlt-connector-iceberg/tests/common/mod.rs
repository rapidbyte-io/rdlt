//! Polaris + RUSTFS fixture (research R8 + the T001 addendum — every fact
//! below was VERIFIED live at the environment gate). Host networking with
//! randomized ports (the vended s3 endpoint must be reachable by Polaris
//! AND the test client — container-internal DNS would vend unreachable
//! endpoints). SKIP-NOT-FAIL without a runtime socket.
#![allow(dead_code)] // consumers differ per test binary

use std::collections::HashMap;

use rdlt_connector_iceberg::{AuthOptions, IcebergConfig};

pub const CLIENT_ID: &str = "root";
pub const CLIENT_SECRET: &str = "s3cr3t";
pub const S3_KEY: &str = "ice-key";
pub const S3_SECRET: &str = "ice-secret";
pub const BUCKET: &str = "ice";
pub const WAREHOUSE: &str = "rdlt";

pub struct CatalogFixture {
    // Held for Drop: containers are force-removed when the fixture drops.
    _rustfs: ContainerGuard,
    _polaris: ContainerGuard,
    pub catalog_uri: String,
    pub s3_endpoint: String,
    pub admin_token: String,
    http: reqwest::Client,
}

/// Plain-podman container guard (testcontainers cannot express host
/// network mode against podman's compat API — it tries to CREATE a
/// network named "host"; the bench harness precedent applies instead).
pub struct ContainerGuard {
    name: String,
}

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        let _ = std::process::Command::new("podman")
            .args(["rm", "-f", &self.name])
            .output();
    }
}

fn run_container(prefix: &str, image: &str, envs: &[(String, String)]) -> ContainerGuard {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let name = format!(
        "rdlt-ice-{prefix}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    let mut command = std::process::Command::new("podman");
    command.args(["run", "-d", "--rm", "--name", &name, "--network", "host"]);
    for (key, value) in envs {
        command.args(["-e", &format!("{key}={value}")]);
    }
    command.arg(image);
    let output = command.output().expect("podman run");
    assert!(
        output.status.success(),
        "starting {image}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    ContainerGuard { name }
}

fn runtime_available() -> bool {
    std::process::Command::new("podman")
        .arg("ps")
        .output()
        .is_ok_and(|o| o.status.success())
}

/// A free port from a PID-disjoint range. `bind(:0)` alone races: nextest
/// runs each test in its own process, two fixtures can be handed the same
/// ephemeral port in the release-then-reuse window, and the second test
/// then talks to the FIRST test's containers (observed as create-catalog
/// flakes). Spreading candidate ranges by PID makes cross-process
/// collisions structurally unlikely; the bind probe still verifies each
/// candidate is actually free.
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    let pid = std::process::id();
    for _ in 0..2000 {
        let slot = NEXT.fetch_add(1, Ordering::Relaxed) as u32;
        // Stay BELOW the kernel ephemeral range (32768+): testcontainers
        // and other suites bind random ephemeral ports concurrently, and a
        // fixture squatting there collides with them (observed live).
        let candidate = 21000 + ((pid.wrapping_mul(641) + slot * 7) % 11000) as u16;
        if std::net::TcpListener::bind(("127.0.0.1", candidate)).is_ok() {
            return candidate;
        }
    }
    panic!("no free port found in the PID-derived range");
}

impl CatalogFixture {
    /// Start RUSTFS + Polaris, bootstrap a catalog over the bucket, grant
    /// the admin principal, create nothing else — or skip visibly (None).
    pub async fn start() -> Option<Self> {
        if !runtime_available() {
            eprintln!("SKIP: no container runtime socket — iceberg live cell not run");
            return None;
        }
        let s3_port = free_port();
        let api_port = free_port();
        let health_port = free_port();

        // RUSTFS on the host network at a random port (T001: RUSTFS_ADDRESS
        // is honored; anonymous requests answer S3-style XML).
        let rustfs = run_container(
            "rustfs",
            "docker.io/rustfs/rustfs:latest",
            &[
                ("RUSTFS_ADDRESS".into(), format!("0.0.0.0:{s3_port}")),
                ("RUSTFS_ACCESS_KEY".into(), S3_KEY.into()),
                ("RUSTFS_SECRET_KEY".into(), S3_SECRET.into()),
            ],
        );
        let s3_endpoint = format!("http://127.0.0.1:{s3_port}");
        // Any HTTP answer means RUSTFS is listening (anonymous requests
        // get S3-style error XML — never 2xx).
        wait_http_answers(&format!("{s3_endpoint}/"), 100, false).await;

        // Polaris (T001 facts): bootstrap credentials REALM,id,secret;
        // quarkus ports remapped; server-side S3 access via AWS_* env.
        let polaris = run_container(
            "polaris",
            "docker.io/apache/polaris:latest",
            &[
                (
                    "POLARIS_BOOTSTRAP_CREDENTIALS".into(),
                    format!("POLARIS,{CLIENT_ID},{CLIENT_SECRET}"),
                ),
                ("polaris.realm-context.realms".into(), "POLARIS".into()),
                ("QUARKUS_HTTP_PORT".into(), api_port.to_string()),
                ("QUARKUS_MANAGEMENT_PORT".into(), health_port.to_string()),
                ("AWS_REGION".into(), "us-east-1".into()),
                ("AWS_ACCESS_KEY_ID".into(), S3_KEY.into()),
                ("AWS_SECRET_ACCESS_KEY".into(), S3_SECRET.into()),
            ],
        );
        let base = format!("http://127.0.0.1:{api_port}");
        // Health must be 2xx: Quarkus answers 503 DOWN while Polaris is
        // still initializing (review F7) — any-answer is not readiness.
        wait_http_answers(
            &format!("http://127.0.0.1:{health_port}/q/health"),
            400,
            true,
        )
        .await;

        // Bucket + catalog + grants via the ONE bootstrap implementation
        // shared with the bench fixture (review F10 — the Rust copy had
        // already diverged from benches/fixtures/polaris_bootstrap.py).
        // Host networking: the client and Polaris see the same endpoint.
        bootstrap_catalog(&base, &s3_endpoint);

        let http = reqwest::Client::new();
        // OAuth (T001): client_credentials at /api/catalog/v1/oauth/tokens.
        let token: serde_json::Value = http
            .post(format!("{base}/api/catalog/v1/oauth/tokens"))
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", CLIENT_ID),
                ("client_secret", CLIENT_SECRET),
                ("scope", "PRINCIPAL_ROLE:ALL"),
            ])
            .send()
            .await
            .expect("oauth reachable")
            .json()
            .await
            .expect("oauth json");
        let admin_token = token["access_token"]
            .as_str()
            .expect("access_token present")
            .to_owned();

        Some(Self {
            _rustfs: rustfs,
            _polaris: polaris,
            catalog_uri: format!("{base}/api/catalog"),
            s3_endpoint,
            admin_token,
            http,
        })
    }

    fn management_base(&self) -> String {
        self.catalog_uri.trim_end_matches("/api/catalog").to_owned() + "/api/management/v1"
    }

    /// A ready destination config over this fixture (oauth2, no storage
    /// override — the vended-config path).
    pub fn config(&self, namespace: &str) -> IcebergConfig {
        IcebergConfig::new(
            self.catalog_uri.clone(),
            WAREHOUSE,
            AuthOptions::oauth2(CLIENT_ID, CLIENT_SECRET, ["PRINCIPAL_ROLE:ALL".to_string()]),
            namespace,
        )
        .with_create_namespace(true)
    }

    /// Raw table metadata JSON straight from the catalog (independent
    /// oracle for partition specs and layout assertions).
    pub async fn table_metadata(&self, namespace: &str, table: &str) -> serde_json::Value {
        self.http
            .get(format!(
                "{}/v1/{WAREHOUSE}/namespaces/{namespace}/tables/{table}",
                self.catalog_uri
            ))
            .bearer_auth(&self.admin_token)
            .send()
            .await
            .expect("load table")
            .json()
            .await
            .expect("table json")
    }

    /// Raw snapshot summaries for a table, newest-last — the receipt
    /// oracle (reads the catalog directly, independent of the crate).
    pub async fn snapshot_summaries(
        &self,
        namespace: &str,
        table: &str,
    ) -> Vec<HashMap<String, String>> {
        let response: serde_json::Value = self
            .http
            .get(format!(
                "{}/v1/{WAREHOUSE}/namespaces/{namespace}/tables/{table}",
                self.catalog_uri
            ))
            .bearer_auth(&self.admin_token)
            .send()
            .await
            .expect("load table")
            .json()
            .await
            .expect("table json");
        let mut snapshots: Vec<(i64, HashMap<String, String>)> = response["metadata"]["snapshots"]
            .as_array()
            .map(|list| {
                list.iter()
                    .map(|s| {
                        let ts = s["timestamp-ms"].as_i64().unwrap_or(0);
                        let summary = s["summary"]
                            .as_object()
                            .map(|m| {
                                m.iter()
                                    .filter_map(|(k, v)| {
                                        v.as_str().map(|v| (k.clone(), v.to_owned()))
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        (ts, summary)
                    })
                    .collect()
            })
            .unwrap_or_default();
        snapshots.sort_by_key(|(ts, _)| *ts);
        snapshots.into_iter().map(|(_, s)| s).collect()
    }
}

async fn wait_http_answers(url: &str, attempts: u32, require_success: bool) {
    let client = reqwest::Client::new();
    for _ in 0..attempts {
        if let Ok(response) = client
            .get(url)
            .timeout(std::time::Duration::from_secs(2))
            .send()
            .await
            && (!require_success || response.status().is_success())
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    panic!(
        "{url} never answered{}",
        if require_success { " 2xx" } else { "" }
    );
}

/// One bootstrap implementation for bucket + catalog + grants — the
/// SAME script the bench fixture runs (benches/fixtures/
/// polaris_bootstrap.py); under host networking the client-side and
/// Polaris-side S3 endpoints are identical.
fn bootstrap_catalog(polaris_base: &str, s3_endpoint: &str) {
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("repo root")
        .join("benches/fixtures/polaris_bootstrap.py");
    let output = std::process::Command::new("python3")
        .arg(script)
        .args([
            polaris_base,
            s3_endpoint,
            s3_endpoint,
            S3_KEY,
            S3_SECRET,
            CLIENT_ID,
            CLIENT_SECRET,
            WAREHOUSE,
            BUCKET,
        ])
        .output()
        .expect("python3 for the catalog bootstrap");
    assert!(
        output.status.success(),
        "polaris bootstrap failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
