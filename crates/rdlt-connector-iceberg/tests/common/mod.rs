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
    admin_token: String,
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
        let candidate = 21000 + ((pid.wrapping_mul(641) + slot * 7) % 40000) as u16;
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
        wait_http_answers(&format!("{s3_endpoint}/"), 100).await;
        create_bucket(&s3_endpoint, BUCKET).await;

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
        wait_http_answers(&format!("http://127.0.0.1:{health_port}/q/health"), 200).await;

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

        let fixture = Self {
            _rustfs: rustfs,
            _polaris: polaris,
            catalog_uri: format!("{base}/api/catalog"),
            s3_endpoint,
            admin_token,
            http,
        };
        fixture.create_catalog().await;
        Some(fixture)
    }

    /// The T001-verified catalog payload: S3-compatible storage with
    /// stsUnavailable + pathStyle, credentials AND s3.region in the
    /// properties (opendal requires the region; Polaris vends all of it
    /// through /v1/config defaults — the config-level delegation path).
    async fn create_catalog(&self) {
        let payload = serde_json::json!({
            "catalog": {
                "name": WAREHOUSE,
                "type": "INTERNAL",
                "properties": {
                    "default-base-location": format!("s3://{BUCKET}/warehouse"),
                    "s3.endpoint": self.s3_endpoint,
                    "s3.path-style-access": "true",
                    "s3.access-key-id": S3_KEY,
                    "s3.secret-access-key": S3_SECRET,
                    "s3.region": "us-east-1"
                },
                "storageConfigInfo": {
                    "storageType": "S3",
                    "allowedLocations": [format!("s3://{BUCKET}/warehouse")],
                    "endpoint": self.s3_endpoint,
                    "pathStyleAccess": true,
                    "stsUnavailable": true
                }
            }
        });
        let response = self
            .http
            .post(format!(
                "{}/api/management/v1/catalogs",
                self.catalog_uri.trim_end_matches("/api/catalog").to_owned() + ""
            ))
            .bearer_auth(&self.admin_token)
            .json(&payload)
            .send()
            .await
            .expect("create catalog");
        assert!(
            response.status().is_success() || response.status().as_u16() == 409,
            "create catalog: {}",
            response.status()
        );
        // Grants (T001): catalog_admin gets CATALOG_MANAGE_CONTENT and is
        // assigned to the service_admin principal role.
        let management = self.management_base();
        for (url, body) in [
            (
                format!("{management}/catalogs/{WAREHOUSE}/catalog-roles/catalog_admin/grants"),
                serde_json::json!({"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}),
            ),
            (
                format!("{management}/principal-roles/service_admin/catalog-roles/{WAREHOUSE}"),
                serde_json::json!({"catalogRole": {"name": "catalog_admin"}}),
            ),
        ] {
            let response = self
                .http
                .put(&url)
                .bearer_auth(&self.admin_token)
                .json(&body)
                .send()
                .await
                .expect("grant call");
            assert!(
                response.status().is_success(),
                "grant {url}: {}",
                response.status()
            );
        }
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

async fn wait_http_answers(url: &str, attempts: u32) {
    let client = reqwest::Client::new();
    for _ in 0..attempts {
        if let Ok(response) = client
            .get(url)
            .timeout(std::time::Duration::from_secs(2))
            .send()
            .await
        {
            let _ = response;
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    panic!("{url} never answered");
}

/// Fixture-only SigV4 PUT-bucket via python3 stdlib (the 015 pattern —
/// never product code).
async fn create_bucket(endpoint: &str, bucket: &str) {
    let script = format!(
        r#"
import datetime, hashlib, hmac, urllib.request, sys
ak, sk, region = "{S3_KEY}", "{S3_SECRET}", "us-east-1"
endpoint = "{endpoint}"
host = endpoint.split("://", 1)[1]
t = datetime.datetime.now(datetime.timezone.utc)
amz, ds = t.strftime("%Y%m%dT%H%M%SZ"), t.strftime("%Y%m%d")
payload = hashlib.sha256(b"").hexdigest()
creq = f"PUT\n/{bucket}\n\nhost:{{host}}\nx-amz-content-sha256:{{payload}}\nx-amz-date:{{amz}}\n\nhost;x-amz-content-sha256;x-amz-date\n{{payload}}"
scope = f"{{ds}}/{{region}}/s3/aws4_request"
sts = "AWS4-HMAC-SHA256\n" + amz + "\n" + scope + "\n" + hashlib.sha256(creq.encode()).hexdigest()
def sign(k, m): return hmac.new(k, m.encode(), hashlib.sha256).digest()
sig = hmac.new(sign(sign(sign(sign(("AWS4"+sk).encode(), ds), region), "s3"), "aws4_request"), sts.encode(), hashlib.sha256).hexdigest()
req = urllib.request.Request(endpoint + "/{bucket}", method="PUT", headers={{
    "x-amz-date": amz, "x-amz-content-sha256": payload,
    "Authorization": f"AWS4-HMAC-SHA256 Credential={{ak}}/{{scope}}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={{sig}}"}})
try:
    urllib.request.urlopen(req, timeout=10)
except urllib.error.HTTPError as e:
    if e.code not in (200, 409):
        sys.exit(f"create bucket: HTTP {{e.code}}: {{e.read()[:200]}}")
"#
    );
    let out = std::process::Command::new("python3")
        .arg("-c")
        .arg(&script)
        .output()
        .expect("python3 for the fixture bucket-create");
    assert!(
        out.status.success(),
        "create bucket failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
