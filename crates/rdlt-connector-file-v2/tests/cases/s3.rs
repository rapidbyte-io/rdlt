//! RUSTFS container fixture: one Apache-2.0 S3-compatible server per
//! fixture through testcontainers, health-checked by a signed request,
//! seeded through `object_store` directly. SKIP-NOT-FAIL: cells return
//! `None` visibly when no container runtime socket is reachable.

#![allow(dead_code)] // shared across two binaries; neither uses every helper

use object_store::ObjectStore;
use rdlt_connector_file_v2::{LocationOptions, S3Options};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

pub const ACCESS_KEY: &str = "rdlt-test-access";
pub const SECRET_KEY: &str = "rdlt-test-secret";
pub const BUCKET: &str = "raw";
/// Pinned: a floating `latest` re-resolves whenever upstream pushes,
/// so a broken upstream build would fail our gate with no change on
/// our side. Bump deliberately, with the live cells green.
pub const RUSTFS_TAG: &str = "1.0.0-beta.11";

pub struct S3Fixture {
    // Held for Drop: the container stops when the fixture drops.
    _container: ContainerAsync<GenericImage>,
    pub endpoint: String,
}

impl S3Fixture {
    /// Start RUSTFS, or skip visibly (None) without a runtime socket.
    pub async fn start() -> Option<Self> {
        if !rdlt_testkit::gate::runtime_available() {
            eprintln!("SKIP: no container runtime socket — s3 live cell not run");
            return None;
        }
        let container = GenericImage::new("docker.io/rustfs/rustfs", RUSTFS_TAG)
            .with_exposed_port(9000.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Starting"))
            .with_env_var("RUSTFS_ACCESS_KEY", ACCESS_KEY)
            .with_env_var("RUSTFS_SECRET_KEY", SECRET_KEY)
            .with_label(rdlt_testkit::gate::RECLAIM_LABEL, "1")
            .start()
            .await
            .expect("start rustfs container (runtime socket present)");
        let port = container
            .get_host_port_ipv4(9000)
            .await
            .expect("mapped port");
        let fixture = Self {
            _container: container,
            endpoint: format!("http://127.0.0.1:{port}"),
        };
        fixture.wait_ready().await;
        create_bucket(&fixture.endpoint, BUCKET);
        Some(fixture)
    }

    /// Readiness = a signed request answering; NotFound counts (the
    /// server is up, the object simply is not there).
    async fn wait_ready(&self) {
        let store = self.store();
        for _ in 0..100 {
            match store.head(&object_store::path::Path::from("probe")).await {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => return,
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(150)).await,
            }
        }
        panic!("rustfs did not become ready within the deadline");
    }

    fn store(&self) -> object_store::aws::AmazonS3 {
        object_store::aws::AmazonS3Builder::new()
            .with_endpoint(&self.endpoint)
            .with_bucket_name(BUCKET)
            .with_region("us-east-1")
            .with_access_key_id(ACCESS_KEY)
            .with_secret_access_key(SECRET_KEY)
            .with_allow_http(true)
            .build()
            .expect("seed client")
    }

    /// Seed one object, retried — many fixtures start concurrently
    /// under the full suite and one hiccup must not kill a cell.
    pub async fn put(&self, key: &str, body: &[u8]) {
        let store = self.store();
        let mut last = None;
        for attempt in 0..3u64 {
            match store
                .put(
                    &object_store::path::Path::from(key),
                    bytes::Bytes::copy_from_slice(body).into(),
                )
                .await
            {
                Ok(_) => return,
                Err(e) => {
                    last = Some(e);
                    tokio::time::sleep(std::time::Duration::from_millis(200 * (attempt + 1))).await;
                }
            }
        }
        panic!("seed put `{key}` failed after retries: {last:?}");
    }

    pub async fn exists(&self, key: &str) -> bool {
        self.store()
            .head(&object_store::path::Path::from(key))
            .await
            .is_ok()
    }

    /// The `location:` block for this fixture.
    pub fn location_options(&self) -> LocationOptions {
        LocationOptions::s3(S3Options::new(
            self.endpoint.clone(),
            BUCKET,
            ACCESS_KEY,
            SECRET_KEY,
        ))
    }
}

/// Minimal SigV4 PUT-bucket — `object_store` exposes no bucket admin
/// ops, and python3 + stdlib does the signing in twenty lines (fixture
/// only, never product code).
fn create_bucket(endpoint: &str, bucket: &str) {
    let script = format!(
        r#"
import datetime, hashlib, hmac, urllib.request, sys
ak, sk, region = "{ACCESS_KEY}", "{SECRET_KEY}", "us-east-1"
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
    if e.code not in (200, 409):  # 409 = already exists
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
