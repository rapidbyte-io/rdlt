"""Shared, stdlib-only helpers for the Airbyte competitor module (feature 018,
T022). Imported by both setup.py (one-time cluster + connection setup) and
driver.py (per-cell timed sync). No third-party dependencies — the API is
plain urllib, S3 verification is the same hand-rolled SigV4 the bench's
fixtures/seed_s3.py uses, and Postgres row counts / cluster RSS go through
`podman exec` into the already-running fixture and kind-node containers. That
keeps `.venv` optional (the harness runs `<module>/.venv/bin/python driver.py`
only if a venv exists, else system python3).

Everything the spike pinned lives here so the two entrypoints stay thin:
  - the runtime facts from specs/018-bench-refinement/spike/{01,02,03,05}-*.md,
  - the two setup deltas (ingress scaled to 0; node pids-limit raised),
  - the port-forward supervision + short-call discipline the spike demanded.
"""

import datetime
import hashlib
import hmac
import json
import os
import re
import shutil
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

# --- pinned runtime facts (spike 01/02/03) -------------------------------

API_BASE = "http://127.0.0.1:8600"           # host port-forward target
PF_SVC = "svc/airbyte-abctl-airbyte-server-svc"
PF_LOCAL_PORT = 8600
PF_REMOTE_PORT = 8001
NAMESPACE = "airbyte-abctl"
NODE_CONTAINER = "airbyte-abctl-control-plane"   # the kind node (whole cluster)
AUTH_SECRET = "airbyte-auth-secrets"
# Pods reach host-published fixtures at this pasta host-mapping address; the
# host-side driver reaches the same fixtures at 127.0.0.1 (spike 02).
POD_HOST_IP = "169.254.1.2"
NODE_PIDS_LIMIT = 32768                        # setup delta 2 target

STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state.json")


# --- small process helpers -----------------------------------------------

def _kubeconfig():
    return os.environ.get(
        "KUBECONFIG", os.path.expanduser("~/.airbyte/abctl/abctl.kubeconfig")
    )


def _kubectl_bin():
    found = shutil.which("kubectl")
    if found:
        return found
    guess = os.path.expanduser(
        "~/.local/share/mise/installs/kubectl/latest/kubectl"
    )
    if os.path.exists(guess):
        return guess
    return "kubectl"  # last resort; will fail loudly if truly absent


def _podman_env():
    env = dict(os.environ)
    sock = f"/run/user/{os.getuid()}/podman/podman.sock"
    # The rootless socket makes `podman exec` reach the containers regardless of
    # where this script runs (host vs distrobox); harmless if already set.
    if os.path.exists(sock) and "DOCKER_HOST" not in env:
        env["DOCKER_HOST"] = f"unix://{sock}"
    return env


def kubectl(*args, check=True, timeout=60):
    """Run kubectl with the abctl kubeconfig; return stdout (stripped)."""
    env = dict(os.environ)
    env["KUBECONFIG"] = _kubeconfig()
    out = subprocess.run(
        [_kubectl_bin(), *args],
        capture_output=True, text=True, env=env, timeout=timeout,
    )
    if check and out.returncode != 0:
        raise RuntimeError(
            f"kubectl {' '.join(args)} failed: {out.stderr.strip() or out.stdout.strip()}"
        )
    return out.stdout.strip()


def podman(*args, check=True, timeout=120):
    out = subprocess.run(
        ["podman", *args],
        capture_output=True, text=True, env=_podman_env(), timeout=timeout,
    )
    if check and out.returncode != 0:
        raise RuntimeError(
            f"podman {' '.join(args)} failed: {out.stderr.strip() or out.stdout.strip()}"
        )
    return out.stdout.strip()


# --- cluster reachability + setup deltas ----------------------------------

def cluster_reachable():
    """True if the abctl cluster answers kubectl. Cheap; used by the
    prerequisite probe (must NOT depend on the port-forward)."""
    try:
        kubectl("get", "ns", NAMESPACE, "-o", "name", timeout=20)
        return True
    except Exception:
        return False


def enforce_setup_deltas():
    """Idempotently (re)apply the two runtime remediations the spike recorded:
    ingress-nginx scaled to 0 (its hostPort 443 otherwise hijacks all
    in-cluster :443), and the kind-node pids-limit raised (abctl's default
    2048 is exhausted by the idle stack). Both are safe to repeat."""
    kubectl(
        "-n", "ingress-nginx", "scale", "deploy/ingress-nginx-controller",
        "--replicas=0", check=False, timeout=30,
    )
    try:
        current = podman(
            "inspect", NODE_CONTAINER,
            "--format", "{{.HostConfig.PidsLimit}}", timeout=30,
        )
        if current.strip() not in (str(NODE_PIDS_LIMIT), "-1", "0"):
            podman(
                "update", f"--pids-limit={NODE_PIDS_LIMIT}", NODE_CONTAINER,
                timeout=30,
            )
    except Exception as exc:  # non-fatal: report, let caller decide
        print(f"warn: could not verify/raise node pids-limit: {exc}")


# --- auth + port-forward supervision (spike 03 gotchas) -------------------

def _creds():
    cid = _b64(kubectl(
        "-n", NAMESPACE, "get", "secret", AUTH_SECRET,
        "-o", "jsonpath={.data.instance-admin-client-id}",
    ))
    csec = _b64(kubectl(
        "-n", NAMESPACE, "get", "secret", AUTH_SECRET,
        "-o", "jsonpath={.data.instance-admin-client-secret}",
    ))
    return cid, csec


def _b64(s):
    import base64
    return base64.b64decode(s).decode()


def mint_token():
    """Mint a fresh access token. Tokens are short-lived, so callers re-mint
    per poll loop rather than caching (spike 03)."""
    cid, csec = _creds()
    status, data = http(
        "POST", "/api/v1/applications/token",
        body={"client_id": cid, "client_secret": csec},
    )
    if status != 200 or "access_token" not in (data or {}):
        raise RuntimeError(f"token mint failed: HTTP {status}: {data}")
    return data["access_token"]


def _pf_healthy():
    try:
        req = urllib.request.Request(f"{API_BASE}/api/v1/health")
        with urllib.request.urlopen(req, timeout=4) as resp:
            return resp.status == 200
    except Exception:
        return False


def ensure_port_forward():
    """Start-if-absent, idempotent. The port-forward drops on long synchronous
    calls, so this is re-callable between short calls; jobs complete
    server-side regardless of the HTTP connection (spike 03)."""
    if _pf_healthy():
        return
    env = dict(os.environ)
    env["KUBECONFIG"] = _kubeconfig()
    # Detached so it outlives this call and can be reused by later driver
    # invocations in the same session; stdout/err discarded.
    subprocess.Popen(
        [_kubectl_bin(), "-n", NAMESPACE, "port-forward", PF_SVC,
         f"{PF_LOCAL_PORT}:{PF_REMOTE_PORT}"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True, env=env,
    )
    for _ in range(30):
        if _pf_healthy():
            return
        time.sleep(0.5)
    raise RuntimeError("port-forward did not become healthy within 15s")


# --- Airbyte HTTP --------------------------------------------------------

def http(method, path, token=None, body=None, timeout=30, base=API_BASE):
    """One short HTTP call to the Airbyte API. Returns (status, parsed).
    Never holds a long synchronous call — callers fire jobs then poll."""
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(base + path, data=data, method=method,
                                 headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, raw.decode(errors="replace")


def api(method, path, token, body=None, timeout=30):
    """http() against the API, re-establishing the port-forward and re-minting
    the token once on a transport failure (HTTP 000-style) or 401."""
    ensure_port_forward()
    status, data = http(method, path, token=token, body=body, timeout=timeout)
    if status in (0, 401, 403) or status is None:
        ensure_port_forward()
        token = mint_token()
        status, data = http(method, path, token=token, body=body, timeout=timeout)
    return status, data, token


# --- job driving (spike 03 field pins) -----------------------------------

TERMINAL = {"succeeded", "failed", "cancelled", "incomplete"}


def trigger_job(token, connection_id, job_type):
    """POST a sync/reset job; returns (jobId, token)."""
    status, data, token = api(
        "POST", "/api/public/v1/jobs", token,
        body={"connectionId": connection_id, "jobType": job_type},
    )
    if status not in (200, 201) or not isinstance(data, dict) or "jobId" not in data:
        raise RuntimeError(f"{job_type} trigger failed: HTTP {status}: {data}")
    return data["jobId"], token


def poll_job(token, job_id, cap_s=1800, interval_s=15):
    """Poll GET /jobs/{id} to a terminal state; returns the terminal job dict.
    Re-mints the token every loop (short-lived) and tolerates transient
    port-forward drops."""
    deadline = time.time() + cap_s
    last = None
    while time.time() < deadline:
        token = mint_token()
        status, data, token = api("GET", f"/api/public/v1/jobs/{job_id}", token)
        if isinstance(data, dict) and data.get("status"):
            last = data
            if data["status"] in TERMINAL:
                return data
        time.sleep(interval_s)
    raise RuntimeError(f"job {job_id} did not reach terminal within {cap_s}s "
                       f"(last={last})")


def iso8601_seconds(dur):
    """Parse an ISO-8601 duration like 'PT49S'/'PT1M5S' to float seconds.
    Airbyte's job `duration` field (the platform job wall — spike 03)."""
    if not dur or not isinstance(dur, str):
        return None
    m = re.fullmatch(
        r"PT(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?", dur
    )
    if not m:
        return None
    h, mi, s = (float(x) if x else 0.0 for x in m.groups())
    return h * 3600 + mi * 60 + s


# --- destination verification --------------------------------------------

def pg_count(container, db, schema, table):
    """Row count of <schema>.<table> in database <db>, via psql inside the
    already-running fixture postgres container (stdlib-only — no pg driver).
    Returns None if the table does not exist."""
    sql = (f"SELECT count(*) FROM information_schema.tables WHERE "
           f"table_schema='{schema}' AND table_name='{table}';")
    exists = podman("exec", container, "psql", "-U", "postgres", "-d", db,
                    "-tAc", sql, check=False)
    if exists.strip() != "1":
        return None
    got = podman("exec", container, "psql", "-U", "postgres", "-d", db,
                 "-tAc", f'SELECT count(*) FROM "{schema}"."{table}";')
    return int(got.strip())


def node_memory_peak_kb():
    """The kind-node container's cgroup v2 memory.peak (KiB): a WHOLE-CLUSTER
    high-water mark (every Airbyte pod runs inside this one node container).
    This is the driver-reported peak_rss_kb — the module states plainly it is
    a cluster statistic, never a per-process RSS."""
    try:
        peak = podman("exec", NODE_CONTAINER, "cat", "/sys/fs/cgroup/memory.peak",
                      check=False)
        return int(peak.strip()) // 1024
    except Exception:
        return None


# --- S3 (RUSTFS) SigV4, stdlib-only — cleanup + verify for lake dests -----

_S3_REGION = "us-east-1"


def _s3_sign(method, endpoint, access, secret, bucket, key="", query=None,
             body=b""):
    host = endpoint.split("://", 1)[1]
    canon_uri = "/" + bucket + (("/" + key) if key else "")
    items = sorted((query or {}).items())
    canon_query = "&".join(
        f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}"
        for k, v in items
    )
    t = datetime.datetime.now(datetime.timezone.utc)
    amz, ds = t.strftime("%Y%m%dT%H%M%SZ"), t.strftime("%Y%m%d")
    payload = hashlib.sha256(body).hexdigest()
    creq = (f"{method}\n{canon_uri}\n{canon_query}\n"
            f"host:{host}\nx-amz-content-sha256:{payload}\nx-amz-date:{amz}\n\n"
            f"host;x-amz-content-sha256;x-amz-date\n{payload}")
    scope = f"{ds}/{_S3_REGION}/s3/aws4_request"
    sts = ("AWS4-HMAC-SHA256\n" + amz + "\n" + scope + "\n"
           + hashlib.sha256(creq.encode()).hexdigest())

    def _k(k, m):
        return hmac.new(k, m.encode(), hashlib.sha256).digest()

    signing = _k(_k(_k(_k(("AWS4" + secret).encode(), ds), _S3_REGION), "s3"),
                 "aws4_request")
    sig = hmac.new(signing, sts.encode(), hashlib.sha256).hexdigest()
    url = endpoint + canon_uri + (("?" + canon_query) if canon_query else "")
    headers = {
        "x-amz-date": amz,
        "x-amz-content-sha256": payload,
        "Authorization": (f"AWS4-HMAC-SHA256 Credential={access}/{scope}, "
                          f"SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
                          f"Signature={sig}"),
    }
    return urllib.request.Request(url, data=body if body else None,
                                  method=method, headers=headers)


def s3_list_keys(endpoint, access, secret, bucket, prefix):
    """List object keys under a prefix (ListObjectsV2). Returns [key, ...]."""
    keys, token = [], None
    while True:
        query = {"list-type": "2", "prefix": prefix}
        if token:
            query["continuation-token"] = token
        req = _s3_sign("GET", endpoint, access, secret, bucket, query=query)
        with urllib.request.urlopen(req, timeout=60) as resp:
            xml = resp.read().decode(errors="replace")
        keys.extend(re.findall(r"<Key>(.*?)</Key>", xml))
        m = re.search(r"<NextContinuationToken>(.*?)</NextContinuationToken>", xml)
        if m:
            token = m.group(1)
        else:
            break
    return keys


def s3_delete_prefix(endpoint, access, secret, bucket, prefix):
    """Delete every object under a prefix (per-run clean-dest for lake cells —
    the harness fixture reset does NOT cover lake output prefixes)."""
    deleted = 0
    for key in s3_list_keys(endpoint, access, secret, bucket, prefix):
        req = _s3_sign("DELETE", endpoint, access, secret, bucket, key=key)
        try:
            urllib.request.urlopen(req, timeout=60)
            deleted += 1
        except urllib.error.HTTPError as e:
            if e.code not in (204, 200, 404):
                raise
    return deleted


# --- state.json ----------------------------------------------------------

def load_state():
    with open(STATE_PATH) as fh:
        return json.load(fh)


def save_state(state):
    with open(STATE_PATH, "w") as fh:
        json.dump(state, fh, indent=2, sort_keys=True)
        fh.write("\n")
