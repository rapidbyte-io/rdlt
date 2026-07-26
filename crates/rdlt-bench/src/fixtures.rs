//! Fixture lifecycle: create → seed with recorded identity →
//! share within an invocation → teardown. Kinds: `none`, `generated_files`,
//! `postgres_container`, `service` (background process, e.g. the mock REST API).

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::{BenchError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FixtureKind {
    None,
    /// `generate_sh` runs once (in the fixture's data dir); `hash` files are
    /// blake3-hashed as the dataset identity.
    GeneratedFiles,
    /// Podman/docker postgres; `seed_sql` piped through psql once, its stdout
    /// captured as the dataset identity (seed scripts print their own hashes).
    PostgresContainer,
    /// Long-running background process (killed at teardown); readiness = TCP
    /// connect on `ready_port`.
    Service,
    /// Generic podman/docker container (015: the RUSTFS object store):
    /// `image` + `port:container_port` mapping + `run_args` (before the
    /// image: -e etc.); readiness = TCP connect on the host `port`;
    /// teardown = `rm -f` like the postgres kind.
    Container,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureDef {
    pub id: String,
    pub kind: FixtureKind,
    /// Shell line (sh -c) run once at startup, after substitutions.
    #[serde(default)]
    pub generate_sh: Option<String>,
    /// Files (post-substitution paths) whose blake3 hashes are the identity.
    /// Spelled `hash` in fixtures.toml (the file format is frozen).
    #[serde(default, rename = "hash")]
    pub hash_files: Vec<String>,
    #[serde(default)]
    pub image: Option<String>,
    /// Extra args after the image (e.g. `-c wal_level=logical`).
    #[serde(default)]
    pub container_args: Vec<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub seed_sql: Option<PathBuf>,
    /// SQL executed between runs (drop destination schemas etc.).
    #[serde(default)]
    pub reset_sql: Option<String>,
    /// Connection string exposed to pipeline templates as `{{conn}}`.
    #[serde(default)]
    pub conn: Option<String>,
    /// Container kind: args BEFORE the image (`-e KEY=V`, extra mounts…).
    #[serde(default)]
    pub run_args: Vec<String>,
    /// Container kind: the container-side port `port` maps to.
    #[serde(default)]
    pub container_port: Option<u16>,
    #[serde(default)]
    pub service_sh: Option<String>,
    #[serde(default)]
    pub ready_port: Option<u16>,
    /// Shell run between runs (like `reset_sql`, but a script — 016: drop
    /// the Iceberg namespace so every run loads fresh). Substituted.
    #[serde(default)]
    pub reset_sh: Option<String>,
    /// Shell run at teardown BEFORE container removal — for fixtures whose
    /// `generate_sh` brings up sidecar resources (e.g. a second container)
    /// the harness does not manage directly (016: RUSTFS beside Polaris).
    #[serde(default)]
    pub teardown_sh: Option<String>,
}

/// Shared values every postgres_container fixture inherits unless it states
/// its own — the image and the conn-string template differ only by port.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureDefaults {
    postgres_image: Option<String>,
    /// `{{port}}` is filled from each fixture's own `port`.
    postgres_conn: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureFile {
    #[serde(default)]
    defaults: FixtureDefaults,
    /// Reusable SQL/script blocks; a fixture opts in with `reset_sql = "@name"`.
    #[serde(default)]
    snippets: BTreeMap<String, String>,
    #[serde(default, rename = "fixture")]
    fixtures: Vec<FixtureDef>,
}

pub fn load_fixtures(path: &Path) -> Result<Vec<FixtureDef>> {
    let FixtureFile {
        defaults,
        snippets,
        mut fixtures,
    } = crate::load_toml(path)?;
    for def in &mut fixtures {
        // Resolve a `reset_sql = "@name"` reference against [snippets].
        if let Some(stripped) = def.reset_sql.as_deref().and_then(|s| s.strip_prefix('@')) {
            let resolved = snippets.get(stripped).cloned().ok_or_else(|| {
                BenchError(format!(
                    "fixture `{}`: unknown reset_sql snippet `@{stripped}`",
                    def.id
                ))
            })?;
            def.reset_sql = Some(resolved);
        }
        // Fill postgres defaults where the fixture omitted them.
        if def.kind == FixtureKind::PostgresContainer {
            if def.image.is_none() {
                def.image = defaults.postgres_image.clone();
            }
            if def.conn.is_none()
                && let (Some(tmpl), Some(port)) = (&defaults.postgres_conn, def.port)
            {
                def.conn = Some(tmpl.replace("{{port}}", &port.to_string()));
            }
        }
        def.validate()?;
    }
    Ok(fixtures)
}

impl FixtureDef {
    /// Load-time cross-field validation, before any container is touched.
    fn validate(&self) -> Result<()> {
        // reset_sql runs `psql` inside a postgres container; on any other kind
        // Started::reset would silently no-op, so declaring it elsewhere is a
        // config error, not a run-time surprise.
        if self.reset_sql.is_some() && self.kind != FixtureKind::PostgresContainer {
            return Err(BenchError(format!(
                "fixture `{}`: reset_sql requires a postgres_container fixture",
                self.id
            )));
        }
        Ok(())
    }
}

pub fn container_engine() -> Result<String> {
    for candidate in ["podman", "docker"] {
        if Command::new(candidate)
            .arg("--version")
            .output()
            .is_ok_and(|o| o.status.success())
        {
            return Ok(candidate.to_owned());
        }
    }
    Err(BenchError(
        "no container engine (podman or docker) found".into(),
    ))
}

/// A started fixture. Teardown on drop: containers removed, services killed;
/// the data dir is a TempDir and cleans itself.
#[derive(Debug)]
pub struct Started {
    pub def: FixtureDef,
    pub data_dir: tempfile::TempDir,
    pub hashes: BTreeMap<String, String>,
    container: Option<(String, String)>, // (engine, name)
    service: Option<std::process::Child>,
    teardown_sh: Option<String>, // substituted
    reset_sh: Option<String>,    // substituted
}

impl Started {
    pub fn conn(&self) -> Option<&str> {
        self.def.conn.as_deref()
    }

    /// Run `reset_sql`/`reset_sh` (if declared) — called before every
    /// warmup/counted run.
    pub fn reset(&self) -> Result<()> {
        if let Some(script) = &self.reset_sh {
            let status = Command::new("sh").args(["-c", script]).status()?;
            if !status.success() {
                return Err(BenchError(format!(
                    "fixture `{}` reset_sh failed ({status})",
                    self.def.id
                )));
            }
        }
        let (Some((engine, name)), Some(sql)) = (&self.container, &self.def.reset_sql) else {
            return Ok(());
        };
        // Pipe the SQL through psql's stdin rather than `-c` so a reset script
        // may switch databases with `\connect` (the per-product destination
        // databases live on one server). ON_ERROR_STOP makes any failing
        // statement in the script fail the whole reset, not just the last one.
        let mut child = Command::new(engine)
            .args([
                "exec",
                "-i",
                name,
                "psql",
                "-q",
                "-v",
                "ON_ERROR_STOP=1",
                "-U",
                "postgres",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .expect("stdin piped")
            .write_all(sql.as_bytes())?;
        let out = child.wait_with_output()?;
        if !out.status.success() {
            return Err(BenchError(format!(
                "fixture `{}` reset failed: {}",
                self.def.id,
                String::from_utf8_lossy(&out.stderr)
            )));
        }
        Ok(())
    }
}

fn teardown(
    container: &mut Option<(String, String)>,
    service: &mut Option<std::process::Child>,
    ready_port: Option<u16>,
    teardown_sh: &mut Option<String>,
) {
    // The FIRST act stays force-removing the fixture container (the
    // pre-016 invariant); the sidecar script runs after, bounded — a
    // wedged script must not hang the bench process in Drop forever.
    if let Some((engine, name)) = container.take() {
        let _ = Command::new(&engine).args(["rm", "-f", &name]).output();
    }
    if let Some(script) = teardown_sh.take() {
        let _ = Command::new("timeout")
            .args(["30", "sh", "-c", &script])
            .status();
    }
    if let Some(mut child) = service.take() {
        let _ = child.kill();
        let _ = child.wait();
        // `sh -c "… && cargo run …"` wrappers orphan the real service
        // (finding 5): also kill whatever still holds the ready port.
        if let Some(port) = ready_port {
            let _ = Command::new("sh")
                .args([
                    "-c",
                    &format!("fuser -k {port}/tcp 2>/dev/null || pkill -f 'mock_api' || true"),
                ])
                .status();
        }
    }
}

impl Drop for Started {
    fn drop(&mut self) {
        teardown(
            &mut self.container,
            &mut self.service,
            self.def.ready_port,
            &mut self.teardown_sh,
        );
    }
}

/// Error-path teardown (finding 8): everything `start()` brings up is held
/// here first; on success the guard is disarmed into `Started`, on any `?`
/// its Drop removes the container and kills the service.
#[derive(Debug, Default)]
struct CleanupGuard {
    container: Option<(String, String)>,
    service: Option<std::process::Child>,
    ready_port: Option<u16>,
    teardown_sh: Option<String>,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        teardown(
            &mut self.container,
            &mut self.service,
            self.ready_port,
            &mut self.teardown_sh,
        );
    }
}

fn wait_tcp(port: u16, what: &str) -> Result<()> {
    // Generous: `cargo run --release --example …` services may build first.
    let deadline = Instant::now() + Duration::from_secs(180);
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    Err(BenchError(format!(
        "{what}: port {port} never became ready"
    )))
}

/// Bring up one detached container and arm the cleanup guard with it. The
/// port mapping and any args BEFORE the image (`-e KEY=V`, extra mounts) vary
/// by kind; everything else — `rm -f` the stale name, `run -d --name`, image,
/// trailing `container_args`, the success check — is shared. Returns the
/// container name (`rdlt-bench-<id>`) callers wait on / seed through.
fn start_container(
    engine: &str,
    def: &FixtureDef,
    port_map: String,
    pre_image_args: &[&str],
    guard: &mut CleanupGuard,
) -> Result<String> {
    let image = def
        .image
        .as_deref()
        .ok_or_else(|| BenchError(format!("fixture `{}`: missing image", def.id)))?;
    let name = format!("rdlt-bench-{}", def.id);
    let _ = Command::new(engine).args(["rm", "-f", &name]).output();
    let status = Command::new(engine)
        // `--label rdlt-test=1`: the workspace reclaim convention (see
        // rdlt-testkit::containers). A bench run killed mid-cell never reaches
        // its guard's Drop, and an unlabelled leftover is invisible to
        // `make reclaim`.
        .args([
            "run",
            "-d",
            "--label",
            "rdlt-test=1",
            "--name",
            &name,
            "-p",
            &port_map,
        ])
        .args(pre_image_args.iter().copied())
        .arg(image)
        .args(&def.container_args)
        .status()?;
    if !status.success() {
        return Err(BenchError(format!("starting container {name} failed")));
    }
    guard.container = Some((engine.to_owned(), name.clone()));
    Ok(name)
}

fn run_sh(script: &str, cwd: &Path) -> Result<()> {
    let status = Command::new("sh")
        .args(["-c", script])
        .current_dir(cwd)
        .status()?;
    if !status.success() {
        return Err(BenchError(format!(
            "generate_sh failed ({status}): {script}"
        )));
    }
    Ok(())
}

/// Bring up the postgres container: start it, wait for readiness, seed it
/// (retrying across the image's post-initdb restart), capture the seed's
/// printed dataset identity, and spawn any sidecar service beside it.
fn bring_up_postgres(
    def: &FixtureDef,
    subs: &BTreeMap<String, String>,
    data: &Path,
    hashes: &mut BTreeMap<String, String>,
    guard: &mut CleanupGuard,
) -> Result<()> {
    let sub = |s: &str| crate::template::substitute(s, subs);
    let engine = container_engine()?;
    let port = def
        .port
        .ok_or_else(|| BenchError(format!("fixture `{}`: missing port", def.id)))?;
    let name = start_container(
        &engine,
        def,
        format!("{port}:5432"),
        &["-e", "POSTGRES_PASSWORD=postgres"],
        guard,
    )?;
    // pg_isready inside the container, then the host-published port.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let ready = Command::new(&engine)
            .args(["exec", &name, "pg_isready", "-U", "postgres"])
            .output()
            .is_ok_and(|o| o.status.success());
        if ready {
            break;
        }
        if Instant::now() > deadline {
            return Err(BenchError(format!("{name}: postgres never became ready")));
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    if let Some(seed) = &def.seed_sql {
        let seed_path = sub(&seed.display().to_string());
        // The postgres image restarts once after initdb: pg_isready
        // can pass against the init-phase temporary server, so retry
        // seeding across the restart gap.
        let mut out = None;
        for attempt in 0..5 {
            if attempt > 0 {
                std::thread::sleep(Duration::from_secs(2));
            }
            let seed_file = std::fs::File::open(&seed_path)
                .map_err(|e| BenchError(format!("opening seed {seed_path}: {e}")))?;
            let result = Command::new(&engine)
                .args(["exec", "-i", &name, "psql", "-q", "-U", "postgres"])
                .stdin(seed_file)
                .output()?;
            let ok = result.status.success();
            out = Some(result);
            if ok {
                break;
            }
        }
        let out = out.expect("at least one attempt ran");
        if !out.status.success() {
            return Err(BenchError(format!(
                "seeding `{}` failed after retries: {}",
                def.id,
                String::from_utf8_lossy(&out.stderr)
            )));
        }
        // Seed scripts print their own dataset identity — capture it.
        let stdout = String::from_utf8_lossy(&out.stdout);
        let identity: Vec<&str> = stdout
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .collect();
        if !identity.is_empty() {
            hashes.insert("seed_output".into(), identity.join(" | "));
        }
    }
    // Sidecar service beside the container (the REST→PG cell needs
    // both the mock API and a Postgres destination).
    if let Some(script) = &def.service_sh {
        spawn_service(def, &sub(script), data, guard)?;
    }
    Ok(())
}

/// Bring up a generic container (015: the RUSTFS object store): start it with
/// its `port:container_port` mapping and `run_args`, then wait for the host
/// port to accept connections.
fn bring_up_container(def: &FixtureDef, guard: &mut CleanupGuard) -> Result<()> {
    let engine = container_engine()?;
    let port = def
        .port
        .ok_or_else(|| BenchError(format!("fixture `{}`: missing port", def.id)))?;
    let container_port = def
        .container_port
        .ok_or_else(|| BenchError(format!("fixture `{}`: missing container_port", def.id)))?;
    let run_args: Vec<&str> = def.run_args.iter().map(String::as_str).collect();
    start_container(
        &engine,
        def,
        format!("{port}:{container_port}"),
        &run_args,
        guard,
    )?;
    wait_tcp(port, &def.id)?;
    Ok(())
}

pub fn start(def: &FixtureDef, subs: &BTreeMap<String, String>) -> Result<Started> {
    let data = tempfile::tempdir().map_err(|e| BenchError(format!("tempdir: {e}")))?;
    let mut subs = subs.clone();
    subs.insert("data".into(), data.path().display().to_string());
    let sub = |s: &str| crate::template::substitute(s, &subs);

    let mut hashes = BTreeMap::new();
    let mut guard = CleanupGuard {
        container: None,
        service: None,
        ready_port: def.ready_port,
        teardown_sh: def.teardown_sh.as_deref().map(&sub),
    };

    match def.kind {
        FixtureKind::None | FixtureKind::GeneratedFiles => {}
        FixtureKind::PostgresContainer => {
            bring_up_postgres(def, &subs, data.path(), &mut hashes, &mut guard)?;
        }
        FixtureKind::Container => bring_up_container(def, &mut guard)?,
        FixtureKind::Service => {
            let script = def
                .service_sh
                .as_deref()
                .ok_or_else(|| BenchError(format!("fixture `{}`: missing service_sh", def.id)))?;
            spawn_service(def, &sub(script), data.path(), &mut guard)?;
        }
    }

    // Any kind may generate files (datasets, source-config documents) — for
    // container kinds this runs after the service is up.
    if let Some(script) = &def.generate_sh {
        run_sh(&sub(script), data.path())?;
    }

    for pattern in &def.hash_files {
        let path = sub(pattern);
        let bytes = std::fs::read(&path).map_err(|e| BenchError(format!("hashing {path}: {e}")))?;
        hashes.insert(path, blake3::hash(&bytes).to_hex().to_string());
    }

    // Success: disarm the guard into the Started (whose Drop owns teardown).
    let container = guard.container.take();
    let service = guard.service.take();
    let teardown_sh = guard.teardown_sh.take();
    let reset_sh = def.reset_sh.as_deref().map(&sub);
    Ok(Started {
        def: def.clone(),
        data_dir: data,
        hashes,
        container,
        service,
        teardown_sh,
        reset_sh,
    })
}

/// Spawn a service sidecar into the guard. REFUSES if the ready port is
/// already bound: a stale orphan from a previous session would otherwise be
/// measured in place of the fresh service (finding 5).
fn spawn_service(
    def: &FixtureDef,
    script: &str,
    cwd: &Path,
    guard: &mut CleanupGuard,
) -> Result<()> {
    if let Some(port) = def.ready_port
        && std::net::TcpStream::connect(("127.0.0.1", port)).is_ok()
    {
        return Err(BenchError(format!(
            "fixture `{}`: port {port} is ALREADY bound before the service started — \
             a stale service from a previous session? kill it (fuser -k {port}/tcp) and retry",
            def.id
        )));
    }
    let child = Command::new("sh")
        .args(["-c", script])
        .current_dir(cwd)
        .spawn()?;
    guard.service = Some(child);
    if let Some(port) = def.ready_port {
        wait_tcp(port, &def.id)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_fixture_starts_bare() {
        let def = FixtureDef {
            id: "none".into(),
            kind: FixtureKind::None,
            generate_sh: None,
            container_args: vec![],
            hash_files: vec![],
            image: None,
            port: None,
            seed_sql: None,
            reset_sql: None,
            conn: None,
            service_sh: None,
            run_args: Vec::new(),
            container_port: None,
            ready_port: None,
            reset_sh: None,
            teardown_sh: None,
        };
        let started = start(&def, &BTreeMap::new()).unwrap();
        assert!(started.hashes.is_empty());
        assert!(started.conn().is_none());
        started.reset().unwrap(); // no-op without a container
    }

    #[test]
    fn generated_files_run_and_hash_identity() {
        let def = FixtureDef {
            id: "gen".into(),
            kind: FixtureKind::GeneratedFiles,
            generate_sh: Some("printf 'hello' > {{data}}/f.txt".into()),
            container_args: vec![],
            hash_files: vec!["{{data}}/f.txt".into()],
            image: None,
            port: None,
            seed_sql: None,
            reset_sql: None,
            conn: None,
            service_sh: None,
            ready_port: None,
            run_args: Vec::new(),
            container_port: None,
            reset_sh: None,
            teardown_sh: None,
        };
        let started = start(&def, &BTreeMap::new()).unwrap();
        let (_, hash) = started.hashes.iter().next().unwrap();
        assert_eq!(hash, &blake3::hash(b"hello").to_hex().to_string());
    }

    #[test]
    fn fixtures_toml_rejects_unknown_fields() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("fixtures.toml");
        std::fs::write(&p, "[[fixture]]\nid='x'\nkind='none'\nwat=1\n").unwrap();
        let err = load_fixtures(&p).unwrap_err().to_string();
        assert!(err.contains("wat"), "{err}");
    }
}
