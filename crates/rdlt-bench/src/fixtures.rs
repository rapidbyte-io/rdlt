//! Fixture lifecycle (data-model.md §2): create → seed with recorded identity →
//! share within an invocation → teardown. Kinds: `none`, `generated_files`,
//! `postgres_container`, `service` (background process, e.g. the mock REST API).

use std::collections::BTreeMap;
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
    #[serde(default)]
    pub hash: Vec<String>,
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
    #[serde(default)]
    pub service_sh: Option<String>,
    #[serde(default)]
    pub ready_port: Option<u16>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureFile {
    #[serde(default, rename = "fixture")]
    fixtures: Vec<FixtureDef>,
}

pub fn load_fixtures(path: &Path) -> Result<Vec<FixtureDef>> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| BenchError(format!("reading {}: {e}", path.display())))?;
    let file: FixtureFile =
        toml::from_str(&raw).map_err(|e| BenchError(format!("parsing {}: {e}", path.display())))?;
    Ok(file.fixtures)
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
    pub data: tempfile::TempDir,
    pub hashes: BTreeMap<String, String>,
    container: Option<(String, String)>, // (engine, name)
    service: Option<std::process::Child>,
}

impl Started {
    pub fn conn(&self) -> Option<&str> {
        self.def.conn.as_deref()
    }

    /// Run `reset_sql` (if declared) — called before every warmup/counted run.
    pub fn reset(&self) -> Result<()> {
        let (Some((engine, name)), Some(sql)) = (&self.container, &self.def.reset_sql) else {
            return Ok(());
        };
        let out = Command::new(engine)
            .args(["exec", name, "psql", "-q", "-U", "postgres", "-c", sql])
            .output()?;
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
) {
    if let Some((engine, name)) = container.take() {
        let _ = Command::new(&engine).args(["rm", "-f", &name]).output();
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
        teardown(&mut self.container, &mut self.service, self.def.ready_port);
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
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        teardown(&mut self.container, &mut self.service, self.ready_port);
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

pub fn start(def: &FixtureDef, subs: &BTreeMap<String, String>) -> Result<Started> {
    let data = tempfile::tempdir().map_err(|e| BenchError(format!("tempdir: {e}")))?;
    let mut subs = subs.clone();
    subs.insert("data".into(), data.path().display().to_string());
    let sub = |s: &str| crate::runner::substitute(s, &subs);

    let mut hashes = BTreeMap::new();
    let mut guard = CleanupGuard {
        container: None,
        service: None,
        ready_port: def.ready_port,
    };

    match def.kind {
        FixtureKind::None | FixtureKind::GeneratedFiles => {}
        FixtureKind::PostgresContainer => {
            let engine = container_engine()?;
            let image = def
                .image
                .as_deref()
                .ok_or_else(|| BenchError(format!("fixture `{}`: missing image", def.id)))?;
            let port = def
                .port
                .ok_or_else(|| BenchError(format!("fixture `{}`: missing port", def.id)))?;
            let name = format!("rdlt-bench-{}", def.id);
            let _ = Command::new(&engine).args(["rm", "-f", &name]).output();
            let status = Command::new(&engine)
                .args([
                    "run",
                    "-d",
                    "--name",
                    &name,
                    "-e",
                    "POSTGRES_PASSWORD=postgres",
                    "-p",
                    &format!("{port}:5432"),
                    image,
                ])
                .args(&def.container_args)
                .status()?;
            if !status.success() {
                return Err(BenchError(format!("starting container {name} failed")));
            }
            guard.container = Some((engine.clone(), name.clone()));
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
                spawn_service(def, &sub(script), data.path(), &mut guard)?;
            }
        }
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

    for pattern in &def.hash {
        let path = sub(pattern);
        let bytes = std::fs::read(&path).map_err(|e| BenchError(format!("hashing {path}: {e}")))?;
        hashes.insert(path, blake3::hash(&bytes).to_hex().to_string());
    }

    // Success: disarm the guard into the Started (whose Drop owns teardown).
    let container = guard.container.take();
    let service = guard.service.take();
    Ok(Started {
        def: def.clone(),
        data,
        hashes,
        container,
        service,
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
            hash: vec![],
            image: None,
            port: None,
            seed_sql: None,
            reset_sql: None,
            conn: None,
            service_sh: None,
            ready_port: None,
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
            hash: vec!["{{data}}/f.txt".into()],
            image: None,
            port: None,
            seed_sql: None,
            reset_sql: None,
            conn: None,
            service_sh: None,
            ready_port: None,
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
