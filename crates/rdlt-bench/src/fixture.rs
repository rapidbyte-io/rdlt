//! Fixtures: the stores a cell measures against, from
//! `benches/harness/fixtures/fixtures.toml`. Lifecycle: create → seed with
//! a recorded dataset identity → share within one invocation → teardown.
//! Kinds: `none`, `postgres_container` (seeded through psql, reset between
//! runs), `container` (any image with a published port).

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::template;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    None,
    /// Podman/docker postgres; `seed_sql` piped through psql once, its stdout
    /// captured as the dataset identity (seed scripts print their own hashes).
    PostgresContainer,
    /// Generic podman/docker container: `image` + `port:container_port`
    /// mapping + `run_args` (before the image: -e etc.); readiness = TCP
    /// connect on the host `port`; teardown = `rm -f` like the postgres kind.
    Container,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Def {
    pub id: String,
    pub kind: Kind,
    /// Shell line (sh -c) run once at startup in the fixture's data dir,
    /// after substitutions — datasets, source-config documents; for
    /// container kinds it runs once the service is up.
    #[serde(default)]
    pub generate_sh: Option<String>,
    /// Files (post-substitution paths) whose blake3 hashes are the identity.
    /// Spelled `hash` in fixtures.toml.
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
}

/// Shared values every postgres_container fixture inherits unless it states
/// its own — the image and the conn-string template differ only by port.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Defaults {
    postgres_image: Option<String>,
    /// `{{port}}` is filled from each fixture's own `port`.
    postgres_conn: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    #[serde(default)]
    defaults: Defaults,
    /// Reusable SQL blocks; a fixture opts in with `reset_sql = "@name"`.
    #[serde(default)]
    snippets: BTreeMap<String, String>,
    #[serde(default, rename = "fixture")]
    fixtures: Vec<Def>,
}

pub fn load(path: &Path) -> Result<Vec<Def>> {
    let File {
        defaults,
        snippets,
        mut fixtures,
    } = crate::error::load_toml(path)?;
    for def in &mut fixtures {
        // Resolve a `reset_sql = "@name"` reference against [snippets].
        if let Some(stripped) = def.reset_sql.as_deref().and_then(|s| s.strip_prefix('@')) {
            let resolved = snippets.get(stripped).cloned().ok_or_else(|| {
                Error(format!(
                    "fixture `{}`: unknown reset_sql snippet `@{stripped}`",
                    def.id
                ))
            })?;
            def.reset_sql = Some(resolved);
        }
        // Fill postgres defaults where the fixture omitted them.
        if def.kind == Kind::PostgresContainer {
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

impl Def {
    /// Load-time cross-field validation, before any container is touched:
    /// `reset_sql` runs psql inside a postgres container, so declaring it on
    /// any other kind is a config error, not a silent no-op at run time.
    fn validate(&self) -> Result<()> {
        if self.reset_sql.is_some() && self.kind != Kind::PostgresContainer {
            return Err(Error(format!(
                "fixture `{}`: reset_sql requires a postgres_container fixture",
                self.id
            )));
        }
        Ok(())
    }
}

pub(crate) fn container_engine() -> Result<String> {
    for candidate in ["podman", "docker"] {
        if Command::new(candidate)
            .arg("--version")
            .output()
            .is_ok_and(|o| o.status.success())
        {
            return Ok(candidate.to_owned());
        }
    }
    Err(Error("no container engine (podman or docker) found".into()))
}

/// A started fixture. Teardown on drop removes the container; the data dir
/// is a TempDir and cleans itself.
#[derive(Debug)]
pub struct Live {
    pub def: Def,
    pub data_dir: tempfile::TempDir,
    pub hashes: BTreeMap<String, String>,
    container: Option<(String, String)>, // (engine, name)
}

impl Live {
    pub fn conn(&self) -> Option<&str> {
        self.def.conn.as_deref()
    }

    /// Run `reset_sql` (if declared) — called before every warmup/counted
    /// run. Piped through psql's stdin rather than `-c` so a reset script may
    /// switch databases with `\connect`; ON_ERROR_STOP makes any failing
    /// statement fail the whole reset, not just the last one.
    pub fn reset(&self) -> Result<()> {
        let (Some((engine, name)), Some(sql)) = (&self.container, &self.def.reset_sql) else {
            return Ok(());
        };
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
            return Err(Error(format!(
                "fixture `{}` reset failed: {}",
                self.def.id,
                String::from_utf8_lossy(&out.stderr)
            )));
        }
        Ok(())
    }
}

fn teardown(container: &mut Option<(String, String)>) {
    if let Some((engine, name)) = container.take() {
        let _ = Command::new(&engine).args(["rm", "-f", &name]).output();
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        teardown(&mut self.container);
    }
}

/// Error-path teardown: the container `start()` brings up is held here
/// first; on success the guard is disarmed into `Live`, on any `?` its Drop
/// removes the container.
#[derive(Debug, Default)]
struct CleanupGuard {
    container: Option<(String, String)>,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        teardown(&mut self.container);
    }
}

/// Wait for a host port to accept connections. Generous: an image's first
/// start may initialize its data before it listens.
fn wait_tcp(port: u16, what: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(180);
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    Err(Error(format!("{what}: port {port} never became ready")))
}

/// Bring up one detached container and arm the cleanup guard with it. The
/// port mapping and any args BEFORE the image (`-e KEY=V`, extra mounts) vary
/// by kind; everything else — `rm -f` the stale name, `run -d --name`, image,
/// trailing `container_args`, the success check — is shared. Returns the
/// container name (`rdlt-bench-<id>`) callers wait on / seed through.
fn start_container(
    engine: &str,
    def: &Def,
    port_map: String,
    pre_image_args: &[&str],
    guard: &mut CleanupGuard,
) -> Result<String> {
    let image = def
        .image
        .as_deref()
        .ok_or_else(|| Error(format!("fixture `{}`: missing image", def.id)))?;
    let name = format!("rdlt-bench-{}", def.id);
    let _ = Command::new(engine).args(["rm", "-f", &name]).output();
    let status = Command::new(engine)
        // `--label rdlt-test=1` is the workspace's reclaim convention: a bench
        // run killed mid-cell never reaches its guard's Drop, and an
        // unlabelled leftover is invisible to `make reclaim`.
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
        return Err(Error(format!("starting container {name} failed")));
    }
    guard.container = Some((engine.to_owned(), name.clone()));
    Ok(name)
}

/// Bring up the postgres container: start it, wait for readiness, seed it
/// (retrying across the image's post-initdb restart), and capture the seed's
/// printed dataset identity.
fn bring_up_postgres(
    def: &Def,
    subs: &BTreeMap<String, String>,
    hashes: &mut BTreeMap<String, String>,
    guard: &mut CleanupGuard,
) -> Result<()> {
    let engine = container_engine()?;
    let port = def
        .port
        .ok_or_else(|| Error(format!("fixture `{}`: missing port", def.id)))?;
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
            return Err(Error(format!("{name}: postgres never became ready")));
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    if let Some(seed) = &def.seed_sql {
        let seed_path = template::substitute(&seed.display().to_string(), subs);
        // The postgres image restarts once after initdb: pg_isready can pass
        // against the init-phase temporary server, so retry seeding across
        // the restart gap.
        let mut out = None;
        for attempt in 0..5 {
            if attempt > 0 {
                std::thread::sleep(Duration::from_secs(2));
            }
            let seed_file = std::fs::File::open(&seed_path)
                .map_err(|e| Error(format!("opening seed {seed_path}: {e}")))?;
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
            return Err(Error(format!(
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
    Ok(())
}

/// Bring up a generic container: start it with its `port:container_port`
/// mapping and `run_args`, then wait for the host port to accept
/// connections.
fn bring_up_container(def: &Def, guard: &mut CleanupGuard) -> Result<()> {
    let engine = container_engine()?;
    let port = def
        .port
        .ok_or_else(|| Error(format!("fixture `{}`: missing port", def.id)))?;
    let container_port = def
        .container_port
        .ok_or_else(|| Error(format!("fixture `{}`: missing container_port", def.id)))?;
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

/// Start one fixture: its kind's bring-up, then `generate_sh` in the fresh
/// data dir (exposed as `{{data}}`), then the identity hashes.
pub fn start(def: &Def, subs: &BTreeMap<String, String>) -> Result<Live> {
    let data = tempfile::tempdir().map_err(|e| Error(format!("tempdir: {e}")))?;
    let mut subs = subs.clone();
    subs.insert("data".into(), data.path().display().to_string());
    let sub = |s: &str| template::substitute(s, &subs);

    let mut hashes = BTreeMap::new();
    let mut guard = CleanupGuard::default();

    match def.kind {
        Kind::None => {}
        Kind::PostgresContainer => bring_up_postgres(def, &subs, &mut hashes, &mut guard)?,
        Kind::Container => bring_up_container(def, &mut guard)?,
    }

    if let Some(script) = &def.generate_sh {
        let script = sub(script);
        let status = Command::new("sh")
            .args(["-c", &script])
            .current_dir(data.path())
            .status()?;
        if !status.success() {
            return Err(Error(format!("generate_sh failed ({status}): {script}")));
        }
    }

    for pattern in &def.hash_files {
        let path = sub(pattern);
        let bytes = std::fs::read(&path).map_err(|e| Error(format!("hashing {path}: {e}")))?;
        hashes.insert(path, blake3::hash(&bytes).to_hex().to_string());
    }

    // Success: disarm the guard into the Live (whose Drop owns teardown).
    Ok(Live {
        def: def.clone(),
        data_dir: data,
        hashes,
        container: guard.container.take(),
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn none_def(id: &str) -> Def {
        Def {
            id: id.into(),
            kind: Kind::None,
            generate_sh: None,
            container_args: vec![],
            hash_files: vec![],
            image: None,
            port: None,
            seed_sql: None,
            reset_sql: None,
            conn: None,
            run_args: Vec::new(),
            container_port: None,
        }
    }

    #[test]
    fn none_fixture_starts_bare() {
        let started = start(&none_def("none"), &BTreeMap::new()).unwrap();
        assert!(started.hashes.is_empty());
        assert!(started.conn().is_none());
        started.reset().unwrap(); // no-op without a container
    }

    /// Any kind may generate files into its data dir and hash them as the
    /// dataset identity — the container fixtures do exactly this.
    #[test]
    fn generate_sh_runs_in_the_data_dir_and_hashes_the_identity() {
        let mut def = none_def("gen");
        def.generate_sh = Some("printf 'hello' > {{data}}/f.txt".into());
        def.hash_files = vec!["{{data}}/f.txt".into()];
        let started = start(&def, &BTreeMap::new()).unwrap();
        let (_, hash) = started.hashes.iter().next().unwrap();
        assert_eq!(hash, &blake3::hash(b"hello").to_hex().to_string());
    }

    #[test]
    fn fixtures_toml_rejects_unknown_fields() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("fixtures.toml");
        std::fs::write(&p, "[[fixture]]\nid='x'\nkind='none'\nwat=1\n").unwrap();
        let err = load(&p).unwrap_err().to_string();
        assert!(err.contains("wat"), "{err}");
    }

    /// Only the three kinds exist: a registry spelling any other is refused
    /// at load, naming the value.
    #[test]
    fn an_unknown_fixture_kind_is_rejected_naming_it() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("fixtures.toml");
        std::fs::write(&p, "[[fixture]]\nid='x'\nkind='sidecar'\n").unwrap();
        let err = load(&p).unwrap_err().to_string();
        assert!(err.contains("sidecar"), "{err}");
    }

    #[test]
    fn reset_sql_on_a_non_postgres_kind_is_a_load_error() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("fixtures.toml");
        std::fs::write(
            &p,
            "[[fixture]]\nid='x'\nkind='none'\nreset_sql='select 1'\n",
        )
        .unwrap();
        let err = load(&p).unwrap_err().to_string();
        assert!(err.contains("requires a postgres_container"), "{err}");
    }
}
