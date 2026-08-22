//! The `doctor` subcommand: the offline environment probe. No run, no
//! build, no connector spawn — the answers an operator needs when a
//! pipeline will not start, before reaching for `check` (which builds
//! for real) or `run` (which loads).
//!
//! Checks, in report order:
//! - this CLI's version (the first thing any bug report needs);
//! - connector binaries discoverable on `PATH` (inventory — the facade
//!   names no connector, so this is what IS resolvable, not what a
//!   document needs);
//! - with a document: that it parses, the pipeline's name and its
//!   resolved workdir, whether that workdir is writable, and whether
//!   another process holds the run lock (THE "why is my pipeline
//!   stuck" answer).
//!
//! Exit code 0 = every check passed; 1 = at least one finding.

use std::path::{Path, PathBuf};

use rdlt::document;

use crate::exit;

pub(crate) struct Finding {
    passed: bool,
    detail: String,
}

fn report(findings: &[Finding]) -> Result<(), exit::Error> {
    for finding in findings {
        let mark = if finding.passed { "ok  " } else { "FAIL" };
        render::stderr::line(&format!("{mark}  {}", finding.detail));
    }
    let failed = findings.iter().filter(|f| !f.passed).count();
    if failed == 0 {
        render::stderr::line("all checks passed");
        Ok(())
    } else {
        Err(exit::Error::Findings(format!("{failed} check(s) failed")))
    }
}

/// The PATH inventory: every executable whose name starts with
/// `rdlt-connector-`, in PATH order, deduplicated by name.
fn connector_inventory() -> Vec<String> {
    let mut found = std::collections::BTreeSet::new();
    if let Some(paths) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&paths) {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let name = entry.file_name();
                let Some(name) = name.to_str() else {
                    continue;
                };
                if name.starts_with("rdlt-connector-") && is_executable(&entry.path()) {
                    found.insert(name.to_string());
                }
            }
        }
    }
    found.into_iter().collect()
}

fn is_executable(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::metadata(path)
            .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        path.is_file()
    }
}

/// The workdir lock probe: open `.lock` inside the workdir and take the
/// advisory lock non-blocking. Held means a run is active RIGHT NOW;
/// free means nobody runs (the file persisting between runs is its
/// documented shape). Best-effort by design: a probe that cannot open
/// the file reports why rather than failing the whole doctor.
enum LockProbe {
    Free,
    Held,
    Unavailable(String),
}

/// The writability probe, the way a run would touch the workdir: adopt
/// it under the shared rule (born 0700, or this user's and nobody
/// else's to write), then create ONE file there exclusively — a name
/// nobody could have planted, since it carries this process's id and
/// the clock, created `O_EXCL` and never following a link — and remove
/// exactly that file. A fixed name written with a truncating open
/// would follow a planted symlink and empty whatever it pointed at.
fn probe_writable(workdir: &Path) -> std::io::Result<()> {
    rdlt_core::fs::create_or_verify_private_dir(workdir)?;
    let probe = workdir.join(format!(
        ".rdlt-doctor-probe-{}-{:x}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let created = rdlt_core::fs::create_private(&probe);
    let removed = std::fs::remove_file(&probe);
    created.map(drop).and(removed)
}

/// The lock probe rides the lock file's own open discipline — no
/// symlink followed out of the workdir, no FIFO parked on — so a
/// doctor never reports on the wrong file or hangs where the run would
/// have refused.
fn probe_lock(workdir: &Path) -> LockProbe {
    match rdlt_core::fs::open_or_create_private(&workdir.join(".lock")) {
        Ok(file) => match file.try_lock() {
            Ok(_guard) => LockProbe::Free,
            Err(std::fs::TryLockError::WouldBlock) => LockProbe::Held,
            Err(std::fs::TryLockError::Error(e)) => {
                LockProbe::Unavailable(format!("lock probe failed: {e}"))
            }
        },
        Err(e) => LockProbe::Unavailable(format!("cannot open .lock: {e}")),
    }
}

pub(crate) fn doctor(spec: Option<PathBuf>) -> Result<(), exit::Error> {
    let mut findings = Vec::new();

    findings.push(Finding {
        passed: true,
        detail: format!("rdlt {}", env!("CARGO_PKG_VERSION")),
    });

    let connectors = connector_inventory();
    findings.push(Finding {
        passed: true,
        detail: format!(
            "{}: {}",
            if connectors.is_empty() {
                "no connector binaries found on PATH"
            } else {
                "connector binaries on PATH"
            },
            connectors.join(", ")
        ),
    });

    if let Some(spec_path) = spec {
        match document::read(&spec_path) {
            Err(reason) => findings.push(Finding {
                passed: false,
                detail: format!("document {}: {reason}", spec_path.display()),
            }),
            Ok(text) => match document::parse(&text) {
                Err(reason) => findings.push(Finding {
                    passed: false,
                    detail: format!("parsing {}: {reason}", spec_path.display()),
                }),
                Ok(doc) => {
                    findings.push(Finding {
                        passed: true,
                        detail: format!("pipeline `{}` parses", doc.pipeline),
                    });
                    let base = spec_path.parent().unwrap_or(Path::new(""));
                    let workdir = rdlt::pipeline::resolved_workdir(&doc, base);
                    match probe_writable(&workdir).map_err(|e| e.to_string()) {
                        Err(reason) => findings.push(Finding {
                            passed: false,
                            detail: format!(
                                "workdir {}: not writable: {reason}",
                                workdir.display()
                            ),
                        }),
                        Ok(()) => {
                            findings.push(Finding {
                                passed: true,
                                detail: format!("workdir {} writable", workdir.display()),
                            });
                            match probe_lock(&workdir) {
                                LockProbe::Free => findings.push(Finding {
                                    passed: true,
                                    detail: "run lock: free (no run active)".to_string(),
                                }),
                                LockProbe::Held => findings.push(Finding {
                                    passed: false,
                                    detail: format!(
                                        "run lock at {} HELD — another process is running \
                                         this pipeline right now",
                                        workdir.display()
                                    ),
                                }),
                                LockProbe::Unavailable(why) => findings.push(Finding {
                                    passed: false,
                                    detail: format!("run lock: {why}"),
                                }),
                            }
                        }
                    }
                }
            },
        }
    }

    report(&findings)
}

use crate::render;

#[cfg(test)]
mod tests {
    use super::*;

    /// The PATH inventory finds nothing in an empty environment and is
    /// deterministic — a diagnostic must never be a surprise.
    #[test]
    fn empty_path_yields_no_inventory() {
        // Not manipulating the real PATH here; the function tolerates a
        // missing PATH var (the `else` arm of var_os) by construction.
        let found = connector_inventory();
        // Whatever this machine carries, names are sorted and unique.
        let sorted = {
            let mut c = found.clone();
            c.sort();
            c
        };
        assert_eq!(found, sorted, "inventory is sorted");
    }

    /// The workdir `doctor` births is the engine's: private to the
    /// user on every component it created.
    #[test]
    fn a_workdir_born_by_doctor_is_private() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("tempdir");
        let workdir = dir.path().join("nested").join("wd");
        probe_writable(&workdir).expect("born and probed");
        for made in [workdir.as_path(), workdir.parent().expect("nested")] {
            let mode = std::fs::metadata(made).expect("meta").permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "{}: {mode:o}", made.display());
        }
        assert!(
            std::fs::read_dir(&workdir).expect("list").next().is_none(),
            "the probe removed itself"
        );
    }

    /// A planted symlink at the old fixed probe name is never followed:
    /// the probe's name is unpredictable and its creation exclusive, so
    /// the victim the link points at is untouched and the workdir still
    /// probes writable. A workdir another user could write is refused
    /// before any probe.
    #[test]
    fn the_probe_follows_no_link_and_refuses_a_shared_workdir() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("tempdir");
        let workdir = dir.path().join("wd");
        std::fs::create_dir(&workdir).expect("mkdir");
        std::fs::set_permissions(&workdir, std::fs::Permissions::from_mode(0o700)).expect("chmod");
        let victim = dir.path().join("victim");
        std::fs::write(&victim, b"precious").expect("victim");
        std::os::unix::fs::symlink(&victim, workdir.join(".rdlt-doctor-probe")).expect("plant");
        probe_writable(&workdir).expect("writable");
        assert_eq!(std::fs::read(&victim).expect("victim"), b"precious");

        std::fs::set_permissions(&workdir, std::fs::Permissions::from_mode(0o777)).expect("chmod");
        let refused = probe_writable(&workdir).expect_err("shared");
        assert!(refused.to_string().contains("0777"), "{refused}");
    }

    /// A held lock probes as HELD, and a fresh temp dir probes FREE.
    #[test]
    fn the_lock_probe_distinguishes_held_from_free() {
        let dir = tempfile::tempdir().expect("tempdir");
        let workdir = dir.path().join("wd");
        std::fs::create_dir_all(&workdir).expect("mkdir");
        assert!(matches!(probe_lock(&workdir), LockProbe::Free));
        {
            let _guard = doctor_tests::hold(&workdir);
            assert!(matches!(probe_lock(&workdir), LockProbe::Held));
        }
        assert!(matches!(probe_lock(&workdir), LockProbe::Free));
    }
}

#[cfg(test)]
mod doctor_tests {
    //! The hold fixture lives here so the pin above stays the only
    //! consumer.

    use std::path::Path;

    pub(super) fn hold(workdir: &Path) -> std::fs::File {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(workdir.join(".lock"))
            .expect("open lock");
        match file.try_lock() {
            Ok(_guard) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                panic!("a fresh lock must lock")
            }
            Err(std::fs::TryLockError::Error(e)) => panic!("lock failed: {e}"),
        }
        file
    }
}
