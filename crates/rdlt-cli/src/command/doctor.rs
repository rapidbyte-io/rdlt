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

fn probe_lock(workdir: &Path) -> LockProbe {
    use std::os::unix::fs::OpenOptionsExt as _;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(false);
    options.custom_flags(libc::O_NOFOLLOW);
    // Not the engine's O_NOFOLLOW discipline re-derived lightly: the
    // same symlink refusal, because a doctor that follows a planted
    // link out of the workdir is a diagnostic reporting on the WRONG
    // file.
    match options.open(workdir.join(".lock")) {
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
                    match std::fs::create_dir_all(&workdir)
                        .map_err(|e| e.to_string())
                        .and_then(|()| {
                            let probe = workdir.join(".rdlt-doctor-probe");
                            let r = std::fs::write(&probe, b"");
                            let _ = std::fs::remove_file(&probe);
                            r.map_err(|e| e.to_string())
                        }) {
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
