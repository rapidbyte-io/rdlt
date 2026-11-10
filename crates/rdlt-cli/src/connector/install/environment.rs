//! The interpreter an install is asked for, and the environment it makes with it, each process
//! a group of its own that dies with the install.

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::process::Stdio;

use rdlt_host::Bubblewrap;
use tokio::process::{Child, Command};

use super::wheel::Wheel;
use super::{InstallError, blocking, io_error};

/// What an interpreter says it is: its version, and where its executable, the base executable an
/// environment it makes links to, and its standard library lie.
#[derive(serde::Deserialize)]
pub(super) struct Facts {
    version: (u32, u32, u32),
    /// Every link resolved.
    executable: PathBuf,
    /// As an environment's interpreter names it.
    base: PathBuf,
    /// Every link resolved.
    stdlib: PathBuf,
}

/// The facts query, run by the interpreter asked.
const FACTS: &str = include_str!("facts.py");

/// The check that the environment holds every package its packages need, run by its
/// interpreter with no `site` and the environment's directory, whose packages' directories it
/// puts on the path by hand: with no `site`, Python before 3.14 knows no environment of its own,
/// and a distribution's default scheme may name its own directories.
const CHECK: &str = include_str!("check.py");

/// What the check exits with where the environment's pip does not import, so the check did not
/// run.
const CHECK_UNRUN: i32 = 100;

/// A child leading a process group of its own: dropped before it ends, as an install dropped on a
/// signal drops it, it is killed with every process of its group.
struct Group(Child);

impl Drop for Group {
    fn drop(&mut self) {
        // A child reaped has no id, and its group's number may be another's by then.
        let pid = self.0.id().and_then(|id| i32::try_from(id).ok());
        if let Some(pid) = pid.and_then(rustix::process::Pid::from_raw) {
            rustix::process::kill_process_group(pid, rustix::process::Signal::KILL).ok();
        }
    }
}

/// Whether `command` ran and succeeded, what it prints for a person copied to standard error as it
/// comes, so standard output holds the command line's result alone.
///
/// Its output reaches no terminal of its own: a group of its own is not the terminal's, and with
/// `tostop` set a write to it would stop the child for ever.
async fn ran(command: &mut Command) -> bool {
    status(command).await.is_some_and(|status| status.success())
}

/// What `command` exited with, run as [`ran`] runs it; none where it did not start or was ended by
/// a signal.
async fn status(command: &mut Command) -> Option<std::process::ExitStatus> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true);
    let mut child = command.spawn().ok()?;
    let (output, errors) = (child.stdout.take(), child.stderr.take());
    let mut group = Group(child);
    let (ended, (), ()) = tokio::join!(group.0.wait(), copied(output), copied(errors));
    ended.ok()
}

/// Copies what a child writes to `from` to this process's standard error, until it closes.
async fn copied(from: Option<impl tokio::io::AsyncRead + Unpin>) {
    if let Some(mut from) = from {
        tokio::io::copy(&mut from, &mut tokio::io::stderr())
            .await
            .ok();
    }
}

/// What `python` says it is, once it is seen to be 3.11 or later.
pub(super) async fn facts(python: &Path) -> Result<Facts, InstallError> {
    let failed = || InstallError::InterpreterFailed {
        python: python.to_owned(),
        asked: "saying what interpreter it is",
    };
    let output = Command::new(python)
        .args(["-I", "-S", "-c", FACTS])
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output()
        .await
        .map_err(|_| failed())?;
    let facts: Facts = serde_json::from_slice(&output.stdout).map_err(|_| failed())?;
    if facts.version < (3, 11, 0) {
        let (major, minor, micro) = facts.version;
        return Err(InstallError::InterpreterTooOld {
            python: python.to_owned(),
            found: format!("{major}.{minor}.{micro}"),
        });
    }
    Ok(facts)
}

/// Refuses, on Linux, an interpreter that lies where a sandbox does not show it, saying how to
/// serve `module`'s `attribute` without an install.
pub(super) fn reachable(facts: &Facts, module: &str, attribute: &str) -> Result<(), InstallError> {
    if cfg!(not(target_os = "linux")) {
        return Ok(());
    }
    let paths = [&facts.executable, &facts.base, &facts.stdlib];
    let outside = paths.into_iter().find(|path| {
        !Bubblewrap::SYSTEM
            .iter()
            .any(|system| path.starts_with(system))
    });
    match outside {
        None => Ok(()),
        Some(path) => {
            let head = attribute
                .split_once('.')
                .map_or(attribute, |(head, _)| head);
            Err(InstallError::InterpreterUnreachable {
                path: path.clone(),
                serving: format!(
                    "from rdlt.connector import serve; from {module} import {head}; \
                     raise SystemExit(serve({attribute}))"
                ),
            })
        }
    }
}

/// Makes the environment at `environment` with `python`'s `venv`, before anything is installed
/// in it.
pub(super) async fn made(python: &Path, environment: &Path) -> Result<(), InstallError> {
    let mut venv = Command::new(python);
    venv.args(["-I", "-m", "venv"]).arg(environment);
    if ran(&mut venv).await {
        Ok(())
    } else {
        Err(InstallError::VenvFailed {
            python: python.to_owned(),
        })
    }
}

/// Installs `wheel`, and what it needs, into `environment` in one run of its pip, which starts
/// before anything of theirs is there: binaries only, so no build backend runs, and with
/// `requirements` every package hashed, the wheel among them, and none but theirs.
pub(super) async fn installed(
    environment: &Path,
    wheel: &Wheel,
    requirements: Option<&Path>,
) -> Result<(), InstallError> {
    let python = environment.join("bin/python");
    let mut pip = Command::new(&python);
    pip.args([
        "-I",
        "-m",
        "pip",
        "--no-input",
        "--disable-pip-version-check",
    ])
    .args(["install", "--only-binary=:all:"]);
    // A direct reference to the file, which no index is asked for.
    let direct = format!("{} @ {}", wheel.name, wheel.uri);
    // Held until pip has read it.
    let pinned = match requirements {
        None => {
            pip.arg(&direct);
            None
        }
        Some(requirements) => {
            let pinned = pinned(&direct, &wheel.sha256).await?;
            pip.args(["--require-hashes", "--no-deps", "-r"])
                .arg(requirements)
                .arg("-r")
                .arg(pinned.path());
            Some(pinned)
        }
    };
    let installing = ran(&mut pip).await;
    blocking(move || drop(pinned)).await;
    if !installing {
        return Err(InstallError::PipFailed {
            what: "the wheel and the packages it needs",
        });
    }
    if requirements.is_none() {
        return Ok(());
    }
    let mut check = Command::new(&python);
    check.args(["-I", "-S", "-c", CHECK]).arg(environment);
    match status(&mut check).await.and_then(|status| status.code()) {
        Some(0) => Ok(()),
        Some(CHECK_UNRUN) => Err(InstallError::CheckUnrun { python }),
        _ => Err(InstallError::PipFailed {
            what: "a package the wheel needs and the requirements leave out",
        }),
    }
}

/// A requirements file naming the wheel, `direct`, by the hash its bytes were read with.
async fn pinned(direct: &str, sha256: &str) -> Result<tempfile::NamedTempFile, InstallError> {
    let line = format!("{direct} --hash=sha256:{sha256}\n");
    blocking(move || {
        let pinned = tempfile::Builder::new()
            .prefix("rdlt-install-")
            .suffix(".txt")
            .tempfile()
            .map_err(io_error("making the wheel's requirements file"))?;
        std::fs::write(pinned.path(), line)
            .map_err(io_error("writing the wheel's requirements file"))?;
        Ok(pinned)
    })
    .await
}
