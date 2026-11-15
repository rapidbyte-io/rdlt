//! The interpreter an install is asked for, the environments it makes with it, and the pip that
//! installs into the tree from outside it, each process a group of its own that dies with the
//! install.

#[cfg(test)]
mod tests;

use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use rdlt_connector::text::CUT;
use rdlt_host::Bubblewrap;
use tokio::io::{
    AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, BufReader,
};
use tokio::process::{Child, Command};

use super::tree::Staging;
use super::wheel::Wheel;
use super::{InstallError, blocking, io_error};
use crate::output;

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

/// Whether `command` ran and succeeded, what it prints for a person written to standard error as
/// it comes, so standard output holds the command line's result alone.
///
/// Its output reaches no terminal of its own: a group of its own is not the terminal's, and with
/// `tostop` set a write to it would stop the child for ever.
async fn ran(command: &mut Command) -> bool {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true);
    let Ok(mut child) = command.spawn() else {
        return false;
    };
    let (output, errors) = (child.stdout.take(), child.stderr.take());
    let mut group = Group(child);
    let (ended, (), ()) = tokio::join!(
        group.0.wait(),
        shown(output, tokio::io::stderr()),
        shown(errors, tokio::io::stderr())
    );
    ended.is_ok_and(|status| status.success())
}

/// Writes what a child writes to `from` to `to` until it closes, a line at a time, each shown,
/// since pip and venv print what a wheel and an index say; of a line longer than a line may be,
/// the start is written, marked as cut.
async fn shown(from: Option<impl AsyncRead + Unpin>, mut to: impl AsyncWrite + Unpin) {
    let Some(from) = from else {
        return;
    };
    let mut reader = BufReader::new(from);
    let (mut piece, mut within_line) = (Vec::new(), false);
    loop {
        piece.clear();
        let mut bounded = (&mut reader).take(output::LINE_BYTES as u64);
        match bounded.read_until(b'\n', &mut piece).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        // The rest of a line whose start was written is no line of its own.
        let continued = within_line;
        within_line = !piece.ends_with(b"\n");
        if continued {
            continue;
        }
        let text = String::from_utf8_lossy(&piece);
        let text = text.trim_end();
        let cut = if within_line && piece.len() == output::LINE_BYTES {
            CUT
        } else {
            ""
        };
        let line = format!("{}\n", output::shown(format_args!("{text}{cut}")));
        // A line that cannot be written is dropped: the child is read to its end all the same.
        to.write_all(line.as_bytes()).await.ok();
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
        Some(path) => Err(InstallError::InterpreterUnreachable {
            path: path.clone(),
            serving: super::tree::serving(module, attribute)
                .trim_end()
                .replace('\n', "; "),
        }),
    }
}

/// Makes, with `python`'s `venv`, the environment pip runs from, holding the pip the base
/// installation carries, and the tree, holding no pip.
pub(super) async fn made(python: &Path, staging: &Staging) -> Result<(), InstallError> {
    let mut installer = Command::new(python);
    installer
        .args(["-I", "-m", "venv"])
        .arg(staging.installer());
    let mut tree = Command::new(python);
    tree.args(["-I", "-m", "venv", "--without-pip"])
        .arg(staging.tree());
    if ran(&mut installer).await && ran(&mut tree).await {
        Ok(())
    } else {
        Err(InstallError::VenvFailed {
            python: python.to_owned(),
        })
    }
}

/// Installs `wheel`, and what it needs, into the tree of `staging` in one run of the pip beside
/// it: binaries only, so no build backend runs, and with `requirements` every package hashed, the
/// wheel among them, and none but theirs.
///
/// The tree is pip's prefix, never on its path, and is read after as data alone: a package that
/// replaced what of the tree is the environment's own is refused.
pub(super) async fn installed(
    staging: &Staging,
    wheel: &Wheel,
    requirements: Option<&Path>,
) -> Result<(), InstallError> {
    let tree = staging.tree();
    let mut pip = Command::new(staging.installer().join("bin/python"));
    // pip reads neither its variables, with `--isolated`, nor its files.
    pip.env("PIP_CONFIG_FILE", "/dev/null")
        .args(["-I", "-m", "pip", "--isolated", "--no-input"])
        .args(["--disable-pip-version-check", "--no-color", "install"])
        .args([
            "--only-binary=:all:",
            "--ignore-installed",
            "--progress-bar=off",
        ])
        .args(["--no-warn-script-location", "--prefix"])
        .arg(&tree);
    // A direct reference to the file, which no index is asked for.
    let direct = format!("{} @ {}", wheel.name, wheel.uri);
    match requirements {
        None => {
            pip.arg(&direct);
        }
        Some(requirements) => {
            let (pinned, line) = (
                staging.pinned(),
                format!("{direct} --hash=sha256:{}\n", wheel.sha256),
            );
            let written = pinned.clone();
            blocking(move || std::fs::write(written, line))
                .await
                .map_err(io_error("writing the wheel's requirements file"))?;
            pip.args(["--require-hashes", "-r"])
                .arg(requirements)
                .arg("-r")
                .arg(pinned);
        }
    }
    let own = blocking(move || Own::of(&tree)).await?;
    if !ran(&mut pip).await {
        return Err(InstallError::PipFailed);
    }
    blocking(move || own.kept()).await
}

/// What of a tree is the environment's own, as venv made it: each link of its `bin/`, the
/// interpreter its launcher runs among them, and its `pyvenv.cfg`, which that interpreter reads as
/// it starts.
struct Own {
    links: Vec<(PathBuf, PathBuf)>,
    configuration: PathBuf,
    configured: Option<Vec<u8>>,
}

impl Own {
    /// What of the tree at `tree` is the environment's own.
    fn of(tree: &Path) -> Result<Self, InstallError> {
        let bin = tree.join("bin");
        let reading = || io_error(format!("reading {}", bin.display()));
        let mut links = Vec::new();
        match std::fs::read_dir(&bin) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            read => {
                for entry in read.map_err(reading())? {
                    let path = entry.map_err(reading())?.path();
                    if let Ok(to) = std::fs::read_link(&path) {
                        links.push((path, to));
                    }
                }
            }
        }
        let configuration = tree.join("pyvenv.cfg");
        let configured = contents(&configuration)?;
        Ok(Self {
            links,
            configuration,
            configured,
        })
    }

    /// Refuses a tree in which what is the environment's own is not as it was.
    fn kept(&self) -> Result<(), InstallError> {
        let replaced = |path: &Path| InstallError::ReplacesEnvironment {
            path: path.to_owned(),
        };
        for (path, to) in &self.links {
            if std::fs::read_link(path).ok().as_ref() != Some(to) {
                return Err(replaced(path));
            }
        }
        if contents(&self.configuration)? != self.configured {
            return Err(replaced(&self.configuration));
        }
        Ok(())
    }
}

/// What the file at `path` holds, none where nothing is there.
fn contents(path: &Path) -> Result<Option<Vec<u8>>, InstallError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(io_error(format!("reading {}", path.display()))(source)),
    }
}
