//! `connector install` installs into the host's first connector directory, making it where it is
//! not there, says what it installed, and exits 2 where it refuses and 74 where a file fails it.

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use crate::support::{Scratch, output_of};

/// An interpreter at `path` that answers the facts query with `facts`, and says of any wheel that
/// it is one connector's.
fn fake_python(path: &Path, facts: &str) -> PathBuf {
    let described = r#"{"name": "tickets", "uri": "file:///absent.whl", "sha256": "00", "entry_points": [["io.example.tickets", "tickets:Tickets"]]}"#;
    let script =
        format!("#!/bin/sh\nif [ \"$#\" = 5 ]; then echo '{described}'; else echo '{facts}'; fi\n");
    std::fs::write(path, script).expect("the script writes");
    let mode = std::fs::Permissions::from_mode(0o755);
    std::fs::set_permissions(path, mode).expect("the script is executable");
    path.to_owned()
}

/// The facts of the system's Python 3.14, as the fakes below answer them.
const FACTS: &str = r#"{"version": [3, 14, 0], "executable": "/usr/bin/python3.14", "base": "/usr/bin/python3", "stdlib": "/usr/lib/python3.14"}"#;

/// As [`fake_python`], answering the system's facts, and running `making`, shell, when asked to
/// make an environment, which it does not make.
fn fake_python_making(path: &Path, making: &str) -> PathBuf {
    let python = fake_python(path, FACTS);
    let answering = std::fs::read_to_string(&python).expect("the script reads");
    let step = format!("#!/bin/sh\nif [ \"$2\" = -m ]; then {making}; exit 0; fi\n");
    let script = answering.replacen("#!/bin/sh\n", &step, 1);
    std::fs::write(&python, script).expect("the script writes");
    python
}

/// `connector install` of `wheel` with `python`, as JSON, its host the scratch's XDG directories'.
async fn installing(scratch: &Scratch, wheel: &Path, python: &Path) -> std::process::Output {
    let mut install = scratch.bare_command(&["connector", "install", "--output", "json"]);
    install.arg(wheel).arg("--python").arg(python);
    output_of(&mut install).await
}

#[tokio::test]
async fn a_refused_install_exits_2_as_config_and_an_unwritable_directory_74() {
    let scratch = Scratch::new();
    let wheel = scratch.path().join("tickets-0.1.0-py3-none-any.whl");
    std::fs::write(&wheel, "").unwrap();
    let old = r#"{"version": [3, 10, 0], "executable": "/usr/bin/python3.10", "base": "/usr/bin/python3", "stdlib": "/usr/lib/python3.10"}"#;
    let python = fake_python(&scratch.path().join("python3"), old);
    let refused = installing(&scratch, &wheel, &python).await;
    assert_eq!(refused.status.code(), Some(2), "{refused:?}");
    let said: serde_json::Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert_eq!(said["error"]["kind"], "config", "{said}");
    let new = r#"{"version": [3, 14, 0], "executable": "/usr/bin/python3.14", "base": "/usr/bin/python3", "stdlib": "/usr/lib/python3.14"}"#;
    let python = fake_python(&scratch.path().join("python3"), new);
    // A file where the default connector directory would be made.
    std::fs::create_dir_all(scratch.path().join("data/rdlt")).unwrap();
    std::fs::write(scratch.path().join("data/rdlt/connectors"), "").unwrap();
    let unwritable = installing(&scratch, &wheel, &python).await;
    assert_eq!(unwritable.status.code(), Some(74), "{unwritable:?}");
    let said: serde_json::Value = serde_json::from_slice(&unwritable.stdout).unwrap();
    assert_eq!(said["error"]["kind"], "io", "{said}");
}

#[tokio::test]
async fn a_refusal_says_its_code_in_json_where_an_error_report_says_its_own() {
    let scratch = Scratch::new();
    let wheel = scratch.path().join("tickets-0.1.0-py3-none-any.whl");
    std::fs::write(&wheel, "").unwrap();
    let facts = r#"{"version": [3, 14, 0], "executable": "/usr/bin/python3.14", "base": "/usr/bin/python3", "stdlib": "/usr/lib/python3.14"}"#;
    let python = fake_python(&scratch.path().join("python3"), facts);
    let connectors = scratch.path().join("data/rdlt/connectors");
    std::fs::create_dir_all(&connectors).unwrap();
    std::fs::write(connectors.join("rdlt-connector-tickets"), "a native binary").unwrap();
    let refused = installing(&scratch, &wheel, &python).await;
    assert_eq!(refused.status.code(), Some(2), "{refused:?}");
    let said: serde_json::Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert_eq!(said["error"]["kind"], "config", "{said}");
    assert_eq!(said["error"]["code"], "name_taken", "{said}");
    assert!(said["error"]["message"].is_string(), "{said}");
}

/// The system's Python 3, 3.11 or later with `venv`, where it is there; it must be where
/// `RDLT_REQUIRE_SYSTEM_PYTHON` says so.
fn system_python() -> Option<PathBuf> {
    let python = PathBuf::from("/usr/bin/python3");
    let check = "import sys, ensurepip, venv; sys.exit(sys.version_info < (3, 11))";
    let usable = std::process::Command::new(&python)
        .args(["-I", "-c", check])
        .status()
        .is_ok_and(|status| status.success());
    let required = std::env::var_os("RDLT_REQUIRE_SYSTEM_PYTHON").is_some_and(|value| value == "1");
    assert!(
        usable || !required,
        "/usr/bin/python3, 3.11 or later with venv, is required here"
    );
    usable.then_some(python)
}

/// A pure-Python wheel at `wheel`, of one connector entry point, made by `python`'s zipfile.
fn made_wheel(python: &Path, wheel: &Path) {
    let script = format!(
        "import zipfile\nwith zipfile.ZipFile({:?}, 'w') as w:\n\
         \x20w.writestr('tickets/__init__.py', 'class Tickets: pass\\n')\n\
         \x20w.writestr('tickets-0.1.0.dist-info/METADATA', 'Metadata-Version: 2.1\\nName: tickets\\nVersion: 0.1.0\\n')\n\
         \x20w.writestr('tickets-0.1.0.dist-info/WHEEL', 'Wheel-Version: 1.0\\nGenerator: test\\nRoot-Is-Purelib: true\\nTag: py3-none-any\\n')\n\
         \x20w.writestr('tickets-0.1.0.dist-info/entry_points.txt', '[rdlt.connectors]\\nio.example.tickets = tickets:Tickets\\n')\n\
         \x20w.writestr('tickets-0.1.0.dist-info/RECORD', '')\n",
        wheel.display().to_string()
    );
    let made = std::process::Command::new(python)
        .arg("-c")
        .arg(script)
        .status();
    assert!(
        made.expect("the interpreter runs").success(),
        "the wheel is made"
    );
}

#[tokio::test]
async fn a_wheel_installs_into_the_default_connector_directory_made_for_it() {
    let Some(python) = system_python() else {
        return;
    };
    let scratch = Scratch::new();
    let wheel = scratch.path().join("tickets-0.1.0-py3-none-any.whl");
    made_wheel(&python, &wheel);
    let installed = installing(&scratch, &wheel, &python).await;
    assert_eq!(installed.status.code(), Some(0), "{installed:?}");
    let said: serde_json::Value = serde_json::from_slice(&installed.stdout).unwrap();
    let tree = std::fs::canonicalize(scratch.path())
        .unwrap()
        .join("data/rdlt/connectors/rdlt-connector-tickets");
    assert_eq!(
        said,
        serde_json::json!({ "installed": tree.display().to_string() })
    );
    assert!(tree.join("bin/rdlt-connector").is_file());
}

/// The entries of `connectors` an install makes while it runs, and leaves only if it is cut off.
fn staged(connectors: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(connectors) else {
        return Vec::new();
    };
    entries
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
        .filter(|name| name.starts_with(".rdlt-install-"))
        .collect()
}

#[tokio::test]
async fn a_signal_ends_an_install_with_every_process_it_started_and_leaves_nothing_staged() {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    let scratch = Scratch::new();
    let connectors = scratch.path().join("data/rdlt/connectors");
    let wheel = scratch.path().join("tickets-0.1.0-py3-none-any.whl");
    std::fs::write(&wheel, "").unwrap();
    // Asked to make the environment, it leaves a process of its own that would outlive it.
    let started = scratch.path().join("started");
    let making = format!("sleep 1000 & echo $! > '{}'; wait", started.display());
    let python = fake_python_making(&scratch.path().join("python3"), &making);
    let mut install = scratch.bare_command(&["connector", "install", "--output", "json"]);
    install.arg(&wheel).arg("--python").arg(&python);
    let child = install.spawn().expect("rdlt starts");
    crate::support::guarded(&child);
    let left = crate::support::bounded(async {
        loop {
            let said = std::fs::read_to_string(&started).unwrap_or_default();
            if let Ok(pid) = said.trim().parse::<i32>() {
                return Pid::from_raw(pid);
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        !staged(&connectors).is_empty(),
        "the environment is being made"
    );
    let pid = Pid::from_raw(i32::try_from(child.id().expect("it runs")).expect("an i32"));
    kill(pid, Signal::SIGTERM).expect("signalled");
    let ended = tokio::time::timeout(std::time::Duration::from_secs(10), child.wait_with_output());
    let ended = ended
        .await
        .expect("it ends within its bound")
        .expect("it ends");
    assert_eq!(ended.status.code(), Some(143), "{ended:?}");
    let gone = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while kill(left, None).is_ok() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    });
    gone.await
        .expect("the process the install's child started is killed with it");
    assert!(staged(&connectors).is_empty(), "{:?}", staged(&connectors));
}

#[tokio::test]
async fn what_the_environment_s_making_says_reaches_standard_error_and_nothing_else() {
    let scratch = Scratch::new();
    let wheel = scratch.path().join("tickets-0.1.0-py3-none-any.whl");
    std::fs::write(&wheel, "").unwrap();
    let making = "echo said-on-its-output; echo said-on-its-errors >&2";
    let python = fake_python_making(&scratch.path().join("python3"), making);
    // The environment is not made, so pip is not there to run.
    let refused = installing(&scratch, &wheel, &python).await;
    assert_eq!(refused.status.code(), Some(2), "{refused:?}");
    let said = String::from_utf8_lossy(&refused.stderr);
    assert!(
        said.contains("said-on-its-output") && said.contains("said-on-its-errors"),
        "{said}"
    );
    let printed: serde_json::Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert_eq!(printed["error"]["code"], "pip_failed", "{printed}");
}

/// On a terminal that stops a process of a background group as it writes, as `stty tostop`
/// makes one, an install whose children write ends, since they write to it through no terminal.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn an_install_ends_on_a_terminal_that_stops_what_writes_from_the_background() {
    use std::io::{Read as _, Write as _};

    use nix::sys::termios::{self, LocalFlags, SetArg};

    // Makes the terminal its controlling one, the group it leads the terminal's.
    let setsid = Path::new("/usr/bin/setsid");
    if !setsid.is_file() {
        writeln!(std::io::stderr(), "skipped: no {} here", setsid.display()).ok();
        return;
    }
    let scratch = Scratch::new();
    let wheel = scratch.path().join("tickets-0.1.0-py3-none-any.whl");
    std::fs::write(&wheel, "").unwrap();
    let making = "echo said-on-its-output; echo said-on-its-errors >&2";
    let python = fake_python_making(&scratch.path().join("python3"), making);
    let opened = nix::pty::openpty(None, None).expect("a pseudo-terminal");
    let mut attributes = termios::tcgetattr(&opened.slave).expect("its attributes");
    attributes.local_flags.insert(LocalFlags::TOSTOP);
    termios::tcsetattr(&opened.slave, SetArg::TCSANOW, &attributes).expect("tostop is set");
    let terminal = || std::process::Stdio::from(opened.slave.try_clone().expect("a descriptor"));
    let mut install = tokio::process::Command::new(setsid);
    install
        .args(["--ctty", "--wait"])
        .arg(env!("CARGO_BIN_EXE_rdlt"))
        .args(["connector", "install", "--output", "json"])
        .arg(&wheel)
        .arg("--python")
        .arg(&python)
        .current_dir(scratch.path())
        .env("XDG_CONFIG_HOME", scratch.path().join("config"))
        .env("XDG_DATA_HOME", scratch.path().join("data"))
        .env(
            "LLVM_PROFILE_FILE",
            std::env::var_os("LLVM_PROFILE_FILE").unwrap_or_default(),
        )
        .env_remove("RUST_LOG")
        .stdin(terminal())
        .stdout(terminal())
        .stderr(terminal())
        .kill_on_drop(true);
    let mut child = install.spawn().expect("rdlt starts");
    // The terminal ends for its reader once no descriptor of this process holds it either.
    drop(install);
    drop(opened.slave);
    let mut master = std::fs::File::from(opened.master);
    let reading = std::thread::spawn(move || {
        let mut seen = Vec::new();
        // The terminal answers an error once its last writer is gone.
        master.read_to_end(&mut seen).ok();
        seen
    });
    let ended = tokio::time::timeout(std::time::Duration::from_secs(30), child.wait()).await;
    let status = ended
        .expect("it ends: no child of it is stopped writing to the terminal")
        .expect("it ends");
    assert_eq!(status.code(), Some(2), "{status:?}");
    let seen = String::from_utf8_lossy(&reading.join().expect("the terminal is read")).into_owned();
    assert!(seen.contains("said-on-its-output"), "{seen}");
}
