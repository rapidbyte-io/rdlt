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
    let facts = r#"{"version": [3, 14, 0], "executable": "/usr/bin/python3.14", "base": "/usr/bin/python3", "stdlib": "/usr/lib/python3.14"}"#;
    let python = fake_python(&scratch.path().join("python3"), facts);
    let answering = std::fs::read_to_string(&python).unwrap();
    let making = format!(
        "#!/bin/sh\nif [ \"$2\" = -m ]; then sleep 1000 & echo $! > '{}'; wait; exit 0; fi\n",
        started.display()
    );
    let script = answering.replacen("#!/bin/sh\n", &making, 1);
    std::fs::write(&python, script).unwrap();
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
