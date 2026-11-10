//! `connector install` installs into the host's first connector directory, making it where it is
//! not there, says what it installed, and exits 2 where it refuses and 74 where a file fails it.

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use crate::support::{Scratch, output_of};

/// An interpreter at `path` that answers the facts query with `facts` and does nothing else.
fn fake_python(path: &Path, facts: &str) -> PathBuf {
    std::fs::write(path, format!("#!/bin/sh\necho '{facts}'\n")).expect("the script writes");
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
    let old = r#"{"version": [3, 10, 0], "executable": "/usr/bin/python3.10", "stdlib": "/usr/lib/python3.10"}"#;
    let python = fake_python(&scratch.path().join("python3"), old);
    let refused = installing(&scratch, &scratch.path().join("absent.whl"), &python).await;
    assert_eq!(refused.status.code(), Some(2), "{refused:?}");
    let said: serde_json::Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert_eq!(said["error"]["kind"], "config", "{said}");
    let new = r#"{"version": [3, 14, 0], "executable": "/usr/bin/python3.14", "stdlib": "/usr/lib/python3.14"}"#;
    let python = fake_python(&scratch.path().join("python3"), new);
    // A file where the default connector directory would be made.
    std::fs::create_dir_all(scratch.path().join("data/rdlt")).unwrap();
    std::fs::write(scratch.path().join("data/rdlt/connectors"), "").unwrap();
    let unwritable = installing(&scratch, &scratch.path().join("absent.whl"), &python).await;
    assert_eq!(unwritable.status.code(), Some(74), "{unwritable:?}");
    let said: serde_json::Value = serde_json::from_slice(&unwritable.stdout).unwrap();
    assert_eq!(said["error"]["kind"], "io", "{said}");
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

#[tokio::test]
async fn a_wheel_installs_into_the_default_connector_directory_made_for_it() {
    let Some(python) = system_python() else {
        return;
    };
    let scratch = Scratch::new();
    let wheel = scratch.path().join("tickets-0.1.0-py3-none-any.whl");
    let script = format!(
        "import zipfile\nwith zipfile.ZipFile({:?}, 'w') as w:\n\
         \x20w.writestr('tickets/__init__.py', 'class Tickets: pass\\n')\n\
         \x20w.writestr('tickets-0.1.0.dist-info/METADATA', 'Metadata-Version: 2.1\\nName: tickets\\nVersion: 0.1.0\\n')\n\
         \x20w.writestr('tickets-0.1.0.dist-info/WHEEL', 'Wheel-Version: 1.0\\nGenerator: test\\nRoot-Is-Purelib: true\\nTag: py3-none-any\\n')\n\
         \x20w.writestr('tickets-0.1.0.dist-info/entry_points.txt', '[rdlt.connectors]\\nio.example.tickets = tickets:Tickets\\n')\n\
         \x20w.writestr('tickets-0.1.0.dist-info/RECORD', '')\n",
        wheel.display().to_string()
    );
    let made = std::process::Command::new(&python)
        .arg("-c")
        .arg(script)
        .status();
    assert!(made.unwrap().success(), "the wheel is made");
    let installed = installing(&scratch, &wheel, &python).await;
    assert_eq!(installed.status.code(), Some(0), "{installed:?}");
    let said: serde_json::Value = serde_json::from_slice(&installed.stdout).unwrap();
    let tree = scratch
        .path()
        .join("data/rdlt/connectors/rdlt-connector-tickets");
    assert_eq!(
        said,
        serde_json::json!({ "installed": tree.display().to_string() })
    );
    assert!(tree.join("bin/rdlt-connector").is_file());
}
