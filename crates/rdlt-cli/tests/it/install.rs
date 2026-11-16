//! `connector install` installs into the host's first connector directory, making it where it is
//! not there, says what it installed, and exits 2 where it refuses and 74 where a file fails it.

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use crate::support::{Scratch, output_of};

/// An interpreter at `path` that answers the facts query with `facts`, and says of any wheel that
/// it is one connector's.
fn fake_python(path: &Path, facts: &str) -> PathBuf {
    let described = r#"{"name": "tickets", "sha256": "00", "entry_points": [["io.example.tickets", "tickets:Tickets"]], "requires": []}"#;
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

/// `connector install`, as JSON, its host the scratch's XDG directories': what it asks a network
/// for goes through a proxy that answers nothing, so no install here reaches one.
fn install_command(scratch: &Scratch) -> tokio::process::Command {
    let mut install = scratch.bare_command(&["connector", "install", "--output", "json"]);
    for proxy in [
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
    ] {
        install.env(proxy, "http://127.0.0.1:9");
    }
    install.env_remove("no_proxy").env_remove("NO_PROXY");
    install
}

/// `connector install` of `wheel` with `python`, as [`install_command`] runs it.
async fn installing(scratch: &Scratch, wheel: &Path, python: &Path) -> std::process::Output {
    let mut install = install_command(scratch);
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

/// A pure-Python wheel at `wheel`, of one connector entry point, needing `requires`, made by
/// `python`'s zipfile.
fn made_wheel(python: &Path, wheel: &Path, requires: &str) {
    let script = format!(
        "import zipfile\nwith zipfile.ZipFile({:?}, 'w') as w:\n\
         \x20w.writestr('tickets/__init__.py', 'class Tickets: pass\\n')\n\
         \x20w.writestr('tickets-0.1.0.dist-info/METADATA', 'Metadata-Version: 2.1\\nName: tickets\\nVersion: 0.1.0\\n{requires}')\n\
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
    made_wheel(&python, &wheel, "");
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

#[tokio::test]
async fn an_install_reads_none_of_pip_s_configuration() {
    let Some(python) = system_python() else {
        return;
    };
    let scratch = Scratch::new();
    let wheel = scratch.path().join("tickets-0.1.0-py3-none-any.whl");
    made_wheel(&python, &wheel, "");
    let (configured, set) = (
        scratch.path().join("configured"),
        scratch.path().join("set"),
    );
    let file = scratch.path().join("pip.conf");
    std::fs::write(
        &file,
        format!("[install]\ntarget = {}\n", configured.display()),
    )
    .unwrap();
    let mut install = install_command(&scratch);
    install
        .arg(&wheel)
        .arg("--python")
        .arg(&python)
        .env("PIP_CONFIG_FILE", &file)
        .env("PIP_TARGET", &set);
    let installed = output_of(&mut install).await;
    assert_eq!(installed.status.code(), Some(0), "{installed:?}");
    assert!(
        !configured.exists(),
        "pip's configuration file placed the packages"
    );
    assert!(!set.exists(), "pip's variables placed the packages");
    let tree = scratch
        .path()
        .join("data/rdlt/connectors/rdlt-connector-tickets");
    let imported = std::process::Command::new(tree.join("bin/python"))
        .args(["-I", "-c", "import tickets"])
        .output()
        .expect("the tree's interpreter runs");
    assert!(imported.status.success(), "{imported:?}");
}

/// A pure-Python wheel of the distribution `name` 0.1.0 in `dir`, needing `requires`, made by
/// `python`'s zipfile.
fn made_dependency(python: &Path, dir: &Path, name: &str, requires: &str) -> PathBuf {
    let wheel = dir.join(format!("{name}-0.1.0-py3-none-any.whl"));
    let script = format!(
        "import zipfile\nwith zipfile.ZipFile({:?}, 'w') as w:\n\
         \x20w.writestr('{name}/__init__.py', '')\n\
         \x20w.writestr('{name}-0.1.0.dist-info/METADATA', 'Metadata-Version: 2.1\\nName: {name}\\nVersion: 0.1.0\\n{requires}')\n\
         \x20w.writestr('{name}-0.1.0.dist-info/WHEEL', 'Wheel-Version: 1.0\\nGenerator: test\\nRoot-Is-Purelib: true\\nTag: py3-none-any\\n')\n\
         \x20w.writestr('{name}-0.1.0.dist-info/RECORD', '')\n",
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
    wheel
}

/// A source distribution of `planted` 1.0 in `dir`, whose build backend, in its tree, writes
/// `marker` when it is imported.
fn made_source(python: &Path, dir: &Path, marker: &Path) -> PathBuf {
    let built = dir.join("planted-1.0.tar.gz");
    let backend = format!(
        "import pathlib\npathlib.Path({:?}).write_text('built')\n",
        marker.display().to_string()
    );
    let project =
        "[build-system]\nrequires = []\nbuild-backend = 'backend'\nbackend-path = ['.']\n";
    let script = format!(
        "import io, tarfile\nwith tarfile.open({built:?}, 'w:gz') as archive:\n\
         \x20for name, text in [('pyproject.toml', {project:?}), ('backend.py', {backend:?}), \
         ('PKG-INFO', 'Metadata-Version: 2.1\\nName: planted\\nVersion: 1.0\\n')]:\n\
         \x20 data = text.encode()\n\
         \x20 info = tarfile.TarInfo('planted-1.0/' + name)\n\
         \x20 info.size = len(data)\n\
         \x20 archive.addfile(info, io.BytesIO(data))\n",
        built = built.display().to_string()
    );
    let made = std::process::Command::new(python)
        .arg("-c")
        .arg(script)
        .status();
    assert!(
        made.expect("the interpreter runs").success(),
        "the source distribution is made"
    );
    built
}

/// The SHA-256 digest of the file at `path`, in hex.
fn sha256(path: &Path) -> String {
    use sha2::Digest as _;
    let bytes = std::fs::read(path).expect("the file reads");
    format!("{:x}", sha2::Sha256::digest(bytes))
}

#[tokio::test]
async fn a_source_a_hashed_wheel_needs_by_its_url_is_refused_for_its_hash_unbuilt() {
    let Some(python) = system_python() else {
        return;
    };
    let scratch = Scratch::new();
    let marker = scratch.path().join("built");
    let source = made_source(&python, scratch.path(), &marker);
    let needs = format!("Requires-Dist: planted @ file://{}\\n", source.display());
    let found = scratch.path().join("found");
    std::fs::create_dir(&found).unwrap();
    let carrier = made_dependency(&python, &found, "carrier", &needs);
    let wheel = scratch.path().join("tickets-0.1.0-py3-none-any.whl");
    made_wheel(&python, &wheel, "Requires-Dist: carrier\\n");
    let requirements = scratch.path().join("requirements.txt");
    let line = format!("carrier==0.1.0 --hash=sha256:{}", sha256(&carrier));
    let text = format!("--no-index\n--find-links {}\n{line}\n", found.display());
    std::fs::write(&requirements, text).unwrap();
    let mut install = install_command(&scratch);
    install
        .arg(&wheel)
        .arg("--python")
        .arg(&python)
        .arg("--requirements")
        .arg(&requirements);
    let refused = output_of(&mut install).await;
    assert_eq!(refused.status.code(), Some(2), "{refused:?}");
    let printed: serde_json::Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert_eq!(printed["error"]["code"], "pip_failed", "{printed}");
    // pip's own words say which rule refused it, before anything was built.
    let said = String::from_utf8_lossy(&refused.stderr);
    assert!(
        said.contains("Hashes are required in --require-hashes mode"),
        "{said}"
    );
    assert!(!marker.exists(), "a build backend ran as the installer");
}

#[tokio::test]
async fn an_interpreter_named_by_a_relative_path_is_found_where_the_install_was_asked() {
    let scratch = Scratch::new();
    let wheel = scratch.path().join("tickets-0.1.0-py3-none-any.whl");
    std::fs::write(&wheel, "").unwrap();
    let made = scratch.path().join("made");
    let making = format!("touch '{}'", made.display());
    fake_python_making(&scratch.path().join("python3"), &making);
    let mut install = install_command(&scratch);
    install.arg(&wheel).arg("--python").arg("./python3");
    let refused = output_of(&mut install).await;
    let printed: serde_json::Value = serde_json::from_slice(&refused.stdout).unwrap();
    // It made the environments, and pip, which it did not make, is not there to run.
    assert_eq!(printed["error"]["code"], "pip_failed", "{printed}");
    assert!(made.exists(), "the interpreter named made no environment");
}

#[tokio::test]
async fn an_interpreter_named_without_a_path_is_found_on_the_path() {
    let scratch = Scratch::new();
    let wheel = scratch.path().join("tickets-0.1.0-py3-none-any.whl");
    std::fs::write(&wheel, "").unwrap();
    // Found on the path alone: nothing of its name is where the install is asked.
    let found = scratch.path().join("found");
    std::fs::create_dir(&found).unwrap();
    let made = scratch.path().join("made");
    let making = format!("touch '{}'", made.display());
    fake_python_making(&found.join("python3"), &making);
    let path = std::env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![found];
    paths.extend(std::env::split_paths(&path));
    let mut install = install_command(&scratch);
    install
        .arg(&wheel)
        .arg("--python")
        .arg("python3")
        .env("PATH", std::env::join_paths(paths).unwrap());
    let refused = output_of(&mut install).await;
    let printed: serde_json::Value = serde_json::from_slice(&refused.stdout).unwrap();
    // It made the environments, and pip, which it did not make, is not there to run.
    assert_eq!(printed["error"]["code"], "pip_failed", "{printed}");
    assert!(
        made.exists(),
        "the interpreter on the path made no environment"
    );
}

#[tokio::test]
async fn a_package_the_requirements_found_but_did_not_hash_is_refused_for_its_hash() {
    let Some(python) = system_python() else {
        return;
    };
    let scratch = Scratch::new();
    let found = scratch.path().join("found");
    std::fs::create_dir(&found).unwrap();
    made_dependency(&python, &found, "needed", "");
    let wheel = scratch.path().join("tickets-0.1.0-py3-none-any.whl");
    made_wheel(&python, &wheel, "Requires-Dist: needed\\n");
    let requirements = scratch.path().join("requirements.txt");
    let text = format!("--no-index\n--find-links {}\n", found.display());
    std::fs::write(&requirements, text).unwrap();
    let mut install = install_command(&scratch);
    install
        .arg(&wheel)
        .arg("--python")
        .arg(&python)
        .arg("--requirements")
        .arg(&requirements);
    let refused = output_of(&mut install).await;
    assert_eq!(refused.status.code(), Some(2), "{refused:?}");
    let printed: serde_json::Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert_eq!(printed["error"]["code"], "pip_failed", "{printed}");
    // pip found it, and refused it as unhashed: its own words say which rule.
    let said = String::from_utf8_lossy(&refused.stderr);
    assert!(said.contains("--require-hashes mode"), "{said}");
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
async fn what_the_environment_s_making_says_reaches_standard_error_shown_and_nothing_else() {
    let scratch = Scratch::new();
    let wheel = scratch.path().join("tickets-0.1.0-py3-none-any.whl");
    std::fs::write(&wheel, "").unwrap();
    let making = "printf 'said-on-its-output \\033[2J\\n'; echo said-on-its-errors >&2";
    let python = fake_python_making(&scratch.path().join("python3"), making);
    // The environment is not made, so pip is not there to run.
    let refused = installing(&scratch, &wheel, &python).await;
    assert_eq!(refused.status.code(), Some(2), "{refused:?}");
    let said = String::from_utf8_lossy(&refused.stderr);
    assert!(
        said.contains("said-on-its-output \\u{1b}[2J") && said.contains("said-on-its-errors"),
        "{said}"
    );
    assert!(
        !said.contains('\u{1b}'),
        "a control sequence reached the terminal: {said:?}"
    );
    let printed: serde_json::Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert_eq!(printed["error"]["code"], "pip_failed", "{printed}");
}

#[tokio::test]
async fn the_environment_s_making_is_given_only_what_it_needs_in_the_install_s_own_directory() {
    let scratch = Scratch::new();
    let wheel = scratch.path().join("tickets-0.1.0-py3-none-any.whl");
    std::fs::write(&wheel, "").unwrap();
    let making = "echo \"probe=${RDLT_PROBE:-held} pip=$PIP_CONFIG_FILE home=${HOME:+given} \
                  in=$(pwd)\"";
    let python = fake_python_making(&scratch.path().join("python3"), making);
    let mut install = install_command(&scratch);
    install
        .arg(&wheel)
        .arg("--python")
        .arg(&python)
        .env("RDLT_PROBE", "given");
    let refused = output_of(&mut install).await;
    let said = String::from_utf8_lossy(&refused.stderr);
    for given in ["probe=held", "pip=/dev/null", "home=given"] {
        assert!(said.contains(given), "{given}: {said}");
    }
    let connectors = std::fs::canonicalize(scratch.path())
        .unwrap()
        .join("data/rdlt/connectors");
    let staging = format!("in={}/.rdlt-install-", connectors.display());
    assert!(said.contains(&staging), "{staging}: {said}");
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
