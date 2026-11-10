use std::fmt::Write as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use super::{InstallError, Installation, install};

/// A wheel of one connector entry point, as the fake interpreter below describes every wheel.
const DESCRIBED: &str = r#"{"name": "tickets", "uri": "file:///absent.whl", "sha256": "00", "entry_points": [["io.example.tickets", "tickets:Tickets"]]}"#;

/// An interpreter in `dir` that answers the facts query with `facts`, says `described` of any
/// wheel, exits with `reading` when asked to read one, and touches `made` when asked to make an
/// environment, which it does not make.
fn fake_python(dir: &Path, facts: &str, described: &str, reading: u8) -> PathBuf {
    let path = dir.join("python3");
    let made = dir.join("made");
    let script = format!(
        "#!/bin/sh\nif [ \"$2\" = -m ]; then touch '{}'; \
         elif [ \"$#\" = 5 ]; then echo '{described}'; exit {reading}; \
         else echo '{facts}'; fi\n",
        made.display()
    );
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

/// An install of an empty file named as a wheel, which the fake interpreter describes.
fn installation(dir: &Path, python: PathBuf) -> Installation {
    let wheel = dir.join("tickets-0.1.0-py3-none-any.whl");
    std::fs::write(&wheel, "").unwrap();
    Installation {
        wheel,
        python: Some(python),
        requirements: None,
        connectors: dir.join("connectors"),
    }
}

/// The facts of a Python of `version` installed at `prefix`.
fn facts(version: &str, prefix: &str) -> String {
    let short: String = version.split(", ").take(2).collect::<Vec<_>>().join(".");
    format!(
        r#"{{"version": [{version}], "executable": "{prefix}/bin/python{short}", "base": "{prefix}/bin/python3", "stdlib": "{prefix}/lib/python{short}"}}"#
    )
}

/// What installing with an interpreter that says `facts`, and `described` of the wheel, answers.
async fn refusal(facts: &str, described: &str, reading: u8) -> InstallError {
    let dir = tempfile::tempdir().unwrap();
    let python = fake_python(dir.path(), facts, described, reading);
    install(&installation(dir.path(), python))
        .await
        .unwrap_err()
}

#[tokio::test]
async fn an_interpreter_is_refused_for_what_it_says_it_is() {
    for (said, refused) in [
        ("not what was asked".to_owned(), "interpreter_failed"),
        (facts("3, 10, 14", "/usr"), "interpreter_too_old"),
        (facts("2, 7, 18", "/usr"), "interpreter_too_old"),
        // Accepted, it is asked to make the environment, and pip is not there.
        (facts("3, 11, 0", "/usr"), "pip_failed"),
    ] {
        let error = refusal(&said, DESCRIBED, 0).await;
        assert_eq!(error.code(), refused, "{said}: {error}");
    }
    let dir = tempfile::tempdir().unwrap();
    let absent = installation(dir.path(), dir.path().join("absent"));
    assert_eq!(
        install(&absent).await.unwrap_err().code(),
        "interpreter_failed"
    );
}

#[tokio::test]
async fn a_wheel_is_read_by_its_interpreter_before_anything_is_made() {
    let python = facts("3, 14, 0", "/usr");
    let failed = refusal(&python, "garbled", 0).await;
    assert!(
        matches!(&failed, InstallError::InterpreterFailed { asked, .. } if asked.contains("wheel")),
        "{failed}"
    );
    let crashed = refusal(&python, DESCRIBED, 3).await;
    assert_eq!(crashed.code(), "interpreter_failed", "{crashed}");
    let refused = refusal(&python, r#"{"refused": "File is not a zip file"}"#, 0).await;
    assert_eq!(refused.code(), "wheel", "{refused}");
    let none =
        r#"{"name": "tickets", "uri": "file:///absent.whl", "sha256": "00", "entry_points": []}"#;
    let none = refusal(&python, none, 0).await;
    assert!(
        matches!(none, InstallError::EntryPoints { found: 0 }),
        "{none}"
    );
    let bad = DESCRIBED.replace("io.example.tickets", "Not An Id");
    assert_eq!(refusal(&python, &bad, 0).await.code(), "connector_id");
}

#[tokio::test]
async fn a_launcher_the_kernel_could_not_start_is_refused_before_the_environment_is_made() {
    let dir = tempfile::tempdir().unwrap();
    let python = fake_python(dir.path(), &facts("3, 14, 0", "/usr"), DESCRIBED, 0);
    let spaced = Installation {
        connectors: dir.path().join("my connectors"),
        ..installation(dir.path(), python)
    };
    let refused = install(&spaced).await.unwrap_err();
    assert_eq!(refused.code(), "shebang_whitespace", "{refused}");
    assert!(
        !dir.path().join("made").exists(),
        "the environment was made"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn an_interpreter_the_sandbox_cannot_show_is_refused_on_linux() {
    let home = r#"{"version": [3, 14, 0], "executable": "/home/someone/.local/python/bin/python3.14", "base": "/home/someone/.local/python/bin/python3", "stdlib": "/home/someone/.local/python/lib/python3.14"}"#;
    let refused = refusal(home, DESCRIBED, 0).await;
    assert_eq!(refused.code(), "interpreter_unreachable");
    let said = refused.to_string();
    assert!(
        said.contains("--python /usr/bin/python3"),
        "it names the system Python remedy: {said}"
    );
    assert!(
        said.contains("from tickets import Tickets") && said.contains("--listen"),
        "it says how to serve the connector without an install: {said}"
    );
    for outside in [
        r#"{"version": [3, 14, 0], "executable": "/usr/bin/python3.14", "base": "/usr/bin/python3", "stdlib": "/opt/python/lib/python3.14"}"#,
        r#"{"version": [3, 14, 0], "executable": "/usr/bin/python3.14", "base": "/home/someone/bin/python3", "stdlib": "/usr/lib/python3.14"}"#,
    ] {
        let refused = refusal(outside, DESCRIBED, 0).await;
        assert_eq!(refused.code(), "interpreter_unreachable", "{outside}");
    }
}

#[tokio::test]
async fn what_is_no_wheel_file_is_refused_before_any_interpreter_runs() {
    let dir = tempfile::tempdir().unwrap();
    let ran = dir.path().join("ran");
    let python = dir.path().join("python3");
    std::fs::write(&python, format!("#!/bin/sh\ntouch '{}'\n", ran.display())).unwrap();
    std::fs::set_permissions(&python, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::create_dir(dir.path().join("directory.whl")).unwrap();
    std::fs::write(dir.path().join("archive.zip"), "").unwrap();
    for wheel in [
        PathBuf::from("tickets"),
        PathBuf::from("-r"),
        PathBuf::from("--index-url=https://example.invalid/simple"),
        dir.path().join("directory.whl"),
        dir.path().join("archive.zip"),
        dir.path().join("absent.whl"),
    ] {
        let asked = Installation {
            wheel: wheel.clone(),
            ..installation(dir.path(), python.clone())
        };
        let refused = install(&asked).await.unwrap_err();
        assert_eq!(refused.code(), "wheel", "{}: {refused}", wheel.display());
        assert!(!ran.exists(), "{}: an interpreter ran", wheel.display());
    }
    let unreadable = dir.path().join("unreadable.whl");
    std::fs::write(&unreadable, "").unwrap();
    std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).unwrap();
    let shut = dir.path().join("shut");
    std::fs::create_dir(&shut).unwrap();
    std::fs::write(shut.join("held.whl"), "").unwrap();
    std::fs::set_permissions(&shut, std::fs::Permissions::from_mode(0o000)).unwrap();
    // The superuser reads them all the same.
    if std::fs::File::open(&unreadable).is_err() {
        for wheel in [unreadable, shut.join("held.whl")] {
            let asked = Installation {
                wheel: wheel.clone(),
                ..installation(dir.path(), python.clone())
            };
            let refused = install(&asked).await.unwrap_err();
            assert_eq!(refused.code(), "io", "{}: {refused}", wheel.display());
        }
    }
    std::fs::set_permissions(&shut, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[tokio::test]
async fn what_an_install_that_was_killed_left_is_swept_by_the_next() {
    let dir = tempfile::tempdir().unwrap();
    let python = fake_python(dir.path(), &facts("3, 14, 0", "/usr"), DESCRIBED, 0);
    let asked = installation(dir.path(), python);
    let stale = asked.connectors.join(".rdlt-install-killed");
    std::fs::create_dir_all(stale.join("bin")).unwrap();
    // It fails once it has swept: the fake interpreter makes no environment.
    let refused = install(&asked).await.unwrap_err();
    assert_eq!(refused.code(), "pip_failed", "{refused}");
    assert!(!stale.exists(), "what was left is swept");
}

/// A pure-Python wheel of the distribution `name`, needing `requires`, whose entry points are
/// `entry_points`, with `files` beside its package, made with the standard library's zipfile.
fn wheel(
    dir: &Path,
    python: &Path,
    name: &str,
    requires: &[&str],
    entry_points: &str,
    files: &[(&str, &str)],
) -> PathBuf {
    let built = dir.join(format!("{name}-0.1.0-py3-none-any.whl"));
    let requires: String = requires
        .iter()
        .map(|required| ["Requires-Dist: ", required, "\\n"].concat())
        .collect();
    let mut script = format!(
        "import zipfile\nwith zipfile.ZipFile({built:?}, 'w') as w:\n\
         \x20w.writestr('{name}/__init__.py', 'class Tickets: pass\\n')\n\
         \x20w.writestr('{name}-0.1.0.dist-info/METADATA', 'Metadata-Version: 2.1\\nName: {name}\\nVersion: 0.1.0\\n{requires}')\n\
         \x20w.writestr('{name}-0.1.0.dist-info/WHEEL', 'Wheel-Version: 1.0\\nGenerator: test\\nRoot-Is-Purelib: true\\nTag: py3-none-any\\n')\n\
         \x20w.writestr('{name}-0.1.0.dist-info/entry_points.txt', {entry_points:?})\n\
         \x20w.writestr('{name}-0.1.0.dist-info/RECORD', '')\n",
        built = built.display().to_string()
    );
    for (file, text) in files {
        writeln!(script, " w.writestr({file:?}, {text:?})").unwrap();
    }
    let made = std::process::Command::new(python)
        .arg("-c")
        .arg(script)
        .status();
    assert!(made.unwrap().success(), "the wheel {name} is made");
    built
}

/// The entry points of the connector wheel the installs below install.
const TICKETS: &str = "[rdlt.connectors]\nio.example.tickets = tickets:Tickets\n";

/// The connector wheel the installs below install: one entry point, `io.example.tickets`.
fn tickets(dir: &Path, python: &Path, requires: &[&str]) -> PathBuf {
    wheel(dir, python, "tickets", requires, TICKETS, &[])
}

/// An interpreter the sandbox shows: the system's, where it is 3.11 or later with `venv`; none
/// elsewhere, unless `RDLT_REQUIRE_SYSTEM_PYTHON` says one must be here.
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

/// Installs `wheel` with `python` into `connectors`, with `requirements` where given; the tree.
async fn installed(
    python: &Path,
    wheel: PathBuf,
    requirements: Option<PathBuf>,
    connectors: &Path,
) -> Result<PathBuf, InstallError> {
    let asked = Installation {
        wheel,
        python: Some(python.to_owned()),
        requirements,
        connectors: connectors.to_owned(),
    };
    let installed = install(&asked).await?;
    assert_eq!(installed.left, None);
    Ok(installed.tree)
}

/// A temporary directory, where it lies: the tree an install makes is named so.
fn resolved() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = std::fs::canonicalize(dir.path()).unwrap();
    (dir, path)
}

#[tokio::test]
async fn a_wheel_installs_as_a_directory_connector_with_its_launcher() {
    let Some(python) = system_python() else {
        return;
    };
    let (_dir, dir) = resolved();
    let connectors = dir.join("connectors");
    let built = tickets(&dir, &python, &[]);
    let tree = installed(&python, built.clone(), None, &connectors)
        .await
        .unwrap();
    assert_eq!(tree, connectors.join("rdlt-connector-tickets"));
    let launcher = std::fs::read_to_string(tree.join("bin/rdlt-connector")).unwrap();
    let first = launcher.lines().next().unwrap();
    assert_eq!(first, format!("#!{} -I", tree.join("bin/python").display()));
    assert!(launcher.contains("from tickets import Tickets"));
    let recorded = std::fs::read_to_string(tree.join("rdlt-connector.id")).unwrap();
    assert_eq!(recorded, "io.example.tickets\n");
    let executable = tree.join("bin/rdlt-connector").metadata().unwrap();
    assert_eq!(executable.permissions().mode() & 0o777, 0o755);
    for entry in walkdir::WalkDir::new(&tree).follow_links(false) {
        let entry = entry.unwrap();
        let mode = entry
            .path()
            .symlink_metadata()
            .unwrap()
            .permissions()
            .mode();
        let link = entry.path_is_symlink();
        assert!(
            link || mode & 0o022 == 0,
            "{} is writable by others",
            entry.path().display()
        );
    }
    std::fs::write(tree.join("left-behind"), "of the tree replaced").unwrap();
    let again = installed(&python, built, None, &connectors).await.unwrap();
    assert_eq!(again, tree, "installing again replaces the tree");
    assert!(
        !tree.join("left-behind").exists(),
        "the tree replaced is gone"
    );
    assert!(tree.join("bin/rdlt-connector").is_file());
    let mut left: Vec<String> = std::fs::read_dir(&connectors)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    left.sort_unstable();
    assert_eq!(left, [".rdlt-install.lock", "rdlt-connector-tickets"]);
}

#[tokio::test]
async fn a_wheel_that_names_no_one_connector_is_refused() {
    let Some(python) = system_python() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let connectors = dir.path().join("connectors");
    for (name, entry_points, refused) in [
        ("none", "", "entry_points"),
        (
            "two",
            "[rdlt.connectors]\nio.example.a = two:Tickets\nio.example.b = two:Tickets\n",
            "entry_points",
        ),
        (
            "badid",
            "[rdlt.connectors]\nNot An Id = badid:Tickets\n",
            "connector_id",
        ),
    ] {
        let made = wheel(dir.path(), &python, name, &[], entry_points, &[]);
        let error = installed(&python, made, None, &connectors)
            .await
            .unwrap_err();
        assert_eq!(error.code(), refused, "{name}: {error}");
    }
    assert!(!connectors.exists(), "nothing is made for a wheel refused");
}

/// The SHA-256 digest of the file at `path`, in hex, as pip checks a hash.
fn sha256(path: &Path) -> String {
    use sha2::Digest as _;
    format!("{:x}", sha2::Sha256::digest(std::fs::read(path).unwrap()))
}

/// A requirements line for the wheel at `path`, with its hash.
fn pinned(name: &str, path: &Path) -> String {
    format!(
        "{name} @ file://{} --hash=sha256:{}\n",
        path.display(),
        sha256(path)
    )
}

#[tokio::test]
async fn a_requirements_file_installs_only_what_it_hashes_and_all_that_is_needed() {
    let Some(python) = system_python() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let connectors = dir.path().join("connectors");
    let dependency = wheel(dir.path(), &python, "needed", &[], "", &[]);
    let requirements = |name: &str, text: &str| {
        let path = dir.path().join(name);
        std::fs::write(&path, text).unwrap();
        Some(path)
    };
    let hashed = requirements("hashed.txt", &pinned("needed", &dependency));
    let made = tickets(dir.path(), &python, &["needed"]);
    let tree = installed(&python, made.clone(), hashed, &connectors)
        .await
        .unwrap();
    assert!(tree.join("bin/rdlt-connector").is_file());
    let uri = format!("file://{}", dependency.display());
    let unhashed = requirements("unhashed.txt", &format!("needed @ {uri}\n"));
    let refused = installed(&python, made.clone(), unhashed, &connectors)
        .await
        .unwrap_err();
    assert_eq!(refused.code(), "pip_failed", "{refused}");
    let other = wheel(dir.path(), &python, "other", &[], "", &[]);
    let incomplete = requirements("incomplete.txt", &pinned("other", &other));
    let refused = installed(&python, made, incomplete, &connectors)
        .await
        .unwrap_err();
    assert_eq!(refused.code(), "pip_failed", "{refused}");
}

#[tokio::test]
async fn no_code_of_the_wheel_runs_while_it_is_installed() {
    let Some(python) = system_python() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let connectors = dir.path().join("connectors");
    let marker = dir.path().join("ran");
    // `site` runs a `.pth` file's import lines whenever an interpreter of the environment starts.
    let startup = format!(
        "import pathlib; pathlib.Path({:?}).write_text('ran')\n",
        marker.display().to_string()
    );
    let files = [("tickets-startup.pth", startup.as_str())];
    let built = wheel(dir.path(), &python, "tickets", &[], TICKETS, &files);
    installed(&python, built.clone(), None, &connectors)
        .await
        .unwrap();
    assert!(!marker.exists(), "the wheel's .pth ran as its installer");
    let other = wheel(dir.path(), &python, "other", &[], "", &[]);
    let requirements = dir.path().join("requirements.txt");
    std::fs::write(&requirements, pinned("other", &other)).unwrap();
    installed(&python, built, Some(requirements), &connectors)
        .await
        .unwrap();
    assert!(
        !marker.exists(),
        "the wheel's .pth ran as its installer, with requirements"
    );
}

#[tokio::test]
async fn an_install_replaces_only_an_earlier_install_of_its_connector() {
    let Some(python) = system_python() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let connectors = dir.path().join("connectors");
    std::fs::create_dir(&connectors).unwrap();
    let native = connectors.join("rdlt-connector-tickets");
    std::fs::write(&native, "a native connector's binary").unwrap();
    let built = tickets(dir.path(), &python, &[]);
    let refused = installed(&python, built.clone(), None, &connectors)
        .await
        .unwrap_err();
    assert_eq!(refused.code(), "name_taken", "{refused}");
    assert_eq!(
        std::fs::read_to_string(&native).unwrap(),
        "a native connector's binary"
    );
    std::fs::remove_file(&native).unwrap();
    let tree = installed(&python, built.clone(), None, &connectors)
        .await
        .unwrap();
    let entry_points = "[rdlt.connectors]\ncom.acme.tickets = acme:Tickets\n";
    let acme = wheel(dir.path(), &python, "acme", &[], entry_points, &[]);
    let refused = installed(&python, acme, None, &connectors)
        .await
        .unwrap_err();
    assert_eq!(refused.code(), "name_taken", "{refused}");
    let launcher = std::fs::read_to_string(tree.join("bin/rdlt-connector")).unwrap();
    assert!(
        launcher.contains("from tickets import Tickets"),
        "{launcher}"
    );
    installed(&python, built, None, &connectors)
        .await
        .expect("an install of its own connector replaces it");
}
