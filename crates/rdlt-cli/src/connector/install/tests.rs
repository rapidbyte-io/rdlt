use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use super::{Installation, install, launcher};

/// An interpreter that answers the facts query with `facts` and does nothing else.
fn fake_python(dir: &Path, facts: &str) -> PathBuf {
    let path = dir.join("python3");
    std::fs::write(&path, format!("#!/bin/sh\necho '{facts}'\n")).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

fn installation(dir: &Path, python: PathBuf) -> Installation {
    Installation {
        wheel: dir.join("absent.whl"),
        python: Some(python),
        requirements: None,
        connectors: dir.join("connectors"),
    }
}

/// The facts of a Python of `version` installed at `prefix`.
fn facts(version: &str, prefix: &str) -> String {
    let short: String = version.split(", ").take(2).collect::<Vec<_>>().join(".");
    format!(
        r#"{{"version": [{version}], "executable": "{prefix}/bin/python{short}", "stdlib": "{prefix}/lib/python{short}"}}"#
    )
}

#[test]
fn an_interpreter_is_refused_for_what_it_says_it_is() {
    for (said, refused) in [
        ("not what was asked".to_owned(), "interpreter_failed"),
        (facts("3, 10, 14", "/usr"), "interpreter_too_old"),
        (facts("2, 7, 18", "/usr"), "interpreter_too_old"),
        // Accepted, the interpreter it names is asked to make the environment, and is not there.
        (facts("3, 11, 0", "/usr/rdlt-absent"), "venv_failed"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let python = fake_python(dir.path(), &said);
        let error = install(&installation(dir.path(), python)).unwrap_err();
        assert_eq!(error.code(), refused, "{said}: {error}");
    }
    let dir = tempfile::tempdir().unwrap();
    let absent = installation(dir.path(), dir.path().join("absent"));
    assert_eq!(install(&absent).unwrap_err().code(), "interpreter_failed");
}

#[cfg(target_os = "linux")]
#[test]
fn an_interpreter_the_sandbox_cannot_show_is_refused_on_linux() {
    let dir = tempfile::tempdir().unwrap();
    let home = r#"{"version": [3, 14, 0], "executable": "/home/someone/.local/python/bin/python3.14", "stdlib": "/home/someone/.local/python/lib/python3.14"}"#;
    let python = fake_python(dir.path(), home);
    let refused = install(&installation(dir.path(), python)).unwrap_err();
    assert_eq!(refused.code(), "interpreter_unreachable");
    let said = refused.to_string();
    assert!(
        said.contains("--python /usr/bin/python3"),
        "it names the system Python remedy: {said}"
    );
    assert!(
        said.contains("remotely") && said.contains("--listen"),
        "it names the remote remedy: {said}"
    );
    let library = r#"{"version": [3, 14, 0], "executable": "/usr/bin/python3.14", "stdlib": "/opt/python/lib/python3.14"}"#;
    let python = fake_python(dir.path(), library);
    let refused = install(&installation(dir.path(), python)).unwrap_err();
    assert_eq!(refused.code(), "interpreter_unreachable");
}

#[test]
fn a_launcher_imports_its_entry_point_and_serves_it_with_the_tree_s_interpreter() {
    let tree = Path::new("/connectors/rdlt-connector-tickets");
    let written = launcher(tree, "tickets.source:Sources.Tickets").unwrap();
    let expected = "#!/connectors/rdlt-connector-tickets/bin/python -I\n\
                    from rdlt.connector import serve\n\
                    from tickets.source import Sources\n\
                    raise SystemExit(serve(Sources.Tickets))\n";
    assert_eq!(String::from_utf8(written).unwrap(), expected);
}

#[test]
fn an_entry_point_that_names_no_attribute_of_a_module_is_refused() {
    let tree = Path::new("/connectors/rdlt-connector-tickets");
    for target in [
        "tickets",
        "tickets:",
        ":Tickets",
        "tickets:Tickets [extra]",
        "tickets:Tickets\nimport os",
        "tickets; import os:Tickets",
        "1tickets:Tickets",
        "tickets..source:Tickets",
        "tickets:Tickets.",
        "tickets:Tic-kets",
    ] {
        let refused = launcher(tree, target).unwrap_err();
        assert_eq!(refused.code(), "entry_points", "{target:?}");
    }
}

#[test]
fn a_launcher_whose_first_line_the_kernel_would_not_take_whole_is_refused() {
    // `#!`, the tree, `/bin/python` and ` -I`: 255 bytes before its newline, and then 256.
    let fits = PathBuf::from(format!("/{}", "c".repeat(238)));
    let written = launcher(&fits, "tickets:Tickets").expect("a first line of 255 bytes fits");
    assert_eq!(written.iter().position(|byte| *byte == b'\n'), Some(255));
    let long = PathBuf::from(format!("/{}", "c".repeat(239)));
    let refused = launcher(&long, "tickets:Tickets").unwrap_err();
    assert_eq!(refused.code(), "shebang_too_long");
    for spaced in ["/my connectors/tickets", "/tabbed\tconnectors/tickets"] {
        let refused = launcher(Path::new(spaced), "tickets:Tickets").unwrap_err();
        assert_eq!(refused.code(), "shebang_whitespace", "{spaced:?}");
    }
}

/// A pure-Python wheel of the distribution `name`, needing `requires`, whose entry points are
/// `entry_points`, made with the standard library's zipfile.
fn wheel(dir: &Path, python: &Path, name: &str, requires: &[&str], entry_points: &str) -> PathBuf {
    let built = dir.join(format!("{name}-0.1.0-py3-none-any.whl"));
    let requires: String = requires
        .iter()
        .map(|required| ["Requires-Dist: ", required, "\\n"].concat())
        .collect();
    let script = format!(
        "import zipfile\nwith zipfile.ZipFile({built:?}, 'w') as w:\n\
         \x20w.writestr('{name}/__init__.py', 'class Tickets: pass\\n')\n\
         \x20w.writestr('{name}-0.1.0.dist-info/METADATA', 'Metadata-Version: 2.1\\nName: {name}\\nVersion: 0.1.0\\n{requires}')\n\
         \x20w.writestr('{name}-0.1.0.dist-info/WHEEL', 'Wheel-Version: 1.0\\nGenerator: test\\nRoot-Is-Purelib: true\\nTag: py3-none-any\\n')\n\
         \x20w.writestr('{name}-0.1.0.dist-info/entry_points.txt', {entry_points:?})\n\
         \x20w.writestr('{name}-0.1.0.dist-info/RECORD', '')\n",
        built = built.display().to_string()
    );
    let made = std::process::Command::new(python)
        .arg("-c")
        .arg(script)
        .status();
    assert!(made.unwrap().success(), "the wheel {name} is made");
    built
}

/// The connector wheel the installs below install: one entry point, `io.example.tickets`.
fn tickets(dir: &Path, python: &Path, requires: &[&str]) -> PathBuf {
    let entry_points = "[rdlt.connectors]\nio.example.tickets = tickets:Tickets\n";
    wheel(dir, python, "tickets", requires, entry_points)
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

/// Installs `wheel` with `python` into `connectors`, with `requirements` where given.
fn installed(
    python: &Path,
    wheel: PathBuf,
    requirements: Option<PathBuf>,
    connectors: &Path,
) -> Result<PathBuf, super::InstallError> {
    install(&Installation {
        wheel,
        python: Some(python.to_owned()),
        requirements,
        connectors: connectors.to_owned(),
    })
}

#[test]
fn a_wheel_installs_as_a_directory_connector_with_its_launcher() {
    let Some(python) = system_python() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let connectors = dir.path().join("connectors");
    let built = tickets(dir.path(), &python, &[]);
    let tree = installed(&python, built.clone(), None, &connectors).unwrap();
    assert_eq!(tree, connectors.join("rdlt-connector-tickets"));
    let launcher = std::fs::read_to_string(tree.join("bin/rdlt-connector")).unwrap();
    let first = launcher.lines().next().unwrap();
    assert_eq!(first, format!("#!{} -I", tree.join("bin/python").display()));
    assert!(launcher.contains("from tickets import Tickets"));
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
    let again = installed(&python, built, None, &connectors).unwrap();
    assert_eq!(again, tree, "installing again replaces the tree");
    assert!(
        !tree.join("left-behind").exists(),
        "the tree replaced is gone"
    );
    assert!(tree.join("bin/rdlt-connector").is_file());
    let left: Vec<_> = std::fs::read_dir(&connectors).unwrap().collect();
    assert_eq!(left.len(), 1, "nothing but the tree is left: {left:?}");
}

#[test]
fn a_wheel_that_names_no_one_connector_is_refused() {
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
        let made = wheel(dir.path(), &python, name, &[], entry_points);
        let error = installed(&python, made, None, &connectors).unwrap_err();
        assert_eq!(error.code(), refused, "{name}: {error}");
    }
    let left: Vec<_> = std::fs::read_dir(&connectors).unwrap().collect();
    assert!(left.is_empty(), "no staging is left: {left:?}");
}

/// The SHA-256 digest of the file at `path`, in hex, as pip checks a hash.
fn sha256(path: &Path) -> String {
    use sha2::Digest as _;
    format!("{:x}", sha2::Sha256::digest(std::fs::read(path).unwrap()))
}

#[test]
fn a_requirements_file_installs_only_what_it_hashes_and_all_that_is_needed() {
    let Some(python) = system_python() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let connectors = dir.path().join("connectors");
    let dependency = wheel(dir.path(), &python, "needed", &[], "");
    let digest = sha256(&dependency);
    let uri = format!("file://{}", dependency.display());
    let requirements = |name: &str, text: &str| {
        let path = dir.path().join(name);
        std::fs::write(&path, text).unwrap();
        Some(path)
    };
    let hashed = requirements(
        "hashed.txt",
        &format!("needed @ {uri} --hash=sha256:{digest}\n"),
    );
    let made = tickets(dir.path(), &python, &["needed"]);
    let tree = installed(&python, made.clone(), hashed, &connectors).unwrap();
    assert!(tree.join("bin/rdlt-connector").is_file());
    let unhashed = requirements("unhashed.txt", &format!("needed @ {uri}\n"));
    let refused = installed(&python, made.clone(), unhashed, &connectors).unwrap_err();
    assert_eq!(refused.code(), "pip_failed", "{refused}");
    let other = wheel(dir.path(), &python, "other", &[], "");
    let other = format!(
        "other @ file://{} --hash=sha256:{}\n",
        other.display(),
        sha256(&other)
    );
    let incomplete = requirements("incomplete.txt", &other);
    let refused = installed(&python, made, incomplete, &connectors).unwrap_err();
    assert_eq!(refused.code(), "pip_failed", "{refused}");
}
