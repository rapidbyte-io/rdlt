use std::fmt::Write as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use super::{InstallError, Installation, install};

/// A wheel of one connector entry point, as the fake interpreter below describes every wheel.
const DESCRIBED: &str = r#"{"name": "tickets", "sha256": "00", "entry_points": [["io.example.tickets", "tickets:Tickets"]], "requires": []}"#;

/// An interpreter in `dir` that answers the facts query with `facts`, says `described` of any
/// wheel, exits with `reading` when asked to read one, and touches `made` when asked to make an
/// environment, which it does not make.
fn fake_python(dir: &Path, facts: &str, described: &str, reading: u8) -> PathBuf {
    let path = dir.join("python3");
    let made = dir.join("made");
    let script = format!(
        "#!/bin/sh\nif [ \"$2\" = -m ]; then touch '{}'; \
         elif [ \"$#\" = 5 ]; then printf '%s\\n' '{described}'; exit {reading}; \
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
    let none = r#"{"name": "tickets", "sha256": "00", "entry_points": [], "requires": []}"#;
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
    std::fs::create_dir(dir.path().join("directory-0.1.0-py3-none-any.whl")).unwrap();
    // Files pip would not read as wheels, whatever their last characters.
    let misnamed = [
        "archive.zip",
        ".whl",
        "..whl",
        "...whl",
        "x.whl",
        "tickets-0.1.0.whl",
        ".tickets-0.1.0-py3-none-any.whl",
        "tickets-0.1.0-py3-none-any.WHL",
        "tickets-0.1.0.tar.gz",
        "tickets-0.1.0-py3-none-any.tar.whl",
    ];
    for name in misnamed {
        std::fs::write(dir.path().join(name), "").unwrap();
    }
    let named = [
        PathBuf::from("tickets"),
        PathBuf::from("-r"),
        PathBuf::from("--index-url=https://example.invalid/simple"),
        dir.path().join("directory-0.1.0-py3-none-any.whl"),
        dir.path().join("absent-0.1.0-py3-none-any.whl"),
    ];
    for wheel in named
        .into_iter()
        .chain(misnamed.iter().map(|name| dir.path().join(name)))
    {
        let asked = Installation {
            wheel: wheel.clone(),
            ..installation(dir.path(), python.clone())
        };
        let refused = install(&asked).await.unwrap_err();
        assert_eq!(refused.code(), "wheel", "{}: {refused}", wheel.display());
        assert!(!ran.exists(), "{}: an interpreter ran", wheel.display());
    }
    let unreadable = dir.path().join("unreadable-0.1.0-py3-none-any.whl");
    std::fs::write(&unreadable, "").unwrap();
    std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).unwrap();
    let shut = dir.path().join("shut");
    std::fs::create_dir(&shut).unwrap();
    let held = shut.join("held-0.1.0-py3-none-any.whl");
    std::fs::write(&held, "").unwrap();
    std::fs::set_permissions(&shut, std::fs::Permissions::from_mode(0o000)).unwrap();
    // The superuser reads them all the same.
    if std::fs::File::open(&unreadable).is_err() {
        for wheel in [unreadable, held] {
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
async fn the_wheel_is_described_and_installed_from_a_copy_of_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let python = fake_python(dir.path(), &facts("3, 14, 0", "/usr"), DESCRIBED, 0);
    // It keeps what it is asked to read, and says where it was.
    let (read, seen) = (dir.path().join("read"), dir.path().join("seen"));
    let script = std::fs::read_to_string(&python).unwrap().replacen(
        "elif [ \"$#\" = 5 ]; then ",
        &format!(
            "elif [ \"$#\" = 5 ]; then cp \"$5\" '{}'; printf '%s' \"$5\" > '{}'; ",
            seen.display(),
            read.display()
        ),
        1,
    );
    std::fs::write(&python, script).unwrap();
    let asked = installation(dir.path(), python);
    std::fs::write(&asked.wheel, "the wheel's bytes").unwrap();
    install(&asked).await.unwrap_err();
    let read = PathBuf::from(std::fs::read_to_string(read).unwrap());
    assert_ne!(read, asked.wheel, "the operator's file is read");
    assert_eq!(
        read.file_name(),
        asked.wheel.file_name(),
        "pip reads a wheel's name"
    );
    assert_eq!(std::fs::read(seen).unwrap(), b"the wheel's bytes");
    assert!(!read.exists(), "the copy is removed with the install");
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
    // Regular files, executable: pip installs a script executable only so.
    for (file, text) in files {
        writeln!(
            script,
            " i = zipfile.ZipInfo({file:?})\n i.external_attr = 0o100755 << 16\n w.writestr(i, {text:?})"
        )
        .unwrap();
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

/// File names of wheels, each a wheel's as pip reads it.
pub(super) const WHEELS: [&str; 6] = [
    "tickets-0.1.0-py3-none-any.whl",
    "rdlt-0.0.0-cp311-abi3-manylinux_2_28_x86_64.whl",
    "Rdlt_Connector.Tickets-1!2.0.post1+local.7-1build-cp311.cp312-abi3-macosx_11_0_arm64.whl",
    "torch-2.0.0+cu118-cp311-cp311-linux_x86_64.whl",
    // A `.tar` that ends no stem before `.whl`.
    "tickets-0.1.0-py3.tar-none-any.whl",
    "tickets-0.1.0+x.tar-py3-none-any.whl",
];

/// File names that are no wheel's as pip reads them, whatever their last characters.
pub(super) const NO_WHEELS: [&str; 20] = [
    ".whl",
    "..whl",
    "...whl",
    "x.whl",
    "tickets-0.1.0.whl",
    "tickets-0.1.0-py3-none.whl",
    "tickets-0.1.0-x-y-py3-none-any.whl",
    "tickets-0.1.0-py3-none-any.WHL",
    "tickets-0.1.0-py3-none-any.whl.zip",
    "tickets-0.1.0-py3-none-any.tar.gz",
    ".tickets-0.1.0-py3-none-any.whl",
    "tic kets-0.1.0-py3-none-any.whl",
    "tickets-0.1.0.tar.gz-py3-none-any.whl",
    "tickets-1.0/x-py3-none-any.whl",
    "tickets-0.1.0-build-py3-none-any.whl",
    "tickets-0.1.0--none-any.whl",
    "tickets-0.1.0-py3-n%41ne-any.whl",
    // pip takes the `.tar` before `.whl` into the extension, in any case.
    "tickets-0.1.0-py3-none-any.tar.whl",
    "tickets-0.1.0-py3-none-x.TAR.whl",
    "tickets-0.1.0-1-py3-none-any.Tar.whl",
];

/// An interpreter the sandbox shows: the system's, where it is 3.11 or later with `venv`; none
/// elsewhere, unless `RDLT_REQUIRE_SYSTEM_PYTHON` says one must be here.
pub(super) fn system_python() -> Option<PathBuf> {
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
///
/// No install here reaches a network: a requirements file says `--no-index`, and a wheel installed
/// without one needs nothing.
async fn installed(
    python: &Path,
    wheel: PathBuf,
    requirements: Option<PathBuf>,
    connectors: &Path,
) -> Result<PathBuf, InstallError> {
    match &requirements {
        Some(path) => {
            let text = std::fs::read_to_string(path).unwrap();
            let offline = text.lines().any(|line| line.trim() == "--no-index");
            assert!(offline, "{}: no --no-index", path.display());
        }
        None => {
            if let Ok(described) = super::wheel::described(python, &wheel, &wheel).await {
                assert_eq!(
                    described.requires,
                    Vec::<String>::new(),
                    "{}",
                    wheel.display()
                );
            }
        }
    }
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
    let imported = std::process::Command::new(tree.join("bin/python"))
        .args(["-I", "-c", "import tickets"])
        .status()
        .unwrap();
    assert!(
        imported.success(),
        "the tree's interpreter imports the wheel's package"
    );
    let pip = walkdir::WalkDir::new(&tree)
        .into_iter()
        .map(Result::unwrap)
        .find(|entry| entry.file_name().to_string_lossy().starts_with("pip"));
    assert!(pip.is_none(), "the tree holds {pip:?}");
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

/// Requirements lines for the wheel at `path`, of the distribution `name` 0.1.0: where it is found,
/// and its version with its hash.
fn pinned(name: &str, path: &Path) -> String {
    let found = path.parent().unwrap().display();
    format!(
        "--find-links {found}\n{name}==0.1.0 --hash=sha256:{}\n",
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
    // Where pip finds the dependency, which only the first requirements below hash.
    let found = dir.path().join("found");
    std::fs::create_dir(&found).unwrap();
    let dependency = wheel(&found, &python, "needed", &[], "", &[]);
    // No index is asked for what a file leaves out.
    let requirements = |name: &str, text: &str| {
        let path = dir.path().join(name);
        std::fs::write(&path, format!("--no-index\n{text}")).unwrap();
        Some(path)
    };
    let hashed = requirements("hashed.txt", &pinned("needed", &dependency));
    let made = tickets(dir.path(), &python, &["needed"]);
    let tree = installed(&python, made.clone(), hashed, &connectors)
        .await
        .unwrap();
    let imported = std::process::Command::new(tree.join("bin/python"))
        .args(["-I", "-c", "import tickets, needed"])
        .status()
        .unwrap();
    assert!(
        imported.success(),
        "the tree holds the wheel and what it needs"
    );
    // pip finds the dependency where the files below say, and takes it once it is hashed.
    let links = format!("--find-links {}\n", found.display());
    let found_and_hashed = format!(
        "{links}needed==0.1.0 --hash=sha256:{}\n",
        sha256(&dependency)
    );
    let found_and_hashed = requirements("found-and-hashed.txt", &found_and_hashed);
    installed(&python, made.clone(), found_and_hashed, &connectors)
        .await
        .expect("a dependency found and hashed is installed");
    let uri = format!("file://{}", dependency.display());
    let other = wheel(dir.path(), &python, "other", &[], "", &[]);
    for (name, text, refused) in [
        // Refused before anything is read.
        ("unhashed.txt", format!("needed @ {uri}\n"), "requirements"),
        // Found as above, and refused by pip for the hash the file does not give it.
        (
            "left-out.txt",
            format!("{links}{}", pinned("other", &other)),
            "pip_failed",
        ),
    ] {
        let answered = installed(
            &python,
            made.clone(),
            requirements(name, &text),
            &connectors,
        );
        let error = answered.await.unwrap_err();
        assert_eq!(error.code(), refused, "{name}: {error}");
        let other = std::process::Command::new(tree.join("bin/python"))
            .args(["-I", "-c", "import other"])
            .status()
            .unwrap();
        assert!(!other.success(), "{name}: the tree in place changed");
    }
}

/// A source distribution of `planted` 1.0 in `dir`, whose build backend, in its tree, writes
/// `marker` when it is imported; and its hash.
fn planted(dir: &Path, python: &Path, marker: &Path) -> (PathBuf, String) {
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
    assert!(made.unwrap().success(), "the source distribution is made");
    let hash = sha256(&built);
    (built, hash)
}

/// The source of `planted` that [`planted`] archives, as a zip named `.whl` in `dir`, which pip
/// reads as no wheel; and its hash.
fn zipped(dir: &Path, python: &Path, source: &Path) -> (PathBuf, String) {
    let built = dir.join(".whl");
    let script = format!(
        "import tarfile, zipfile\n\
         with tarfile.open({source:?}) as tar, zipfile.ZipFile({built:?}, 'w') as archive:\n\
         \x20for member in tar.getmembers():\n\
         \x20 archive.writestr(member.name.split('/', 1)[1], tar.extractfile(member).read())\n",
        source = source.display().to_string(),
        built = built.display().to_string()
    );
    let made = std::process::Command::new(python)
        .arg("-c")
        .arg(script)
        .status();
    assert!(made.unwrap().success(), "the zip is made");
    let hash = sha256(&built);
    (built, hash)
}

/// Every requirements file, each with what the wheel needs and the refusal it is answered with,
/// that would have pip build `source`, in `found`, a source distribution of `planted`, its hash
/// `hash`, or `zip`, its zip named `.whl`, of hash `zipped`, were it not refused.
fn sources(
    found: &Path,
    source: &Path,
    hash: &str,
    (zip, zipped): (&Path, &str),
) -> Vec<(Vec<String>, Option<String>, &'static str)> {
    let by_url = format!("planted @ file://{}", source.display());
    let links = format!("--no-index\n--find-links {}\n", found.display());
    // From where pip runs, `<connectors>/.rdlt-install-*`, up to the directory beside it.
    let walked = format!(
        "planted @ https://h.example/x-1-py3-none-any.whl?/../../../../../found/{} \
         --hash=sha256:{hash}\n",
        source.file_name().unwrap().to_str().unwrap()
    );
    let file = |text: String| (Vec::new(), Some(text), "requirements");
    vec![
        // What the wheel itself needs by a URL.
        (vec![by_url.clone()], None, "wheel"),
        // A requirements file naming it by its URL.
        file(format!("--no-index\n{by_url} --hash=sha256:{hash}\n")),
        // A requirements file letting pip take sources.
        file(format!(
            "--no-binary :all:\n{links}planted==1.0 --hash=sha256:{hash}\n"
        )),
        // A requirement whose markers would end where an option of pip's begins.
        file(format!(
            "{links}planted==1.0;--no-binary=planted --hash=sha256:{hash}\n"
        )),
        // A URL ending in `.whl` that pip reads as no wheel's.
        file(format!(
            "--no-index\nplanted @ file://{} --hash=sha256:{zipped}\n",
            zip.display()
        )),
        // A wheel's URL whose query walks, as pip reads it, to a source beside the install.
        file(format!("--no-index\n{walked}")),
    ]
}

#[tokio::test]
async fn nothing_pip_would_build_from_its_source_is_installed() {
    let Some(python) = system_python() else {
        return;
    };
    let (_dir, dir) = resolved();
    let connectors = dir.join("connectors");
    let marker = dir.join("built");
    let found = dir.join("found");
    std::fs::create_dir(&found).unwrap();
    let (source, hash) = planted(&found, &python, &marker);
    let (zip, zipped_hash) = zipped(&found, &python, &source);
    let cases = sources(&found, &source, &hash, (&zip, &zipped_hash));
    for (index, (requires, text, refused)) in cases.into_iter().enumerate() {
        let requires: Vec<&str> = requires.iter().map(String::as_str).collect();
        let made = tickets(&dir, &python, &requires);
        let requirements = text.map(|text| {
            let path = dir.join(format!("requirements-{index}.txt"));
            std::fs::write(&path, text).unwrap();
            path
        });
        let answered = installed(&python, made, requirements, &connectors).await;
        let error = answered.unwrap_err();
        assert_eq!(error.code(), refused, "{index}: {error}");
        assert!(
            !marker.exists(),
            "{index}: a build backend ran as the installer"
        );
    }
}

/// A way a wheel's files could run as its installer: its name, the files that plant it, how an
/// interpreter of the tree that holds them runs them, the module it plants where a base
/// installation may have one of its own that comes first, and the refusal an install answers it
/// with, where it is refused.
#[derive(Debug)]
struct Route {
    name: &'static str,
    files: Vec<(String, String)>,
    running: &'static [&'static str],
    shadowed: Option<&'static str>,
    refused: Option<&'static str>,
}

/// Every way a wheel's files could run as its installer, each writing `marker(name)` when it runs.
fn routes(marker: &dyn Fn(&str) -> PathBuf) -> Vec<Route> {
    let writes = |name: &str| {
        format!(
            "import pathlib; pathlib.Path({:?}).write_text('ran')\n",
            marker(name).display().to_string()
        )
    };
    let file = |path: &str, text: String| (path.to_owned(), text);
    vec![
        // `site` runs a `.pth` file's import lines whenever an interpreter of the environment
        // starts, and imports `sitecustomize`.
        Route {
            name: "pth",
            files: vec![file("tickets-startup.pth", writes("pth"))],
            running: &["-I", "-c", "pass"],
            shadowed: None,
            refused: None,
        },
        Route {
            name: "sitecustomize",
            files: vec![file("sitecustomize.py", writes("sitecustomize"))],
            running: &["-I", "-c", "pass"],
            // Debian's and Ubuntu's Python carry one in their standard library.
            shadowed: Some("sitecustomize"),
            refused: None,
        },
        // A package of pip's name, over the environment's own pip where it has one.
        Route {
            name: "pip",
            files: vec![
                file("pip/__init__.py", writes("pip")),
                file("pip/__main__.py", writes("pip")),
            ],
            running: &["-I", "-m", "pip"],
            shadowed: None,
            refused: None,
        },
        // A module pip's own code imports where it finds one.
        Route {
            name: "attr",
            files: vec![file("attr.py", writes("attr"))],
            running: &["-I", "-c", "import attr"],
            shadowed: None,
            refused: None,
        },
    ]
    .into_iter()
    .chain(refused(marker))
    .collect()
}

/// The ways a wheel's files could run as its installer that the install refuses.
fn refused(marker: &dyn Fn(&str) -> PathBuf) -> Vec<Route> {
    let file = |path: &str, text: String| (path.to_owned(), text);
    vec![
        // A script over the environment's interpreter.
        Route {
            name: "interpreter",
            files: vec![file(
                "tickets-0.1.0.data/scripts/python",
                format!("#!/bin/sh\ntouch '{}'\n", marker("interpreter").display()),
            )],
            running: &["-I", "-c", "pass"],
            shadowed: None,
            refused: Some("replaces_environment"),
        },
        // A path for the interpreter beside it.
        Route {
            name: "path",
            files: vec![file(
                "tickets-0.1.0.data/scripts/python._pth",
                "/elsewhere\n".to_owned(),
            )],
            running: &[],
            shadowed: None,
            refused: Some("replaces_environment"),
        },
        // A configuration beside the interpreter, which it reads before the tree's.
        Route {
            name: "configuration",
            files: vec![file(
                "tickets-0.1.0.data/scripts/pyvenv.cfg",
                "home = /elsewhere\n".to_owned(),
            )],
            running: &[],
            shadowed: None,
            refused: Some("replaces_environment"),
        },
    ]
}

/// Asserts that the interpreter of the tree at `tree` runs each of `routes`, which its install did
/// not: what each planted is in place.
fn run_by_the_tree(tree: &Path, routes: &[Route], marker: &dyn Fn(&str) -> PathBuf) {
    for route in routes {
        if let Err(why) = run_or_shadowed(tree, route, marker) {
            panic!("{}: {why}", route.name);
        }
    }
}

/// What the tree's interpreter imports for `module`: the file it is in, every link resolved, or
/// why it imports none, and the interpreter's path.
const FOUND: &str = "import json, os, sys\n\
                     try:\n\
                     \x20   found = os.path.realpath(__import__(sys.argv[1]).__file__)\n\
                     except Exception as error:\n\
                     \x20   found = repr(error)\n\
                     print(json.dumps({'found': found, 'path': sys.path}))\n";

/// Whether the interpreter of the tree at `tree` runs `route`, writing `marker(route.name)`.
///
/// For a route whose module a base installation may have first, it may instead import a module of
/// that name that lies outside the tree, which hides what was planted from every interpreter of
/// that base. Why neither holds, otherwise, with where the interpreter found the module and its
/// path.
fn run_or_shadowed(
    tree: &Path,
    route: &Route,
    marker: &dyn Fn(&str) -> PathBuf,
) -> Result<(), String> {
    std::fs::remove_file(marker(route.name)).ok();
    let python = tree.join("bin/python");
    let started = std::process::Command::new(&python)
        .args(route.running)
        .output()
        .unwrap();
    if marker(route.name).exists() {
        return Ok(());
    }
    let ran = format!("the tree's interpreter does not run it: {started:?}");
    let Some(module) = route.shadowed else {
        return Err(ran);
    };
    let found = std::process::Command::new(&python)
        .args(["-I", "-c", FOUND, module])
        .output()
        .unwrap();
    let said: serde_json::Value = serde_json::from_slice(&found.stdout)
        .map_err(|_| format!("{ran}; asking where it finds {module}: {found:?}"))?;
    let inside = std::fs::canonicalize(tree).unwrap();
    let file = said["found"].as_str().map(Path::new);
    if file.is_some_and(|file| file.is_absolute() && !file.starts_with(&inside)) {
        Ok(())
    } else {
        Err(format!(
            "{ran}, and no module of the base's shadows it: {said}"
        ))
    }
}

/// A tree at `tree` whose interpreter is `python`, its path starting with `first` and its own
/// directory `site`, which holds `planted` as `sitecustomize.py` where there is one; the
/// interpreter imports `sitecustomize` as `site` would, first found first.
fn tree_with_base(tree: &Path, python: &Path, first: &[&Path], planted: Option<&str>) {
    let site = tree.join("site");
    std::fs::create_dir_all(tree.join("bin")).unwrap();
    std::fs::create_dir_all(&site).unwrap();
    if let Some(planted) = planted {
        std::fs::write(site.join("sitecustomize.py"), planted).unwrap();
    }
    let mut path: Vec<String> = first
        .iter()
        .map(|path| path.display().to_string())
        .collect();
    path.push(site.display().to_string());
    let script = format!(
        "#!/bin/sh\nshift 2\nFIRST='{}' exec '{}' -I -S -c 'import os, sys\n\
         sys.path[:0] = os.environ[\"FIRST\"].split(\":\")\n\
         try:\n    import sitecustomize\nexcept ImportError:\n    pass\n\
         script = sys.argv[1]\nsys.argv = [\"-c\"] + sys.argv[2:]\n\
         exec(compile(script, \"<string>\", \"exec\"))' \"$@\"\n",
        path.join(":"),
        python.display()
    );
    std::fs::write(tree.join("bin/python"), script).unwrap();
    std::fs::set_permissions(
        tree.join("bin/python"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
}

#[test]
fn a_route_that_did_not_run_passes_its_control_only_where_the_base_shadows_its_module() {
    let Some(python) = system_python() else {
        return;
    };
    let (_dir, dir) = resolved();
    let marker = |name: &str| dir.join(format!("ran-{name}"));
    let routes = routes(&marker);
    let route = routes
        .iter()
        .find(|route| route.name == "sitecustomize")
        .unwrap();
    let writes = route.files[0].1.as_str();
    // A base whose own `sitecustomize`, which writes nothing, comes before the tree's.
    let base = dir.join("base");
    std::fs::create_dir(&base).unwrap();
    std::fs::write(base.join("sitecustomize.py"), "").unwrap();
    for (case, first, planted, passes) in [
        ("shadowed", vec![base.as_path()], Some(writes), true),
        ("run", vec![], Some(writes), true),
        ("planted, writing nothing", vec![], Some(""), false),
        ("none to import", vec![], None, false),
    ] {
        let tree = dir.join(case);
        tree_with_base(&tree, &python, &first, planted);
        let checked = run_or_shadowed(&tree, route, &marker);
        assert_eq!(checked.is_ok(), passes, "{case}: {checked:?}");
        if let Err(why) = checked {
            assert!(why.contains("\"path\""), "{case}: {why}");
        }
    }
}

#[tokio::test]
async fn no_code_of_the_wheel_runs_while_it_is_installed() {
    let Some(python) = system_python() else {
        return;
    };
    let (_dir, dir) = resolved();
    let connectors = dir.join("connectors");
    let marker = |name: &str| dir.join(format!("ran-{name}"));
    let other = wheel(&dir, &python, "other", &[], "", &[]);
    let requirements = dir.join("requirements.txt");
    let text = format!("--no-index\n{}", pinned("other", &other));
    std::fs::write(&requirements, text).unwrap();
    let (refused, taken): (Vec<Route>, Vec<Route>) = routes(&marker)
        .into_iter()
        .partition(|route| route.refused.is_some());
    // One wheel of every route an install takes, and one of each route it refuses.
    let mut planted = vec![taken];
    planted.extend(refused.into_iter().map(|route| vec![route]));
    let mut ran = Vec::new();
    for routes in &planted {
        let files: Vec<(&str, &str)> = routes
            .iter()
            .flat_map(|route| &route.files)
            .map(|(path, text)| (path.as_str(), text.as_str()))
            .collect();
        let refusal = routes.first().and_then(|route| route.refused);
        let built = wheel(&dir, &python, "tickets", &[], TICKETS, &files);
        for asked in [None, Some(requirements.clone())] {
            let with = if asked.is_some() { "with" } else { "without" };
            let answered = installed(&python, built.clone(), asked, &connectors).await;
            for route in routes.iter().filter(|route| marker(route.name).exists()) {
                ran.push(format!("{} {with} requirements", route.name));
            }
            match (answered, refusal) {
                (Ok(tree), None) => run_by_the_tree(&tree, routes, &marker),
                (Err(error), Some(code)) if error.code() == code => {}
                (answered, refusal) => {
                    let names: Vec<&str> = routes.iter().map(|route| route.name).collect();
                    let said = format!("{answered:?}, not {refusal:?}");
                    ran.push(format!("{names:?} {with} requirements: {said}"));
                }
            }
            for route in routes {
                std::fs::remove_file(marker(route.name)).ok();
            }
        }
    }
    assert!(
        ran.is_empty(),
        "the wheel's code ran as its installer: {ran:#?}"
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

#[tokio::test]
async fn a_wheel_whose_metadata_names_no_distribution_is_refused_before_anything_is_made() {
    for name in [
        "--index-url=http://x",
        "tickets\n--index-url=http://x",
        "tickets\n",
        "-tickets",
        "tickets-",
        ".tickets",
        "tic kets",
        "",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let named = serde_json::json!({
            "name": name,
            "sha256": "00",
            "entry_points": [["io.example.tickets", "tickets:Tickets"]],
            "requires": [],
        });
        let python = fake_python(
            dir.path(),
            &facts("3, 14, 0", "/usr"),
            &named.to_string(),
            0,
        );
        let asked = installation(dir.path(), python);
        let refused = install(&asked).await.unwrap_err();
        assert_eq!(refused.code(), "wheel", "{name:?}: {refused}");
        assert!(
            !asked.connectors.exists(),
            "{name:?}: the connector directory was made"
        );
        assert!(
            !dir.path().join("made").exists(),
            "{name:?}: the environment was made"
        );
    }
    for name in ["tickets", "T", "rdlt-connector-tickets", "a.b_c-1"] {
        let named = DESCRIBED.replace("\"name\": \"tickets\"", &format!("\"name\": \"{name}\""));
        let refused = refusal(&facts("3, 14, 0", "/usr"), &named, 0).await;
        assert_eq!(refused.code(), "pip_failed", "{name}: {refused}");
    }
}
