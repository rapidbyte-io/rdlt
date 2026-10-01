use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

use rdlt_connector::{ConnectorError, ConnectorErrorKind};

use super::super::database::connect;
use super::located;

fn refusal<T: std::fmt::Debug>(outcome: Result<T, ConnectorError>) -> (ConnectorErrorKind, String) {
    let error = outcome.expect_err("the path is refused");
    (error.kind(), error.code().unwrap_or_default().to_owned())
}

fn config(code: &str) -> (ConnectorErrorKind, String) {
    (ConnectorErrorKind::Config, code.to_owned())
}

fn mode(path: &Path, mode: u32) {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("a mode");
}

/// The names in `directory`, sorted.
fn listed(directory: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(directory)
        .expect("the directory lists")
        .map(|entry| {
            entry
                .expect("an entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

// The working directory is the process's: nextest gives each test its own, and every other test
// of the crate names its files from the root.
#[test]
fn a_name_sqlite_would_read_as_a_uri_is_refused_and_opens_nothing() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    std::env::set_current_dir(directory.path()).expect("the directory is entered");
    // A database others reach, which the name would open as a URI past the check of its mode.
    drop(rusqlite::Connection::open("exposed.db").expect("a database"));
    mode(Path::new("exposed.db"), 0o666);
    let inside = directory.path().display();
    let names = [
        "file:exposed.db".to_owned(),
        "file:fresh.db?nolock=1".to_owned(),
        "file:lost.db?mode=memory".to_owned(),
        "file:fresh.db?immutable=1".to_owned(),
        "file:%65xposed.db".to_owned(),
        "FILE:exposed.db".to_owned(),
        format!("file:{inside}/exposed.db"),
        format!("file://{inside}/exposed.db"),
        format!("file://localhost{inside}/exposed.db?vfs=unix-none"),
    ];
    for name in &names {
        let refused = refusal(connect(Path::new(name)));
        assert_eq!(refused, config("database_path_invalid"), "{name}");
    }
    assert_eq!(listed(directory.path()), ["exposed.db"]);
    // A name from the working directory is the file it names there.
    drop(connect(Path::new("plain.db")).expect("a plain name opens"));
    assert!(directory.path().join("plain.db").is_file());
}

#[test]
fn a_name_holding_what_a_uri_would_read_as_parameters_or_escapes_is_the_file_it_names() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let names = [
        "named.db?mode=memory",
        "locked.db?nolock=1",
        "a%2fb.db",
        "%66ile:x.db",
        "frag.db#x",
        "file:in-a-directory.db",
    ];
    for name in names {
        let path = directory.path().join(name);
        let connection = connect(&path).unwrap_or_else(|error| panic!("{name}: {error}"));
        connection
            .execute_batch("CREATE TABLE t (a INTEGER)")
            .expect("a table is created");
        drop(connection);
        let found = std::fs::metadata(&path).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert!(found.len() > 0, "{name}");
        assert_eq!(found.permissions().mode() & 0o777, 0o600, "{name}");
    }
    let mut expected: Vec<&str> = names.to_vec();
    expected.sort_unstable();
    let kept: Vec<String> = listed(directory.path())
        .into_iter()
        .filter(|name| !name.ends_with("-wal") && !name.ends_with("-shm"))
        .collect();
    assert_eq!(kept, expected);
}

#[test]
fn a_name_that_is_no_file_s_is_refused() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    for name in ["", "/", "..", "x/..", "x/.", "nul\0.db"] {
        let path = directory.path().join(name);
        let refused = refusal(located(&path, true));
        assert_eq!(refused, config("database_path_invalid"), "{name:?}");
    }
    assert_eq!(
        refusal(located(Path::new(""), true)),
        config("database_path_invalid")
    );
}

#[test]
fn a_database_that_is_missing_is_created_only_where_asked() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("orders.db");
    assert_eq!(located(&path, false).expect("the place is private"), None);
    assert!(!path.exists());
    assert_eq!(located(&path, true).expect("created"), Some(path.clone()));
    let found = std::fs::metadata(&path).expect("the file is there");
    assert_eq!(found.permissions().mode() & 0o777, 0o600);
    assert_eq!(located(&path, false).expect("found"), Some(path));
}
