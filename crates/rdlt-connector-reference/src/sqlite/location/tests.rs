use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use rdlt_connector::{ConnectorError, ConnectorErrorKind};

use super::super::database::connect;
use super::{SIDE_FILES, located};

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

/// `name` with `suffix` after it.
fn beside(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    name.into()
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
fn a_database_s_directory_is_a_root_held_as_the_files_connectors_hold_theirs() {
    let outer = tempfile::tempdir().expect("a temporary directory");
    let directory = outer.path().join("data");
    std::fs::create_dir(&directory).expect("a directory");
    let path = directory.join("orders.db");
    mode(&directory, 0o777);
    assert_eq!(refusal(connect(&path)), config("not_private"));
    assert!(!path.exists());
    mode(&directory, 0o755);
    drop(connect(&path).expect("a directory only its user writes holds a database"));
    // A directory that is none, and one that is missing.
    let file = outer.path().join("file");
    std::fs::write(&file, b"").expect("a file");
    let refused = refusal(connect(&file.join("orders.db")));
    assert_eq!(refused, config("not_a_directory"));
    let (kind, _) = refusal(connect(&outer.path().join("missing").join("orders.db")));
    assert_eq!(kind, ConnectorErrorKind::Config);
}

#[test]
fn a_database_and_each_file_beside_it_is_a_private_file_and_no_link() {
    assert_eq!(SIDE_FILES, ["", "-wal", "-shm", "-journal"]);
    for suffix in SIDE_FILES {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let path = directory.path().join("orders.db");
        drop(connect(&path).expect("the database is created"));
        let planted = beside(&path, suffix);
        let elsewhere = directory.path().join("elsewhere");
        std::fs::write(&elsewhere, b"").expect("a file");
        mode(&elsewhere, 0o600);
        // A file its group or others reach.
        for exposed in [0o666, 0o644, 0o640, 0o604, 0o660, 0o606, 0o610, 0o601] {
            if !planted.exists() {
                std::fs::write(&planted, b"").expect("a file is planted");
            }
            mode(&planted, exposed);
            let refused = refusal(connect(&path));
            assert_eq!(refused, config("not_private"), "{suffix} {exposed:o}");
        }
        // A link, to a private file or to nothing, and what is no file.
        std::fs::remove_file(&planted).expect("the file is removed");
        for target in [elsewhere.clone(), directory.path().join("nowhere")] {
            std::os::unix::fs::symlink(&target, &planted).expect("a link is planted");
            let refused = refusal(connect(&path));
            assert_eq!(refused, config("not_a_regular_file"), "{suffix}");
            std::fs::remove_file(&planted).expect("the link is removed");
        }
        std::fs::create_dir(&planted).expect("a directory is planted");
        let refused = refusal(connect(&path));
        assert_eq!(refused, config("not_a_regular_file"), "{suffix}");
    }
}

#[test]
fn a_link_at_the_database_s_name_leads_no_load_into_another_database() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let other = directory.path().join("other.db");
    drop(connect(&other).expect("the other database"));
    let link = directory.path().join("mine.db");
    std::os::unix::fs::symlink(&other, &link).expect("a link");
    assert_eq!(refusal(connect(&link)), config("not_a_regular_file"));
    let tables: i64 = connect(&other)
        .expect("the other database opens")
        .query_row("SELECT count(*) FROM sqlite_schema", [], |row| row.get(0))
        .expect("the schema counts");
    assert_eq!(tables, 0);
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

/// The locks this process holds through its descriptors' files, as the kernel lists them.
#[cfg(target_os = "linux")]
fn locks() -> Vec<String> {
    let process = std::process::id().to_string();
    let mut held: Vec<String> = std::fs::read_to_string("/proc/locks")
        .expect("the kernel lists locks")
        .lines()
        .map(|line| line.split_whitespace().skip(1).collect::<Vec<_>>())
        .filter(|fields| fields.first() == Some(&"POSIX") && fields.get(3) == Some(&&*process))
        .map(|fields| fields.join(" "))
        .collect();
    held.sort();
    held
}

/// The variable that names the database [`another_process_writes_the_database_it_is_given`]
/// writes.
const WRITTEN: &str = "RDLT_TEST_WRITTEN_DATABASE";

/// Run only by [`written_by_another_process`], in a process of its own: takes the write lock of
/// the database it is given without waiting, and writes a row.
#[test]
#[ignore = "run by a test that names a database"]
fn another_process_writes_the_database_it_is_given() {
    let path = std::env::var_os(WRITTEN).expect("a database is named");
    let connection = rusqlite::Connection::open(path).expect("the database opens");
    connection
        .busy_timeout(std::time::Duration::ZERO)
        .expect("no wait");
    connection
        .execute_batch("BEGIN IMMEDIATE; INSERT INTO t VALUES (2); COMMIT;")
        .expect("the write lock is free");
}

/// Whether another process takes the write lock of the database at `path` now and writes it.
fn written_by_another_process(path: &Path) -> bool {
    let test = "sqlite::location::tests::another_process_writes_the_database_it_is_given";
    let ran = std::process::Command::new(std::env::current_exe().expect("this test's binary"))
        .args(["--exact", test, "--ignored", "--test-threads", "1"])
        .env(WRITTEN, path)
        .output()
        .expect("the test binary runs");
    let said = String::from_utf8_lossy(&ran.stdout);
    assert!(said.contains("running 1 test"), "the writer ran: {said}");
    ran.status.success()
}

/// A connection writing the database at `path`, its transaction open and its lock held.
fn writing(path: &Path) -> rusqlite::Connection {
    let writer = connect(path).expect("the database opens");
    writer
        .execute_batch("CREATE TABLE IF NOT EXISTS t (a INTEGER); BEGIN IMMEDIATE")
        .expect("the write lock is taken");
    writer
        .execute("INSERT INTO t VALUES (1)", [])
        .expect("a row");
    writer
}

#[test]
fn checking_a_database_s_place_holds_out_whoever_an_open_transaction_holds_out() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("orders.db");
    let writer = writing(&path);
    assert!(
        !written_by_another_process(&path),
        "the writer's lock holds"
    );
    // What every open, check and read-back of the destination does first, and a second
    // connection of the process, which checks the place again.
    for _ in 0..2 {
        located(&path, true).expect("the place is private");
        located(&path, false).expect("the place is private");
    }
    drop(connect(&path).expect("a second connection"));
    drop(super::super::database::reading(&path).expect("a reading connection"));
    assert!(
        !written_by_another_process(&path),
        "another process wrote during the writer's transaction"
    );
    writer.execute_batch("COMMIT").expect("the writer commits");
    assert!(written_by_another_process(&path), "the lock is free");
}

#[cfg(target_os = "linux")]
#[test]
fn checking_a_database_s_place_releases_no_lock_the_process_holds() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("orders.db");
    let writer = writing(&path);
    let before = locks();
    assert!(!before.is_empty(), "the writer holds locks");
    located(&path, true).expect("the place is private");
    located(&path, false).expect("the place is private");
    assert_eq!(locks(), before);
    writer.execute_batch("COMMIT").expect("the writer commits");
}

#[test]
fn a_database_of_more_names_than_one_is_refused() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("orders.db");
    drop(connect(&path).expect("the database is created"));
    let other = directory.path().join("other.db");
    std::fs::hard_link(&path, &other).expect("a second name");
    assert_eq!(refusal(connect(&path)), config("not_a_regular_file"));
    assert_eq!(refusal(connect(&other)), config("not_a_regular_file"));
    std::fs::remove_file(&other).expect("the name is removed");
    drop(connect(&path).expect("the database has one name"));
}
