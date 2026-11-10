//! `connector new` writes the scaffold renamed; the `template` test binary certifies its source
//! as written.

use crate::support::Scratch;
#[cfg(target_os = "linux")]
use crate::support::output_of;

#[tokio::test]
async fn connector_new_writes_the_scaffold_renamed_and_refuses_what_exists() {
    let scratch = Scratch::new();
    let into = scratch.path().to_str().unwrap();
    let made = scratch
        .rdlt(&["connector", "new", "tickets", "--into", into])
        .await;
    assert_eq!(
        made.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&made.stderr)
    );
    let project = scratch.path().join("rdlt-connector-tickets");
    let library = std::fs::read_to_string(project.join("src/lib.rs")).unwrap();
    assert!(
        library.contains("#[source(id = \"io.example.tickets\")]")
            && library.contains("pub struct Tickets")
    );
    let main = std::fs::read_to_string(project.join("src/main.rs")).unwrap();
    insta::assert_snapshot!("scaffold_main", main);
    insta::assert_snapshot!(
        "scaffold_manifest",
        std::fs::read_to_string(project.join("Cargo.toml")).unwrap()
    );
    let again = scratch
        .rdlt(&["connector", "new", "tickets", "--into", into])
        .await;
    assert_eq!(again.status.code(), Some(74));
    let bad = scratch
        .rdlt(&["connector", "new", "Bad-Name", "--into", into])
        .await;
    assert_eq!(bad.status.code(), Some(64));
    for bad in ["ok-name", "1abc", "Bad"] {
        let refused = scratch
            .rdlt(&["connector", "new", bad, "--into", into])
            .await;
        assert_eq!(refused.status.code(), Some(64), "{bad}");
    }
    let accepted = scratch
        .rdlt(&["connector", "new", "with_digits_1", "--into", into])
        .await;
    assert_eq!(accepted.status.code(), Some(0), "{accepted:?}");
    for colliding in ["config", "people", "result"] {
        let refused = scratch
            .rdlt(&["connector", "new", colliding, "--into", into])
            .await;
        assert_eq!(refused.status.code(), Some(64), "{colliding}");
    }
}

/// A directory whose name is no text is reported as it is, never a panic, where a file system
/// takes such a name, as macOS's does not.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_project_beneath_a_path_that_is_not_text_is_reported() {
    use std::os::unix::ffi::OsStrExt as _;

    let scratch = Scratch::new();
    let into = scratch
        .path()
        .join(std::ffi::OsStr::from_bytes(b"into-\xff"));
    std::fs::create_dir(&into).unwrap();
    let mut new =
        scratch.bare_command(&["connector", "new", "bytes", "--output", "json", "--into"]);
    new.arg(&into);
    let made = output_of(&mut new).await;
    assert_eq!(made.status.code(), Some(0), "{made:?}");
    let wrote: serde_json::Value = serde_json::from_slice(&made.stdout).unwrap();
    assert!(wrote["wrote"].is_string(), "{wrote}");
}

/// The project's files beneath `project`, each by its path within it, sorted.
fn files_of(project: &std::path::Path) -> Vec<String> {
    let mut files = Vec::new();
    let mut ahead = vec![project.to_owned()];
    while let Some(directory) = ahead.pop() {
        for entry in std::fs::read_dir(&directory).expect("the project lists") {
            let path = entry.expect("an entry").path();
            if path.is_dir() {
                ahead.push(path);
            } else {
                let within = path.strip_prefix(project).expect("within the project");
                files.push(within.display().to_string());
            }
        }
    }
    files.sort();
    files
}

#[tokio::test]
async fn connector_new_writes_the_python_scaffold_renamed() {
    let scratch = Scratch::new();
    let into = scratch.path().to_str().unwrap();
    let new = |name: &'static str| ["connector", "new", name, "--lang", "python", "--into", into];
    let made = scratch
        .rdlt(&[&new("tickets")[..], &["--output", "plain"]].concat())
        .await;
    assert_eq!(made.status.code(), Some(0), "{made:?}");
    assert!(
        String::from_utf8_lossy(&made.stdout).contains("`pytest`"),
        "{made:?}"
    );
    let project = scratch.path().join("rdlt-connector-tickets");
    let package = "src/rdlt_connector_tickets/__init__.py";
    let expected = [
        "README.md",
        "pyproject.toml",
        package,
        "tests/test_certify.py",
    ];
    assert_eq!(files_of(&project), expected);
    let read = |file: &str| std::fs::read_to_string(project.join(file)).unwrap();
    let manifest = read("pyproject.toml");
    assert!(
        manifest.contains(r#"name = "rdlt-connector-tickets""#),
        "{manifest}"
    );
    let entry_point = r#""io.example.tickets" = "rdlt_connector_tickets:Tickets""#;
    assert!(manifest.contains(entry_point), "{manifest}");
    let source = read(package);
    assert!(
        source.contains(r#"@source(id="io.example.tickets", config=TicketsConfig)"#)
            && source.contains("class Tickets:"),
        "{source}"
    );
    let test = read("tests/test_certify.py");
    assert!(
        test.contains("from rdlt_connector_tickets import Tickets")
            && test.contains("certify_source(Tickets,"),
        "{test}"
    );
    // A name the Rust scaffold takes for itself is the Python one's to give, and the reverse.
    let people = scratch.rdlt(&new("people")).await;
    assert_eq!(people.status.code(), Some(0), "{people:?}");
    for colliding in ["config", "cursor", "position", "none", "true"] {
        let refused = scratch.rdlt(&new(colliding)).await;
        assert_eq!(refused.status.code(), Some(64), "{colliding}");
    }
    let rust = scratch
        .rdlt(&["connector", "new", "position", "--into", into])
        .await;
    assert_eq!(rust.status.code(), Some(0), "{rust:?}");
}
