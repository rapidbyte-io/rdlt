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
