//! `connector new` writes the scaffold renamed; the `template` test binary certifies its source
//! as written.

use crate::support::Scratch;

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
}
