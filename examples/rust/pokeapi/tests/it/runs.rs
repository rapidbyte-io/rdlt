//! The example's program, run as its README runs it, against the local server.

use std::path::Path;
use std::process::Command;

use crate::server::{Answer, Api};

/// What the program prints, loading from `api` into `database`.
fn run(database: &Path, api: &Api) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_pokeapi"))
        .arg(database)
        .arg(&api.url)
        .output()
        .expect("the program runs");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    String::from_utf8(output.stdout).expect("the program prints text")
}

/// Each run of the program in the example's README's console block, as its arguments after
/// `cargo run -q -p pokeapi --` and what it prints, in order.
fn shown() -> Vec<(Vec<String>, String)> {
    let example = Path::new(env!("CARGO_MANIFEST_DIR"));
    let readme = std::fs::read_to_string(example.join("README.md")).expect("a README");
    let block = readme
        .split("```console\n")
        .nth(1)
        .expect("a console block");
    let running = concat!("cargo run -q -p ", env!("CARGO_PKG_NAME"), " -- ");
    let mut runs: Vec<(Vec<String>, String)> = Vec::new();
    for line in block.split("```").next().unwrap_or_default().lines() {
        if let Some(command) = line.strip_prefix("$ ") {
            if let Some(arguments) = command.strip_prefix(running) {
                let arguments = arguments.split_whitespace().map(ToOwned::to_owned);
                runs.push((arguments.collect(), String::new()));
            } else {
                assert!(
                    !command.starts_with("cargo"),
                    "it runs no example: {command}"
                );
            }
        } else if let Some((_, printed)) = runs.last_mut() {
            printed.push_str(line);
            printed.push('\n');
        }
    }
    runs
}

/// The count `query` answers in the SQLite database at `path`.
pub(crate) fn count(path: &Path, query: &str) -> i64 {
    let database = rusqlite::Connection::open(path).expect("the database opens");
    let counted = database.query_row(query, [], |row| row.get(0));
    counted.unwrap_or_else(|error| panic!("{query}: {error}"))
}

#[test]
fn a_second_run_resumes_and_commits_nothing_as_the_readme_shows() {
    let api = Api::serve(200);
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("pokeapi.db");
    // `cargo run -p pokeapi` runs the program the test runs, and the README names the database
    // alone, where the test names its own and the local server's root.
    let manifest =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"));
    let default_run = concat!("default-run = \"", env!("CARGO_PKG_NAME"), "\"");
    assert!(manifest.expect("a manifest").contains(default_run));
    let shown = shown();
    assert_eq!(shown.len(), 2);
    for (arguments, printed) in shown {
        assert_eq!(arguments.len(), 1, "{arguments:?}");
        assert_eq!(run(&database, &api), printed);
    }
    let distinct = count(&database, r#"SELECT count(DISTINCT "id") FROM "pokemon""#);
    assert_eq!(distinct, 151);
    // A pokemon's nested lists land as JSON in its row: here, its two abilities.
    let abilities = r#"SELECT count(*) FROM "pokemon" WHERE json_array_length("abilities") = 2"#;
    assert_eq!(count(&database, abilities), 151);
    // Both streams read at once, and the API is asked one thing at a time.
    assert_eq!(api.most_at_once(), 1);
}

#[test]
fn a_failure_the_engine_retries_loses_no_row() {
    let answers = vec![
        Answer::failed("/api/v2/pokemon/3", 500),
        Answer::cut("/api/v2/pokemon/5"),
        Answer::waiting("/api/v2/type", 1),
    ];
    let api = Api::answering(200, answers);
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("pokeapi.db");
    assert_eq!(run(&database, &api), "Succeeded: committed 172 rows\n");
    assert_eq!(api.unserved(), Vec::<&str>::new());
    let rows = count(&database, r#"SELECT count(*) FROM "pokemon""#);
    let distinct = count(&database, r#"SELECT count(DISTINCT "id") FROM "pokemon""#);
    assert_eq!((rows, distinct), (151, 151));
}
