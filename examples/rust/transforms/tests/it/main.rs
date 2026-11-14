#![forbid(unsafe_code)]
//! The transforms example prints what its README shows, and fails where its mask has no key.

use std::path::Path;
use std::process::{Command, Output};

/// The mask's key, which the program reads from the environment.
const KEY: &str = "ORDERS_KEY";

/// How the program ends, loading into `database` with `environment` set and no other key.
fn ended(database: &Path, environment: &[Variable]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_transforms"))
        .arg(database)
        .env_remove(KEY)
        .envs(environment.iter().map(|(name, value)| (name, value)))
        .output()
        .expect("the program runs")
}

/// A variable the README exports: its name and its value.
type Variable = (String, String);

/// A run of the program the README shows: its arguments after `cargo run -q -p transforms --`,
/// and what it prints.
type Shown = (Vec<String>, String);

/// The README's console block: the variables it exports, then each run of the program, in order.
fn shown() -> (Vec<Variable>, Vec<Shown>) {
    let example = Path::new(env!("CARGO_MANIFEST_DIR"));
    let readme = std::fs::read_to_string(example.join("README.md")).expect("a README");
    let block = readme
        .split("```console\n")
        .nth(1)
        .expect("a console block");
    let running = concat!("cargo run -q -p ", env!("CARGO_PKG_NAME"), " -- ");
    let mut exported = Vec::new();
    let mut runs: Vec<Shown> = Vec::new();
    for line in block.split("```").next().unwrap_or_default().lines() {
        if let Some(command) = line.strip_prefix("$ ") {
            if let Some(arguments) = command.strip_prefix(running) {
                let arguments = arguments.split_whitespace().map(ToOwned::to_owned);
                runs.push((arguments.collect(), String::new()));
            } else if let Some(variable) = command.strip_prefix("export ") {
                let (name, value) = variable.split_once('=').expect("a variable and its value");
                exported.push((name.to_owned(), value.to_owned()));
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
    (exported, runs)
}

#[test]
fn a_load_and_a_refused_one_print_what_the_readme_shows() {
    let (exported, runs) = shown();
    assert_eq!(
        exported.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        [KEY]
    );
    assert_eq!(runs.len(), 1);
    let directory = tempfile::tempdir().unwrap();
    // The README names the database alone, where the test names its own.
    let database = directory.path().join("orders.db");
    for (arguments, printed) in runs {
        assert_eq!(arguments.len(), 1, "{arguments:?}");
        let output = ended(&database, &exported);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stderr}");
        assert_eq!(String::from_utf8(output.stdout).unwrap(), printed);
    }
    // The table's columns: what the steps made, in their order, then the engine's own.
    let database = rusqlite::Connection::open(&database).unwrap();
    let mut named = database
        .prepare("SELECT name FROM pragma_table_info('orders') ORDER BY cid")
        .unwrap();
    let columns: Vec<String> = named
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let made = [
        "id",
        "email",
        "quantity",
        "price_cents",
        "total_cents",
        "status",
        "currency",
    ];
    let engine = ["_rdlt_load_epoch", "_rdlt_loaded_at"];
    assert_eq!(columns, [made.as_slice(), engine.as_slice()].concat());
}

#[test]
fn a_load_whose_mask_has_no_key_exits_1() {
    let directory = tempfile::tempdir().unwrap();
    let output = ended(&directory.path().join("orders.db"), &[]);
    assert_eq!(output.status.code(), Some(1));
    // A reference that does not resolve is named by its code, its value never shown.
    assert!(String::from_utf8_lossy(&output.stderr).contains("secret_unresolved"));
}
