#![forbid(unsafe_code)]
//! The quick start prints what its READMEs show, and the repository's README shows it whole.

use std::path::Path;
use std::process::Command;

/// What the quick start prints, run on `database`.
fn run(database: &Path) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_quickstart"))
        .arg(database)
        .output()
        .expect("the quick start runs");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    String::from_utf8(output.stdout).expect("the quick start prints text")
}

/// The text of the file at `path`, relative to the example.
fn read(path: &str) -> String {
    let example = Path::new(env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(example.join(path)).unwrap_or_else(|error| panic!("{path}: {error}"))
}

/// The quick start as the repository's README shows it.
fn code() -> String {
    format!("```rust\n{}```\n", read("src/main.rs"))
}

/// Each run of the quick start in `text`'s first console block, which runs it as `running` does,
/// as its arguments after `running` and what it prints, in order.
fn shown(text: &str, running: &str) -> Vec<(Vec<String>, String)> {
    let block = text
        .split("```console\n")
        .nth(1)
        .expect("a console block is shown");
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

#[test]
fn a_second_run_resumes_and_commits_nothing_as_the_readmes_show() {
    let readme = read("../../../README.md");
    let after = readme
        .find(&code())
        .expect("the README shows the quick start");
    // This README runs the example from `examples/rust`, the repository's README a project of
    // the reader's own; each names the database alone, where the test names its own.
    let running = concat!("cargo run -q -p ", env!("CARGO_PKG_NAME"), " -- ");
    for runs in [
        shown(&read("README.md"), running),
        shown(&readme[after..], "cargo run -q -- "),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("demo.db");
        assert_eq!(runs.len(), 2);
        for (arguments, printed) in runs {
            assert_eq!(arguments.len(), 1, "{arguments:?}");
            assert_eq!(run(&database), printed);
        }
    }
}

#[test]
fn the_readme_shows_the_quick_start_whole() {
    assert!(read("../../../README.md").contains(&code()));
}
