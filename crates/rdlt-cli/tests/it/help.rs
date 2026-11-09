//! Every verb's help, held by snapshots, and the CLI reference generated from it.

use crate::support::Scratch;

#[tokio::test]
async fn each_verb_s_help_is_as_recorded() {
    let scratch = Scratch::new();
    for verb in [
        "init",
        "check",
        "discover",
        "run",
        "state",
        "schema",
        "connectors",
        "certify",
        "connector",
        "completions",
    ] {
        let output = scratch.rdlt(&[verb, "--help"]).await;
        assert_eq!(output.status.code(), Some(0), "{verb}");
        insta::assert_snapshot!(
            format!("help_{verb}"),
            String::from_utf8_lossy(&output.stdout)
        );
    }
    let top = scratch.rdlt(&["--help"]).await;
    insta::assert_snapshot!("help_rdlt", String::from_utf8_lossy(&top.stdout));
}

#[tokio::test]
async fn completions_print_for_a_shell_and_a_wrong_flag_is_a_usage_error() {
    let scratch = Scratch::new();
    let completions = scratch.rdlt(&["completions", "bash"]).await;
    assert_eq!(completions.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&completions.stdout).contains("_rdlt"));
    let wrong = scratch.rdlt(&["run", "--no-such-flag"]).await;
    assert_eq!(wrong.status.code(), Some(64));
    assert!(
        wrong.stdout.is_empty(),
        "a usage error says nothing on standard output"
    );
}

#[tokio::test]
async fn a_usage_error_under_an_explicit_output_json_is_json_on_standard_output() {
    let scratch = Scratch::new();
    let wrong = scratch
        .rdlt(&["run", "--no-such-flag", "--output", "json"])
        .await;
    assert_eq!(wrong.status.code(), Some(64));
    let said: serde_json::Value = serde_json::from_slice(&wrong.stdout).expect("one JSON value");
    assert_eq!(said["error"]["kind"], "usage", "{said}");
    assert!(said["error"]["message"].is_string(), "{said}");
    let plain = scratch
        .rdlt(&["run", "--no-such-flag", "--output", "plain"])
        .await;
    assert_eq!(plain.status.code(), Some(64));
    assert!(plain.stdout.is_empty(), "plain text goes to standard error");
    // It names what is missing, as the rest of clap's message would.
    let missing = scratch.rdlt(&["check", "--output", "json"]).await;
    let said: serde_json::Value = serde_json::from_slice(&missing.stdout).expect("one JSON value");
    let message = said["error"]["message"].as_str().expect("text");
    assert!(message.contains("<DOCUMENT>"), "{message}");
    assert!(!message.contains("Usage:"), "{message}");
    let banana = scratch.rdlt(&["connectors", "--log-level", "banana"]).await;
    assert_eq!(banana.status.code(), Some(64), "a level that is none");
}

#[test]
fn the_committed_reference_is_generated_from_the_help() {
    let committed =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/cli.md"))
            .expect("the reference is committed");
    assert_eq!(
        committed,
        rdlt_cli::reference(),
        "regenerate it with `cargo run -q -p rdlt-cli --example reference > docs/cli.md`"
    );
}
