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
