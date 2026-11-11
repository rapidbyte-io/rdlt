use rdlt::{Code, ErrorKind, ErrorReport, Origin};

use super::{HINTS, certify_failure, failure, hint};
use crate::Exit;
use crate::output::Failure;
use crate::ui::Ui;

fn report(origin: Origin, kind: ErrorKind, code: Option<&'static str>) -> ErrorReport {
    ErrorReport {
        kind,
        origin,
        stream: None,
        code: code.map(Code::new),
        connector_code: None,
        message: "opening the database at /x/y.db".to_owned(),
        causes: vec!["unable to open database file".to_owned()],
        retryable: false,
    }
}

fn rendered(report: ErrorReport) -> String {
    failure(
        &Failure {
            exit: Exit::Destination,
            code: None,
            message: "unused where a report is given".to_owned(),
            report: Some(Box::new(report)),
            file_not_found: false,
        },
        &Ui::for_tests(),
    )
}

#[test]
fn a_failure_says_who_failed_why_and_a_hint() {
    let text = rendered(report(
        Origin::Destination,
        ErrorKind::Destination,
        Some("grant_outside"),
    ));
    // The hint wraps at the width standard error reports, whatever the test runs with.
    let unwrapped = text.replace("\n  ", " ");
    assert_eq!(
        unwrapped,
        "✗ destination failed: opening the database at /x/y.db caused by: unable to open database \
         file hint: the sandbox can write only what you grant; pass --writable naming a root that \
         holds the path\n"
    );
    assert!(text.starts_with(
        "✗ destination failed: opening the database at /x/y.db\n  caused by: unable to open database file\n  hint: "
    ));
}

#[test]
fn the_engine_s_own_failures_have_no_side_named() {
    let text = rendered(report(
        Origin::Engine,
        ErrorKind::Config,
        Some("config_invalid"),
    ));
    assert_eq!(
        text.lines().next(),
        Some("✗ opening the database at /x/y.db"),
        "{text}"
    );
    assert!(!text.contains("hint:"), "no hint for this code: {text}");
    let source = rendered(report(Origin::Source, ErrorKind::Source, None));
    assert!(source.starts_with("✗ source failed: "), "{source}");
}

#[test]
fn a_cause_the_message_holds_is_not_repeated() {
    let mut report = report(Origin::Engine, ErrorKind::Config, None);
    report.message = "the read failed: boom".to_owned();
    report.causes = vec!["boom".to_owned(), "deeper".to_owned()];
    let text = rendered(report);
    assert_eq!(
        text.lines().collect::<Vec<_>>(),
        ["✗ the read failed: boom", "  caused by: deeper"]
    );
}

#[test]
fn a_failure_of_the_command_line_s_own_is_one_line() {
    let text = failure(
        &Failure::usage("name a connector, or --document"),
        &Ui::for_tests(),
    );
    assert_eq!(text, "✗ name a connector, or --document\n");
    assert_eq!(
        certify_failure("no such binary", &Ui::for_tests()),
        "✗ no such binary\n"
    );
}

#[test]
fn a_connector_s_text_cannot_steer_the_terminal() {
    let mut report = report(Origin::Source, ErrorKind::Source, None);
    report.message = "bad\u{1b}[2J".to_owned();
    report.causes = vec!["worse\u{7}".to_owned()];
    let text = rendered(report);
    assert!(
        !text.contains('\u{1b}') && !text.contains('\u{7}'),
        "{text:?}"
    );
}

#[test]
fn hints_are_given_only_for_their_codes_and_say_what_a_weakened_default_costs() {
    let with = |code: &'static str| {
        hint(
            &report(Origin::Engine, ErrorKind::Config, Some(code)),
            false,
        )
    };
    for code in [
        "connector_not_found",
        "cursor_missing",
        "grant_outside",
        "sandbox_missing",
        "sandbox_unavailable",
        "sandbox_unsupported",
    ] {
        assert!(with(code).is_some(), "{code}");
    }
    assert_eq!(with("config_invalid"), None);
    assert_eq!(
        hint(&report(Origin::Engine, ErrorKind::Config, None), false),
        None
    );
    for (_, hint) in HINTS {
        if hint.contains("--trust-binaries") {
            assert!(
                hint.contains("your own access"),
                "a hint that weakens a default says what it costs: {hint}"
            );
        }
        if hint.contains("--tables") {
            assert!(
                hint.contains("drops its table"),
                "a hint that drops a table says so: {hint}"
            );
        }
    }
    assert!(hint(&report(Origin::Engine, ErrorKind::Fenced, None), false).is_some());
}

#[test]
fn a_document_gets_the_init_hint_only_where_it_is_not_there() {
    let unread = report(
        Origin::Engine,
        ErrorKind::Config,
        Some("document_unreadable"),
    );
    assert_eq!(
        hint(&unread, false),
        None,
        "a directory given as the document"
    );
    assert_eq!(
        hint(&unread, true),
        Some("`rdlt init` writes a starter document")
    );
}

/// A connector is untrusted, and sandboxed for that reason: the words that would ask the user to
/// drop that sandbox must never be at its choosing.
#[test]
fn a_connector_s_own_code_cannot_select_any_of_the_host_s_hints() {
    let hosts = HINTS.iter().map(|(code, _)| *code);
    for code in hosts.chain(["document_unreadable"]) {
        let mut named = report(Origin::Destination, ErrorKind::Config, None);
        named.connector_code = Some(code.to_owned());
        assert_eq!(hint(&named, true), None, "a connector chose {code}");
    }
}

#[test]
fn a_connector_s_own_code_finds_its_hint_and_allows_the_path_may_be_wrong() {
    let mut named = report(Origin::Destination, ErrorKind::Config, None);
    named.connector_code = Some("database_not_found".to_owned());
    let said = hint(&named, false).expect("a hint for the database's failure to open");
    assert!(said.starts_with("the path may be wrong"), "{said}");
    named.connector_code = Some("any_other".to_owned());
    assert_eq!(hint(&named, false), None);
}
