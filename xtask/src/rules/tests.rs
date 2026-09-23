use std::path::Path;

use super::{FileRole, Rule, Severity, check};
use crate::lexer::scan;

const PRODUCTION: FileRole = FileRole {
    test: false,
    mod_rs: false,
};

fn rules(source: &str) -> Vec<Rule> {
    check(PRODUCTION, &scan(source))
        .into_iter()
        .map(|f| f.rule)
        .collect()
}

#[test]
fn each_rule_fires_on_its_violation() {
    let cases: &[(&str, Rule)] = &[
        ("// fixed in Round-3\n", Rule::TrackerId),
        ("// since 044 this is lazy\n", Rule::TrackerId),
        ("// see specs/foo.md\n", Rule::TrackerId),
        ("// the seat for this check\n", Rule::Jargon),
        ("// pinned by the test below\n", Rule::Jargon),
        ("// we honestly retry\n", Rule::Jargon),
        ("// this MUST happen first\n", Rule::Shouting),
        ("// TODO: later\n", Rule::TodoWithoutIssue),
        ("// FIXME(#1) broken\n", Rule::TodoWithoutIssue),
        ("// a\n// b\n// c\n// d\n", Rule::LongLineComment),
        ("/// Does a. Then b.\nfn f() {}\n", Rule::LongDocSummary),
        (
            "fn f() -> Result<u8, String> { g() }\n",
            Rule::StringlyError,
        ),
        (
            "fn f() -> Result<Vec<u8>, &'static str> { g() }\n",
            Rule::StringlyError,
        ),
        ("tokio::select! { _ = a => {} }\n", Rule::UnbiasedSelect),
    ];
    for (source, rule) in cases {
        assert_eq!(rules(source), vec![*rule], "source: {source:?}");
    }
}

#[test]
fn allowed_forms_pass() {
    let clean = [
        "// SAFETY: the buffer outlives the view\n",
        "// TODO(#12): widen once upstream lands\n",
        "// `Pin<T>` keeps the future in place\n",
        "// Holds the JSON and HTTP state for R2 storage\n",
        "// a\n// b\n// c\n\n// d\n",
        "/// Does a, e.g. b.\n///\n/// Second paragraph. Many sentences.\nfn f() {}\n",
        "/// Returns `a.b()`.\nfn f() {}\n",
        "fn f() -> Result<(), Error<String>> { g() }\n",
        "fn f() -> Result<String, Error> { g() }\n",
        "tokio::select! {\n    biased;\n    _ = a => {}\n}\n",
        "let s = \"select! { x }\";\n",
        "let warehouse = 1; // warehouse writer\n",
    ];
    for source in clean {
        assert_eq!(rules(source), Vec::<Rule>::new(), "source: {source:?}");
    }
}

#[test]
fn test_files_may_use_string_errors_and_grow_long() {
    let role = FileRole {
        test: true,
        mod_rs: false,
    };
    let long = "fn f() -> Result<u8, String> { g() }\n".repeat(500);
    assert!(check(role, &scan(&long)).is_empty());
}

#[test]
fn file_roles_come_from_the_path() {
    assert!(FileRole::of(Path::new("crates/a/src/x/mod.rs")).mod_rs);
    assert!(FileRole::of(Path::new("crates/a/src/x/tests.rs")).test);
    assert!(FileRole::of(Path::new("crates/a/tests/it/main.rs")).test);
    assert!(!FileRole::of(Path::new("crates/a/src/lib.rs")).test);
}

#[test]
fn heavy_commenting_and_long_files_only_warn() {
    let noisy = "// c\nfn a() {}\n".repeat(30);
    assert!(rules(&noisy).contains(&Rule::CommentRatio));
    assert!(rules(&"fn a() {}\n".repeat(401)).contains(&Rule::FileLength));
    assert_eq!(Rule::CommentRatio.severity(), Severity::Warning);
    assert_eq!(Rule::FileLength.severity(), Severity::Warning);
    assert_eq!(Rule::Jargon.severity(), Severity::Error);
}
