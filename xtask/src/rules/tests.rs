use std::path::Path;

use super::{FileRole, Rule, Severity, UnsafeCode, check};
use crate::lexer::scan;

const PRODUCTION: FileRole = FileRole {
    test: false,
    mod_rs: false,
    unsafe_code: UnsafeCode::Other,
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
        ("fn f() { unsafe { g() } }\n", Rule::Unsafe),
        ("unsafe fn f() {}\n", Rule::Unsafe),
        (
            "fn f() -> Result<\n    (),\n    String,\n> {\n    g()\n}\n",
            Rule::StringlyError,
        ),
        (
            "fn f() -> Result<(), std::string::String> { g() }\n",
            Rule::StringlyError,
        ),
    ];
    for (source, rule) in cases {
        assert_eq!(rules(source), vec![*rule], "source: {source:?}");
    }
}

#[test]
fn an_unclosed_fence_does_not_hide_later_items_from_the_word_checks() {
    let source = "/// Example.\n///\n/// ```\nfn f() {}\n\n/// Holds the seat.\nfn g() {}\n";
    assert_eq!(rules(source), vec![Rule::Jargon]);
}

#[test]
fn allowed_forms_pass() {
    let clean = [
        "/// Example.\n///\n/// ```\n/// const VERSION: &str = \"the one\";\n/// ```\nfn f() {}\n",
        "// SAFETY: the buffer outlives the view\n",
        "// TODO(#12): widen once upstream lands\n",
        "// `Pin<T>` keeps the future in place\n",
        "// Holds the JSON and HTTP state for R2 storage\n",
        "// a\n// b\n// c\n\n// d\n",
        "/// Does a, e.g. b.\n///\n/// Second paragraph. Many sentences.\nfn f() {}\n",
        "/// Returns `a.b()`.\nfn f() {}\n",
        "fn f() -> Result<(), Error<String>> { g() }\n",
        "fn f() -> Result<String, Error> { g() }\n",
        "fn f() -> Result<BTreeMap<String, String>, Error> { g() }\n",
        "fn f(r: Result<u8, E>, m: BTreeMap<u8, String>) {}\n",
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
        ..PRODUCTION
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
fn unsafe_code_lives_only_in_the_audited_module_and_every_other_crate_forbids_it() {
    let audited = FileRole {
        unsafe_code: UnsafeCode::Audited,
        ..PRODUCTION
    };
    let unsafe_block = scan("fn f() { unsafe { g() } }\n");
    assert!(check(audited, &unsafe_block).is_empty());
    let root = FileRole {
        unsafe_code: UnsafeCode::CrateRoot,
        ..PRODUCTION
    };
    let rules_of = |source| -> Vec<Rule> {
        check(root, &scan(source))
            .into_iter()
            .map(|f| f.rule)
            .collect()
    };
    assert_eq!(rules_of("//! A crate.\n"), vec![Rule::UnforbiddenUnsafe]);
    assert!(rules_of("//! A crate.\n\n#![forbid(unsafe_code)]\n").is_empty());
    // The keyword inside a comment, a string or an identifier is not code.
    assert!(rules("// unsafe\nconst S: &str = \"unsafe\";\nfn unsafe_code() {}\n").is_empty());
}

#[test]
fn the_audited_module_and_the_crate_roots_come_from_the_path() {
    let of = |path| FileRole::of(Path::new(path)).unsafe_code;
    let cases = [
        (
            "crates/rdlt-connector/src/serve/inherited.rs",
            UnsafeCode::Audited,
        ),
        ("crates/rdlt-connector/src/serve/read.rs", UnsafeCode::Other),
        ("crates/rdlt-engine/src/lib.rs", UnsafeCode::CrateRoot),
        ("xtask/src/main.rs", UnsafeCode::CrateRoot),
        // The crate holding the audited module denies unsafe code instead of forbidding it.
        ("crates/rdlt-connector/src/lib.rs", UnsafeCode::Other),
        ("crates/rdlt-engine/src/table/lib.rs", UnsafeCode::Other),
        ("crates/rdlt-engine/tests/it/main.rs", UnsafeCode::Other),
    ];
    for (path, expected) in cases {
        assert_eq!(of(path), expected, "{path}");
    }
}

#[test]
fn heavy_commenting_and_long_files_only_warn() {
    let noisy = "// c\nfn a() {}\n".repeat(30);
    assert!(rules(&noisy).contains(&Rule::CommentRatio));
    let documented = "/// Docs.\nfn a() {}\n".repeat(30);
    assert!(
        !rules(&documented).contains(&Rule::CommentRatio),
        "rustdoc is required, so it does not count"
    );
    assert!(rules(&"fn a() {}\n".repeat(401)).contains(&Rule::FileLength));
    assert_eq!(Rule::CommentRatio.severity(), Severity::Warning);
    assert_eq!(Rule::FileLength.severity(), Severity::Warning);
    assert_eq!(Rule::Jargon.severity(), Severity::Error);
}
