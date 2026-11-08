use std::fmt::Write as _;

use crate::Document;
use crate::document::tests::{misplaced, refusals};

const CANARY: &str = "CANARY-value-9f3";

fn head() -> String {
    "version: 1\npipeline: p\nsource: { connector: io.example.a }\n\
     destination: { connector: io.example.b }\n"
        .to_owned()
}

fn refused(text: &str) -> crate::Error {
    Document::parse(text).expect_err("refused")
}

fn code(error: &crate::Error) -> Option<&'static str> {
    error.code().map(rdlt_engine::Code::as_str)
}

/// The text of `error`, its `Debug`, its report, and each cause's text and `Debug`.
pub(crate) fn said(error: &crate::Error) -> String {
    let report = serde_json::to_string(&error.report()).expect("a report writes");
    let mut said = format!("{error} {error:?} {report}");
    let mut cause = std::error::Error::source(error);
    while let Some(held) = cause {
        write!(said, " {held} {held:?}").expect("text writes");
        cause = held.source();
    }
    said
}

/// Whether `said` holds `canary`, as text or as the bytes a buffer's `Debug` lists.
pub(crate) fn holds(said: &str, canary: &str) -> bool {
    let bytes: Vec<String> = canary.bytes().map(|byte| byte.to_string()).collect();
    said.contains(canary) || said.contains(&bytes.join(", "))
}

#[test]
fn errors_place_the_fault_name_its_key_and_suggest_a_near_key() {
    let cases = [
        (
            "unknown_key",
            format!("{}streams:\n  - name: s\n    wrte: append\n", head()),
        ),
        (
            "unknown_word",
            format!("{}streams:\n  - name: s\n    write: apend\n", head()),
        ),
        (
            "missing_key",
            "version: 1\npipeline: p\nsource: { connector: io.example.a }\nstreams: []\n"
                .to_owned(),
        ),
        (
            "wrong_kind",
            format!(
                "{}streams:\n  - name: s\nresources: {{ writers: many }}\n",
                head()
            ),
        ),
        (
            "bad_size",
            format!(
                "{}streams: []\nresources: {{ memory: 12 parsecs }}\n",
                head()
            ),
        ),
        ("syntax", format!("{}streams: [ {{ name: s\n", head())),
    ];
    for (name, text) in cases {
        insta::assert_snapshot!(name, refused(&text).to_string());
    }
}

/// Refusals of the model's every place, in YAML and in JSON, and of the reader's every limit.
#[test]
fn no_error_quotes_a_value() {
    let model = refusals().into_iter().chain(misplaced()).flat_map(|value| {
        let yaml = serde_saphyr::to_string(&value).expect("JSON writes as YAML");
        [value.to_string(), yaml]
    });
    let placed = [
        format!("{}streams:\n  - name: s\n    write: {CANARY}\n", head()),
        format!(
            "{}streams:\n  - name: s\n    read: {{ {CANARY}: 1 }}\n",
            head()
        ),
        format!("{}streams: []\nresources: {{ memory: {CANARY} }}\n", head()),
        format!("{}streams: []\ncommit: {{ every: {CANARY} }}\n", head()),
        format!("{}streams: []\nuntil: {CANARY}\n", head()),
        format!("version: {CANARY}\n"),
        format!("{}streams: [{CANARY}]\n", head()),
        CANARY.to_owned(),
        format!("\"{CANARY}\""),
        format!("{}streams: [&{CANARY} {{ name: s }}]\n", head()),
        format!("{}streams: [*{CANARY}]\n", head()),
        format!("{}streams: [!{CANARY} {{ name: s }}]\n", head()),
        format!("{}streams: [{{ name: !!str {CANARY} }}]\n", head()),
        format!("{}streams: [ {CANARY}\n", head()),
        format!("{}pipeline: {CANARY}\nstreams: []\n", head()),
        format!("{}streams: [{{ name: s }}]\n---\n{CANARY}: 1\n", head()),
        format!("{}streams: [{{ name: s, <<: {CANARY} }}]\n", head()),
        format!("{}streams: []\n# {}\n", head(), CANARY.repeat(200_000)),
    ];
    for text in model.chain(placed) {
        let said = said(&refused(&text));
        for canary in [CANARY, "CANARY", "31337424", "9876543"] {
            assert!(!holds(&said, canary), "{said} for {text}");
        }
    }
}

#[test]
fn a_key_given_twice_is_refused_where_it_repeats() {
    let text = format!("{}streams: []\nstreams: []\n", head());
    let error = refused(&text);
    assert_eq!(code(&error), Some("document_duplicate_key"));
    assert!(error.to_string().starts_with("6:"), "{error}");
}

#[test]
fn a_mapping_refuses_a_key_it_holds_however_it_is_written() {
    let side = "source: { connector: a.b, config: ";
    for (written, place) in [
        ("{ \"11\": 1, 11: 2 }", "4:"),
        ("{ true: 1, \"true\": 2 }", "4:"),
        ("{ a: { x: 1, \"x\": 2 } }", "4:"),
        ("{ b: { 0xB: 1, 11: 2 } }", "4:"),
    ] {
        let text = format!(
            "version: 1\npipeline: p\ndestination: {{ connector: a.b }}\n{side}{written} }}\nstreams: []\n"
        );
        let error = refused(&text);
        assert_eq!(code(&error), Some("document_duplicate_key"), "{text}");
        assert!(error.to_string().starts_with(place), "{error} for {text}");
    }
    let text = "version: 1\npipeline: p\ndestination: { connector: a.b }\nsource: { connector: a.b, config: { 11: 1, 12: 2, a: [1, 1], b: x, c: x, x: b } }\nstreams: []\n";
    assert!(Document::parse(text).is_ok());
}

#[test]
fn text_past_the_limit_once_unescaped_is_refused_as_too_large() {
    let escapes = "\\L".repeat(400_000);
    let text = format!(
        "{}streams: [{{ name: s }}]\nretry: {{ initial: \"{escapes}\" }}\n",
        head()
    );
    let error = refused(&text);
    assert_eq!(code(&error), Some("document_too_large"), "{error}");
}

#[test]
fn a_far_unknown_key_gets_no_suggestion() {
    let near = refused(&format!(
        "{}streams: []\nretyr: {{ max_attempts: 2 }}\n",
        head()
    ));
    assert_eq!(code(&near), Some("document_unknown_key"));
    assert!(near.to_string().contains("did you mean `retry`?"), "{near}");
    let far = refused(&format!("{}streams: []\nzebra: 1\n", head()));
    assert_eq!(code(&far), Some("document_unknown_key"));
    assert!(!far.to_string().contains("did you mean"), "{far}");
}

#[test]
fn keys_inside_config_are_never_checked() {
    let text = "version: 1\npipeline: p\nsource: { connector: io.example.a, config: { wrte: 1, retyr: 2 } }\ndestination: { connector: io.example.b }\nstreams: []\n";
    assert!(Document::parse(text).is_ok());
}

#[test]
fn a_bom_and_crlf_document_reports_the_right_place() {
    let text = format!("{}streams:\n  - name: s\n    wrte: append\n", head()).replace('\n', "\r\n");
    let error = refused(&format!("\u{feff}{text}"));
    assert!(error.to_string().starts_with("7:5:"), "{error}");
    let scanned = format!(
        "\u{feff}&a {}",
        format!("{}streams: []\n", head()).replace('\n', "\r\n")
    );
    let error = refused(&scanned);
    assert_eq!(code(&error), Some("document_refused"));
    assert!(error.to_string().starts_with("1:1:"), "{error}");
}

#[test]
fn a_merge_key_is_refused() {
    let error = refused(&format!("{}streams: [{{ <<: {{ name: s }} }}]\n", head()));
    assert_eq!(code(&error), Some("document_refused"));
}

#[test]
fn a_missing_key_and_a_wrong_kind_say_so_at_their_place() {
    for (text, expected, path) in [
        (
            "version: 1\npipeline: p\nsource: { connector: io.example.a }\nstreams: []\n"
                .to_owned(),
            "document_missing_key",
            "the key `destination` is missing",
        ),
        (
            format!("{}streams: [{{ name: 5 }}]\n", head()),
            "document_invalid",
            "streams[0].name: its value is of the wrong kind: expected text",
        ),
        (
            format!(
                "{}streams: []\nresources: {{ memory: 12 parsecs }}\n",
                head()
            ),
            "document_invalid",
            "resources.memory: its value is not one this place takes: expected a size such as 512MiB",
        ),
        (
            format!(
                "{}streams: [{{ name: s, write: {{ merge: {{ key: [a] }}, history: {{ key: [a] }} }} }}]\n",
                head()
            ),
            "document_invalid",
            "streams[0].write: it holds the wrong number of items",
        ),
    ] {
        let error = refused(&text);
        assert_eq!(code(&error), Some(expected), "{text}");
        assert!(error.to_string().contains(path), "{error} for {text}");
    }
}

#[test]
fn a_refused_word_is_named_by_its_choices_and_never_quoted() {
    let error = refused(&format!(
        "{}streams:\n  - name: s\n    write: apend\n",
        head()
    ));
    assert_eq!(code(&error), Some("document_invalid"));
    let said = error.to_string();
    assert!(said.contains("streams[0].write"), "{said}");
    assert!(said.contains("did you mean `append`?"), "{said}");
    assert!(!said.contains("apend"), "{said}");
}

#[test]
fn each_of_the_reader_s_refusals_carries_its_code() {
    use serde_saphyr::budget::BudgetBreach;
    use serde_saphyr::{Error as Read, Location};
    let unknown = Location::UNKNOWN;
    let budget = |breach| Read::Budget {
        breach,
        location: unknown,
    };
    for (error, expected) in [
        (
            budget(BudgetBreach::Depth { depth: 33 }),
            "document_too_deep",
        ),
        (
            budget(BudgetBreach::Nodes { nodes: 100_001 }),
            "document_too_deep",
        ),
        (
            budget(BudgetBreach::Anchors { anchors: 1 }),
            "document_refused",
        ),
        (
            Read::MultipleDocuments {
                hint: "",
                location: unknown,
            },
            "document_refused",
        ),
        (
            Read::MergeKeyNotAllowed { location: unknown },
            "document_refused",
        ),
        (Read::Eof { location: unknown }, "document_syntax"),
    ] {
        let fault = super::Fault::of(&error, None, "the document");
        assert_eq!(fault.code().as_str(), expected, "{error:?}");
    }
}
