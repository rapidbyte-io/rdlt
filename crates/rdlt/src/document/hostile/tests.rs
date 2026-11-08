use crate::Document;
use crate::limits::{DOCUMENT_BYTES, DOCUMENT_DEPTH, DOCUMENT_NODES};

fn code(text: &str) -> Option<&'static str> {
    Document::parse(text)
        .err()
        .and_then(|error| error.code())
        .map(rdlt_engine::Code::as_str)
}

/// The head of a document, naming its connectors and no stream.
fn head() -> &'static str {
    "version: 1\npipeline: p\nsource: { connector: io.example.a }\n\
     destination: { connector: io.example.b }\n"
}

/// A list of `items` `0` scalars, a node for each and one for the list.
fn scalars(items: usize) -> String {
    format!("[{}]", "0,".repeat(items))
}

/// A list of `levels` of lists around a `1`.
fn nested(levels: usize) -> String {
    format!("{}1{}", "[".repeat(levels), "]".repeat(levels))
}

#[test]
fn a_document_within_its_limits_reads_in_yaml_and_in_json() {
    let yaml = format!("{}streams: [{{ name: s }}]\n", head());
    let json = r#"{"version":1,"pipeline":"p","source":{"connector":"io.example.a"},"destination":{"connector":"io.example.b"},"streams":[{"name":"s"}]}"#;
    assert_eq!(
        Document::parse(&yaml).unwrap(),
        Document::parse(json).unwrap()
    );
}

#[test]
fn each_expansion_and_every_tag_is_refused_at_its_place() {
    let base = head();
    for (text, place) in [
        (format!("{base}streams: [&s {{ name: s }}]\n"), "5:11:"),
        (
            format!("{base}x: &a 1\nstreams: [{{ name: *a }}]\n"),
            "5:4:",
        ),
        (format!("{base}streams: [{{ name: *a }}]\n"), "5:19:"),
        (format!("{base}streams: [!!map {{ name: s }}]\n"), "5:17:"),
        (format!("{base}streams: [{{ name: !!str s }}]\n"), "5:25:"),
        (format!("{base}streams: [{{ name: ! s }}]\n"), "5:21:"),
        (
            format!("{base}streams: [{{ name: s }}]\n---\n{base}streams: []\n"),
            "6:1:",
        ),
    ] {
        let error = Document::parse(&text).expect_err(&text);
        assert_eq!(
            error.code().map(rdlt_engine::Code::as_str),
            Some("document_refused"),
            "{text}"
        );
        assert!(error.to_string().starts_with(place), "{error} for {text}");
    }
    let second = format!("{base}streams: []\n---\n{base}streams: []\n");
    let error = Document::parse(&second).unwrap_err();
    assert!(error.to_string().contains("a second document"), "{error}");
}

#[test]
fn a_document_past_its_limits_is_refused_and_one_at_them_reads() {
    let large = format!("# {}\n", "x".repeat(DOCUMENT_BYTES));
    assert_eq!(code(&large), Some("document_too_large"));

    let text = format!("{}streams: []\n", head());
    let padded = |bytes: usize| format!("{text}# {}\n", "x".repeat(bytes - text.len() - 3));
    assert_eq!(padded(DOCUMENT_BYTES).len(), DOCUMENT_BYTES);
    assert_eq!(code(&padded(DOCUMENT_BYTES)), None);
    assert_eq!(
        code(&padded(DOCUMENT_BYTES + 1)),
        Some("document_too_large")
    );

    let sides = "version: 1\npipeline: p\ndestination: { connector: io.example.b }\nstreams: []\n";
    let configured =
        |value: &str| format!("source: {{ connector: io.example.a, config: {value} }}\n{sides}");
    let levels = DOCUMENT_DEPTH - 2;
    assert_eq!(code(&configured(&nested(levels))), None);
    let deep = Document::parse(&configured(&nested(levels + 1))).unwrap_err();
    assert_eq!(
        deep.code().map(rdlt_engine::Code::as_str),
        Some("document_too_deep")
    );
    assert!(deep.to_string().starts_with("1:"), "{deep}");

    let many = DOCUMENT_NODES - 17;
    assert_eq!(code(&configured(&scalars(many))), None);
    assert_eq!(
        code(&configured(&scalars(many + 1))),
        Some("document_too_deep")
    );
    let deep_mapping = format!("{}1{}", "{ a: ".repeat(40), " }".repeat(40));
    assert_eq!(code(&configured(&deep_mapping)), Some("document_too_deep"));
}

#[test]
fn a_nest_of_collections_past_what_the_parser_scans_is_refused_as_too_deep() {
    for nest in ["[".repeat(300), "{a: ".repeat(300)] {
        let text = format!("version: 1\nsource: {{ connector: a.b, config: {nest}\n");
        assert_eq!(code(&text), Some("document_too_deep"), "{}", &nest[..3]);
    }
}

#[test]
fn a_whole_document_that_is_a_scalar_is_refused_without_quoting_it() {
    for text in ["CANARY", "\"CANARY\"", "31337", "3.25", "true", "'CANARY'"] {
        let error = Document::parse(text).expect_err(text);
        let said = format!("{error}");
        assert!(!said.contains("CANARY") && !said.contains(text), "{said}");
    }
}
