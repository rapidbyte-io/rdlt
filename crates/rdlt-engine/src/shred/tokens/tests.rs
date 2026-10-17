use proptest::prelude::*;

use crate::shred::meter::{Columns, Meter};
use crate::shred::observe::Observed;
use crate::shred::observing::Look;
use crate::shred::tests::written;
use crate::shred::visit::Context;

/// What the JSON value `text` is observed as, a token at a time where `by_tokens` and from its
/// parsed document otherwise, as a record's value is; `None` where it is refused.
fn observed(text: &str, by_tokens: bool) -> Option<Observed> {
    let context = Context::new(Meter::reserved(), Columns::new(u64::MAX));
    let mut node = Observed::Null;
    let look = Look {
        node: &mut node,
        context: &context,
        depth: 1,
        target: None,
    };
    let walked = if by_tokens {
        super::visit(text, look, &context).is_ok()
    } else {
        crate::shred::exact::visit(text.as_bytes(), look, &context).is_ok()
    };
    walked.then_some(node)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(1024)))]

    #[test]
    fn a_value_walked_a_token_at_a_time_is_observed_as_its_parsed_document_is(
        value in written(),
    ) {
        prop_assert_eq!(
            observed(&value.text, true),
            observed(&value.text, false),
            "{}",
            value.text
        );
    }
}

#[test]
fn numbers_and_text_at_their_edges_are_walked_as_the_parsed_document_walks_them() {
    let wide = format!("1{}", "0".repeat(39));
    let vast = format!("-{}", "9".repeat(80));
    let numbers = [
        "-0",
        "-0.0",
        "1E2",
        "0.12345678901234567891",
        "1e400",
        "-1e400",
        "1e1234567890123456789",
        "18446744073709551615",
        "18446744073709551616",
        "-9223372036854775809",
        wide.as_str(),
        vast.as_str(),
    ];
    for number in numbers {
        for text in [number.to_owned(), format!("{{\"n\":[{number}]}}")] {
            assert_eq!(observed(&text, true), observed(&text, false), "{text}");
        }
    }
    // Text that is not one value, and an object repeating a key however it is escaped.
    let refused = [
        "",
        "1 2",
        "[1,",
        r#"{"a"}"#,
        r#"{"a":1,}"#,
        "01",
        "nul",
        r#"{"a":1,"a":2}"#,
        r#"{"\u0061":1,"a":2}"#,
    ];
    for text in refused {
        assert_eq!(observed(text, true), None, "{text}");
        assert_eq!(observed(text, false), None, "{text}");
    }
}
