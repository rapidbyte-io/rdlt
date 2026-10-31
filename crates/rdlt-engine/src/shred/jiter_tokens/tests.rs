use proptest::prelude::*;

use crate::shred::meter::{Columns, Meter};
use crate::shred::observe::Observed;
use crate::shred::observing::Look;
use crate::shred::tests::written;
use crate::shred::visit::Context;

/// What the JSON value `text` is observed as, over jiter where `over_jiter` and over the
/// engine's reader otherwise, as a record's value is; `None` where it is refused.
fn observed(text: &str, over_jiter: bool) -> Option<Observed> {
    let context = Context::new(Meter::reserved(), Columns::new(u64::MAX));
    let mut node = Observed::Null;
    let look = Look {
        node: &mut node,
        context: &context,
        depth: 1,
        target: None,
    };
    let walked = if over_jiter {
        super::visit(text.as_bytes(), look, &context).is_ok()
    } else {
        crate::shred::tokens::visit(text, look, &context).is_ok()
    };
    walked.then_some(node)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(1024)))]

    #[test]
    fn a_value_walked_over_jiter_is_observed_as_the_reader_walks_it(value in written()) {
        prop_assert_eq!(
            observed(&value.text, true),
            observed(&value.text, false),
            "{}",
            value.text
        );
    }
}

#[test]
fn numbers_nesting_and_refusals_at_their_edges_are_walked_as_the_reader_walks_them() {
    let wide = format!("1{}", "0".repeat(39));
    let vast = format!("-{}", "9".repeat(80));
    let deepest = format!("{}{}", "[".repeat(63), "]".repeat(63));
    let too_deep = format!("{}{}", "[".repeat(64), "]".repeat(64));
    let texts = [
        "-0",
        "-0.0",
        "1E2",
        "0.12345678901234567891",
        "1e400",
        "-1e400",
        "1e123456789012345678",
        "1e1234567890123456789",
        "1e0001234567890123456789",
        "18446744073709551615",
        "18446744073709551616",
        "-9223372036854775809",
        wide.as_str(),
        vast.as_str(),
        deepest.as_str(),
        too_deep.as_str(),
        "",
        "1 2",
        "[1,",
        r#"{"a"}"#,
        r#"{"a":1,}"#,
        "01",
        "1.",
        "-",
        "nul",
        "NaN",
        "Infinity",
        r#"{"a":1,"a":2}"#,
        r#"{"a":1,"a":2}"#,
        r#""\ud800""#,
        "\"\u{1}\"",
    ];
    for number in texts {
        for text in [number.to_owned(), format!("{{\"n\":[{number}]}}")] {
            assert_eq!(observed(&text, true), observed(&text, false), "{text}");
        }
    }
}
