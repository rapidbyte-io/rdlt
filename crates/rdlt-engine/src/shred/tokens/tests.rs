use std::sync::Arc;

use proptest::prelude::*;
use serde::de::DeserializeSeed as _;

use crate::shred::meter::{Columns, Meter};
use crate::shred::numbers::{Model, number};
use crate::shred::observe::{Observed, Shape};
use crate::shred::observing::Look;
use crate::shred::tests::written;
use crate::shred::visit::Context;

/// What the JSON value `text` is observed as, walked a token at a time as a record's value is;
/// `None` where it is refused.
fn walked(text: &str) -> Option<Observed> {
    let context = Context::new(Meter::reserved(), Columns::new(u64::MAX));
    let mut node = Observed::Null;
    let look = Look {
        node: &mut node,
        context: &context,
        depth: 1,
        target: None,
    };
    super::visit(text, look, &context).is_ok().then_some(node)
}

/// What the fast parse, sonic-rs streaming the value into the same seed, observes the JSON value
/// `text` as, and whether it is sure of every number it read, rather than asking for an exact
/// parse; `None` where it refuses the value.
fn fast(text: &str) -> Option<(Observed, bool)> {
    let context = Context::new(Meter::reserved(), Columns::new(u64::MAX));
    let mut node = Observed::Null;
    let look = Look {
        node: &mut node,
        context: &context,
        depth: 1,
        target: None,
    };
    let mut deserializer = sonic_rs::Deserializer::from_slice(text.as_bytes());
    let parsed = look
        .deserialize(&mut deserializer)
        .and_then(|()| deserializer.end());
    parsed.ok()?;
    context.confirm(text.as_bytes());
    Some((node, !context.imprecise()))
}

/// What a number its text reads as `model` is observed as; `None` where it is refused.
fn reads(model: &Model) -> Option<Observed> {
    match model {
        Model::Int(integer) => Some(Observed::Int {
            exact: integer.unsigned_abs() <= 1 << 53,
        }),
        Model::Wide(_) => Some(Observed::Wide),
        Model::Huge(_) => Some(Observed::Huge),
        Model::Vast(_) => Some(Observed::Vast),
        Model::Beyond => Some(Observed::Json),
        Model::Float(_) => Some(Observed::Float),
        Model::Refused(_) => None,
    }
}

/// What a record holding `item` alone in an array under the key `n` is observed as.
fn listed(item: Observed) -> Observed {
    let mut shape = Shape::default();
    shape.push(Arc::from("n"), Observed::Array(Box::new(item), 1));
    Observed::Object(shape)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(1024)))]

    #[test]
    fn a_value_walked_a_token_at_a_time_is_observed_as_the_fast_parse_observes_it(
        value in written(),
    ) {
        match fast(&value.text) {
            // A number the fast parse may have misread is the next test's to hold.
            Some((_, false)) => {}
            Some((observed, true)) => {
                prop_assert_eq!(walked(&value.text), Some(observed), "{}", value.text);
            }
            None => prop_assert_eq!(walked(&value.text), None, "{}", value.text),
        }
    }

    #[test]
    fn a_number_walked_a_token_at_a_time_is_observed_as_its_text_reads(number in number()) {
        let read = reads(&number.model);
        prop_assert_eq!(walked(&number.text), read.clone(), "{}", number.text);
        let record = format!("{{\"n\":[{}]}}", number.text);
        prop_assert_eq!(walked(&record), read.map(listed), "{}", record);
    }
}

#[test]
fn numbers_at_their_edges_are_observed_as_their_text_reads() {
    let vast = format!("1{}", "0".repeat(39));
    let beyond = format!("-{}", "9".repeat(80));
    let numbers = [
        ("-0", Some(Observed::Float)),
        ("-0.0", Some(Observed::Float)),
        ("1E2", Some(Observed::Float)),
        ("0.12345678901234567891", Some(Observed::Float)),
        ("1e400", None),
        ("-1e400", None),
        ("1e1234567890123456789", None),
        ("9007199254740992", Some(Observed::Int { exact: true })),
        ("9007199254740993", Some(Observed::Int { exact: false })),
        ("18446744073709551615", Some(Observed::Wide)),
        ("18446744073709551616", Some(Observed::Huge)),
        ("-9223372036854775809", Some(Observed::Huge)),
        (vast.as_str(), Some(Observed::Vast)),
        (beyond.as_str(), Some(Observed::Json)),
    ];
    for (number, read) in numbers {
        assert_eq!(walked(number), read, "{number}");
        let record = format!("{{\"n\":[{number}]}}");
        assert_eq!(walked(&record), read.map(listed), "{record}");
    }
}

#[test]
fn text_that_is_not_one_value_is_refused_by_the_walk_and_the_fast_parse() {
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
        r#"{"a":1,"a":2}"#,
    ];
    for text in refused {
        assert_eq!(walked(text), None, "{text}");
        assert_eq!(fast(text), None, "{text}");
    }
}
