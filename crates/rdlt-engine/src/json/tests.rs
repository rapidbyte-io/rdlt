use std::borrow::Cow;

use proptest::prelude::*;
use rdlt_connector::limits::MAX_NESTING_DEPTH;

use super::number::PLAIN_BYTES;

use super::{
    EXPONENT_DIGITS, JsonError, Reader, Token, canonical_float, canonical_float32,
    canonical_number, check,
};

fn tokens(text: &str) -> Result<Vec<Token<'_>>, JsonError> {
    let mut reader = Reader::new(text);
    let mut tokens = Vec::new();
    while let Some(token) = reader.next()? {
        tokens.push(token);
    }
    Ok(tokens)
}

fn nested(depth: usize) -> String {
    format!("{}{}", "[".repeat(depth), "]".repeat(depth))
}

#[test]
fn a_value_reads_as_its_tokens_in_order() {
    use Token as T;
    let text = r#" {"a": [1, -2.5e3, "x\n\u00e9\ud83d\ude00"], "": {}, "b": [true, false, null]} "#;
    assert_eq!(
        tokens(text).unwrap(),
        vec![
            T::BeginObject,
            T::Key(Cow::Borrowed("a")),
            T::BeginArray,
            T::Number("1"),
            T::Number("-2.5e3"),
            T::String(Cow::Owned("x\né😀".to_owned())),
            T::EndArray,
            T::Key(Cow::Borrowed("")),
            T::BeginObject,
            T::EndObject,
            T::Key(Cow::Borrowed("b")),
            T::BeginArray,
            T::Bool(true),
            T::Bool(false),
            T::Null,
            T::EndArray,
            T::EndObject,
        ]
    );
    assert_eq!(tokens("[]").unwrap(), vec![T::BeginArray, T::EndArray]);
    assert_eq!(tokens(" 0 ").unwrap(), vec![T::Number("0")]);
    assert_eq!(
        tokens(r#""\"\\\/\b\f\r\t""#).unwrap(),
        vec![T::String(Cow::Owned("\"\\/\u{8}\u{c}\r\t".to_owned()))]
    );
}

#[test]
fn text_that_is_not_one_json_value_is_refused() {
    let refused = [
        "",
        " ",
        "[",
        "]",
        "{",
        "}",
        "[1,]",
        "[,1]",
        "[1 2]",
        "{\"a\"}",
        "{\"a\":}",
        "{\"a\":1,}",
        "{\"a\" 1}",
        "{1:1}",
        "{\"a\":1]",
        "[1}",
        "1 2",
        "[] []",
        "tr",
        "nul",
        "falsey",
        "01",
        "-",
        "1.",
        ".5",
        "1e",
        "1e+",
        "+1",
        "0x1",
        "\"abc",
        "\"a\u{1}b\"",
        "\"\\x\"",
        "\"\\u12\"",
        "\"\\u12g4\"",
        "\"\\ud800\"",
        "\"\\ud800\\u0041\"",
        "\"\\udc00\"",
        "'a'",
        "NaN",
        "Infinity",
    ];
    for text in refused {
        assert!(
            matches!(check(text), Err(JsonError::Invalid(_))),
            "{text:?} is refused as not JSON"
        );
    }
}

#[test]
fn nesting_is_read_to_the_limit_and_refused_one_past_it_on_a_small_stack() {
    let limit = usize::try_from(MAX_NESTING_DEPTH).unwrap();
    let reading = std::thread::Builder::new()
        .stack_size(64 << 10)
        .spawn(move || {
            (
                check(&nested(limit)),
                check(&nested(limit + 1)),
                check(&format!(
                    "{}1{}",
                    "{\"a\":".repeat(limit + 1),
                    "}".repeat(limit + 1)
                )),
                check(&nested(1_000_000)),
            )
        })
        .unwrap();
    let (at, past, objects, deep) = reading.join().unwrap();
    assert_eq!(at, Ok(()));
    assert_eq!(past, Err(JsonError::TooDeep));
    assert_eq!(objects, Err(JsonError::TooDeep));
    assert_eq!(deep, Err(JsonError::TooDeep));
}

#[test]
fn an_exponent_is_read_to_its_digit_limit_beside_its_leading_zeros() {
    let digits = "9".repeat(EXPONENT_DIGITS);
    assert_eq!(check(&format!("1e{digits}")), Ok(()));
    assert_eq!(check(&format!("1e-000{digits}")), Ok(()));
    assert_eq!(check(&format!("1e1{digits}")), Err(JsonError::Exponent));
    assert_eq!(JsonError::Exponent.code(), "limit_exceeded");
    assert_eq!(JsonError::TooDeep.code(), "limit_exceeded");
    assert_eq!(JsonError::Invalid("x").code(), "json_invalid");
}

#[test]
fn numbers_of_one_value_share_one_canonical_text() {
    let cases = [
        ("1", "1"),
        ("1.0", "1"),
        ("10e-1", "1"),
        ("0.1e1", "1"),
        ("1E0", "1"),
        ("1e+0", "1"),
        ("-0", "0"),
        ("-0.000e5", "0"),
        ("0", "0"),
        ("1e2", "100"),
        ("100", "100"),
        ("1.5", "1.5"),
        ("-12.50", "-12.5"),
        ("0.001", "0.001"),
        ("1e-3", "0.001"),
        ("12.34e1", "123.4"),
        ("12.34e-5", "0.0001234"),
        ("18446744073709551616", "18446744073709551616"),
        ("18446744073709551617", "18446744073709551617"),
        ("0.12345678901234567891", "0.12345678901234567891"),
        ("1e400", "1e400"),
        ("-1.25e401", "-1.25e401"),
        ("125e-500", "1.25e-498"),
    ];
    for (written, canonical) in cases {
        assert_eq!(canonical_number(written).unwrap(), canonical, "{written}");
    }
    assert!(canonical_number("1 ").is_err());
    assert!(canonical_number("[1]").is_err());
    assert!(canonical_number(&format!("1e1{}", "0".repeat(EXPONENT_DIGITS))).is_err());
}

#[test]
fn a_float_ties_to_its_even_shortest_text_and_a_float_json_lacks_keeps_its_name() {
    // Both -833696597088206.2 and -833696597088206.3 read back as this float.
    let tie = f64::from_bits(14_053_396_810_545_282_674);
    assert_eq!(canonical_float(tie), "-833696597088206.2");
    assert_eq!(canonical_float(-0.0), "0");
    assert_eq!(canonical_float(1e21), "1000000000000000000000");
    assert_eq!(canonical_float(2.5e-7), "0.00000025");
    let names = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY].map(canonical_float);
    assert_eq!(names, ["NaN", "inf", "-inf"]);
    let names = [f32::NAN, f32::INFINITY, f32::NEG_INFINITY].map(canonical_float32);
    assert_eq!(names, ["NaN", "inf", "-inf"]);
    assert_eq!(canonical_float32(0.1), "0.1");
}

#[test]
fn a_canonical_text_is_plain_to_its_length_limit_and_scientific_past_it() {
    let limit = i64::try_from(PLAIN_BYTES).unwrap();
    // Twelve digits and a place that makes the plain text exactly the limit, then one more.
    let at = canonical_number(&format!("123456789012e{}", limit - 12)).unwrap();
    assert_eq!(at.len(), PLAIN_BYTES);
    assert!(!at.contains('e'));
    let past = canonical_number(&format!("123456789012e{}", limit - 11)).unwrap();
    assert_eq!(past, format!("1.23456789012e{limit}"));
    // A fraction: `0.` and its zeros before the digits.
    let small = canonical_number(&format!("12e-{}", limit - 2)).unwrap();
    assert_eq!(small.len(), PLAIN_BYTES);
    let smaller = canonical_number(&format!("12e-{}", limit - 1)).unwrap();
    assert_eq!(smaller, format!("1.2e-{}", limit - 2));
    // Digits around a point: the point and every digit.
    let long = "7".repeat(PLAIN_BYTES - 1);
    assert_eq!(
        canonical_number(&format!("{long}e-1")).unwrap().len(),
        PLAIN_BYTES
    );
    let longer = "7".repeat(PLAIN_BYTES);
    assert!(
        canonical_number(&format!("{longer}e-1"))
            .unwrap()
            .contains('e')
    );
    assert_eq!(
        canonical_number(&"9".repeat(PLAIN_BYTES + 1)).unwrap(),
        format!("9.{}e{}", "9".repeat(PLAIN_BYTES), PLAIN_BYTES)
    );
}

/// JSON-ish text: values of every kind nested eight deep, numbers of 25 digits and exponents of
/// three, every whitespace byte and the bytes JSON refuses as whitespace, strings of escapes,
/// surrogate pairs and lone halves, raw control characters and text beyond ASCII, and stray
/// bytes.
fn jsonish() -> impl Strategy<Value = String> {
    let space = "[ \\t\\n\\r\\x0b\\x0c]{0,2}";
    let leaf = prop_oneof![
        Just("null".to_owned()),
        Just("true".to_owned()),
        Just("false".to_owned()),
        Just("fa".to_owned()),
        Just("-0".to_owned()),
        "-?0?[0-9]{1,25}(\\.[0-9]{0,25})?([eE][+-]?[0-9]{0,3})?",
        "\"([a-zé😀\\x00-\\x1f\\x7f \\\\\"/]|\\\\[nturbf\"\\\\/x0]|\\\\u[dD][89abAB][0-9a-fA-F]{2}(\\\\u[dD][c-fC-F][0-9a-fA-F]{2})?|\\\\u[dD][c-fC-F][0-9a-fA-F]{2}|\\\\u[0-9a-fA-F]{2,4})*\"?",
    ];
    let leaf =
        (space, leaf, space).prop_map(|(before, leaf, after)| format!("{before}{leaf}{after}"));
    leaf.prop_recursive(8, 64, 5, move |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..5)
                .prop_map(|items| format!("[{}]", items.join(","))),
            prop::collection::vec(("[ \\t\\n]?\"[a-c\\\\]{0,2}\"[ \\t\\n]?", inner), 0..4)
                .prop_map(|members| {
                    let members: Vec<String> = members
                        .into_iter()
                        .map(|(key, value)| format!("{key}:{value}"))
                        .collect();
                    format!("{{{}}}", members.join(","))
                }),
            ("[ ,:\\]\\}\\[\\{\\t\\n]{0,2}", Just(String::new())).prop_map(|(stray, _)| stray),
        ]
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(4096)))]

    #[test]
    fn text_is_json_exactly_where_serde_json_reads_it(text in jsonish()) {
        let ours = check(&text);
        let theirs = serde_json::from_str::<serde_json::Value>(&text);
        // serde_json refuses a number beyond a float's range, which JSON holds.
        if let Err(error) = &theirs {
            prop_assume!(!error.to_string().contains("out of range"));
        }
        prop_assert_eq!(ours.is_ok(), theirs.is_ok(), "{:?}: {:?}", text, ours);
    }

    #[test]
    fn a_float_has_the_canonical_text_of_its_shortest_json_text(bits in any::<u64>()) {
        let float = f64::from_bits(bits);
        prop_assume!(float.is_normal() || float.is_subnormal());
        let canonical = canonical_float(float);
        prop_assert_eq!(canonical.parse::<f64>().unwrap().to_bits(), bits);
        prop_assert_eq!(
            &canonical,
            &canonical_number(&serde_json::to_string(&float).unwrap()).unwrap()
        );
        // Written by another writer with the same digits, plain or not, it reads alike.
        prop_assert_eq!(
            canonical_number(&format!("{float:e}")).unwrap(),
            canonical_number(&format!("{float}")).unwrap()
        );
        let single = u32::try_from(bits >> 32).unwrap();
        let float = f32::from_bits(single);
        prop_assume!(float.is_normal() || float.is_subnormal());
        prop_assert_eq!(canonical_float32(float).parse::<f32>().unwrap().to_bits(), single);
    }

    #[test]
    fn numbers_of_distinct_values_have_distinct_canonical_texts(
        left in "-?(0|[1-9][0-9]{0,2})(\\.[0-9]{1,3})?(e-?[0-9]{1,3})?",
        right in "-?(0|[1-9][0-9]{0,2})(\\.[0-9]{1,3})?(e-?[0-9]{1,3})?",
    ) {
        let same = canonical_number(&left).unwrap() == canonical_number(&right).unwrap();
        prop_assert_eq!(same, exact(&left) == exact(&right));
    }
}

/// The value of the JSON number `written` as digits and a power of ten, by a route of its own.
fn exact(written: &str) -> (bool, String, i64) {
    let negative = written.starts_with('-');
    let unsigned = written.trim_start_matches('-');
    let (mantissa, exponent) = unsigned.split_once('e').unwrap_or((unsigned, "0"));
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let mut digits: Vec<u8> = format!("{whole}{fraction}").into_bytes();
    let mut place = exponent.parse::<i64>().unwrap() - i64::try_from(fraction.len()).unwrap();
    while digits.last() == Some(&b'0') {
        digits.pop();
        place += 1;
    }
    let digits = String::from_utf8(digits).unwrap();
    let digits = digits.trim_start_matches('0').to_owned();
    if digits.is_empty() {
        return (false, String::new(), 0);
    }
    (negative, digits, place)
}

#[test]
fn an_exponent_is_within_the_limit_by_its_digits_beside_its_leading_zeros() {
    use super::{exponent_within, may_hold_long_exponent};
    let digits = "9".repeat(EXPONENT_DIGITS);
    for (written, within) in [
        (format!("1e{digits}"), true),
        (format!("1e-{digits}"), true),
        (format!("1.5E+000{digits}"), true),
        (format!("1e1{digits}"), false),
        (format!("0e-1{digits}"), false),
        ("12.5".to_owned(), true),
    ] {
        assert_eq!(exponent_within(&written), within, "{written}");
        assert_eq!(
            may_hold_long_exponent(written.as_bytes()),
            !within,
            "{written}"
        );
    }
    // Text that only reads like one, a letter before its exponent, is no number.
    assert!(!may_hold_long_exponent(
        format!("\"xe1{digits}\"").as_bytes()
    ));
    assert!(may_hold_long_exponent(
        format!("\"x1e1{digits}\"").as_bytes()
    ));
}

#[test]
fn objects_and_arrays_nested_to_the_limit_are_read_as_serde_json_reads_them() {
    // Sixty-four levels, objects and arrays in turn, with whitespace between their tokens.
    let open: String = (0..64)
        .map(|level| if level % 2 == 0 { "[ " } else { "{\"k\":\t" })
        .collect();
    let close: String = (0..64)
        .rev()
        .map(|level| if level % 2 == 0 { "]" } else { "\n}" })
        .collect();
    let text = format!("{open}1{close}");
    assert!(serde_json::from_str::<serde_json::Value>(&text).is_ok());
    assert_eq!(check(&text), Ok(()));
    assert_eq!(check(&format!("[{text}]")), Err(JsonError::TooDeep));
}

#[test]
fn a_vast_number_s_power_counts_from_its_first_significant_digit() {
    // Leading zeros before its digits: 0.00125e500 is 1.25 × 10⁴⁹⁷.
    assert_eq!(canonical_number("0.00125e500").unwrap(), "1.25e497");
    // Text that opens with an exponent's letter is no number.
    let digits = "1".repeat(EXPONENT_DIGITS + 1);
    assert!(!super::may_hold_long_exponent(
        format!("e{digits}").as_bytes()
    ));
}
