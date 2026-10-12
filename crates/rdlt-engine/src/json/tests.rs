use std::borrow::Cow;

use proptest::prelude::*;
use rdlt_wire::limits::NESTING_DEPTH;
use sonic_rs::JsonValueTrait as _;

use super::number::PLAIN_BYTES;

use super::{
    EXPONENT_DIGITS, JsonError, Reader, Text, Token, canonical_float, canonical_number,
    write_float, write_number,
};

fn tokens(text: &str) -> Result<Vec<Token<'_>>, JsonError> {
    let mut reader = Reader::new(text);
    let mut tokens = Vec::new();
    while let Some(token) = reader.next()? {
        tokens.push(token);
    }
    Ok(tokens)
}

fn check(text: &str) -> Result<(), JsonError> {
    tokens(text).map(drop)
}

/// The string a reader reads from `written`, the text after its opening quote.
fn text(written: &str) -> Text<'_> {
    Text::read(written, 0).unwrap().0
}

/// The strings and keys of `text` in order, their escapes read.
fn strings(text: &str) -> Vec<String> {
    let strings = tokens(text)
        .unwrap()
        .into_iter()
        .filter_map(|token| match token {
            Token::String(text) | Token::Key(text) => Some(text.unescaped().into_owned()),
            _ => None,
        });
    strings.collect()
}

fn nested(depth: usize) -> String {
    format!("{}{}", "[".repeat(depth), "]".repeat(depth))
}

#[test]
fn a_value_reads_as_its_tokens_in_order() {
    use Token as T;
    let written =
        r#" {"a": [1, -2.5e3, "x\n\u00e9\ud83d\ude00"], "": {}, "b": [true, false, null]} "#;
    assert_eq!(
        tokens(written).unwrap(),
        vec![
            T::BeginObject,
            T::Key(text(r#"a""#)),
            T::BeginArray,
            T::Number("1"),
            T::Number("-2.5e3"),
            T::String(text(r#"x\n\u00e9\ud83d\ude00""#)),
            T::EndArray,
            T::Key(text(r#"""#)),
            T::BeginObject,
            T::EndObject,
            T::Key(text(r#"b""#)),
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
    assert_eq!(strings(written), ["a", "x\né😀", "", "b"]);
    assert_eq!(strings(r#""\"\\\/\b\f\r\t""#), ["\"\\/\u{8}\u{c}\r\t"]);
}

#[test]
fn a_string_without_escapes_is_borrowed_and_one_with_them_is_built() {
    assert!(matches!(text(r#"a é""#).unescaped(), Cow::Borrowed("a é")));
    assert!(matches!(text(r#"a\u0020""#).unescaped(), Cow::Owned(owned) if owned == "a "));
}

#[test]
fn a_reset_reader_reads_its_new_text_as_a_new_reader_would() {
    let mut reader = Reader::new(r#"[{"a": "bc", "d": ["#);
    for _ in 0..6 {
        reader.next().unwrap();
    }
    reader.reset("]");
    assert_eq!(reader.next(), Err(JsonError::Invalid("a value is due")));
    reader.reset(r#"{"x"#);
    assert_eq!(reader.next(), Ok(Some(Token::BeginObject)));
    assert!(reader.next().is_err());
    reader.reset("1");
    assert_eq!(reader.next(), Ok(Some(Token::Number("1"))));
    assert_eq!(reader.next(), Ok(None));
    reader.reset(" {} ");
    assert_eq!(reader.next(), Ok(Some(Token::BeginObject)));
    assert_eq!(reader.next(), Ok(Some(Token::EndObject)));
    assert_eq!(reader.next(), Ok(None));
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
    let limit = usize::try_from(NESTING_DEPTH).unwrap();
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
        // A 32-bit float is the 64-bit float it widens to, whose text reads back as both.
        let single = u32::try_from(bits >> 32).unwrap();
        let float = f32::from_bits(single);
        prop_assume!(float.is_normal() || float.is_subnormal());
        let widened = canonical_float(f64::from(float));
        prop_assert_eq!(widened.parse::<f64>().unwrap().to_bits(), f64::from(float).to_bits());
        prop_assert_eq!(widened.parse::<f32>().unwrap().to_bits(), single);
    }

    #[test]
    fn a_float_s_canonical_text_is_its_shortest_text_s_canonical_number(
        value in prop::num::f64::POSITIVE | prop::num::f64::NEGATIVE | prop::num::f64::NORMAL
            | prop::num::f64::SUBNORMAL | prop::num::f64::ZERO
    ) {
        let mut direct = Vec::new();
        write_float(value, &mut direct);
        let mut oracle = Vec::new();
        write_number(ryu::Buffer::new().format_finite(value), &mut oracle).unwrap();
        prop_assert_eq!(direct, oracle);
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
    use super::exponent_within;
    let may_hold_long_exponent = |bytes: &[u8]| super::long_numbers(bytes).exponent;
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
    // A number without one, and a letter before an `e`, hold no exponent.
    assert!(super::may_hold_exponent(
        format!("[1e1{digits}]").as_bytes()
    ));
    assert!(super::may_hold_exponent(b"{\"a\":1.5E3}"));
    assert!(!super::may_hold_exponent(b"{\"name\":0.0}"));
    assert!(!super::may_hold_exponent(b"e1"));
    // However many such numbers it holds.
    assert!(may_hold_long_exponent(
        format!("[1e1{digits},1e1{digits}]").as_bytes()
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
    assert!(!super::long_numbers(format!("e{digits}").as_bytes()).exponent);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(4096)))]

    #[test]
    fn an_integer_sonic_reads_as_a_float_is_always_found(
        digits in "[1-9][0-9]{17,40}", sign in prop::bool::ANY, wrap in 0_usize..3,
    ) {
        let number = format!("{}{digits}", if sign { "-" } else { "" });
        let text = match wrap {
            0 => number.clone(),
            1 => format!(r#"{{"a":{number}}}"#),
            _ => format!("[1,{number},2]"),
        };
        // As a value of its own, and through serde as the shredder's visitors read it.
        let read_as_float = matches!(
            sonic_rs::from_str::<sonic_rs::Value>(&number).map(|v| v.is_f64()),
            Ok(true)
        ) || matches!(
            sonic_rs::from_str::<serde_json::Value>(&number).map(|v| v.is_f64()),
            Ok(true)
        );
        prop_assert!(!read_as_float || super::long_numbers(text.as_bytes()).integer);
    }

    #[test]
    fn a_run_of_digits_is_long_by_its_length_alone(
        digits in "[0-9]{1,40}", before in "[ ,:\\[\"a-z.+-]{0,3}", after in "[ ,\\]}\".eE]{0,3}",
    ) {
        let text = format!("{before}{digits}{after}");
        prop_assert_eq!(super::long_numbers(text.as_bytes()).integer, digits.len() >= 19);
    }
}

#[test]
fn a_run_of_nineteen_digits_or_more_may_be_an_integer_sonic_reads_as_a_float() {
    for (text, integer) in [
        ("999999999999999999", false),
        ("-999999999999999999", false),
        ("9223372036854775807", true),
        ("9223372036854775808", true),
        ("-9223372036854775808", true),
        ("-9223372036854775809", true),
        ("18446744073709551616", true),
        ("{\"a\":-18446744073709551616}", true),
        ("[1,99999999999999999999,2]", true),
        ("[18446744073709551616,18446744073709551616]", true),
        ("123456789.123456789", false),
        ("1234567890123456789.5", true),
        ("0.1234567890123456789", true),
        ("1e123456789012345678", false),
        ("1e1234567890123456789", true),
        ("1.5e30", false),
        ("\"1234567890123456789\"", true),
    ] {
        assert_eq!(
            super::long_numbers(text.as_bytes()).integer,
            integer,
            "{text}"
        );
    }
    // Every integer below nineteen digits sonic-rs reads as an integer; from there, some as
    // floats.
    let read = |text: &str| sonic_rs::from_str::<serde_json::Value>(text).unwrap();
    assert!(!read("-999999999999999999").is_f64());
    assert!(!read("-9223372036854775808").is_f64());
    assert!(read("-9223372036854775809").is_f64());
    assert!(!read("18446744073709551615").is_f64());
    assert!(read("18446744073709551616").is_f64());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(4096)))]

    #[test]
    fn a_reader_s_strings_unescaped_are_what_serde_json_reads(
        pieces in prop::collection::vec(prop_oneof![
            "[a-z ]{0,6}",
            Just(r#"\""#.to_owned()), Just(r"\\".to_owned()), Just(r"\/".to_owned()),
            Just(r"\b".to_owned()), Just(r"é".to_owned()), Just(r"😀".to_owned()),
            Just(r"\ud83d".to_owned()), Just("\u{1}".to_owned()), Just(r"\x".to_owned()),
            Just(r"\u00e9".to_owned()), Just(r"\u00E9".to_owned()),
            Just(r"\ud83d\ude00".to_owned()),
            Just(r"\n".to_owned()), Just(r"\t".to_owned()), Just(r"\f".to_owned()),
            Just(r"\r".to_owned()),
        ], 0..8)
    ) {
        let body = pieces.concat();
        let text = format!(r#"{{"k{body}":"{body}"}}"#);
        let read = || -> Result<Vec<String>, JsonError> {
            let mut reader = Reader::new(&text);
            let mut strings = Vec::new();
            while let Some(token) = reader.next()? {
                if let Token::String(text) | Token::Key(text) = token {
                    strings.push(text.unescaped().into_owned());
                }
            }
            Ok(strings)
        };
        let theirs: Result<serde_json::Map<String, serde_json::Value>, _> = serde_json::from_str(&text);
        match (read(), theirs) {
            (Ok(strings), Ok(map)) => {
                let (key, value) = map.into_iter().next().unwrap();
                prop_assert_eq!(strings, vec![key, value.as_str().unwrap().to_owned()]);
            }
            (Err(_), Err(_)) => {}
            (ours, theirs) => prop_assert!(false, "{ours:?} against {theirs:?}"),
        }
    }
}
