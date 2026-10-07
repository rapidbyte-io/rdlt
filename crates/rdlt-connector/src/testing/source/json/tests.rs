use std::future::Future;
use std::task::{Context, Poll, Waker};

use super::{Records, canonical, counted};
use crate::testing::limits::{RECORD_BYTES, YIELD_BYTES};

/// The output of `future`, and how many times it yielded before it.
fn polled<F: Future>(future: F) -> (F::Output, usize) {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    let mut yields = 0;
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return (output, yields),
            Poll::Pending => yields += 1,
        }
    }
}

fn records(text: &str) -> Vec<&str> {
    Records::new(text.as_bytes())
        .map(|record| &text[record])
        .collect()
}

#[test]
fn a_push_is_the_rows_of_its_arrays_and_each_value_beside_one() {
    let cases: [(&str, &[&str]); 18] = [
        ("", &[]),
        ("  \n", &[]),
        ("[]", &[]),
        ("[ ]\n", &[]),
        ("[1]", &["1"]),
        ("[ {\"a\": 1}, {\"a\": 2} ]", &["{\"a\": 1}", "{\"a\": 2}"]),
        ("{\"a\":1}\n{\"a\":2}\n", &["{\"a\":1}", "{\"a\":2}"]),
        ("[[1,2],[3]]", &["[1,2]", "[3]"]),
        ("[1] [2]", &["1", "2"]),
        ("1 true null \"x y\"", &["1", "true", "null", "\"x y\""]),
        (
            "[\"a]b\", \"c\\\"d,\", {\"k\": \"}]\"}]",
            &["\"a]b\"", "\"c\\\"d,\"", "{\"k\": \"}]\"}"],
        ),
        ("[\"\\\\\", 2]", &["\"\\\\\"", "2"]),
        ("[-1.5e3,\ttrue]", &["-1.5e3", "true"]),
        // What never closes is one record to the end, which no parse takes.
        ("[{\"a\": [1, 2", &["{\"a\": [1, 2"]),
        ("[\"open", &["\"open"]),
        ("]} 7", &["7"]),
        // A backslash outside a string escapes nothing: the quote after it opens one.
        ("\\\" 7", &["\\\" 7"]),
        // A container ends where the brackets it opened close, not where one within it does.
        ("[{\"a\": [1]}, 2]", &["{\"a\": [1]}", "2"]),
    ];
    for (text, expected) in cases {
        assert_eq!(records(text), expected, "{text:?}");
        assert_eq!(
            polled(counted(text.as_bytes())).0.ok(),
            Some(expected.len())
        );
    }
}

#[test]
fn rows_are_made_canonical_one_by_one_and_what_does_not_parse_is_left_out() {
    let cases = [
        ("", "[]"),
        (
            "[ {\"b\": 1, \"a\": 2}, {\"a\": 2} ]",
            "[{\"a\":2,\"b\":1},{\"a\":2}]",
        ),
        ("{\"a\":1}\n{\"a\":2}\n", "[{\"a\":1},{\"a\":2}]"),
        ("[1, yes, \"x\", {\"a\": ]", "[1,\"x\"]"),
        ("[[1, 2], \"\\u0041\"]", "[[1,2],\"A\"]"),
    ];
    for (text, expected) in cases {
        let (rows, _) = polled(canonical(text.as_bytes()));
        assert_eq!(rows, expected.as_bytes(), "{text:?}");
        // What is canonical stays as it is.
        assert_eq!(polled(canonical(&rows)).0, rows);
    }
}

#[test]
fn a_record_is_parsed_only_within_its_bytes() {
    let record = |bytes: usize| format!("[\"{}\"]", "x".repeat(bytes - 2));
    assert_eq!(
        polled(counted(record(RECORD_BYTES).as_bytes())).0.ok(),
        Some(1)
    );
    let beyond = polled(counted(record(RECORD_BYTES + 1).as_bytes())).0;
    assert!(beyond.is_err_and(|beyond| beyond.unobserved));
    // The limit is of each record, not of them all.
    let many = format!("[{0},{0}]", &record(RECORD_BYTES)[1..=RECORD_BYTES]);
    assert_eq!(polled(counted(many.as_bytes())).0.ok(), Some(2));
}

#[test]
fn a_push_is_scanned_and_made_canonical_in_pieces_between_which_it_yields() {
    for (pieces, yields) in [(0, 0), (1, 1), (2, 2)] {
        // Rows of two bytes each, a piece of them and one row more.
        let rows = pieces * YIELD_BYTES / 2 + 1;
        let text = format!("[{}]", vec!["0"; rows].join(","));
        let (counted, yielded) = polled(counted(text.as_bytes()));
        assert_eq!((counted.ok(), yielded), (Some(rows), yields), "{pieces}");
        let (rows, yielded) = polled(canonical(text.as_bytes()));
        assert_eq!((rows.len(), yielded), (text.len(), yields), "{pieces}");
    }
}
