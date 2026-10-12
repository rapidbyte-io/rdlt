use bytes::Bytes;
use proptest::prelude::*;

use super::{Chunk, Form, Part, chunks};
use crate::shred::ShredError;

/// The records of each chunk of `pushes`, as text.
fn found(pushes: &[&str], chunk_bytes: usize) -> Vec<Vec<String>> {
    let pushes: Vec<Bytes> = pushes
        .iter()
        .map(|push| Bytes::from((*push).to_owned()))
        .collect();
    chunks(&pushes, chunk_bytes)
        .unwrap()
        .iter()
        .map(|chunk| {
            let records: Vec<String> = chunk
                .records()
                .map(|record| {
                    String::from_utf8(record.expect("the cut finds it again").to_vec()).unwrap()
                })
                .collect();
            assert!(!records.is_empty(), "a chunk holds a record");
            assert!(records.len() <= chunk.count());
            records
        })
        .collect()
}

fn refused(push: &str) -> String {
    let pushes = [Bytes::from(push.to_owned())];
    match chunks(&pushes, 1 << 20) {
        Err(ShredError::Invalid(what)) => what,
        Err(other) => panic!("{push:?} was refused as {other}"),
        Ok(_) => panic!("{push:?} was cut"),
    }
}

#[test]
fn a_chunk_keeps_a_span_of_each_push_however_many_records_it_holds() {
    let lines = "{}\n".repeat(100_000);
    let array = format!("[{}{{}}]", "{},".repeat(99_999));
    let pushes = [Bytes::from(lines), Bytes::from(array)];
    let chunks = chunks(&pushes, usize::MAX).unwrap();
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].parts.len(), 2, "one span a push");
    assert_eq!(chunks[0].count(), 200_000);
    assert_eq!(chunks[0].records().count(), 200_000);
    assert!(chunks[0].records().all(|record| record == Ok(&b"{}"[..])));
}

#[test]
fn chunks_end_at_the_record_that_fills_them_across_pushes() {
    let pushes = [
        "{\"a\":1}\n{\"a\":2}\n{\"a\":3}",
        "[{\"a\":4}, {\"a\":5}]",
        " \n",
    ];
    // Seven bytes a record and one between: a chunk of ten bytes ends at every second record.
    assert_eq!(
        found(&pushes, 10),
        [
            vec!["{\"a\":1}", "{\"a\":2}"],
            vec!["{\"a\":3}", "{\"a\":4}"],
            vec!["{\"a\":5}"],
        ]
    );
    let pushes: Vec<Bytes> = pushes.iter().map(|push| Bytes::from(*push)).collect();
    let chunks = chunks(&pushes, 10).unwrap();
    let counted: Vec<usize> = chunks.iter().map(Chunk::count).collect();
    assert_eq!(counted, [2, 2, 1]);
    // The second chunk holds the end of one push and the start of the next.
    assert_eq!(chunks[1].parts.len(), 2);
}

#[test]
fn an_array_closes_its_chunk_only_where_its_last_part_fills_it() {
    // Seven bytes an element and one between, ten a chunk: the chunk closes at every second
    // element. The second array's last element fills nothing, so the line after it joins it.
    assert_eq!(
        found(
            &["[{\"a\":1},{\"a\":2},{\"a\":3},{\"a\":4}]", "{\"a\":5}\n"],
            10
        ),
        [
            vec!["{\"a\":1}", "{\"a\":2}"],
            vec!["{\"a\":3}", "{\"a\":4}"],
            vec!["{\"a\":5}"],
        ]
    );
    assert_eq!(
        found(&["[{\"a\":1},{\"a\":2},{\"a\":3}]", "{\"a\":4}\n"], 10),
        [
            vec!["{\"a\":1}", "{\"a\":2}"],
            vec!["{\"a\":3}", "{\"a\":4}"],
        ]
    );
}

#[test]
fn lines_are_cut_at_the_first_line_end_past_each_chunk_s_bytes() {
    let push = Bytes::from_static(b"{\"a\":1}\n{\"a\":22}\n{\"a\":333}\n{\"a\":4444}\n");
    let chunks = chunks(&[push], 10).unwrap();
    let spans: Vec<Vec<&[u8]>> = chunks
        .iter()
        .map(|c| c.records().map(Result::unwrap).collect())
        .collect();
    assert_eq!(
        spans,
        [
            vec![&b"{\"a\":1}"[..], b"{\"a\":22}"],
            vec![b"{\"a\":333}", b"{\"a\":4444}"]
        ]
    );
}

#[test]
fn a_crlf_line_end_is_cut_whole_and_a_blank_chunk_is_dropped() {
    let push = Bytes::from_static(b"{\"a\":1}\r\n\r\n\r\n   \n{\"a\":2}\r\n");
    let chunks = chunks(&[push], 4).unwrap();
    let records: Vec<&[u8]> = chunks
        .iter()
        .flat_map(|c| c.records().map(Result::unwrap))
        .collect();
    assert_eq!(records, [&b"{\"a\":1}"[..], b"{\"a\":2}"]);
    assert!(chunks.iter().all(|chunk| chunk.records().count() > 0));
}

#[test]
fn an_array_that_is_not_one_is_refused_where_it_breaks() {
    assert!(refused("[{},]").contains("an empty element"));
    assert!(refused("[,{}]").contains("an empty element"));
    assert!(refused("[{}").contains("it does not end"));
    assert!(refused("[{}] x").contains("more follows it"));
    assert!(refused("[{}}]").contains("unbalanced brackets"));
    assert!(found(&["[]", " [ ] ", ""], 1).is_empty());
}

/// A record whose string holds brackets, commas, escaped quotes and runs of escaped backslashes,
/// so it may end in `\\"`, `\\\\"` or hold `\"`.
const RECORD: &str = r#"\{"k":"([a-z,\]\[{} ]|\\\\|\\"){0,6}"\}"#;

/// JSON whitespace around a record: in lines, its line end, and blank, whitespace-only and
/// carriage-return lines among it.
const GAP: &str = "([ \t]{0,2}\r?\n){0,3}[ \t\r]{0,2}";

/// A push of `records`, as an array or as lines, with `gaps` around them: the first before the
/// push's `[` or first line, the last after its `]` or last line, which ends in a line end
/// where `ended` says.
fn push_of(records: &[String], array: bool, gaps: &[String], ended: bool) -> String {
    let mut push = gaps[0].clone();
    if array {
        push.push('[');
    }
    for (index, record) in records.iter().enumerate() {
        let last = index + 1 == records.len();
        if array {
            push.push_str(&gaps[index]);
        }
        push.push_str(record);
        push.push_str(&gaps[index + 1]);
        if !array && (ended || !last) {
            push.push('\n');
        } else if array && !last {
            push.push(',');
        }
    }
    if array {
        push.push_str(&gaps[0]);
        push.push(']');
    }
    push.push_str(&gaps[13]);
    push
}

proptest! {
    /// Cut across pushes of either form, whitespace-only ones and lines without a last line end
    /// among them, whatever the whitespace and the chunk size, the chunks hold each push's records
    /// in order, each chunk at least one and no more than it counts.
    #[test]
    fn every_record_is_found_once_in_order(
        pushes in proptest::collection::vec(
            (
                proptest::collection::vec(RECORD, 0..12),
                any::<bool>(),
                proptest::collection::vec(GAP, 14),
                any::<bool>(),
            ),
            1..4,
        ),
        chunk_bytes in 1_usize..=64,
    ) {
        let texts: Vec<Bytes> = pushes
            .iter()
            .map(|(records, array, gaps, ended)| {
                Bytes::from(push_of(records, *array, gaps, *ended))
            })
            .collect();
        let expected: Vec<&str> = pushes
            .iter()
            .flat_map(|(records, ..)| records.iter().map(String::as_str))
            .collect();
        let chunks = chunks(&texts, chunk_bytes).unwrap();
        let mut cut = Vec::new();
        for chunk in &chunks {
            let records: Vec<&[u8]> = chunk.records().map(Result::unwrap).collect();
            prop_assert!(!records.is_empty());
            prop_assert!(records.len() <= chunk.count());
            cut.extend(records.into_iter().map(|record| std::str::from_utf8(record).unwrap()));
        }
        prop_assert_eq!(cut, expected);
    }
}

#[test]
fn a_span_that_breaks_when_it_is_found_again_is_an_error_not_an_end() {
    // A span no cut of a whole push makes: its first element closes a bracket it never opened.
    let chunk = Chunk {
        parts: vec![Part {
            push: Bytes::from_static(b"[{}},{}]"),
            form: Form::Elements,
            span: 1..7,
            elements: 2,
        }],
    };
    let found: Vec<_> = chunk.records().collect();
    assert!(
        matches!(found.as_slice(), [Err(ShredError::Internal(_))]),
        "{found:?}"
    );
    // Its second element breaks, after one it found: nothing follows the error.
    let chunk = Chunk {
        parts: vec![Part {
            push: Bytes::from_static(b"[{},{}}]"),
            form: Form::Elements,
            span: 1..7,
            elements: 2,
        }],
    };
    let found: Vec<_> = chunk.records().take(3).collect();
    assert!(
        matches!(found.as_slice(), [Ok(b"{}"), Err(ShredError::Internal(_))]),
        "{found:?}"
    );
}
