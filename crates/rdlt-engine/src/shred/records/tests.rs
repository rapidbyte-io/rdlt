use bytes::Bytes;
use proptest::prelude::*;

use super::chunks;
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
                .map(|record| String::from_utf8(record.to_vec()).unwrap())
                .collect();
            assert_eq!(records.len(), chunk.rows);
            records
        })
        .collect()
}

fn refused(push: &str) -> String {
    let pushes = [Bytes::from(push.to_owned())];
    match chunks(&pushes, 1 << 20) {
        Err(ShredError::Invalid(what)) => what,
        Err(other) => panic!("{push:?} was refused as {other}"),
        Ok(_) => panic!("{push:?} was scanned"),
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
    assert_eq!((chunks[0].rows, chunks[0].before), (200_000, 0));
    assert_eq!(chunks[0].records().count(), 200_000);
    assert!(chunks[0].records().all(|record| record == b"{}"));
}

#[test]
fn chunks_end_at_the_record_that_fills_them_across_pushes() {
    let pushes = [
        "{\"a\":1}\n{\"a\":2}\n{\"a\":3}",
        "[{\"a\":4}, {\"a\":5}]",
        " \n",
    ];
    // Seven bytes a record: a chunk of ten bytes ends at every second record.
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
    let before: Vec<usize> = chunks.iter().map(|chunk| chunk.before).collect();
    assert_eq!(before, [0, 2, 4]);
    // The second chunk holds the end of one push and the start of the next.
    assert_eq!(chunks[1].parts.len(), 2);
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

proptest! {
    /// Records are found again as they were scanned, whatever the form, the whitespace and the
    /// chunk size.
    #[test]
    fn every_record_is_found_once_in_order(
        records in proptest::collection::vec("\\{\"k\":\"[a-z,\\]\\[{} ]{0,6}\"\\}", 0..40),
        spaces in proptest::collection::vec("[ \t\r\n]{0,3}", 41),
        chunk_bytes in 1_usize..200,
        array in any::<bool>(),
    ) {
        let mut push = String::new();
        if array {
            push.push('[');
        }
        for (index, record) in records.iter().enumerate() {
            push.push_str(&spaces[index]);
            push.push_str(record);
            if array {
                push.push_str(&spaces[index + 1]);
                if index + 1 < records.len() {
                    push.push(',');
                }
            } else {
                push.push('\n');
            }
        }
        if array {
            push.push(']');
        }
        let found: Vec<String> = found(&[push.as_str()], chunk_bytes).concat();
        prop_assert_eq!(found, records);
    }
}
