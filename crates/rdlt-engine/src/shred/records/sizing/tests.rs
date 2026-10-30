use std::num::NonZeroUsize;

use bytes::Bytes;

use super::{ROWS, Sizing};

/// A flush of pushes of `texts`.
fn flush(texts: &[&str]) -> Vec<Bytes> {
    texts
        .iter()
        .map(|text| Bytes::from((*text).to_owned()))
        .collect()
}

/// Chunks of at least `rows` records and at most `limit` bytes, for `cores` cores.
fn sizing(rows: usize, limit: usize, cores: usize) -> Sizing {
    Sizing {
        rows,
        limit: NonZeroUsize::new(limit).unwrap(),
        cores: NonZeroUsize::new(cores).unwrap(),
    }
}

#[test]
fn a_chunk_takes_its_rows_records_or_a_share_of_two_jobs_a_core_whichever_is_more() {
    // A thousand records of ten bytes, line end included.
    let records = "{\"a\":123}\n".repeat(1_000);
    let pushes = flush(&[&records]);
    // A hundred records take 1,000 bytes; two jobs for each of four cores take 1,250 each.
    assert_eq!(sizing(100, 1 << 20, 4).chunk_bytes(&pushes), 1_250);
    // Two hundred take 2,000.
    assert_eq!(sizing(200, 1 << 20, 4).chunk_bytes(&pushes), 2_000);
    // Two jobs for each of two cores take 2,500.
    assert_eq!(sizing(200, 1 << 20, 2).chunk_bytes(&pushes), 2_500);
}

#[test]
fn a_chunk_takes_no_more_than_the_limit_however_many_bytes_its_rows_take() {
    let records = "{\"a\":123}\n".repeat(1_000);
    let pushes = flush(&[&records]);
    assert_eq!(sizing(200, 1_500, 4).chunk_bytes(&pushes), 1_500);
    assert_eq!(sizing(100, 1_000, 4).chunk_bytes(&pushes), 1_000);
    // Records of ten thousand bytes: a partition's rows of them take more than its limit.
    let wide = format!("{{\"a\":\"{}\"}}\n", "x".repeat(9_991)).repeat(800);
    let limit = NonZeroUsize::new(2 << 20).unwrap();
    let cores = NonZeroUsize::new(4).unwrap();
    assert!(ROWS * 10_000 > limit.get());
    assert_eq!(
        Sizing::new(limit, cores).chunk_bytes(&flush(&[&wide])),
        limit.get()
    );
}

#[test]
fn a_flush_whose_first_record_outruns_the_sample_is_cut_at_the_limit() {
    // Short records follow, past the sample: they would make chunks far smaller.
    let long = format!("{{\"a\":\"{}\"}}\n", "x".repeat(70_000));
    let short = "{\"a\":1}\n".repeat(10_000);
    let pushes = flush(&[&long, &short]);
    assert_eq!(sizing(1, 1 << 20, 64).chunk_bytes(&pushes), 1 << 20);
}

#[test]
fn an_array_s_records_are_sampled_at_its_top_level_commas() {
    // A thousand elements of eleven bytes, each holding a comma of its own: 12,001 bytes and
    // 999 commas between elements, so thirteen bytes a record.
    let array = format!("[{}{{\"a\":[1,2]}}]", "{\"a\":[1,2]},".repeat(999));
    let pushes = flush(&[&array]);
    assert_eq!(sizing(100, 1 << 20, 1_000).chunk_bytes(&pushes), 1_300);
}

#[test]
fn an_empty_flush_or_one_of_whitespace_is_cut_at_the_limit() {
    assert_eq!(sizing(1, 100, 1).chunk_bytes(&[]), 100);
    assert_eq!(
        sizing(1, 100, 1).chunk_bytes(&flush(&["", " \n\n\t\r\n"])),
        100
    );
}

#[test]
fn the_sample_reads_on_into_the_pushes_after_one_with_no_record_end() {
    // Seven bytes ending no record, then sixteen ending two: twelve bytes a record.
    let pushes = flush(&["{\"a\":1}", "{\"a\":2}\n{\"a\":3}\n"]);
    assert_eq!(sizing(10, 1_000, 1).chunk_bytes(&pushes), 120);
}

#[test]
fn a_fixed_sizing_cuts_chunks_of_its_bytes_whatever_the_records_and_the_cores() {
    let records = "{}\n".repeat(100_000);
    let pushes = flush(&[&records]);
    assert_eq!(Sizing::fixed(1 << 20).chunk_bytes(&pushes), 1 << 20);
    assert_eq!(Sizing::fixed(7).chunk_bytes(&pushes), 7);
    assert_eq!(Sizing::fixed(7).chunk_bytes(&[]), 7);
}
