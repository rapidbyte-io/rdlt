use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use bytes::Bytes;
use rdlt_connector::PipelineId;

use super::{ADMITTED, CHUNK_BYTES, Corpus, PUSH_BYTES, Parse};
use super::{
    Refused, check_json, json_column, normalize, pruned, shred, shred_counted, shred_on, shred_with,
};
use crate::compute::Cores;
use crate::env::Env;
use crate::error::codes;
use crate::{EngineConfig, SystemEnv};

#[test]
fn shredding_inline_and_on_a_pool_gives_the_same_batches() {
    let pushes = [
        Bytes::from_static(b"{\"a\":1}\n{\"a\":2}"),
        Bytes::from_static(b"[{\"a\":3}]"),
    ];
    let inline = shred(&pushes, 8).unwrap();
    let cores = Cores::new(NonZeroUsize::new(2).unwrap());
    let env: Arc<dyn Env> = Arc::new(SystemEnv::try_new(cores).unwrap());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let pooled = runtime.block_on(shred_on(&env, &pushes, 8)).unwrap();
    assert_eq!(inline, pooled);
    assert_eq!(inline.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
}

#[test]
fn shredding_each_push_alone_shreds_every_row_of_the_corpus() {
    let pushes = Corpus::Nested.pushes(2 * PUSH_BYTES + PUSH_BYTES / 2);
    assert!(pushes.len() > 1);
    assert!(pushes.last().unwrap().len() < pushes[0].len());
    let whole: usize = shred(&pushes, CHUNK_BYTES)
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    let alone: usize = pushes
        .iter()
        .flat_map(|push| shred(std::slice::from_ref(push), CHUNK_BYTES).unwrap())
        .map(|batch| batch.num_rows())
        .sum();
    assert_eq!(alone, whole);
}

#[test]
fn counting_a_shred_counts_one_parse_a_chunk_of_plain_rows() {
    let pushes = Corpus::Nested.pushes(4 << 20);
    let counts = shred_counted(&pushes, CHUNK_BYTES).unwrap();
    assert_eq!(counts.parsed, 4);
    assert_eq!((counts.tripped, counts.rebuilt, counts.exact), (0, 0, 0));
}

/// `batches` as one batch.
fn joined(batches: &[RecordBatch]) -> RecordBatch {
    arrow_select::concat::concat_batches(&batches[0].schema(), batches).unwrap()
}

#[test]
fn shredding_any_way_the_engine_may_gives_the_batches_it_gives() {
    // Each cliff corpus drifts, trips its allowance or needs exact numbers, over several chunks.
    for corpus in Corpus::CLIFFS {
        let pushes = corpus.pushes(1 << 20);
        let engine = joined(&shred(&pushes, 64 << 10).unwrap());
        assert!(engine.num_rows() > 1000, "{}", corpus.name());
        for (chunk_bytes, admitted, parse) in [
            (64 << 10, ADMITTED, Parse::Fast),
            (1 << 20, 1 << 20, Parse::Fast),
            (64 << 10, 1 << 20, Parse::Exact),
            (64 << 10, ADMITTED, Parse::Exact),
        ] {
            let shredded = joined(&shred_with(&pushes, chunk_bytes, admitted, parse).unwrap());
            assert_eq!(
                shredded,
                engine,
                "{} {chunk_bytes} {admitted} {parse:?}",
                corpus.name()
            );
        }
    }
}

#[test]
fn a_way_of_shredding_takes_its_allowance_and_parse() {
    let fast = super::limits(7, Parse::Fast);
    assert_eq!((fast.admitted, fast.exact), (7, false));
    let exact = super::limits(ADMITTED, Parse::Exact);
    assert_eq!((exact.admitted, exact.exact), (ADMITTED, true));
    assert_eq!(exact.columns, rdlt_wire::limits::SCHEMA_COLUMNS);
}

#[test]
fn a_refusal_carries_the_errors_code_and_message() {
    let refused = shred(&[Bytes::from_static(b"[1]")], 8).unwrap_err();
    assert_eq!(
        refused,
        Refused {
            code: codes::JSON_NOT_OBJECT.as_str(),
            message: "a record is not a JSON object".to_owned(),
        }
    );
    assert_eq!(
        shred(&[Bytes::from_static(b"[")], 8).unwrap_err().code,
        codes::JSON_INVALID.as_str()
    );
}

#[test]
fn normalizing_a_shredded_batch_names_each_table_by_its_path() {
    let push = Bytes::from_static(br#"{"id":1,"items":[{"sku":"a","tags":["x"]}]}"#);
    let batches = shred(&[push], 1 << 20).unwrap();
    let parts = normalize(&batches[0], 8, &["id"]).unwrap();
    let paths: Vec<Vec<String>> = parts.iter().map(|(path, _)| path.clone()).collect();
    assert_eq!(
        paths,
        [
            Vec::new(),
            vec!["items".to_owned()],
            vec!["items".to_owned(), "tags".to_owned()],
        ]
    );
    assert!(parts.iter().all(|(_, batch)| batch.num_rows() == 1));
}

#[test]
fn pruning_a_normalized_batch_drops_each_dropped_roots_children_with_it() {
    let push = Bytes::from_static(
        b"{\"id\":0,\"items\":[{\"tags\":[\"x\"]}]}\n{\"id\":1,\"items\":[{\"tags\":[\"y\",\"z\"]}]}",
    );
    let batches = shred(&[push], 1 << 20).unwrap();
    // Root 0 goes with its item and its tag; root 1, its item and its two tags are left.
    assert_eq!(
        pruned(&batches[0], 8, NonZeroUsize::new(2).unwrap()).unwrap(),
        4
    );
}

#[test]
fn a_sample_log_is_the_frames_each_encoded_alone_after_the_preamble() {
    use crate::wal::frame::{frames, limits, preamble};

    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let batch = RecordBatch::try_from_iter([("id", ids)]).expect("a valid batch");
    let log = super::sample_log(batch);
    let limits = limits(EngineConfig::default().memory().get());
    let mut expected = preamble().to_vec();
    for frame in frames(&log, limits).expect("the log's frames read") {
        expected.extend_from_slice(&frame.encode().expect("the frame encodes"));
    }
    assert_eq!(log, expected);
}

#[test]
fn a_sample_log_reads_back_with_its_one_commit_and_a_garbled_one_is_refused() {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let batch = RecordBatch::try_from_iter([("id", ids)]).expect("a valid batch");
    let log = super::sample_log(batch);
    assert_eq!(super::scan_log(&log), Ok(1));
    // Garble the commit frame's payload, then make its checksum match, as a bug would.
    let mut garbled = log.clone();
    // Past the chunk's preamble.
    let mut offset = 14;
    let mut frames = Vec::new();
    while offset + 9 <= garbled.len() {
        let len = u32::from_le_bytes(
            garbled[offset + 1..offset + 5]
                .try_into()
                .expect("four bytes"),
        );
        frames.push((
            garbled[offset],
            offset,
            usize::try_from(len).expect("a length"),
        ));
        offset += 9 + usize::try_from(len).expect("a length");
    }
    let (_, at, len) = *frames
        .iter()
        .find(|(kind, _, _)| *kind == 5)
        .expect("a commit frame");
    garbled[at + 9] = b'!';
    // The checksum covers the frame's kind and length too.
    let crc = crc32c::crc32c_append(
        crc32c::crc32c(&garbled[at..at + 5]),
        &garbled[at + 9..at + 9 + len],
    );
    garbled[at + 5..at + 9].copy_from_slice(&crc.to_le_bytes());
    let refused = super::scan_log(&garbled).expect_err("the commit does not decode");
    assert_eq!(refused.code, codes::WAL_UNREADABLE.as_str());
}

#[test]
fn a_log_another_load_wrote_is_refused_as_foreign() {
    use crate::wal::frame::{self, End, Frame, Header};
    let other = rdlt_connector::LoadId::from_parts(std::time::UNIX_EPOCH, 2);
    let header = Frame::Header(Header {
        pipeline: PipelineId::parse("fuzzed").expect("a valid pipeline"),
        load: other,
        chunk: 0,
        epoch: rdlt_connector::Epoch(1),
        opened: None,
        origin: other,
    });
    let mut log = frame::preamble().to_vec();
    for frame in [header, Frame::End(End::default())] {
        log.extend_from_slice(&frame.encode().expect("the frame encodes"));
    }
    let refused = super::scan_log(&log).expect_err("another load's log");
    assert_eq!(
        refused.code,
        codes::WAL_FOREIGN.as_str(),
        "{}",
        refused.message
    );
}

#[test]
fn a_column_of_json_checks_with_escapes_where_asked_and_a_broken_value_is_refused() {
    for escaped in [false, true] {
        let batch = json_column(1_000, escaped);
        assert_eq!(batch.num_rows(), 1_000);
        check_json(&batch).unwrap();
    }
    let escaped = json_column(8, true);
    let values = escaped.column(0).as_string::<i32>();
    assert!(values.iter().flatten().any(|value| value.contains('\\')));
    let column = arrow_array::StringArray::from(vec![r#"{"a":"\x01"}"#]);
    let schema = escaped.schema();
    let refused = RecordBatch::try_new(schema, vec![Arc::new(column)]).unwrap();
    assert_eq!(
        check_json(&refused).unwrap_err().code,
        codes::JSON_INVALID.as_str()
    );
}

#[test]
fn lines_that_are_not_objects_are_refused_the_first_an_array_as_invalid_json() {
    // A first line that is an array reads as an array with more after it.
    for (lines, code) in [
        ("[1,2]\n[3]\n", codes::JSON_INVALID),
        ("{\"a\":1}\n[1]\n", codes::JSON_NOT_OBJECT),
        ("\"a\"\n{\"a\":1}\n", codes::JSON_NOT_OBJECT),
    ] {
        let push = Bytes::from_static(lines.as_bytes());
        let refused = shred(&[push], CHUNK_BYTES).unwrap_err();
        assert_eq!(refused.code, code.as_str(), "{lines}");
    }
}
