use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use bytes::Bytes;

use super::{ADMITTED, CHUNK_BYTES, Corpus, Jobs, PUSH_BYTES, Parse};
use super::{
    Refused, check_json, json_column, normalize, pruned, shred, shred_counted, shred_on, shred_with,
};
use crate::SystemEnv;
use crate::compute::Cores;
use crate::env::Env;
use crate::error::codes;

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
    let pooled = runtime.block_on(shred_on(&env, &pushes)).unwrap();
    assert_eq!(joined(&inline), joined(&pooled));
    assert_eq!(inline.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
}

#[test]
fn shredding_on_a_pool_gives_each_of_its_cores_two_jobs_of_many_short_records() {
    // Records of ten bytes: a share of two jobs a core holds more than a partition's rows of them.
    let records = "{\"a\":123}\n".repeat(104_858);
    let pushes = [Bytes::from(records)];
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    for cores in [1, 2, 8] {
        let count = Cores::new(NonZeroUsize::new(cores).unwrap());
        let env: Arc<dyn Env> = Arc::new(SystemEnv::try_new(count).unwrap());
        let batches = runtime.block_on(shred_on(&env, &pushes)).unwrap();
        assert_eq!(batches.len(), 2 * cores, "{cores} cores");
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 104_858);
    }
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
        let sized = Jobs::Sized {
            limit: NonZeroUsize::new(64 << 10).unwrap(),
            cores: NonZeroUsize::new(4).unwrap(),
        };
        for (jobs, admitted, parse) in [
            (sized, ADMITTED, Parse::Fast),
            (Jobs::Fixed(64 << 10), ADMITTED, Parse::Fast),
            (Jobs::Fixed(1 << 20), 1 << 20, Parse::Fast),
            (Jobs::Fixed(64 << 10), 1 << 20, Parse::Exact),
            (Jobs::Fixed(64 << 10), ADMITTED, Parse::Exact),
        ] {
            let shredded = joined(&shred_with(&pushes, jobs, admitted, parse).unwrap());
            assert_eq!(
                shredded,
                engine,
                "{} {jobs:?} {admitted} {parse:?}",
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
