//! What the bench module's allocation counter sees, and what shredding rows of arrays, checking
//! escaped JSON and writing a table's state records allocate and hold, in a process whose global
//! allocator counts.

#![forbid(unsafe_code)]

use std::alloc::System;
use std::hint::black_box;
use std::num::NonZeroU64;

use rdlt_connector::{
    ColumnPath, Field, Identity, LogicalType, NameMap, SchemaVersion, Sequences, StateEntry,
    TablePath, TableSchema,
};
use rdlt_engine::bench::counted;
use stats_alloc::{INSTRUMENTED_SYSTEM, StatsAlloc};

#[global_allocator]
static HEAP: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

#[test]
fn a_run_counts_what_the_global_counting_allocator_served_it() {
    let allocated = counted(HEAP, || {
        let mut grown = black_box(Vec::<u8>::with_capacity(64));
        grown.reserve_exact(1024);
        black_box(grown);
    });
    assert!(allocated.allocations >= 1.0, "{allocated:?}");
    assert!(allocated.reallocations >= 1.0, "{allocated:?}");
    assert!(allocated.bytes >= 1024.0, "{allocated:?}");
}

#[test]
fn shredding_rows_of_arrays_allocates_no_observation_a_row() {
    let pushes = rdlt_engine::bench::Corpus::WithArrays.pushes(1 << 20);
    let rows: usize = pushes
        .iter()
        .map(|push| memchr::memchr_iter(b'\n', push).count())
        .sum();
    let rows = NonZeroU64::new(u64::try_from(rows).unwrap()).unwrap();
    let allocated = counted(HEAP, || {
        rdlt_engine::bench::shred(&pushes, rdlt_engine::bench::CHUNK_BYTES).unwrap();
    });
    let a_row = allocated.per(rows).allocations;
    assert!(a_row <= 0.1, "{a_row} allocations a row");
}

#[test]
fn checking_a_column_of_escaped_json_allocates_nothing_a_row() {
    let batch = rdlt_engine::bench::json_column(8_192, true);
    let allocated = counted(HEAP, || rdlt_engine::bench::check_json(&batch).unwrap());
    assert!(allocated.allocations < 64.0, "{allocated:?}");
}

#[test]
fn the_exact_parse_allocates_no_string_a_number() {
    // `j`, beyond 64 bits, has each chunk parsed again exactly; sonic-rs's tree of a record
    // allocates alike for one number as for ten.
    let exact = |row: &str| {
        let push = bytes::Bytes::from(format!("{row}\n").repeat(1_000));
        counted(HEAP, || {
            rdlt_engine::bench::shred(&[push], 1 << 20).unwrap();
        })
        .allocations
    };
    let ten = exact(
        r#"{"a":1.5,"b":2.5,"c":3.5,"d":4.5,"e":5.5,"f":6.5,"g":7.5,"h":8.5,"i":9.5,"j":18446744073709551616}"#,
    );
    let one = exact(r#"{"j":18446744073709551616}"#);
    assert!(ten - one < 1_000.0, "{ten} allocations against {one}");
}

#[test]
fn rendering_a_drifted_column_allocates_nothing_a_value() {
    // An object in one row and an array in the next: `v` can only be a column of JSON.
    let rows = format!("{}\n{}\n", r#"{"v":{"x":1,"y":"a"}}"#, r#"{"v":[1,2]}"#);
    let drifted = bytes::Bytes::from(rows.repeat(500));
    let plain = bytes::Bytes::from(format!("{}\n", r#"{"v":7}"#).repeat(1_000));
    let shred = |push: bytes::Bytes| {
        counted(HEAP, || {
            rdlt_engine::bench::shred(&[push], 1 << 20).unwrap();
        })
        .allocations
    };
    let (drifted, plain) = (shred(drifted), shred(plain));
    assert!(
        drifted - plain < 100.0,
        "{drifted} allocations against {plain}"
    );
}

#[test]
fn a_table_s_state_records_allocate_their_key_value_and_each_field_s_walk_alone() {
    let table = TablePath::new(["orders", "lines"]).unwrap();
    let columns = ["id", "name", "updated_at"];
    let fields = columns.map(|column| Field::new(column, LogicalType::Int64, true));
    let mut names = NameMap::default();
    for column in columns {
        names.insert(ColumnPath::from(column), column).unwrap();
    }
    // Each record allocates its key's path and text and its value; a schema's also walks each of
    // its three fields from a stack of the field's nodes, once to count its JSON and once to write
    // it.
    let entries = [
        (
            StateEntry::Schema {
                table: table.clone(),
                version: SchemaVersion(1),
                schema: TableSchema::new(fields.to_vec()).unwrap(),
                exact: ["id".into()].into(),
            },
            3.0 + 2.0 * 3.0,
        ),
        (
            StateEntry::Names {
                table: table.clone(),
                physical: "orders__lines".into(),
                names,
            },
            3.0,
        ),
        (
            StateEntry::Sequences {
                table,
                sequences: Sequences::Source,
                history: true,
                key: vec![ColumnPath::from("id")],
                change_time: Some(ColumnPath::from("updated_at")),
                identity: Identity::Key,
            },
            3.0,
        ),
    ];
    for (entry, allocations) in entries {
        let allocated = counted(HEAP, || {
            black_box(entry.to_record());
        });
        assert!(
            allocated.allocations <= allocations,
            "{allocated} for {entry:?}"
        );
    }
}
