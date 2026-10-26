use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_schema::Schema;
use rdlt_connector::Field;
use serde_json::Value;

use super::{Data, columns, document, fields, huge, number};

/// `value` with each number as its first twelve significant digits and each object without its
/// null members, so values a JSON reader rounds alike compare alike.
fn read_alike(value: &Value) -> String {
    match value {
        Value::Number(number) => format!("{:.12e}", number.as_f64().unwrap_or(f64::NAN)),
        Value::Array(items) => {
            let items: Vec<String> = items.iter().map(read_alike).collect();
            format!("[{}]", items.join(","))
        }
        Value::Object(members) => {
            let members: BTreeMap<&String, String> = members
                .iter()
                .filter(|(_, member)| !member.is_null())
                .map(|(name, member)| (name, read_alike(member)))
                .collect();
            format!("{members:?}")
        }
        other => other.to_string(),
    }
}

#[test]
fn every_notation_of_a_number_reads_as_its_value() {
    for digits in [0, 7, 125, 4096] {
        for place in [-420, -30, -1, 0, 24, 420] {
            for negative in [false, true] {
                let sign = if negative { "-" } else { "" };
                let value: f64 = format!("{sign}{digits}e{place}").parse().expect("a number");
                for way in 0..4 {
                    let written = number(digits, place, negative, way);
                    let read: f64 = written.parse().expect("a number");
                    assert_eq!(read.to_bits(), value.to_bits(), "{written}");
                    assert!(
                        !written.trim_start_matches('-').starts_with("00"),
                        "{written} has a leading zero JSON refuses"
                    );
                }
            }
        }
    }
}

#[test]
fn a_document_reads_as_one_value_whichever_way_it_is_written() {
    for value in [Some("v12"), Some("s\"\\\u{1}"), None] {
        for n in [0, 1, 2, 3, 37] {
            let read = |way| {
                let written = document(value, n, way);
                let parsed: Value = serde_json::from_str(&written)
                    .unwrap_or_else(|error| panic!("{written}: {error}"));
                read_alike(&parsed)
            };
            let first = read(0);
            for way in 1..64 {
                assert_eq!(
                    read(way),
                    first,
                    "{value:?} {n} written the way {way} picks"
                );
            }
        }
    }
}

#[test]
fn every_way_writes_a_document_in_other_text() {
    let texts: BTreeSet<String> = (0..64).map(|way| document(Some("v1"), 3, way)).collect();
    assert_eq!(texts.len(), 64);
}

#[test]
fn every_column_carried_is_of_its_field_s_type_and_null_without_data() {
    let rows: Vec<Option<Data<'_>>> = (0..12)
        .map(|n| {
            (n % 5 != 4).then_some(Data {
                value: Some("v1"),
                n,
                way: n * 7,
            })
        })
        .collect();
    let fields: Vec<_> = fields().iter().map(Field::to_arrow).collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns(&rows))
        .expect("each column is of its field's type");
    for column in batch.columns() {
        assert!(column.is_null(4) && column.is_null(9), "{column:?}");
        assert_eq!(column.null_count(), 2, "{column:?}");
    }
}

#[test]
fn a_huge_amount_is_beyond_128_bits_within_its_precision() {
    for n in [1, 5, 9, 10, 120, 99_999] {
        let digits = huge(n).to_string().trim_start_matches('-').len();
        assert!((40..=60).contains(&digits), "{n}: {digits} digits");
    }
}
