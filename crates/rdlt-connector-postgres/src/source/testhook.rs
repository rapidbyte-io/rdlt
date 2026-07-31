//! Test-only surface (hidden): lets integration suites drive reflection
//! without going through a full pipeline. Not a public API.

use std::collections::BTreeMap;

use rdlt_connector::SourceError;

pub use crate::source::reflect::{ReflectedColumn, ReflectedTable};

/// CDC lifecycle surface for the integration suites.
pub use crate::source::cdc::slot as cdc_slot;

pub async fn reflect_for_tests(
    config: &crate::source::PostgresConfig,
) -> Result<BTreeMap<String, ReflectedTable>, SourceError> {
    let client = crate::source::connect(config).await?;
    crate::source::reflect::reflect_schema(&client, config).await
}

/// Canned binary-COPY stream for the gated decoder bench (iai_pg):
/// `rows` tuples over a representative column mix (int8 pk, int4, float8,
/// text, timestamptz, bool, uuid, jsonb). Deterministic bytes.
pub fn bench_wire(rows: usize) -> Vec<u8> {
    let mut wire = b"PGCOPY\n\xff\r\n\0".to_vec();
    wire.extend_from_slice(&0i32.to_be_bytes());
    wire.extend_from_slice(&0i32.to_be_bytes());
    let field = |wire: &mut Vec<u8>, bytes: &[u8]| {
        wire.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
        wire.extend_from_slice(bytes);
    };
    for i in 0..rows as i64 {
        wire.extend_from_slice(&8i16.to_be_bytes());
        field(&mut wire, &i.to_be_bytes());
        field(&mut wire, &((i % 100_000) as i32).to_be_bytes());
        field(&mut wire, &(i as f64 * 0.5).to_be_bytes());
        field(&mut wire, format!("user-{i}").as_bytes());
        field(&mut wire, &(i * 1_000_000).to_be_bytes()); // µs since PG epoch
        field(&mut wire, &[(i % 2) as u8]);
        let mut uuid = [0u8; 16];
        uuid[8..].copy_from_slice(&i.to_be_bytes());
        field(&mut wire, &uuid);
        let mut jsonb = vec![1u8];
        jsonb.extend_from_slice(
            format!(r#"{{"city":"NYC","zip":{}}}"#, 10_001 + i % 100).as_bytes(),
        );
        field(&mut wire, &jsonb);
    }
    wire.extend_from_slice(&(-1i16).to_be_bytes());
    wire
}

/// The gated decoder hot path (bench body): full stream -> Arrow batches;
/// returns decoded rows so the work cannot be optimized away.
pub fn bench_decode(wire: &[u8]) -> u64 {
    use crate::source::copy_decode::{CopyDecoder, FieldPlan};
    use crate::source::type_map::Decode;
    use arrow_schema::{DataType, TimeUnit};

    let plans = vec![
        FieldPlan {
            name: "id".into(),
            decode: Decode::Int8,
            arrow: DataType::Int64,
            not_null: true,
        },
        FieldPlan {
            name: "small".into(),
            decode: Decode::Int4,
            arrow: DataType::Int64,
            not_null: false,
        },
        FieldPlan {
            name: "ratio".into(),
            decode: Decode::Float8,
            arrow: DataType::Float64,
            not_null: false,
        },
        FieldPlan {
            name: "name".into(),
            decode: Decode::Utf8,
            arrow: DataType::Utf8,
            not_null: false,
        },
        FieldPlan {
            name: "at".into(),
            decode: Decode::Timestamp { tz: true },
            arrow: DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            not_null: false,
        },
        FieldPlan {
            name: "ok".into(),
            decode: Decode::Bool,
            arrow: DataType::Boolean,
            not_null: false,
        },
        FieldPlan {
            name: "token".into(),
            decode: Decode::UuidText,
            arrow: DataType::Utf8,
            not_null: false,
        },
        FieldPlan {
            name: "doc".into(),
            decode: Decode::JsonbText,
            arrow: DataType::Utf8,
            not_null: false,
        },
    ];
    let mut decoder = CopyDecoder::new(plans, 8 << 20, 65_536).expect("bench plans are valid");
    // Feed in 64 KiB chunks — socket-realistic boundaries.
    let mut rows = 0u64;
    for chunk in wire.chunks(64 << 10) {
        let batches = decoder.feed(chunk).expect("bench wire is valid");
        rows += batches.iter().map(|b| b.num_rows() as u64).sum::<u64>();
    }
    if let Some(tail) = decoder.finish().expect("trailer") {
        rows += tail.num_rows() as u64;
    }
    rows
}

/// Fuzz entry (targets/pg_pgoutput_decode): arbitrary bytes through the
/// logical-replication message parser — typed errors only, never a panic.
pub fn fuzz_pgoutput_decode(data: &[u8]) {
    let _ = crate::source::cdc::pgoutput::parse(data);
}

/// Fuzz entry (targets/pg_copy_decode): arbitrary bytes through the
/// decoder over a representative multi-type plan — typed errors only,
/// never a panic. The first fuzz byte splits the input into two feeds so
/// chunk-boundary states get fuzzed too.
pub fn fuzz_copy_decode(data: &[u8]) {
    use crate::source::copy_decode::{CopyDecoder, FieldPlan};
    use crate::source::type_map::Decode;
    use arrow_schema::{DataType, TimeUnit};

    let plans = vec![
        FieldPlan {
            name: "a".into(),
            decode: Decode::Int8,
            arrow: DataType::Int64,
            not_null: true,
        },
        FieldPlan {
            name: "b".into(),
            decode: Decode::Utf8,
            arrow: DataType::Utf8,
            not_null: false,
        },
        FieldPlan {
            name: "c".into(),
            decode: Decode::Decimal {
                precision: 10,
                scale: 2,
            },
            arrow: DataType::Decimal128(10, 2),
            not_null: false,
        },
        FieldPlan {
            name: "d".into(),
            decode: Decode::Timestamp { tz: true },
            arrow: DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            not_null: false,
        },
        FieldPlan {
            name: "e".into(),
            decode: Decode::UuidText,
            arrow: DataType::Utf8,
            not_null: false,
        },
        FieldPlan {
            name: "f".into(),
            decode: Decode::JsonbText,
            arrow: DataType::Utf8,
            not_null: false,
        },
        FieldPlan {
            name: "g".into(),
            decode: Decode::Bytea,
            arrow: DataType::Binary,
            not_null: false,
        },
        FieldPlan {
            name: "h".into(),
            decode: Decode::Bool,
            arrow: DataType::Boolean,
            not_null: false,
        },
    ];
    let Ok(mut decoder) = CopyDecoder::new(plans, 4096, 64) else {
        return; // fixed fuzz plans are valid; a build failure is not the target
    };
    let Some((&split, rest)) = data.split_first() else {
        return;
    };
    let cut = (split as usize).min(rest.len());
    let (one, two) = rest.split_at(cut);
    if decoder.feed(one).is_err() {
        return;
    }
    if decoder.feed(two).is_err() {
        return;
    }
    let _ = decoder.finish();
}
