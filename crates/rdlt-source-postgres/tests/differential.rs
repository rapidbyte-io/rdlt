//! T013 (research R8): differential property test — the binary-COPY decoder
//! and a driver-row reference path must produce IDENTICAL Arrow batches for
//! any generated typed row set. The reference decodes through completely
//! independent code (`FromSql` + text casts), so a bug must strike both
//! paths identically to hide.

mod common;

use std::sync::OnceLock;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Decimal128Builder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, Utc};
use common::PgFixture;
use proptest::prelude::*;
use rdlt_connector::{PushPayload, ReadRequest, Source as _, records_channel};
use rdlt_source_postgres::PostgresSource;
use std::sync::Arc;

#[derive(Debug, Clone)]
struct Row {
    id: i64,
    a: Option<i32>,
    b: Option<String>,
    c: Option<f64>,
    /// numeric(12,4) as a scale-4-scaled integer (|value| < 10^8).
    d: Option<i64>,
    /// µs since Unix epoch, kept in a comfortably-in-range window.
    e: Option<i64>,
    f: Option<bool>,
    g: Option<Vec<u8>>,
}

fn row_strategy() -> impl Strategy<Value = Row> {
    (
        any::<i64>(),
        proptest::option::of(any::<i32>()),
        proptest::option::of("[ -~—π🦀]{0,24}"), // printable + unicode, no NUL
        proptest::option::of(prop_oneof![
            any::<f64>(),
            Just(f64::NAN),
            Just(f64::INFINITY)
        ]),
        proptest::option::of(-999_999_999_999i64..=999_999_999_999), // |v| < 10^8 at scale 4
        proptest::option::of(-2_000_000_000_000_000i64..=4_000_000_000_000_000),
        proptest::option::of(any::<bool>()),
        proptest::option::of(proptest::collection::vec(any::<u8>(), 0..32)),
    )
        .prop_map(|(id, a, b, c, d, e, f, g)| Row {
            id,
            a,
            b,
            c,
            d,
            e,
            f,
            g,
        })
}

fn decimal_text(scaled: i64) -> String {
    let sign = if scaled < 0 { "-" } else { "" };
    let abs = scaled.unsigned_abs();
    format!("{sign}{}.{:04}", abs / 10_000, abs % 10_000)
}

fn shared() -> &'static (tokio::runtime::Runtime, PgFixture, String) {
    static SHARED: OnceLock<(tokio::runtime::Runtime, PgFixture, String)> = OnceLock::new();
    SHARED.get_or_init(|| {
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let fixture = rt.block_on(async {
            let f = PgFixture::start().await;
            f.seed(
                "CREATE TABLE diff (id int8 PRIMARY KEY, a int4, b text, c float8, \
                 d numeric(12,4), e timestamptz, f bool, g bytea);",
            )
            .await;
            f
        });
        let conn = fixture.conn_url();
        (rt, fixture, conn)
    })
}

/// The source-under-test path: drive `read()` directly, collect Arrow pushes.
async fn read_via_copy(conn: &str) -> RecordBatch {
    let source = PostgresSource::from_yaml(&format!("conn: \"{conn}\"\ntables:\n  - name: diff\n"))
        .expect("config");
    let specs = source.streams().await.expect("streams");
    let (out, mut input) = records_channel(64 << 20);
    let read = tokio::spawn(async move {
        source
            .read(ReadRequest::new(
                specs.into_iter().next().expect("spec"),
                None,
                out,
            ))
            .await
    });
    let mut batches = Vec::new();
    while let Some(push) = input.recv().await {
        match push.payload {
            PushPayload::Arrow(batch) => batches.push(batch),
            PushPayload::Checkpoint(_) => {}
            PushPayload::RawJson(_) => panic!("structured stream pushed raw json"),
        }
    }
    read.await.expect("join").expect("read");
    // ≤ 32 generated rows and a 64k-row batch cap ⇒ exactly one batch
    // (the empty-table schema batch when zero rows).
    assert_eq!(batches.len(), 1, "expected a single batch for ≤32 rows");
    let batch = batches.into_iter().next().expect("batch");
    // COPY snapshots stream in heap order; the reference query orders by id.
    // Sort here so the comparison is order-insensitive (a harness concern,
    // not product behavior).
    sort_by_id(&batch)
}

fn sort_by_id(batch: &RecordBatch) -> RecordBatch {
    use arrow_array::Int64Array;
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column");
    let mut order: Vec<u32> = (0..batch.num_rows() as u32).collect();
    order.sort_by_key(|&i| ids.value(i as usize));
    let indices = arrow_array::UInt32Array::from(order);
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| arrow_select::take::take(c, &indices, None).expect("take"))
        .collect();
    RecordBatch::try_new(batch.schema(), columns).expect("sorted batch")
}

/// The reference path: independent decode via driver `FromSql` + text casts.
async fn read_via_driver(fixture: &PgFixture) -> RecordBatch {
    let client = fixture.client().await;
    let rows = client
        .query(
            "SELECT id, a, b, c, d::text, e, f, g FROM diff ORDER BY id",
            &[],
        )
        .await
        .expect("reference query");
    let mut id = Int64Builder::new();
    let mut a = Int64Builder::new();
    let mut b = StringBuilder::new();
    let mut c = Float64Builder::new();
    let mut d = Decimal128Builder::new()
        .with_precision_and_scale(12, 4)
        .expect("decimal shape");
    let mut e = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    let mut f = BooleanBuilder::new();
    let mut g = BinaryBuilder::new();
    for row in &rows {
        id.append_value(row.get::<_, i64>(0));
        a.append_option(row.get::<_, Option<i32>>(1).map(i64::from));
        b.append_option(row.get::<_, Option<String>>(2));
        c.append_option(row.get::<_, Option<f64>>(3));
        d.append_option(row.get::<_, Option<String>>(4).map(|text| {
            let negative = text.starts_with('-');
            let digits: i128 = text
                .replace(['-', '.'], "")
                .parse()
                .expect("decimal digits");
            if negative { -digits } else { digits }
        }));
        e.append_option(
            row.get::<_, Option<DateTime<Utc>>>(5)
                .map(|ts| ts.timestamp_micros()),
        );
        f.append_option(row.get::<_, Option<bool>>(6));
        g.append_option(row.get::<_, Option<Vec<u8>>>(7));
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
        Field::new("c", DataType::Float64, true),
        Field::new("d", DataType::Decimal128(12, 4), true),
        Field::new(
            "e",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("f", DataType::Boolean, true),
        Field::new("g", DataType::Binary, true),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(id.finish()),
        Arc::new(a.finish()),
        Arc::new(b.finish()),
        Arc::new(c.finish()),
        Arc::new(d.finish()),
        Arc::new(e.finish()),
        Arc::new(f.finish()),
        Arc::new(g.finish()),
    ];
    RecordBatch::try_new(schema, arrays).expect("reference batch")
}

async fn seed_rows(fixture: &PgFixture, rows: &[Row]) {
    let client = fixture.client().await;
    client
        .execute("TRUNCATE diff", &[])
        .await
        .expect("truncate");
    let stmt = client
        .prepare(
            "INSERT INTO diff VALUES ($1, $2, $3, $4, $5::text::numeric, \
             to_timestamp($6::float8 / 1000000.0), $7, $8) ON CONFLICT (id) DO NOTHING",
        )
        .await
        .expect("prepare");
    for row in rows {
        let d_text = row.d.map(decimal_text);
        let e_us = row.e.map(|v| v as f64);
        client
            .execute(
                &stmt,
                &[
                    &row.id, &row.a, &row.b, &row.c, &d_text, &e_us, &row.f, &row.g,
                ],
            )
            .await
            .expect("insert");
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    #[test]
    fn copy_decoder_matches_driver_reference(rows in proptest::collection::vec(row_strategy(), 0..32)) {
        let (rt, fixture, conn) = shared();
        rt.block_on(async {
            seed_rows(fixture, &rows).await;
            let via_copy = read_via_copy(conn).await;
            let reference = read_via_driver(fixture).await;
            // NaN != NaN under arrow equality; compare float column textually.
            prop_assert_eq!(via_copy.num_rows(), reference.num_rows());
            prop_assert_eq!(via_copy.schema(), reference.schema());
            for (idx, (ours, theirs)) in via_copy.columns().iter().zip(reference.columns()).enumerate() {
                if idx == 3 {
                    prop_assert_eq!(
                        format!("{ours:?}"), format!("{theirs:?}"),
                        "float column (NaN-tolerant debug compare)"
                    );
                } else {
                    prop_assert_eq!(ours, theirs, "column {}", idx);
                }
            }
            Ok(())
        })?;
    }
}
