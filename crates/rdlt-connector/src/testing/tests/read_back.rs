//! Read-backs a destination controls, each of which fails its clause, typed.
//!
//! They hold more rows than a clause wrote, columns of other types, lengths and encodings, and
//! nulls where keys belong.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, Int32Array, NullArray, RecordBatch,
    RunArray, TimestampSecondArray, make_array,
};
use arrow_buffer::NullBuffer;
use serde_json::json;

use super::{Vault, VaultProbe, vault};
use crate::destination::TableRef;
use crate::error::Result;
use crate::spec::BoxFuture;
use crate::testing::limits::REASON_BYTES;
use crate::testing::{Outcome, Probe, Report, certify_destination};

/// A probe that changes what the vault reads back.
struct Tampered<F>(VaultProbe, F);

impl<F> Probe for Tampered<F>
where
    F: Fn(RecordBatch) -> Vec<RecordBatch> + Send + Sync,
{
    fn published<'a>(&'a self, table: &'a TableRef) -> BoxFuture<'a, Result<Vec<RecordBatch>>> {
        Box::pin(async move {
            let batches = self.0.published(table).await?;
            Ok(batches.into_iter().flat_map(&self.1).collect())
        })
    }
}

/// Certifies a correct vault called `name` whose read-backs `tamper` changes, batch by batch.
async fn certify_tampered<F>(name: &str, tamper: F) -> Report
where
    F: Fn(RecordBatch) -> Vec<RecordBatch> + Send + Sync,
{
    let probe = Tampered(VaultProbe(vault(name)), tamper);
    certify_destination::<Vault>(json!({ "store": name }), &probe).await
}

/// `batch` with its column `name`, where it has one, replaced by what `with` makes of it.
fn replaced(
    batch: &RecordBatch,
    name: &str,
    with: impl Fn(&ArrayRef) -> ArrayRef,
) -> Vec<RecordBatch> {
    let schema = batch.schema();
    let columns = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| {
            let column = if field.name() == name {
                with(column)
            } else {
                Arc::clone(column)
            };
            (field.name().clone(), column, true)
        });
    vec![RecordBatch::try_from_iter_with_nullable(columns.collect::<Vec<_>>()).unwrap()]
}

/// `column` with every value null, each still in its buffer under the null.
fn nulled(column: &ArrayRef) -> ArrayRef {
    let data = column
        .to_data()
        .into_builder()
        .nulls(Some(NullBuffer::new_null(column.len())))
        .build()
        .unwrap();
    make_array(data)
}

/// The clauses of `report` that failed.
fn failed(report: &Report) -> Vec<&'static str> {
    report.failures().map(|result| result.clause.id).collect()
}

/// The clauses that read back tables of the certification schema, or its `id` and `name`.
const KEYED: [&str; 14] = [
    "D-COMMIT",
    "D-IDEMPOTENT",
    "D-DISCARD",
    "D-REPLACE",
    "D-SCHEMA",
    "D-MERGE",
    "D-DELETE",
    "D-PARTIAL",
    "D-TRUNCATE",
    "D-HIST",
    "D-ENCODING",
    "D-TABLES",
    "D-LANES",
    "D-OWNED",
];

#[tokio::test]
async fn a_read_back_of_rows_that_cost_no_bytes_fails_its_clauses_within_bounds() {
    // A million rows a batch, of a bit and of nothing each: about 131 KiB read back.
    let rows = 1 << 20;
    let began = Instant::now();
    let report = certify_tampered("read_back_flood", move |batch| {
        if batch.column_by_name("id").is_none() {
            return vec![batch];
        }
        let ids: ArrayRef = Arc::new(BooleanArray::from(vec![true; rows]));
        let names: ArrayRef = Arc::new(NullArray::new(rows));
        vec![RecordBatch::try_from_iter([("id", ids), ("name", names)]).unwrap(); 4]
    })
    .await;
    for clause in KEYED {
        assert!(failed(&report).contains(&clause), "{clause}: {report}");
    }
    for result in &report.results {
        if let Outcome::Failed(reason) = &result.outcome {
            assert!(reason.len() <= REASON_BYTES, "{}", result.clause.id);
        }
    }
    // Expanding and rendering the rows takes minutes; refusing them takes none.
    assert!(
        began.elapsed() < Duration::from_secs(60),
        "{:?}",
        began.elapsed()
    );
}

#[tokio::test]
async fn a_cast_that_returns_another_length_fails_the_clause_instead_of_panicking() {
    let report = certify_tampered("read_back_zero_width", |batch| {
        replaced(&batch, "_rdlt_seq", |column| {
            let empty = vec![Some(Vec::<u8>::new())];
            let values =
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(empty.into_iter(), 0).unwrap();
            let ends = Int32Array::from(vec![i32::try_from(column.len()).unwrap()]);
            Arc::new(RunArray::<Int32Type>::try_new(&ends, &values).unwrap())
        })
    })
    .await;
    assert!(failed(&report).contains(&"D-HIST"), "{report}");
}

#[tokio::test]
async fn a_zoned_timestamp_read_back_where_text_was_written_fails_instead_of_panicking() {
    for column in ["name", "value"] {
        let store = format!("read_back_timestamp_{column}");
        let report = certify_tampered(&store, move |batch| {
            replaced(&batch, column, |written| {
                // An instant no calendar holds once its zone's offset is added.
                let edge = vec![8_210_266_876_799_i64; written.len()];
                Arc::new(TimestampSecondArray::from(edge).with_timezone("+14:00"))
            })
        })
        .await;
        let clause = if column == "name" {
            "D-COMMIT"
        } else {
            "D-CHILDREN"
        };
        assert!(failed(&report).contains(&clause), "{column}: {report}");
    }
}

#[tokio::test]
async fn a_null_key_read_back_fails_every_clause_that_reads_keys() {
    // Each value still lies in its buffer, under the null.
    let report =
        certify_tampered("read_back_null_ids", |batch| replaced(&batch, "id", nulled)).await;
    // The schema clause reads the columns it adds and widens, not the keys.
    for clause in KEYED.into_iter().filter(|clause| *clause != "D-SCHEMA") {
        assert!(failed(&report).contains(&clause), "{clause}: {report}");
    }
}

#[tokio::test]
async fn a_null_in_a_history_column_that_holds_none_fails_its_clause() {
    for column in ["_rdlt_valid_from", "_rdlt_is_current", "_rdlt_seq"] {
        let store = format!("read_back_null{column}");
        let report = certify_tampered(&store, move |batch| replaced(&batch, column, nulled)).await;
        assert!(failed(&report).contains(&"D-HIST"), "{column}: {report}");
    }
}

#[tokio::test]
async fn rows_read_back_beside_the_published_ones_with_no_key_fail_their_clause() {
    for (column, clause) in [("id", "D-ENCODING"), ("value", "D-CHILDREN")] {
        let store = format!("read_back_extra_{column}");
        let report = certify_tampered(&store, move |batch| {
            if batch.column_by_name(column).is_none() {
                return vec![batch];
            }
            // The published rows, and as many again that hold nothing.
            let schema = batch.schema();
            let nulls = schema
                .fields()
                .iter()
                .map(|field| arrow_array::new_null_array(field.data_type(), batch.num_rows()));
            let names = schema.fields().iter().map(|field| field.name().clone());
            let extra = names.zip(nulls).map(|(name, nulls)| (name, nulls, true));
            let extra = RecordBatch::try_from_iter_with_nullable(extra.collect::<Vec<_>>());
            let published = replaced(&batch, "", Arc::clone).remove(0);
            vec![published, extra.unwrap()]
        })
        .await;
        assert!(failed(&report).contains(&clause), "{column}: {report}");
    }
}
