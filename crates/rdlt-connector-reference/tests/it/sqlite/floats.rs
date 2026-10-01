//! Floats SQLite would store as another value are refused, never changed.

use std::sync::Arc;

use arrow_array::{ArrayRef, Float32Array, Float64Array, RecordBatch};
use rdlt_connector::{ConnectorErrorKind, Field, LogicalType, SegmentId, TableChange, TableSchema};

use super::kit::{Shared, refusal, table};

#[tokio::test]
async fn nan_and_negative_zero_are_refused_and_nothing_of_their_batch_is_staged() {
    let shared = Shared::new().await;
    let mut session = shared.open("p", 1).await;
    let floats = table("floats", "floats", false);
    let schema = TableSchema::new(vec![
        Field::new("wide", LogicalType::Float64, true),
        Field::new("narrow", LogicalType::Float32, true),
    ])
    .expect("the schema is valid");
    let create = TableChange::Create {
        table: floats.clone(),
        schema,
    };
    session
        .session
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let batches: [(Vec<f64>, Vec<f32>); 4] = [
        (vec![1.5, f64::NAN], vec![1.5, 2.5]),
        (vec![1.5, -0.0], vec![1.5, 2.5]),
        (vec![1.5, 2.5], vec![1.5, f32::NAN]),
        (vec![1.5, 2.5], vec![1.5, -0.0]),
    ];
    for (wide, narrow) in batches {
        let batch = RecordBatch::try_from_iter([
            ("wide", Arc::new(Float64Array::from(wide)) as ArrayRef),
            ("narrow", Arc::new(Float32Array::from(narrow)) as _),
        ])
        .expect("a valid batch");
        let mut writer = session
            .session
            .session
            .writer(&floats)
            .await
            .expect("a writer opens");
        writer
            .write(SegmentId(1), batch)
            .await
            .expect("the write buffers");
        assert_eq!(
            refusal(writer.flush().await),
            (
                ConnectorErrorKind::Data,
                Some("float_unstorable".to_owned())
            )
        );
        assert_eq!(
            shared.count("SELECT count(*) FROM _rdlt_staging__floats"),
            0
        );
    }
}
