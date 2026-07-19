//! T016: end-to-end parquet → parquet copy through the engine — the structured
//! passthrough path with real files on both ends (spec US2; the ≥2× benchmark path).

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rdlt_dest_parquet::ParquetDir;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_source_file::FileSource;

fn write_parquet(path: &std::path::Path, ids: &[i64]) {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score", DataType::Float64, true),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Float64Array::from(
                ids.iter().map(|i| *i as f64 * 0.5).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter().map(|i| format!("row-{i}")).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch");
    let file = std::fs::File::create(path).expect("create");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
}

fn source_for(dir: &std::path::Path) -> FileSource {
    FileSource::from_yaml(&format!(
        "streams:\n  - name: metrics\n    format: parquet\n    path: \"{}/*.parquet\"\n",
        dir.display()
    ))
    .expect("config")
}

#[tokio::test(flavor = "multi_thread")]
async fn parquet_to_parquet_copy_preserves_types_and_resumes() {
    let input = tempfile::tempdir().expect("tempdir");
    let output = tempfile::tempdir().expect("tempdir");
    write_parquet(&input.path().join("a.parquet"), &[1, 2, 3]);

    let dest = ParquetDir::open(output.path().join("out")).expect("open");
    let report = Engine::new(
        EngineConfig::new("pq-copy"),
        source_for(input.path()),
        dest.clone(),
    )
    .run()
    .await
    .expect("run 1");
    assert_eq!(report.total_rows(), 3);
    assert_eq!(dest.count_rows("metrics").expect("count"), 3);

    // Types survived: read a published part file back and check the schema.
    let table_dir = output.path().join("out").join("metrics");
    let part = std::fs::read_dir(&table_dir)
        .expect("read dir")
        .filter_map(Result::ok)
        .find(|e| e.path().extension().is_some_and(|x| x == "parquet"))
        .expect("published part file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(part.path()).expect("open part"),
    )
    .expect("reader");
    let schema = reader.schema().clone();
    assert_eq!(
        schema.field_with_name("id").expect("id").data_type(),
        &DataType::Int64,
        "no re-inference: Int64 stays Int64"
    );
    assert!(
        schema.field_with_name("_rdlt_load_id").is_ok(),
        "provenance column present"
    );

    // Run 2: a new input file appears → only its rows are copied.
    write_parquet(&input.path().join("b.parquet"), &[4]);
    let report = Engine::new(
        EngineConfig::new("pq-copy"),
        source_for(input.path()),
        dest.clone(),
    )
    .run()
    .await
    .expect("run 2");
    assert_eq!(report.total_rows(), 1, "row-group cursor resume");
    assert_eq!(
        dest.count_rows("metrics").expect("count"),
        4,
        "no duplicates"
    );
}
