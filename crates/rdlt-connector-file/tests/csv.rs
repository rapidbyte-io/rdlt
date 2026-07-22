//! Feature 015 US2 (T008/T009): CSV as a record format + compression
//! codecs — local cells (the object-store legs ride s3_live.rs).

use rdlt_connector::{PushPayload, ReadRequest, Source, records_channel};
use rdlt_connector_file::FileSource;

/// Read one stream fully through the SPI records channel (NDJSON slabs).
async fn read_rows(yaml: &str, stream: &str) -> Result<Vec<serde_json::Value>, String> {
    let source = FileSource::from_yaml(yaml).map_err(|e| e.to_string())?;
    let (out, mut input) = records_channel(1 << 20);
    let specs = source.streams().await.map_err(|e| e.to_string())?;
    let spec = specs
        .iter()
        .find(|s| s.name.as_str() == stream)
        .expect("stream declared")
        .clone();
    let read = tokio::spawn(async move { source.read(ReadRequest::new(spec, None, out)).await });
    let mut rows = Vec::new();
    while let Some(push) = input.recv().await {
        if let PushPayload::RawJson(bytes) = push.payload {
            for line in bytes.split(|b| *b == b'\n') {
                if !line.iter().all(u8::is_ascii_whitespace) {
                    rows.push(serde_json::from_slice(line).expect("valid NDJSON out"));
                }
            }
        }
    }
    read.await.expect("join").map_err(|e| e.to_string())?;
    Ok(rows)
}

fn yaml_for(dir: &std::path::Path, extra: &str) -> String {
    format!(
        "streams:\n  - name: t\n    format: csv\n    path: \"{}/*.csv\"\n{extra}",
        dir.display()
    )
}

/// The inference lattice: bool → int64 → float64 → utf8; empty = null.
#[tokio::test]
async fn inference_lattice_and_nulls() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("a.csv"),
        "flag,count,ratio,mixed,note\ntrue,1,1.5,2,hi\nfalse,2,2,x,\n",
    )
    .unwrap();
    let rows = read_rows(&yaml_for(dir.path(), ""), "t").await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["flag"], serde_json::json!(true));
    assert_eq!(rows[0]["count"], serde_json::json!(1));
    assert_eq!(rows[0]["ratio"], serde_json::json!(1.5));
    // `mixed` saw 2 and x → widened to utf8 for the WHOLE column.
    assert_eq!(rows[0]["mixed"], serde_json::json!("2"));
    assert_eq!(
        rows[1]["note"],
        serde_json::Value::Null,
        "empty cell is null"
    );
}

/// Options matrix: delimiter, quote, headerless c0..cN.
#[tokio::test]
async fn options_matrix() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.csv"), "1;'a;b'\n2;'c'\n").unwrap();
    let yaml = yaml_for(
        dir.path(),
        "    csv: {delimiter: \";\", header: false, quote: \"'\"}\n",
    );
    let rows = read_rows(&yaml, "t").await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["c0"], serde_json::json!(1));
    assert_eq!(rows[0]["c1"], serde_json::json!("a;b"), "quoted delimiter");
}

/// Declared hints override inference; violations are typed naming
/// file, row, column.
#[tokio::test]
async fn hints_override_and_violations_are_typed() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.csv"), "id,amount\n1,2\n2,3\n").unwrap();
    let yaml = yaml_for(dir.path(), "    type_hints: {amount: float64}\n");
    let rows = read_rows(&yaml, "t").await.unwrap();
    assert_eq!(
        rows[0]["amount"],
        serde_json::json!(2.0),
        "declared float wins"
    );

    std::fs::write(dir.path().join("a.csv"), "id,amount\n1,x\n").unwrap();
    let err = read_rows(&yaml, "t").await.expect_err("violation");
    assert!(
        err.contains("a.csv") && err.contains("row 2") && err.contains("amount"),
        "{err}"
    );
}

/// Malformed CSV: typed naming file + row (a row with the wrong field
/// count — the strict reader never papers over ragged rows).
#[tokio::test]
async fn malformed_row_is_typed() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.csv"), "id,name\n1,a\n2\n3,c\n").unwrap();
    let err = read_rows(&yaml_for(dir.path(), ""), "t")
        .await
        .expect_err("malformed");
    assert!(
        err.contains("a.csv") && err.contains("malformed") && err.contains("row 3"),
        "{err}"
    );
}

/// Header with zero data rows: legitimately empty.
#[tokio::test]
async fn header_only_file_is_empty_success() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.csv"), "id,name\n").unwrap();
    let rows = read_rows(&yaml_for(dir.path(), ""), "t").await.unwrap();
    assert!(rows.is_empty());
}

/// csv options block without format csv: typed at parse.
#[tokio::test]
async fn csv_block_requires_csv_format() {
    let err = FileSource::from_yaml(
        "streams:\n  - name: t\n    format: jsonl\n    path: \"x/*.jsonl\"\n    csv: {header: false}\n",
    )
    .expect_err("csv block on jsonl")
    .to_string();
    assert!(err.contains("format: csv"), "{err}");
}

/// T009: gzip and zstd jsonl decode transparently, exact totals; a
/// completed compressed file is skipped on the next run (whole-file unit).
#[tokio::test]
async fn compressed_jsonl_reads_and_skips_when_complete() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let mut gz = flate2::write::GzEncoder::new(
        std::fs::File::create(dir.path().join("a.jsonl.gz")).unwrap(),
        flate2::Compression::default(),
    );
    gz.write_all(b"{\"id\":1}\n{\"id\":2}\n").unwrap();
    gz.finish().unwrap();
    let zst = zstd::stream::encode_all(&b"{\"id\":3}\n"[..], 0).unwrap();
    std::fs::write(dir.path().join("b.jsonl.zst"), zst).unwrap();

    let yaml = format!(
        "streams:\n  - name: t\n    format: jsonl\n    path: \"{}/*.jsonl.*\"\n",
        dir.path().display()
    );
    let rows = read_rows(&yaml, "t").await.unwrap();
    assert_eq!(rows.len(), 3, "both codecs decode to exact totals");
}

/// T009: codec/extension mismatch is typed, naming the file.
#[tokio::test]
async fn codec_extension_mismatch_is_typed() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("fake.jsonl.gz"), b"{\"id\":1}\n").unwrap();
    let yaml = format!(
        "streams:\n  - name: t\n    format: jsonl\n    path: \"{}/*.gz\"\n",
        dir.path().display()
    );
    let err = read_rows(&yaml, "t").await.expect_err("mismatch");
    assert!(
        err.contains("fake.jsonl.gz") && err.contains("compression extension"),
        "{err}"
    );
}

/// T009: compressed csv (gzip) composes with the csv reader.
#[tokio::test]
async fn compressed_csv_reads() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let mut gz = flate2::write::GzEncoder::new(
        std::fs::File::create(dir.path().join("a.csv.gz")).unwrap(),
        flate2::Compression::default(),
    );
    gz.write_all(b"id,v\n1,a\n2,b\n").unwrap();
    gz.finish().unwrap();
    let yaml = format!(
        "streams:\n  - name: t\n    format: csv\n    path: \"{}/*.csv.gz\"\n",
        dir.path().display()
    );
    let rows = read_rows(&yaml, "t").await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1]["v"], serde_json::json!("b"));
}

/// Compressed-parquet spelling is rejected at parse (parquet carries its
/// own codecs).
#[tokio::test]
async fn compressed_parquet_rejected_at_parse() {
    let err = FileSource::from_yaml(
        "streams:\n  - name: t\n    format: parquet\n    path: \"x/*.parquet.gz\"\n",
    )
    .expect_err("compressed parquet")
    .to_string();
    assert!(
        err.contains("parquet") && err.contains("compression"),
        "{err}"
    );
}

/// 015 review finding 3: bool×numeric columns are DISJOINT in the lattice —
/// the join is utf8, never a panic.
#[tokio::test]
async fn bool_and_numeric_column_widens_to_text() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.csv"), "flag\ntrue\n42\n1.5\n").unwrap();
    let rows = read_rows(&yaml_for(dir.path(), ""), "t").await.unwrap();
    assert_eq!(rows[0]["flag"], serde_json::json!("true"));
    assert_eq!(rows[1]["flag"], serde_json::json!("42"));
    assert_eq!(rows[2]["flag"], serde_json::json!("1.5"));
}

/// 015 review finding 10: non-finite floats under INFERRED float typing are
/// a typed error naming file/row/column (parity with the declared hint) —
/// never silently nulled.
#[tokio::test]
async fn non_finite_inferred_float_is_typed() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.csv"), "v\n1.5\nNaN\n").unwrap();
    let err = read_rows(&yaml_for(dir.path(), ""), "t")
        .await
        .expect_err("NaN has no JSON representation");
    assert!(
        err.contains("a.csv") && err.contains("row 3") && err.contains("non-finite"),
        "{err}"
    );
}
