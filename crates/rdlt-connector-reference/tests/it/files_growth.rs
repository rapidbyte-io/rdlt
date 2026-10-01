//! What the files destination keeps is bounded by what it publishes, and what it publishes reads
//! back as it was written.

use std::path::Path;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use rdlt_connector::{
    CommitSeq, ConnectorErrorKind, Field, GenerationId, LogicalType, PublishedReader, TableChange,
    TableRef,
};
use serde_json::json;

use crate::files_source::{poison, poisons};
use crate::fixtures::{
    connect_with, dirs_under, files_under, ids, keyed, latest_manifest, merge_table, meta, open,
    pipeline_dir, published_ids, stage, table,
};

/// The data files under `root`, by their extension.
fn data_files(root: &Path, extension: &str) -> Vec<std::path::PathBuf> {
    files_under(&root.join("_rdlt").join("pipelines"))
        .into_iter()
        .filter(|path| path.extension().is_some_and(|found| found == extension))
        .collect()
}

#[tokio::test]
async fn a_merge_commit_removes_the_copy_it_replaced() {
    for format in ["jsonl", "arrow"] {
        let root = crate::fixtures::tempdir().unwrap();
        let (destination, reader) = connect_with(root.path(), json!({ "format": format })).await;
        let mut opened = open(destination.as_ref(), 1).await;
        let rows = merge_table("rows");
        let mut seq = CommitSeq::FIRST;
        for commit in 1..=12_u8 {
            let (schema, batch) = if commit == 1 {
                keyed(&(0..500).collect::<Vec<_>>(), 1)
            } else {
                keyed(&[0], commit)
            };
            stage(&mut opened, &rows, &schema, batch, u64::from(commit)).await;
            opened
                .session
                .commit(&meta(&opened, 1, seq, &[u64::from(commit)]))
                .await
                .unwrap();
            seq = seq.next();
            let files = data_files(root.path(), format);
            assert_eq!(files.len(), 1, "{format} commit {commit}: {files:?}");
        }
        assert_eq!(published_ids(reader.as_ref(), &rows).await.len(), 500);
        // The directories of what went are gone with it.
        let staging = dirs_under(&pipeline_dir(root.path()).join("staging"));
        assert!(staging.len() <= 6, "{format}: {staging:?}");
    }
}

#[tokio::test]
async fn a_finished_generation_removes_the_files_it_replaced() {
    let root = crate::fixtures::tempdir().unwrap();
    let (destination, reader) = connect_with(root.path(), json!({})).await;
    let mut opened = open(destination.as_ref(), 1).await;
    let rows = table("rows");
    let mut seq = CommitSeq::FIRST;
    for generation in 1..=6_u64 {
        let filling = TableRef {
            generation: Some(GenerationId(generation)),
            ..rows.clone()
        };
        let (schema, batch) = ids(&[i64::try_from(generation).unwrap()]);
        stage(&mut opened, &filling, &schema, batch, generation).await;
        let mut finishing = meta(&opened, 1, seq, &[generation]);
        finishing.finish_generations = vec![(rows.path.clone(), GenerationId(generation))];
        opened.session.commit(&finishing).await.unwrap();
        seq = seq.next();
        assert_eq!(data_files(root.path(), "jsonl").len(), 1, "{generation}");
    }
    assert_eq!(published_ids(reader.as_ref(), &rows).await, [6]);
}

#[tokio::test]
async fn an_append_table_lists_a_bounded_number_of_files_and_keeps_every_row_in_order() {
    for format in ["jsonl", "arrow"] {
        let root = crate::fixtures::tempdir().unwrap();
        let (destination, reader) = connect_with(root.path(), json!({ "format": format })).await;
        let mut opened = open(destination.as_ref(), 1).await;
        let rows = table("rows");
        let mut seq = CommitSeq::FIRST;
        let mut expected = Vec::new();
        // Enough commits to tell a list that grows with them from one that grows with their
        // logarithm, and no more: each takes a dozen syncs, which some file systems make slowly.
        for commit in 0..48_i64 {
            // Three checkpointed batches a commit, each its own segment.
            let mut segments = Vec::new();
            for part in 0..3 {
                let id = commit * 3 + part;
                let (schema, batch) = ids(&[id]);
                let segment = u64::try_from(id).unwrap() + 1;
                stage(&mut opened, &rows, &schema, batch, segment).await;
                segments.push(segment);
                expected.push(id);
            }
            opened
                .session
                .commit(&meta(&opened, 1, seq, &segments))
                .await
                .unwrap();
            seq = seq.next();
        }
        assert_eq!(
            published_ids(reader.as_ref(), &rows).await,
            expected,
            "{format}"
        );
        let files = data_files(root.path(), format);
        // 144 rows in files that at least halve: at most eight.
        assert!(files.len() <= 8, "{format}: {} files", files.len());
        let (_, manifest) = latest_manifest(root.path());
        let listed = manifest["tables"]["rows"]["files"]
            .as_array()
            .unwrap()
            .len();
        assert_eq!(listed, files.len(), "{format}");
        let staging = dirs_under(&pipeline_dir(root.path()).join("staging"));
        assert!(
            staging.len() <= 8 * 6,
            "{format}: {} directories",
            staging.len()
        );
    }
}

#[tokio::test]
async fn an_append_table_whose_schema_changed_keeps_every_row() {
    let root = crate::fixtures::tempdir().unwrap();
    let (destination, reader) = connect_with(root.path(), json!({ "format": "arrow" })).await;
    let mut opened = open(destination.as_ref(), 1).await;
    let rows = table("rows");
    let mut seq = CommitSeq::FIRST;
    for commit in 0..40_i64 {
        let (schema, batch) = ids(&[commit]);
        let segment = u64::try_from(commit).unwrap() + 1;
        if commit < 20 {
            stage(&mut opened, &rows, &schema, batch, segment).await;
        } else {
            // From here every batch holds a second column.
            let add = TableChange::AddColumn {
                table: rows.clone(),
                field: Field::new("more", LogicalType::Int64, true),
            };
            opened.session.apply_schema(&add).await.unwrap();
            let wider = RecordBatch::try_from_iter([
                ("id", Arc::clone(batch.column(0))),
                ("more", Arc::new(Int64Array::from(vec![commit])) as ArrayRef),
            ])
            .unwrap();
            let mut writer = opened.session.writer(&rows).await.unwrap();
            writer
                .write(rdlt_connector::SegmentId(segment), wider)
                .await
                .unwrap();
            writer.flush().await.unwrap();
        }
        opened
            .session
            .commit(&meta(&opened, 1, seq, &[segment]))
            .await
            .unwrap();
        seq = seq.next();
    }
    let all: Vec<i64> = (0..40).collect();
    assert_eq!(published_ids(reader.as_ref(), &rows).await, all);
    assert!(data_files(root.path(), "arrow").len() <= 16);
}

#[tokio::test]
async fn a_commit_repeated_however_far_back_in_its_load_is_answered_with_its_receipt() {
    // A replay repeats a commit whose frame its log still holds, however many commits its load
    // made since: each is answered, none refused and none published again.
    let root = crate::fixtures::tempdir().unwrap();
    let (destination, reader) = connect_with(root.path(), json!({})).await;
    let mut opened = open(destination.as_ref(), 1).await;
    let rows = table("rows");
    let mut seq = CommitSeq::FIRST;
    let mut receipts = Vec::new();
    for commit in 0..40_i64 {
        let (schema, batch) = ids(&[commit]);
        let segment = u64::try_from(commit).unwrap() + 1;
        stage(&mut opened, &rows, &schema, batch, segment).await;
        let committing = meta(&opened, 1, seq, &[segment]);
        receipts.push((
            committing.clone(),
            opened.session.commit(&committing).await.unwrap(),
        ));
        seq = seq.next();
    }
    let version = latest_manifest(root.path()).1["version"].clone();
    for (committing, receipt) in &receipts {
        let again = opened.session.commit(committing).await.unwrap();
        assert_eq!(&again, receipt);
    }
    assert_eq!(latest_manifest(root.path()).1["version"], version);
    let all: Vec<i64> = (0..40).collect();
    assert_eq!(published_ids(reader.as_ref(), &rows).await, all);
}

#[tokio::test]
async fn superseded_catalog_versions_are_removed() {
    let root = crate::fixtures::tempdir().unwrap();
    let (destination, _) = connect_with(root.path(), json!({})).await;
    let mut opened = open(destination.as_ref(), 1).await;
    let (schema, batch) = ids(&[1]);
    stage(&mut opened, &table("t"), &schema, batch, 1).await;
    for column in 0..40 {
        let add = TableChange::AddColumn {
            table: table("t"),
            field: Field::new(format!("c{column}"), LogicalType::Int64, true),
        };
        opened.session.apply_schema(&add).await.unwrap();
    }
    let catalog = root.path().join("_rdlt").join("tables").join("t");
    let versions: Vec<_> = files_under(&catalog)
        .into_iter()
        .filter(|path| path.extension().is_some_and(|found| found == "json"))
        .collect();
    assert_eq!(versions.len(), 9, "{versions:?}");
    let schema = rdlt_connector_reference::files::published(root.path(), "t");
    assert!(schema.is_ok());
}

async fn read_back(reader: &dyn PublishedReader, table: &TableRef) -> Option<ConnectorErrorKind> {
    reader
        .published(table)
        .await
        .err()
        .map(|error| error.kind())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_published_arrow_file_whose_block_lies_outside_it_is_refused() {
    for (field, value) in poisons() {
        let root = crate::fixtures::tempdir().unwrap();
        let (destination, reader) = connect_with(root.path(), json!({ "format": "arrow" })).await;
        let mut opened = open(destination.as_ref(), 1).await;
        let rows = merge_table("rows");
        let (schema, batch) = keyed(&[1, 2, 3], 1);
        stage(&mut opened, &rows, &schema, batch.clone(), 1).await;
        opened
            .session
            .commit(&meta(&opened, 1, CommitSeq::FIRST, &[1]))
            .await
            .unwrap();
        let files = data_files(root.path(), "arrow");
        assert_eq!(files.len(), 1);
        poison(&files[0], field, value);
        assert_eq!(
            read_back(reader.as_ref(), &rows).await,
            Some(ConnectorErrorKind::Data),
            "{field} {value}"
        );
        stage(&mut opened, &rows, &schema, batch, 2).await;
        let commit = opened
            .session
            .commit(&meta(&opened, 1, CommitSeq::FIRST.next(), &[2]))
            .await;
        assert_eq!(
            commit.unwrap_err().kind(),
            ConnectorErrorKind::Data,
            "{field} {value}"
        );
    }
}

#[tokio::test]
async fn a_commit_that_fails_leaves_no_file_it_wrote_and_its_retry_lands() {
    use std::os::unix::fs::PermissionsExt as _;
    for format in ["jsonl", "arrow"] {
        let root = crate::fixtures::tempdir().unwrap();
        let (destination, reader) = connect_with(root.path(), json!({ "format": format })).await;
        let mut opened = open(destination.as_ref(), 1).await;
        let rows = merge_table("rows");
        let (schema, batch) = keyed(&[1, 2], 1);
        stage(&mut opened, &rows, &schema, batch, 1).await;
        let manifests = pipeline_dir(root.path()).join("manifests");
        let mode =
            |mode| std::fs::set_permissions(&manifests, std::fs::Permissions::from_mode(mode));
        mode(0o500).unwrap();
        // Where permissions bind nothing, as for root, the fault cannot be made.
        if std::fs::write(manifests.join("probe"), b"").is_ok() {
            mode(0o700).unwrap();
            return;
        }
        let committing = meta(&opened, 1, CommitSeq::FIRST, &[1]);
        let failed = opened.session.commit(&committing).await;
        mode(0o700).unwrap();
        failed.expect_err("the manifest cannot be written");
        // The staged file stays for the retry; the merged file the commit wrote is gone.
        let files = data_files(root.path(), format);
        assert_eq!(files.len(), 1, "{format}: {files:?}");
        assert!(!files[0].to_string_lossy().contains("merged"), "{files:?}");
        opened
            .session
            .commit(&committing)
            .await
            .expect("the retry lands");
        assert_eq!(published_ids(reader.as_ref(), &rows).await, [1, 2]);
        assert_eq!(data_files(root.path(), format).len(), 1);
    }
}

#[tokio::test]
async fn a_table_written_in_both_formats_reads_back_whole_and_is_merged_in_neither() {
    let root = crate::fixtures::tempdir().unwrap();
    let rows = table("rows");
    let mut seq = CommitSeq::FIRST;
    let mut expected = Vec::new();
    for (load, format) in [(1, "jsonl"), (2, "arrow"), (3, "jsonl")] {
        let (destination, _) = connect_with(root.path(), json!({ "format": format })).await;
        let mut opened = open(destination.as_ref(), load).await;
        for id in 0..4_u64 {
            let value = i64::try_from(u64::try_from(load).unwrap() * 10 + id).unwrap();
            let (schema, batch) = ids(&[value]);
            stage(&mut opened, &rows, &schema, batch, id + 1).await;
            opened
                .session
                .commit(&meta(&opened, load, seq, &[id + 1]))
                .await
                .unwrap();
            seq = seq.next();
            expected.push(value);
        }
    }
    let (_, reader) = connect_with(root.path(), json!({})).await;
    assert_eq!(published_ids(reader.as_ref(), &rows).await, expected);
    // Each session's four commits leave a file of three rows and one of one; none merges with
    // a file of the other format.
    assert_eq!(data_files(root.path(), "jsonl").len(), 4);
    assert_eq!(data_files(root.path(), "arrow").len(), 2);
}

/// As a replay does: one session commits the pending commits of two dead loads, each its
/// load's first commit, under the load and number each was logged with.
#[tokio::test]
async fn two_loads_commits_of_one_number_in_one_session_keep_every_row() {
    for format in ["jsonl", "arrow"] {
        let root = crate::fixtures::tempdir().unwrap();
        let (destination, reader) = connect_with(root.path(), json!({ "format": format })).await;
        let mut opened = open(destination.as_ref(), 9).await;
        let rows = table("rows");
        for (load, base) in [(1_u128, 0_i64), (2, 10)] {
            let mut segments = Vec::new();
            for part in 0..2_i64 {
                let (schema, batch) = ids(&[base + part]);
                let segment = u64::try_from(base + part).unwrap() + 1;
                stage(&mut opened, &rows, &schema, batch, segment).await;
                segments.push(segment);
            }
            opened
                .session
                .commit(&meta(&opened, load, CommitSeq::FIRST, &segments))
                .await
                .expect("the commit lands");
        }
        assert_eq!(
            published_ids(reader.as_ref(), &rows).await,
            [0, 1, 10, 11],
            "{format}"
        );
        // A merge table's rewrite is named for its commit too.
        let merged = merge_table("merged");
        for (load, id) in [(3_u128, 1_i64), (4, 2)] {
            let (schema, batch) = keyed(&[id], 1);
            let segment = u64::try_from(id).unwrap() + 100;
            stage(&mut opened, &merged, &schema, batch, segment).await;
            opened
                .session
                .commit(&meta(&opened, load, CommitSeq::FIRST, &[segment]))
                .await
                .expect("the commit lands");
        }
        assert_eq!(
            published_ids(reader.as_ref(), &merged).await,
            [1, 2],
            "{format}"
        );
    }
}

#[tokio::test]
async fn a_failed_commit_of_a_reused_number_publishes_nothing() {
    use std::os::unix::fs::PermissionsExt as _;
    let root = crate::fixtures::tempdir().unwrap();
    let (destination, reader) = connect_with(root.path(), json!({})).await;
    let mut opened = open(destination.as_ref(), 9).await;
    let rows = merge_table("rows");
    let (schema, batch) = keyed(&[1, 2], 1);
    stage(&mut opened, &rows, &schema, batch, 1).await;
    opened
        .session
        .commit(&meta(&opened, 1, CommitSeq::FIRST, &[1]))
        .await
        .unwrap();
    assert_eq!(published_ids(reader.as_ref(), &rows).await, [1, 2]);
    let (schema, batch) = keyed(&[3], 2);
    stage(&mut opened, &rows, &schema, batch, 2).await;
    let manifests = pipeline_dir(root.path()).join("manifests");
    let mode = |mode| std::fs::set_permissions(&manifests, std::fs::Permissions::from_mode(mode));
    mode(0o500).unwrap();
    let failed = opened
        .session
        .commit(&meta(&opened, 2, CommitSeq::FIRST, &[2]))
        .await;
    mode(0o700).unwrap();
    failed.expect_err("the manifest cannot be written");
    // The commit failed: nothing of it is published.
    assert_eq!(published_ids(reader.as_ref(), &rows).await, [1, 2]);
}

#[tokio::test]
async fn an_arrow_append_table_of_batches_with_dictionaries_stays_short_and_whole() {
    use arrow_array::{DictionaryArray, Int8Array, types::Int8Type};
    let root = crate::fixtures::tempdir().unwrap();
    let (destination, reader) = connect_with(root.path(), json!({ "format": "arrow" })).await;
    let mut opened = open(destination.as_ref(), 1).await;
    let rows = table("rows");
    let schema = rdlt_connector::TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("load", LogicalType::Int64, false),
    ])
    .unwrap();
    let mut seq = CommitSeq::FIRST;
    for commit in 0..100_i64 {
        // As the engine prepares a batch: its per-load constant column is a dictionary.
        let constant = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0_i8]),
            Arc::new(Int64Array::from(vec![commit / 10])),
        )
        .unwrap();
        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(vec![commit])) as ArrayRef),
            ("load", Arc::new(constant) as ArrayRef),
        ])
        .unwrap();
        let segment = u64::try_from(commit).unwrap() + 1;
        stage(&mut opened, &rows, &schema, batch, segment).await;
        opened
            .session
            .commit(&meta(&opened, 1, seq, &[segment]))
            .await
            .unwrap();
        seq = seq.next();
    }
    let files = data_files(root.path(), "arrow");
    assert!(files.len() <= 10, "{} files", files.len());
    // Every row is read back once and in order, its dictionary's value with it.
    let published = reader.published(&rows).await.unwrap();
    let mut read = Vec::new();
    for batch in &published {
        let ids = arrow_cast::cast(batch.column(0), &arrow_schema::DataType::Int64).unwrap();
        let loads = arrow_cast::cast(batch.column(1), &arrow_schema::DataType::Int64).unwrap();
        let (ids, loads) = (
            ids.as_any().downcast_ref::<Int64Array>().unwrap().clone(),
            loads.as_any().downcast_ref::<Int64Array>().unwrap().clone(),
        );
        read.extend(
            ids.values()
                .iter()
                .copied()
                .zip(loads.values().iter().copied()),
        );
    }
    let expected: Vec<(i64, i64)> = (0..100).map(|commit| (commit, commit / 10)).collect();
    assert_eq!(read, expected);
}

#[tokio::test]
async fn a_table_created_again_after_its_drop_keeps_its_catalog() {
    let root = crate::fixtures::tempdir().unwrap();
    let (destination, reader) = connect_with(root.path(), json!({})).await;
    let mut opened = open(destination.as_ref(), 1).await;
    let rows = table("rows");
    let (schema, batch) = ids(&[1]);
    stage(&mut opened, &rows, &schema, batch, 1).await;
    let seq1 = CommitSeq::FIRST;
    opened
        .session
        .commit(&meta(&opened, 1, seq1, &[1]))
        .await
        .unwrap();
    let mut dropping = meta(&opened, 1, seq1.next(), &[]);
    dropping.drop_tables = vec![rdlt_connector::DroppedTable {
        path: rows.path.clone(),
        name: rows.name.clone(),
    }];
    opened.session.commit(&dropping).await.unwrap();
    // The same session creates the table again and publishes a row.
    let (schema, batch) = ids(&[2]);
    stage(&mut opened, &rows, &schema, batch, 2).await;
    let seq3 = seq1.next().next();
    opened
        .session
        .commit(&meta(&opened, 1, seq3, &[2]))
        .await
        .unwrap();
    let catalog = root.path().join("_rdlt").join("tables").join("rows");
    let (_, manifest) = latest_manifest(root.path());
    assert!(catalog.exists(), "the catalog of a published table is gone");
    assert!(manifest.get("dropped").is_none(), "{manifest}");
    assert_eq!(published_ids(reader.as_ref(), &rows).await, [2]);
    // Dropped again, its catalog goes again; created again and not yet written, its catalog
    // stays through another table's commit.
    let seq4 = seq3.next();
    let mut dropping = meta(&opened, 1, seq4, &[]);
    dropping.drop_tables = vec![rdlt_connector::DroppedTable {
        path: rows.path.clone(),
        name: rows.name.clone(),
    }];
    opened.session.commit(&dropping).await.unwrap();
    assert!(!catalog.exists(), "the catalog of a dropped table stays");
    let create = TableChange::Create {
        table: rows.clone(),
        schema: ids(&[]).0,
    };
    opened.session.apply_schema(&create).await.unwrap();
    let (schema, batch) = ids(&[3]);
    stage(&mut opened, &table("other"), &schema, batch, 3).await;
    opened
        .session
        .commit(&meta(&opened, 1, seq4.next(), &[3]))
        .await
        .unwrap();
    assert!(
        catalog.exists(),
        "the catalog of a table created again is gone"
    );
}
