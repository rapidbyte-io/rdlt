use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use rdlt_connector::{CommitMeta, CommitSeq, GenerationId, SegmentSet};

use super::{compact, keyed, tail};
use crate::files::FileFormat;
use crate::files::format::plain::plain;
use crate::files::manifest::{self, Listed};
use crate::files::session::Location;
use crate::files::session::tests::location;
use crate::limits::COMPACT_BYTES;

/// A list of files of `rows` rows and one byte each, named for `format`; none exists.
fn listed(format: FileFormat, rows: &[u64]) -> Vec<Listed> {
    rows.iter()
        .enumerate()
        .map(|(part, rows)| Listed {
            path: format!("staging/7/load/{part}/rows/table/1.{}", format.extension()),
            rows: *rows,
            bytes: 1,
        })
        .collect()
}

#[test]
fn files_merge_from_the_end_while_each_holds_at_most_twice_the_rows_after_it() {
    let (_root, location) = location(FileFormat::Jsonl);
    let cases: [(&[u64], Option<usize>); 13] = [
        (&[], None),
        (&[5], None),
        (&[1, 1], Some(0)),
        (&[2, 1], Some(0)),
        (&[3, 1], None),
        (&[8, 4, 2, 1, 1], Some(0)),
        (&[16, 4, 2, 1, 1], Some(0)),
        (&[17, 4, 2, 1, 1], Some(1)),
        (&[8, 3, 1], None),
        (&[8, 1, 1], Some(1)),
        (&[100, 1, 1, 1], Some(1)),
        (&[6, 1, 1, 1], Some(0)),
        (&[7, 1, 1, 1], Some(1)),
    ];
    for (rows, from) in cases {
        let files = listed(FileFormat::Jsonl, rows);
        assert_eq!(tail(&location, &files), from, "{rows:?}");
    }
}

#[test]
fn files_merge_only_up_to_the_size_of_a_full_file_and_only_in_the_session_s_format() {
    let (_root, location) = location(FileFormat::Jsonl);
    let mut files = listed(FileFormat::Jsonl, &[1, 1, 1]);
    files[1].bytes = COMPACT_BYTES - 1;
    assert_eq!(tail(&location, &files), Some(1), "a file exactly full");
    files[1].bytes = COMPACT_BYTES;
    assert_eq!(tail(&location, &files), None, "a file beyond full");
    files[1].bytes = 1;
    files[0].bytes = COMPACT_BYTES - 1;
    assert_eq!(tail(&location, &files), Some(1));
    // A file of another format merges with none, wherever it stands.
    for other in 0..3 {
        let mut files = listed(FileFormat::Jsonl, &[1, 1, 1]);
        files[other].path = files[other].path.replace(".jsonl", ".arrow");
        let expected = [Some(1), None, None][other];
        assert_eq!(tail(&location, &files), expected, "{other}");
    }
}

fn meta(location: &Location, seq: CommitSeq) -> CommitMeta {
    CommitMeta {
        load_id: location.load_id,
        commit_seq: seq,
        epoch: location.epoch,
        segments: SegmentSet::default(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    }
}

/// Stages `batch` as `part` of the table `rows`; the file as a manifest lists it.
fn staged(location: &Location, part: u64, batch: &RecordBatch) -> Listed {
    let (names, file) = location.staged(&[part.to_string()], "rows", None, part);
    let dir = location.staging(&names).unwrap();
    let written = location
        .format
        .write(&dir, &file, std::slice::from_ref(batch))
        .unwrap();
    Listed {
        path: format!("{}/{file}", names.join("/")),
        rows: written.rows,
        bytes: written.bytes,
    }
}

fn ids(values: &[i64]) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
    RecordBatch::try_from_iter([("id", ids)]).unwrap()
}

/// The ids of every row of `files`, in order.
fn read(location: &Location, files: &[Listed]) -> Vec<i64> {
    use arrow_array::cast::AsArray as _;
    use arrow_array::types::Int64Type;
    let schema = ids(&[]).schema();
    files
        .iter()
        .flat_map(|file| manifest::read(&location.dir, &file.path, &schema).unwrap())
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect()
}

#[test]
fn merged_files_hold_every_row_in_order_and_what_they_merged_stays_until_the_commit() {
    for format in [FileFormat::Jsonl, FileFormat::Arrow] {
        let (_root, location) = location(format);
        let mut files = vec![
            staged(&location, 1, &ids(&[1, 2, 3, 4, 5, 6, 7, 8, 9])),
            staged(&location, 2, &ids(&[10])),
            staged(&location, 3, &ids(&[11])),
        ];
        let before = files.clone();
        let mut created = Vec::new();
        let generation = Some(GenerationId(4));
        let seq = CommitSeq::FIRST;
        compact(
            &location,
            "rows",
            generation,
            &mut files,
            &meta(&location, seq),
            &mut created,
        );
        assert_eq!(files.len(), 2, "{format:?}");
        assert_eq!(files[0], before[0]);
        assert_eq!(files[1].rows, 2);
        assert_eq!(created, [files[1].path.clone()]);
        let commit = format!("/compacted/{}-1-", location.load_id);
        assert!(files[1].path.contains(&commit), "{}", files[1].path);
        assert!(files[1].path.contains("/rows/g4/0."), "{}", files[1].path);
        let size = std::fs::metadata(location.dir.at(&files[1].path))
            .unwrap()
            .len();
        assert_eq!(files[1].bytes, size);
        assert_eq!(read(&location, &files), (1..=11).collect::<Vec<_>>());
        assert_eq!(read(&location, &before), (1..=11).collect::<Vec<_>>());
        // Tried again, as a commit that failed is, it writes another file and leaves the
        // first: no file a commit wrote is ever removed or written over by its name.
        let first = std::fs::read(location.dir.at(&files[1].path)).unwrap();
        let mut again = before.clone();
        compact(
            &location,
            "rows",
            generation,
            &mut again,
            &meta(&location, seq),
            &mut created,
        );
        assert_ne!(again[1].path, files[1].path);
        assert_eq!(
            (again[1].rows, again[1].bytes),
            (files[1].rows, files[1].bytes)
        );
        assert_eq!(
            std::fs::read(location.dir.at(&files[1].path)).unwrap(),
            first
        );
        assert_eq!(created.len(), 2);
    }
}

#[test]
fn a_merge_that_fails_leaves_the_list_and_nothing_it_made() {
    use crate::rooted::trace::{self, Step};
    for format in [FileFormat::Jsonl, FileFormat::Arrow] {
        let (_root, location) = location(format);
        let mut files = vec![
            staged(&location, 1, &ids(&[1])),
            staged(&location, 2, &ids(&[2])),
        ];
        let before = files.clone();
        let made = location
            .dir
            .at("staging/7")
            .join(location.load_id.to_string());
        // A try refused at any step, the syncs of its file and of its directory among them,
        // leaves no part of its file and none of the directories made for it.
        for step in 0.. {
            let mut created = Vec::new();
            let seq = CommitSeq::FIRST;
            trace::fail_at(step);
            compact(
                &location,
                "rows",
                None,
                &mut files,
                &meta(&location, seq),
                &mut created,
            );
            let (refused, steps) = (trace::refused(), trace::steps());
            trace::clear();
            if !refused {
                assert_eq!(files.len(), 1, "{format:?}");
                break;
            }
            assert_eq!(files, before, "{format:?} step {step}: {steps:?}");
            assert!(created.is_empty(), "{format:?} step {step}");
            assert!(!made.join("compacted").exists(), "{format:?} step {step}");
        }
        // A file to merge that is gone: Arrow files cannot be told to share a schema, and JSON
        // lines cannot be read, before anything is created.
        let mut files = vec![before[0].clone(), staged(&location, 3, &ids(&[3]))];
        let unmerged = files.clone();
        std::fs::remove_file(location.dir.at(&files[0].path)).ok();
        let mut created = Vec::new();
        trace::clear();
        let seq = CommitSeq::FIRST.next();
        compact(
            &location,
            "rows",
            None,
            &mut files,
            &meta(&location, seq),
            &mut created,
        );
        assert_eq!(files, unmerged, "{format:?}");
        assert!(created.is_empty(), "{format:?}");
        let made_anything = trace::steps()
            .into_iter()
            .filter(|step| matches!(step, Step::Create(_) | Step::MakeDir(_)))
            .count();
        assert_eq!(made_anything, 0, "{format:?}");
    }
}

#[test]
fn arrow_files_merge_only_with_files_of_their_schema() {
    let (_root, location) = location(FileFormat::Arrow);
    let wider = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![3])) as ArrayRef),
        ("more", Arc::new(Int64Array::from(vec![3])) as ArrayRef),
    ])
    .unwrap();
    let files = vec![
        staged(&location, 1, &ids(&[1])),
        staged(&location, 2, &ids(&[2])),
        staged(&location, 3, &wider),
        staged(&location, 4, &wider),
    ];
    assert_eq!(tail(&location, &files), Some(2));
    assert_eq!(tail(&location, &files[..3]), None);
    assert_eq!(tail(&location, &files[..2]), Some(0));
}

/// A batch of one column `tag` of `tags`, as a dictionary keyed by `Int8`.
fn tagged(tags: &[&str]) -> RecordBatch {
    let tags: arrow_array::DictionaryArray<arrow_array::types::Int8Type> =
        tags.iter().copied().collect();
    RecordBatch::try_from_iter([("tag", Arc::new(tags) as ArrayRef)]).unwrap()
}

#[test]
fn arrow_files_with_dictionaries_merge_as_the_values_they_stand_for() {
    // As the engine writes them: each file with a dictionary of its own.
    let (_root, location) = location(FileFormat::Arrow);
    let text: ArrayRef = Arc::new(StringArray::from(vec!["plain"]));
    let plain = RecordBatch::try_from_iter([("tag", text)]).unwrap();
    let mut files = vec![
        staged(&location, 1, &tagged(&["a", "b", "a"])),
        staged(&location, 2, &tagged(&["c"])),
        staged(&location, 3, &plain),
        staged(&location, 4, &tagged(&["b", "d"])),
    ];
    assert_eq!(tail(&location, &files), Some(0));
    let mut created = Vec::new();
    let seq = CommitSeq::FIRST;
    compact(
        &location,
        "rows",
        None,
        &mut files,
        &meta(&location, seq),
        &mut created,
    );
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].rows, 7);
    let schema = plain.schema();
    let read = manifest::read(&location.dir, &files[0].path, &schema).unwrap();
    let merged = arrow_select::concat::concat_batches(&schema, &read).unwrap();
    let expected: ArrayRef = Arc::new(StringArray::from(vec![
        "a", "b", "a", "c", "plain", "b", "d",
    ]));
    assert_eq!(merged.column(0), &expected);
    // The merged file merges again with the next file, dictionary or not.
    files.push(staged(&location, 5, &tagged(&["e"; 4])));
    assert_eq!(tail(&location, &files), Some(0));
}

#[test]
fn a_dictionary_that_cannot_be_written_as_its_values_keeps_its_file_apart() {
    use arrow_array::builder::{MapBuilder, StringBuilder, StringDictionaryBuilder};
    use arrow_array::types::Int8Type;
    let (_root, location) = location(FileFormat::Arrow);
    let mapped = || {
        let mut map = MapBuilder::new(
            None,
            StringBuilder::new(),
            StringDictionaryBuilder::<Int8Type>::new(),
        );
        map.keys().append_value("k");
        map.values().append_value("v");
        map.append(true).unwrap();
        RecordBatch::try_from_iter([("map", Arc::new(map.finish()) as ArrayRef)]).unwrap()
    };
    let files = vec![
        staged(&location, 1, &mapped()),
        staged(&location, 2, &mapped()),
    ];
    assert!(keyed(&plain(&mapped().schema())));
    assert_eq!(tail(&location, &files), None);
    assert!(!keyed(&plain(&tagged(&["a"]).schema())));
}

#[test]
fn a_dictionary_is_found_however_deep_it_nests() {
    use arrow_schema::{DataType, Field, Fields, Schema, UnionFields, UnionMode};
    let field = |data_type: DataType| Arc::new(Field::new("item", data_type, true));
    let dictionary = || DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
    let entries = |value: DataType| {
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", value, true),
        ]))
    };
    let union = |inner: DataType| {
        let fields = UnionFields::from_fields(vec![Field::new("a", inner, true)]);
        DataType::Union(fields, UnionMode::Dense)
    };
    let nests: Vec<Box<dyn Fn(DataType) -> DataType>> = vec![
        Box::new(|inner| inner),
        Box::new(move |inner| DataType::List(field(inner))),
        Box::new(move |inner| DataType::LargeList(field(inner))),
        Box::new(move |inner| DataType::ListView(field(inner))),
        Box::new(move |inner| DataType::LargeListView(field(inner))),
        Box::new(move |inner| DataType::FixedSizeList(field(inner), 2)),
        Box::new(move |inner| DataType::Map(field(entries(inner)), false)),
        Box::new(move |inner| DataType::Struct(Fields::from(vec![Field::new("f", inner, true)]))),
        Box::new(union),
        Box::new(move |inner| DataType::RunEndEncoded(field(DataType::Int32), field(inner))),
    ];
    for (index, nest) in nests.iter().enumerate() {
        let with = Schema::new(vec![Field::new("c", nest(dictionary()), true)]);
        let without = Schema::new(vec![Field::new("c", nest(DataType::Utf8), true)]);
        assert!(keyed(&with) && !keyed(&without), "{index}");
        // Nested once more, in a list of it.
        let deeper = Schema::new(vec![Field::new(
            "c",
            DataType::List(field(nest(dictionary()))),
            true,
        )]);
        assert!(keyed(&deeper), "{index}");
    }
}
