use std::time::{Duration, UNIX_EPOCH};

use rdlt_connector::{CommitSeq, ConnectorErrorKind, Epoch, LoadId, PipelineId, Receipt};

use super::{
    Listed, MANIFEST_INVALID, MANIFESTS, Manifest, TableFiles, format_of, latest, located,
    pipeline_dir, put, read, staged, sweep, truncated,
};
use crate::files::FileFormat;
use crate::limits::{KEPT_VERSIONS, MANIFEST_BYTES, RECEIPT_LOADS};
use crate::rooted::Dir;

fn receipt(load: u128, commit_seq: CommitSeq) -> Receipt {
    Receipt {
        load_id: LoadId::from_parts(UNIX_EPOCH, load),
        commit_seq,
        committed_at: UNIX_EPOCH + Duration::from_micros(7),
        rows: 1,
        bytes: 2,
    }
}

fn seq(n: u64) -> CommitSeq {
    (1..n).fold(CommitSeq::FIRST, |seq, _| seq.next())
}

/// A pipeline's directory.
fn pipeline() -> (tempfile::TempDir, Dir) {
    let root = tempfile::tempdir().unwrap();
    let dir = Dir::ambient(root.path()).unwrap();
    (root, dir)
}

fn listed(path: &str) -> Listed {
    Listed {
        path: path.to_owned(),
        rows: 1,
        bytes: 1,
    }
}

#[test]
fn a_manifest_keeps_the_receipts_of_the_most_recent_loads() {
    let mut manifest = Manifest::default();
    let loads = u128::try_from(RECEIPT_LOADS).unwrap() + 1;
    for load in 0..loads {
        manifest.record(&receipt(load, CommitSeq::FIRST));
        manifest.record(&receipt(load, CommitSeq::FIRST.next()));
    }
    let oldest = LoadId::from_parts(UNIX_EPOCH, 0);
    assert_eq!(manifest.receipt(oldest, CommitSeq::FIRST), None);
    for seq in [CommitSeq::FIRST, CommitSeq::FIRST.next()] {
        let kept = manifest.receipt(LoadId::from_parts(UNIX_EPOCH, 1), seq);
        assert_eq!(kept, Some(receipt(1, seq)));
    }
    assert_eq!(manifest.receipts.len(), RECEIPT_LOADS * 2);
}

#[test]
fn a_manifest_keeps_every_receipt_of_a_load_it_keeps() {
    let mut manifest = Manifest::default();
    // Two loads committing in turn, far more often than loads are kept.
    for commit in 1..=100 {
        for load in [1, 2] {
            manifest.record(&receipt(load, seq(commit)));
        }
    }
    assert_eq!(manifest.receipts.len(), 200);
    for load in [1, 2] {
        let load_id = LoadId::from_parts(UNIX_EPOCH, load);
        for commit in 1..=100 {
            let kept = manifest.receipt(load_id, seq(commit));
            assert_eq!(kept, Some(receipt(load, seq(commit))), "{commit}");
        }
        assert_eq!(manifest.receipt(load_id, seq(101)), None);
    }
}

#[test]
fn commit_times_are_kept_to_the_microsecond() {
    let at = UNIX_EPOCH + Duration::from_nanos(1_234_567_891);
    assert_eq!(truncated(at), UNIX_EPOCH + Duration::from_micros(1_234_567));
}

#[test]
fn pipeline_directories_are_one_name_and_never_share_one() {
    let name = |id: &str| pipeline_dir(&PipelineId::parse(id).unwrap());
    for id in ["..", ".", "orders", "a.b-c_d"] {
        let name = name(id);
        assert!(crate::rooted::component(name.as_ref()).is_ok(), "{name}");
        assert!(
            name.starts_with(id) && name.len() == id.len() + 17,
            "{name}"
        );
    }
    assert_ne!(
        name("Orders").to_ascii_lowercase(),
        name("orders").to_ascii_lowercase(),
        "ids that differ in case stay apart"
    );
}

#[test]
fn a_manifest_version_is_created_once() {
    let (root, dir) = pipeline();
    let first = Manifest {
        version: 1,
        ..Manifest::default()
    };
    assert!(put(&dir, &first).unwrap());
    let rival = Manifest {
        version: 1,
        epoch: Epoch(9),
        ..Manifest::default()
    };
    assert!(!put(&dir, &rival).unwrap(), "the version exists");
    assert_eq!(latest(&dir).unwrap(), Some(first));
    let leftovers: Vec<_> = std::fs::read_dir(root.path().join(MANIFESTS))
        .unwrap()
        .flatten()
        .map(|entry| entry.file_name())
        .collect();
    assert_eq!(
        leftovers,
        ["00000000000000000001.json"],
        "no temporary is left"
    );
}

#[test]
fn a_version_older_than_the_latest_is_never_created_again() {
    let (root, dir) = pipeline();
    for version in 1..=20 {
        let manifest = Manifest {
            version,
            ..Manifest::default()
        };
        assert!(put(&dir, &manifest).unwrap());
    }
    let kept = std::fs::read_dir(root.path().join(MANIFESTS))
        .unwrap()
        .count();
    assert_eq!(u64::try_from(kept).unwrap(), KEPT_VERSIONS + 1);
    assert!(
        root.path()
            .join(MANIFESTS)
            .join("00000000000000000012.json")
            .exists()
    );
    assert!(
        !root
            .path()
            .join(MANIFESTS)
            .join("00000000000000000011.json")
            .exists()
    );
    let stale = Manifest {
        version: 6,
        epoch: Epoch(1),
        ..Manifest::default()
    };
    assert!(
        !put(&dir, &stale).unwrap(),
        "a session that read version 5 loses, though version 6 was removed"
    );
    assert!(
        !root
            .path()
            .join(MANIFESTS)
            .join("00000000000000000006.json")
            .exists()
    );
    assert_eq!(
        latest(&dir).unwrap().map(|manifest| manifest.version),
        Some(20)
    );
}

#[test]
fn manifests_that_cannot_be_listed_are_an_error_not_an_empty_table() {
    let (root, dir) = pipeline();
    assert_eq!(
        latest(&dir).unwrap(),
        None,
        "a pipeline that never opened has none"
    );
    std::fs::write(root.path().join(MANIFESTS), b"not a directory").unwrap();
    latest(&dir).expect_err("the manifests cannot be listed");
    sweep(&dir).expect_err("nor swept");
}

/// Writes and reads a manifest listing one staged file, after `change`: written and read alike
/// refuse what the destination does not write.
fn changed(change: &dyn Fn(&mut Manifest)) -> rdlt_connector::Result<Option<Manifest>> {
    let mut manifest = Manifest {
        version: 1,
        ..Manifest::default()
    };
    let rows = TableFiles {
        files: vec![listed("staging/1/load/1/rows/table/1.jsonl")],
        ..TableFiles::default()
    };
    manifest.tables.insert("rows".to_owned(), rows);
    change(&mut manifest);
    let (root, dir) = pipeline();
    let written = put(&dir, &manifest);
    if let Err(error) = &written {
        assert_eq!(error.code(), Some(MANIFEST_INVALID));
        assert!(
            !root.path().join(MANIFESTS).exists(),
            "something was written"
        );
        std::fs::create_dir(root.path().join(MANIFESTS)).unwrap();
        let planted = root
            .path()
            .join(MANIFESTS)
            .join("00000000000000000001.json");
        std::fs::write(planted, serde_json::to_vec(&manifest).unwrap()).unwrap();
    }
    let read = latest(&dir);
    assert_eq!(written.is_ok(), read.is_ok());
    read
}

#[test]
fn a_manifest_listing_a_file_outside_its_pipeline_s_staging_is_refused() {
    assert!(changed(&|_| {}).unwrap().is_some());
    let bad_paths = [
        "/etc/passwd",
        "../x",
        "staging/../x",
        "staging",
        "manifests/1.json",
        "staging//x",
        "staging/./x",
        "",
        "stagingx/1",
    ];
    for path in bad_paths {
        for place in 0..3 {
            let refused = changed(&|manifest| {
                let rows = manifest.tables.get_mut("rows").unwrap();
                match place {
                    0 => rows.files.push(listed(path)),
                    1 => {
                        rows.generations
                            .insert(rdlt_connector::GenerationId(1), vec![listed(path)]);
                    }
                    _ => rows.tombstones.push(listed(path)),
                }
            });
            let error = refused.expect_err(path);
            assert_eq!(error.kind(), ConnectorErrorKind::Data, "{path} {place}");
            assert_eq!(error.code(), Some(MANIFEST_INVALID), "{path} {place}");
        }
    }
}

#[test]
fn a_manifest_naming_a_table_that_is_no_identifier_is_refused() {
    for name in ["../x", "", "a/b", "a b"] {
        let name = name.to_owned();
        let places: [&dyn Fn(&mut Manifest); 3] = [
            &|manifest| {
                manifest.tables.insert(name.clone(), TableFiles::default());
            },
            &|manifest| {
                manifest.dropped.insert(name.clone());
            },
            &|manifest| {
                manifest.paths.insert("[\"t\"]".to_owned(), name.clone());
            },
        ];
        for place in places {
            let error = changed(place).expect_err(&name);
            assert_eq!(error.code(), Some(MANIFEST_INVALID), "{name:?}");
        }
    }
}

#[test]
fn a_manifest_whose_file_names_another_version_or_is_no_json_is_refused() {
    let (root, dir) = pipeline();
    let manifest = Manifest {
        version: 5,
        ..Manifest::default()
    };
    assert!(put(&dir, &manifest).unwrap());
    let manifests = root.path().join(MANIFESTS);
    // Names that are not a version's are no manifest: they are not read.
    for stray in [
        "6.json",
        "0000000000000000006.json",
        "x0000000000000000006.json",
        "00000000000000000006.jsn",
    ] {
        std::fs::write(manifests.join(stray), b"junk").unwrap();
    }
    assert_eq!(latest(&dir).unwrap(), Some(manifest));
    std::fs::copy(
        manifests.join("00000000000000000005.json"),
        manifests.join("00000000000000000007.json"),
    )
    .unwrap();
    let error = latest(&dir).unwrap_err();
    assert_eq!(error.code(), Some(MANIFEST_INVALID));
    std::fs::write(manifests.join("00000000000000000008.json"), b"{").unwrap();
    let error = latest(&dir).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
    assert_eq!(error.code(), Some(MANIFEST_INVALID));
}

#[test]
fn a_manifest_beyond_the_limit_is_neither_written_nor_read() {
    let (root, dir) = pipeline();
    let mut manifest = Manifest {
        version: 1,
        ..Manifest::default()
    };
    let value = "x".repeat(usize::try_from(MANIFEST_BYTES).unwrap());
    manifest.state.insert("key".to_owned(), value);
    let error = put(&dir, &manifest).unwrap_err();
    assert_eq!(error.code(), Some("limit_exceeded"));
    assert_eq!(error.limit().map(|limit| limit.limit), Some(MANIFEST_BYTES));
    assert!(!root.path().join(MANIFESTS).exists(), "nothing was written");
    std::fs::create_dir(root.path().join(MANIFESTS)).unwrap();
    let huge = std::fs::File::create(
        root.path()
            .join(MANIFESTS)
            .join("00000000000000000001.json"),
    );
    huge.unwrap().set_len(MANIFEST_BYTES + 1).unwrap();
    let error = latest(&dir).unwrap_err();
    assert_eq!(error.code(), Some("limit_exceeded"));
    let limit = error.limit().unwrap();
    assert_eq!(
        (limit.name, limit.actual),
        ("manifest bytes", MANIFEST_BYTES + 1)
    );
}

#[test]
fn a_staged_path_lies_under_the_pipeline_s_staging() {
    assert_eq!(
        staged("staging/1/x.jsonl").unwrap(),
        ["staging", "1", "x.jsonl"]
    );
    assert_eq!(staged("staging/x").unwrap(), ["staging", "x"]);
    for path in ["staging", "x/staging/y", "Staging/x", "staging/..", ""] {
        assert!(staged(path).is_err(), "{path:?}");
    }
    let (root, dir) = pipeline();
    std::fs::create_dir_all(root.path().join("staging").join("1")).unwrap();
    std::fs::write(root.path().join("staging").join("1").join("x.jsonl"), b"").unwrap();
    let (parent, file) = located(&dir, "staging/1/x.jsonl").unwrap();
    assert_eq!(
        (parent.path(), file),
        (root.path().join("staging/1").as_path(), "x.jsonl")
    );
    let missing = located(&dir, "staging/2/x.jsonl").unwrap_err();
    assert_eq!(missing.code(), Some("file_missing"));
    let outside = located(&dir, "../x.jsonl").unwrap_err();
    assert_eq!(outside.code(), Some("invalid_name"));
    // A listed file is read in the format its name says, and in no other.
    assert_eq!(
        format_of(&dir, "staging/1/x.jsonl").unwrap(),
        FileFormat::Jsonl
    );
    assert_eq!(
        format_of(&dir, "staging/1/x.arrow").unwrap(),
        FileFormat::Arrow
    );
    std::fs::write(root.path().join("staging").join("1").join("x.txt"), b"").unwrap();
    let schema = std::sync::Arc::new(arrow_schema::Schema::empty());
    let unknown = read(&dir, "staging/1/x.txt", &schema).unwrap_err();
    assert_eq!(unknown.kind(), ConnectorErrorKind::Data);
    assert!(read(&dir, "staging/1/x.jsonl", &schema).unwrap().is_empty());
}

#[test]
fn a_manifest_whose_state_is_not_base64_is_refused_written_or_read() {
    let refused = changed(&|manifest| {
        manifest
            .state
            .insert("k".to_owned(), "!!!not base64!!!".to_owned());
    });
    let error = refused.expect_err("the state is no base64");
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
    assert_eq!(error.code(), Some(MANIFEST_INVALID));
    let kept = changed(&|manifest| {
        manifest.state.insert("k".to_owned(), "aGVsbG8=".to_owned());
    });
    let records = kept.unwrap().unwrap().records().unwrap();
    assert_eq!(&records[0].value[..], b"hello");
}
