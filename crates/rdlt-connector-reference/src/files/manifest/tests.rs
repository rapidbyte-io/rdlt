use std::time::{Duration, UNIX_EPOCH};

use rdlt_connector::{CommitSeq, LoadId, PipelineId, Receipt};

use super::{Manifest, RECEIPT_LOADS, latest, pipeline_dir, put, truncated};

fn receipt(load: u128, commit_seq: CommitSeq) -> Receipt {
    Receipt {
        load_id: LoadId::from_parts(UNIX_EPOCH, load),
        commit_seq,
        committed_at: UNIX_EPOCH + Duration::from_micros(7),
        rows: 1,
        bytes: 2,
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
fn commit_times_are_kept_to_the_microsecond() {
    let at = UNIX_EPOCH + Duration::from_nanos(1_234_567_891);
    assert_eq!(truncated(at), UNIX_EPOCH + Duration::from_micros(1_234_567));
}

#[test]
fn pipeline_directories_stay_under_the_root_and_never_share_a_name() {
    let root = std::path::Path::new("/data");
    let dir = |id: &str| pipeline_dir(root, &PipelineId::parse(id).unwrap());
    let parent = dir("..");
    assert_eq!(
        parent.parent(),
        Some(root.join("_rdlt").join("pipelines").as_path())
    );
    let names = |id: &str| dir(id).file_name().unwrap().to_ascii_lowercase();
    assert_ne!(
        names("Orders"),
        names("orders"),
        "ids that differ in case stay apart"
    );
}

#[test]
fn a_manifest_version_is_created_once() {
    let dir = tempfile::tempdir().unwrap();
    let first = Manifest {
        version: 1,
        ..Manifest::default()
    };
    assert!(put(dir.path(), &first).unwrap());
    let rival = Manifest {
        version: 1,
        epoch: rdlt_connector::Epoch(9),
        ..Manifest::default()
    };
    assert!(!put(dir.path(), &rival).unwrap(), "the version exists");
    assert_eq!(latest(dir.path()).unwrap(), Some(first));
    let leftovers: Vec<_> = std::fs::read_dir(dir.path().join("manifests"))
        .unwrap()
        .flatten()
        .map(|entry| entry.file_name())
        .collect();
    assert_eq!(
        leftovers.len(),
        1,
        "no temporary file is left: {leftovers:?}"
    );
}
