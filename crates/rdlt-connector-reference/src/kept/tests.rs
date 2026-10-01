use rdlt_connector::PartitionId;

use super::Kept;

fn partition(id: &str) -> PartitionId {
    PartitionId::parse(id).unwrap()
}

#[test]
fn a_keeper_at_a_path_holds_its_positions_for_the_next_process() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("slot.json");
    let kept: Kept<u64> = Kept::at(&path).unwrap();
    assert_eq!(kept.position("orders", &partition("p0")), None);
    kept.advance("orders", &partition("p0"), 7).unwrap();
    kept.advance("orders", &partition("p1"), 3).unwrap();
    // Never moving back.
    kept.advance("orders", &partition("p0"), 5).unwrap();
    // As the next process finds it.
    let again: Kept<u64> = Kept::at(&path).unwrap();
    assert_eq!(again.position("orders", &partition("p0")), Some(7));
    assert_eq!(again.position("orders", &partition("p1")), Some(3));
    // Its temporary file is gone once it is written.
    let files: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
    assert_eq!(files.len(), 1);
}

#[test]
fn a_keeper_whose_file_is_damaged_is_refused_rather_than_found_empty() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("slot.json");
    let kept: Kept<u64> = Kept::at(&path).unwrap();
    kept.advance("orders", &partition("p0"), 7).unwrap();
    let whole = std::fs::read(&path).unwrap();
    std::fs::write(&path, &whole[..whole.len() / 2]).unwrap();
    assert!(Kept::<u64>::at(&path).is_err());
}

#[test]
fn a_keeper_without_a_path_writes_nothing() {
    let kept: Kept<u64> = Kept::default();
    kept.advance("orders", &partition("p0"), 7).unwrap();
    assert_eq!(kept.position("orders", &partition("p0")), Some(7));
}

#[test]
fn a_keeper_whose_file_cannot_be_read_is_refused_rather_than_found_empty() {
    // A directory where the file should be reads as no file of positions.
    let dir = tempfile::tempdir().unwrap();
    assert!(Kept::<u64>::at(dir.path()).is_err());
}

#[test]
fn a_keeper_reads_its_last_whole_file_beside_one_a_crash_left_half_written() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("slot.json");
    let kept: Kept<u64> = Kept::at(&path).unwrap();
    kept.advance("orders", &partition("p0"), 7).unwrap();
    // A process that died writing its next file left half of it beside the last whole one.
    std::fs::write(dir.path().join("slot.json.writing"), b"[[\"orders\",").unwrap();
    let again: Kept<u64> = Kept::at(&path).unwrap();
    assert_eq!(again.position("orders", &partition("p0")), Some(7));
    // Its next write replaces the half-written file.
    again.advance("orders", &partition("p0"), 9).unwrap();
    let last: Kept<u64> = Kept::at(&path).unwrap();
    assert_eq!(last.position("orders", &partition("p0")), Some(9));
    let files: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
    assert_eq!(files.len(), 1);
}

#[cfg(unix)]
#[test]
fn a_keeper_whose_directory_cannot_be_synced_is_refused_its_advance() {
    use std::os::unix::fs::PermissionsExt as _;
    // A directory files can be renamed into but that cannot be opened, so its entry cannot be
    // made durable.
    let dir = tempfile::tempdir().unwrap();
    let keeping = dir.path().join("keeping");
    std::fs::create_dir(&keeping).unwrap();
    let kept: Kept<u64> = Kept::at(&keeping.join("slot.json")).unwrap();
    std::fs::set_permissions(&keeping, std::fs::Permissions::from_mode(0o300)).unwrap();
    let advanced = kept.advance("orders", &partition("p0"), 7);
    std::fs::set_permissions(&keeping, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(advanced.is_err());
}
