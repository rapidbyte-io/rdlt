use super::Tombstones;

#[test]
fn a_buried_key_admits_only_changes_sequenced_after_its_delete() {
    let mut tombstones = Tombstones::default();
    tombstones.bury("k".into(), "5".into());
    assert!(!tombstones.admits(Some("k"), "4"));
    assert!(!tombstones.admits(Some("k"), "5"));
    assert!(tombstones.admits(Some("k"), "6"));
    assert!(tombstones.admits(Some("other"), "1"));
    tombstones.lift("k");
    assert!(tombstones.admits(Some("k"), "4"));
}

#[test]
fn a_truncate_bounds_every_change_before_it_and_covers_the_tombstones_before_it() {
    let mut tombstones = Tombstones::default();
    tombstones.bury("early".into(), "3".into());
    tombstones.bury("same".into(), "5".into());
    tombstones.bury("late".into(), "7".into());
    tombstones.raise("5".into());
    assert!(!tombstones.admits(None, "4"));
    assert!(tombstones.admits(None, "5"));
    assert!(!tombstones.admits(Some("fresh"), "4"));
    assert!(tombstones.admits(Some("fresh"), "5"));
    // A tombstone the bound covers is dropped; one at or past it still buries its key.
    assert!(!tombstones.by_key.contains_key("early"));
    assert!(!tombstones.admits(Some("same"), "5"));
    assert!(!tombstones.admits(Some("late"), "6"));
    // A truncate before the bound leaves it where it was.
    tombstones.raise("2".into());
    assert!(!tombstones.admits(None, "4"));
}
