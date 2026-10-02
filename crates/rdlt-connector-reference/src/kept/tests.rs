use rdlt_connector::PartitionId;

use super::Kept;

fn partition(id: &str) -> PartitionId {
    PartitionId::parse(id).unwrap()
}

#[test]
fn a_keeper_at_a_path_holds_its_positions_for_the_next_process() {
    let dir = crate::scratch::tempdir().unwrap();
    let path = dir.path().join("slot.json");
    let kept: Kept<u64> = Kept::keeping(&path).unwrap();
    assert_eq!(kept.position("orders", &partition("p0")), None);
    kept.advance("orders", &partition("p0"), 7).unwrap();
    kept.advance("orders", &partition("p1"), 3).unwrap();
    // Never moving back.
    kept.advance("orders", &partition("p0"), 5).unwrap();
    // As the next process finds it.
    let again: Kept<u64> = Kept::at(&path).unwrap();
    assert_eq!(again.position("orders", &partition("p0")), Some(7));
    assert_eq!(again.position("orders", &partition("p1")), Some(3));
    // Its temporary file is gone once it is written: the file and its lock are there.
    let mut files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    files.sort();
    assert_eq!(files, [".slot.json.lock", "slot.json"]);
}

#[test]
fn a_keeper_whose_file_is_damaged_is_refused_rather_than_found_empty() {
    let dir = crate::scratch::tempdir().unwrap();
    let path = dir.path().join("slot.json");
    let kept: Kept<u64> = Kept::keeping(&path).unwrap();
    kept.advance("orders", &partition("p0"), 7).unwrap();
    let whole = std::fs::read(&path).unwrap();
    std::fs::write(&path, &whole[..whole.len() / 2]).unwrap();
    assert!(Kept::<u64>::keeping(&path).is_err());
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
    let dir = crate::scratch::tempdir().unwrap();
    let path = dir.path().join("slot.json");
    std::fs::create_dir(&path).unwrap();
    let refused = Kept::<u64>::keeping(&path).unwrap_err();
    assert_eq!(
        crate::rooted::refusal(&refused),
        Some(crate::rooted::Refusal::NotRegular)
    );
    // So does a file that holds no positions.
    std::fs::remove_dir(&path).unwrap();
    std::fs::write(&path, b"{").unwrap();
    let refused = Kept::<u64>::keeping(&path).unwrap_err();
    assert_eq!(refused.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn a_keeper_reads_its_last_whole_file_beside_one_a_crash_left_half_written() {
    let dir = crate::scratch::tempdir().unwrap();
    let path = dir.path().join("slot.json");
    let kept: Kept<u64> = Kept::keeping(&path).unwrap();
    kept.advance("orders", &partition("p0"), 7).unwrap();
    // A process that died writing its next file left half of it beside the last whole one.
    let left = dir
        .path()
        .join(".slot.json.tmp-00000000000000000000000000000001");
    std::fs::write(left, b"[[\"orders\",").unwrap();
    // A temporary of another file beside it is not this keeper's to remove.
    let other = dir
        .path()
        .join(".other.json.tmp-00000000000000000000000000000001");
    std::fs::write(&other, b"[").unwrap();
    drop(kept);
    let again: Kept<u64> = Kept::keeping(&path).unwrap();
    assert_eq!(again.position("orders", &partition("p0")), Some(7));
    // Opening the keeper removed the half-written file, and only it.
    let files: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
    assert_eq!(
        files.len(),
        3,
        "the file, its lock and the other file's temporary"
    );
    assert!(other.exists());
    again.advance("orders", &partition("p0"), 9).unwrap();
    let last: Kept<u64> = Kept::at(&path).unwrap();
    assert_eq!(last.position("orders", &partition("p0")), Some(9));
    let files: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
    assert_eq!(files.len(), 3);
}

#[test]
fn an_advance_makes_its_rename_durable_in_the_keeper_s_directory() {
    use crate::rooted::trace;
    let dir = crate::scratch::tempdir().unwrap();
    let kept: Kept<u64> = Kept::keeping(&dir.path().join("slot.json")).unwrap();
    trace::clear();
    kept.advance("orders", &partition("p0"), 7).unwrap();
    let synced = trace::synced();
    assert_eq!(synced.last(), Some(&dir.path().to_owned()), "{synced:?}");
}

#[test]
fn a_keeper_whose_directory_cannot_be_written_is_refused_its_advance() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = crate::scratch::tempdir().unwrap();
    let keeping = dir.path().join("keeping");
    std::fs::create_dir(&keeping).unwrap();
    let kept: Kept<u64> = Kept::keeping(&keeping.join("slot.json")).unwrap();
    std::fs::set_permissions(&keeping, std::fs::Permissions::from_mode(0o500)).unwrap();
    // Where permissions bind nothing, as for root, the fault cannot be made.
    let writable = std::fs::write(keeping.join("probe"), b"").is_ok();
    let advanced = kept.advance("orders", &partition("p0"), 7);
    std::fs::set_permissions(&keeping, std::fs::Permissions::from_mode(0o700)).unwrap();
    if !writable {
        assert!(advanced.is_err());
    }
}

#[test]
fn a_link_at_a_temporary_name_or_at_the_keeper_is_never_written_through() {
    let dir = crate::scratch::tempdir().unwrap();
    let elsewhere = crate::scratch::tempdir().unwrap();
    let victim = elsewhere.path().join("authorized_keys");
    std::fs::write(&victim, b"precious").unwrap();
    let path = dir.path().join("slot.json");
    // A link under a name a temporary of the keeper's could have: it is neither written
    // through nor followed when the keeper removes what a crash left.
    let planted = dir
        .path()
        .join(".slot.json.tmp-00000000000000000000000000000001");
    std::os::unix::fs::symlink(&victim, &planted).unwrap();
    let kept: Kept<u64> = Kept::keeping(&path).unwrap();
    kept.advance("orders", &partition("p0"), 7).unwrap();
    assert_eq!(std::fs::read(&victim).unwrap(), b"precious");
    assert!(std::fs::symlink_metadata(&planted).unwrap().is_symlink());
    assert!(std::fs::symlink_metadata(&path).unwrap().is_file());
    // A link where the keeper's own file belongs is refused, read or written.
    let linked = dir.path().join("linked.json");
    std::os::unix::fs::symlink(&victim, &linked).unwrap();
    let refused = Kept::<u64>::keeping(&linked).unwrap_err();
    assert_eq!(refused.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(
        crate::rooted::refusal(&refused),
        Some(crate::rooted::Refusal::NotRegular)
    );
    let dangling = dir.path().join("dangling.json");
    std::os::unix::fs::symlink(elsewhere.path().join("made"), &dangling).unwrap();
    let refused = Kept::<u64>::keeping(&dangling).unwrap_err();
    assert_eq!(
        crate::rooted::refusal(&refused),
        Some(crate::rooted::Refusal::NotRegular)
    );
    assert!(!elsewhere.path().join("made").exists());
    assert_eq!(std::fs::read(&victim).unwrap(), b"precious");
}

#[test]
fn a_keeper_file_is_private() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = crate::scratch::tempdir().unwrap();
    let path = dir.path().join("slot.json");
    let kept: Kept<u64> = Kept::keeping(&path).unwrap();
    kept.advance("orders", &partition("p0"), 7).unwrap();
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
}

#[test]
fn a_keeper_file_that_is_too_large_or_no_regular_file_is_refused_unread() {
    let dir = crate::scratch::tempdir().unwrap();
    let huge = dir.path().join("huge.json");
    std::fs::File::create(&huge)
        .unwrap()
        .set_len(1 << 40)
        .unwrap();
    let refused = Kept::<u64>::keeping(&huge).unwrap_err();
    assert!(matches!(
        crate::rooted::refusal(&refused),
        Some(crate::rooted::Refusal::TooLarge {
            name: "keeper bytes",
            ..
        })
    ));
    let pipe = dir.path().join("pipe.json");
    let made = std::process::Command::new("mkfifo")
        .arg(&pipe)
        .status()
        .unwrap();
    assert!(made.success());
    let (ended, heard) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let refused = Kept::<u64>::keeping(&pipe).err();
        ended.send(refused.as_ref().and_then(crate::rooted::refusal))
    });
    let refused = heard.recv_timeout(std::time::Duration::from_secs(20));
    let not_regular = crate::rooted::Refusal::NotRegular;
    assert_eq!(refused, Ok(Some(not_regular)), "a pipe was waited on");
}

#[test]
fn keepers_are_shared_by_name_and_by_nothing_else() {
    let registry: super::Registry<u64> = super::Registry::new();
    let (named, again) = (registry.named(Some("group")), registry.named(Some("group")));
    named.advance("orders", &partition("p0"), 9).unwrap();
    assert_eq!(again.position("orders", &partition("p0")), Some(9));
    // Another name's keeper, the default keeper included, holds none of it; and every source
    // naming none reaches the default keeper, as a broker's default group.
    let (other, default) = (registry.named(Some("other")), registry.named(None));
    assert_eq!(other.position("orders", &partition("p0")), None);
    assert_eq!(default.position("orders", &partition("p0")), None);
    default.advance("orders", &partition("p0"), 3).unwrap();
    assert_eq!(
        registry.named(None).position("orders", &partition("p0")),
        Some(3)
    );
    assert_eq!(named.position("orders", &partition("p0")), Some(9));
}

#[test]
fn a_keeper_holds_a_bounded_number_of_positions() {
    let kept: Kept<u64> = Kept::default();
    for partition_index in 0..crate::limits::KEEPER_POSITIONS {
        let id = partition(&format!("p{partition_index}"));
        kept.advance("orders", &id, 1).unwrap();
    }
    let beyond = kept.advance("orders", &partition("beyond"), 1);
    assert!(beyond.is_err());
    assert_eq!(kept.position("orders", &partition("beyond")), None);
    // A partition it holds still advances.
    kept.advance("orders", &partition("p0"), 2).unwrap();
    assert_eq!(kept.position("orders", &partition("p0")), Some(2));
}

#[test]
fn an_acknowledgement_that_moves_nothing_writes_nothing() {
    use std::os::unix::fs::MetadataExt as _;
    let dir = crate::scratch::tempdir().unwrap();
    let path = dir.path().join("slot.json");
    let kept: Kept<u64> = Kept::keeping(&path).unwrap();
    kept.advance("orders", &partition("p0"), 7).unwrap();
    let written = std::fs::metadata(&path).unwrap().ino();
    for position in [7, 3] {
        kept.advance("orders", &partition("p0"), position).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), written);
    }
    kept.advance("orders", &partition("p0"), 8).unwrap();
    assert_ne!(std::fs::metadata(&path).unwrap().ino(), written);
}

#[test]
fn a_keeper_file_of_more_positions_than_a_keeper_holds_is_refused() {
    let dir = crate::scratch::tempdir().unwrap();
    let positions = |count: usize| -> Vec<(String, String, u64)> {
        (0..count)
            .map(|index| ("orders".to_owned(), format!("p{index}"), 1))
            .collect()
    };
    let most = crate::limits::KEEPER_POSITIONS;
    for (count, held) in [(most, true), (most + 1, false)] {
        let path = dir.path().join(format!("{count}.json"));
        std::fs::write(&path, serde_json::to_vec(&positions(count)).unwrap()).unwrap();
        let kept = Kept::<u64>::keeping(&path);
        assert_eq!(kept.is_ok(), held, "{count}");
        if let Err(error) = kept {
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        }
    }
}

#[test]
fn a_keeper_names_a_file_in_a_directory_that_exists() {
    assert!(Kept::<u64>::keeping(std::path::Path::new("/")).is_err());
    assert!(Kept::<u64>::keeping(std::path::Path::new("..")).is_err());
    let dir = crate::scratch::tempdir().unwrap();
    assert!(Kept::<u64>::keeping(&dir.path().join("missing").join("slot.json")).is_err());
    // A path of one name is a file of the working directory, which a keeper takes only where
    // it is its user's alone: a checkout others may write, or another user's, has no such file.
    if crate::rooted::Dir::ambient(std::path::Path::new(".")).is_err() {
        assert!(Kept::<u64>::keeping(std::path::Path::new("slot.json")).is_err());
        return;
    }
    let scratch = tempfile::Builder::new().tempdir_in(".").unwrap();
    let name = format!(
        "{}.json",
        scratch.path().file_name().unwrap().to_string_lossy()
    );
    let kept = Kept::<u64>::keeping(std::path::Path::new(&name)).unwrap();
    let advanced = kept.advance("orders", &partition("p0"), 1);
    let written = std::path::Path::new(&name).is_file();
    drop(std::fs::remove_file(&name));
    drop(std::fs::remove_file(format!(".{name}.lock")));
    advanced.unwrap();
    assert!(written);
}

#[test]
fn an_acknowledgement_whose_write_failed_is_kept_when_it_is_made_again() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = crate::scratch::tempdir().unwrap();
    let path = dir.path().join("keeper.json");
    let kept = Kept::<u64>::keeping(&path).unwrap();
    kept.advance("s", &partition("p"), 1).unwrap();
    let mode = |mode| {
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(mode)).unwrap();
    };
    mode(0o500);
    // Where permissions bind nothing, as for root, the fault cannot be made.
    let writable = std::fs::write(dir.path().join("probe"), b"").is_ok();
    let failed = kept.advance("s", &partition("p"), 7);
    mode(0o700);
    if writable {
        return;
    }
    failed.expect_err("the keeper cannot write");
    assert_eq!(kept.position("s", &partition("p")), Some(1));
    // The host acknowledges again what it was refused.
    kept.advance("s", &partition("p"), 7).unwrap();
    let reopened = Kept::<u64>::at(&path).unwrap();
    assert_eq!(reopened.position("s", &partition("p")), Some(7));
}

#[test]
fn an_acknowledgement_is_durable_step_by_step_and_stands_only_once_its_file_does() {
    use crate::rooted::trace::{self, Step};
    let dir = crate::scratch::tempdir().unwrap();
    let path = dir.path().join("keeper.json");
    let kept = Kept::<u64>::keeping(&path).unwrap();
    kept.advance("s", &partition("p"), 1).unwrap();
    trace::clear();
    kept.advance("s", &partition("p"), 2).unwrap();
    // The temporary is made durable before it takes the file's name, and the name after.
    let steps = trace::steps();
    let Step::Create(temporary) = steps[0].clone() else {
        panic!("{steps:?}");
    };
    let expected = [
        Step::Create(temporary.clone()),
        Step::SyncFile(temporary),
        Step::Rename(path.clone()),
        Step::SyncDir(dir.path().to_owned()),
    ];
    assert_eq!(steps, expected);
    // A fault at any step: the acknowledgement fails, the keeper stands where it stood, and the
    // acknowledgement made again is kept.
    for (step, position) in (0..expected.len()).zip(3_u64..) {
        trace::fail_at(step);
        kept.advance("s", &partition("p"), position)
            .expect_err("the step is refused");
        trace::clear();
        assert_eq!(
            kept.position("s", &partition("p")),
            Some(position - 1),
            "step {step}"
        );
        kept.advance("s", &partition("p"), position).unwrap();
        let reopened = Kept::<u64>::at(&path).unwrap();
        assert_eq!(reopened.position("s", &partition("p")), Some(position));
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            2,
            "step {step}"
        );
    }
    // A process that dies at any step leaves the file as it was or as it would be, whole.
    for step in 0..expected.len() {
        let before = Kept::<u64>::at(&path)
            .unwrap()
            .position("s", &partition("p"));
        trace::crash_at(step);
        kept.advance("s", &partition("p"), 100 + u64::try_from(step).unwrap())
            .expect_err("the process died");
        trace::clear();
        let after = Kept::<u64>::at(&path)
            .unwrap()
            .position("s", &partition("p"));
        let renamed = step == 3;
        let expected = if renamed { Some(103) } else { before };
        assert_eq!(after, expected, "step {step}");
    }
}

#[test]
fn a_keeper_trusts_only_a_file_and_a_directory_that_are_its_user_s_alone() {
    use std::io::ErrorKind;
    use std::os::unix::fs::PermissionsExt as _;
    let base = crate::scratch::tempdir().unwrap();
    let shared = base.path().join("shared");
    std::fs::create_dir(&shared).unwrap();
    let path = shared.join("slot.json");
    let mode = |path: &std::path::Path, mode| {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    };
    // As another user would plant it where they may write: a keeper standing far ahead.
    std::fs::write(&path, b"[[\"orders\",\"p0\",999999]]").unwrap();
    for (directory, file, trusted) in [
        (0o700, 0o600, true),
        (0o755, 0o644, true),
        (0o1777, 0o600, false),
        (0o777, 0o600, false),
        (0o770, 0o600, false),
        (0o700, 0o666, false),
        (0o700, 0o660, false),
    ] {
        mode(&shared, directory);
        mode(&path, file);
        let kept = Kept::<u64>::keeping(&path);
        assert_eq!(kept.is_ok(), trusted, "{directory:o} {file:o}");
        if let Err(error) = kept {
            assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        }
    }
    mode(&shared, 0o700);
    mode(&path, 0o600);
    // What is named as a temporary of the keeper's and is no file of this user's is left where
    // it is: never entered, never followed.
    let planted = shared.join(".slot.json.tmp-anything");
    std::fs::create_dir_all(planted.join("deep")).unwrap();
    std::fs::write(planted.join("deep").join("data"), b"x").unwrap();
    std::os::unix::fs::symlink(base.path(), shared.join(".slot.json.tmp-link")).unwrap();
    let kept = Kept::<u64>::keeping(&path).unwrap();
    assert_eq!(kept.position("orders", &partition("p0")), Some(999_999));
    assert!(planted.join("deep").join("data").exists());
    assert!(std::fs::symlink_metadata(shared.join(".slot.json.tmp-link")).is_ok());
}

#[test]
fn every_path_to_one_keeper_file_names_one_keeper() {
    let base = crate::scratch::tempdir().unwrap();
    let dir = base.path().join("keepers");
    std::fs::create_dir(&dir).unwrap();
    std::os::unix::fs::symlink(&dir, base.path().join("linked")).unwrap();
    let registry: super::Registry<u64> = super::Registry::new();
    let first = registry.at(&dir.join("slot.json")).unwrap();
    for spelled in [
        dir.join(".").join("slot.json"),
        dir.join("..").join("keepers").join("slot.json"),
        base.path().join("linked").join("slot.json"),
    ] {
        let again = registry.at(&spelled).unwrap();
        assert!(std::sync::Arc::ptr_eq(&first, &again), "{spelled:?}");
    }
    // Another file of the directory, and the same name in another directory, are other keepers.
    let other = registry.at(&dir.join("other.json")).unwrap();
    assert!(!std::sync::Arc::ptr_eq(&first, &other));
    let elsewhere = crate::scratch::tempdir().unwrap();
    let apart = registry.at(&elsewhere.path().join("slot.json")).unwrap();
    assert!(!std::sync::Arc::ptr_eq(&first, &apart));
}

#[test]
fn a_keeper_file_is_one_keeper_s_for_as_long_as_the_keeper_is_held() {
    use std::io::ErrorKind;
    let dir = crate::scratch::tempdir().unwrap();
    let path = dir.path().join("slot.json");
    let kept: Kept<u64> = Kept::keeping(&path).unwrap();
    kept.advance("orders", &partition("p0"), 7).unwrap();
    // A second keeper of the file, as another process would open, would write what it holds
    // over what the first wrote: it is refused, and removes no temporary of the first's.
    let pending = dir.path().join(".slot.json.tmp-0001");
    std::fs::write(&pending, b"[").unwrap();
    let error = Kept::<u64>::keeping(&path).expect_err("the file is held");
    assert_eq!(error.kind(), ErrorKind::ResourceBusy);
    assert!(pending.exists(), "the refused keeper swept the directory");
    // The same file by another path is the same file.
    let spelled = dir.path().join(".").join("slot.json");
    Kept::<u64>::keeping(&spelled).expect_err("the file is held");
    // Another file of the directory is another keeper's.
    Kept::<u64>::keeping(&dir.path().join("other.json")).expect("another file");
    // Once the first is gone, as its process is, the file is the next keeper's.
    drop(kept);
    let next: Kept<u64> = Kept::keeping(&path).expect("the file is free");
    assert_eq!(next.position("orders", &partition("p0")), Some(7));
    assert!(!pending.exists());
}

#[test]
fn a_lock_a_link_stands_in_for_is_refused() {
    let dir = crate::scratch::tempdir().unwrap();
    let outside = crate::scratch::tempdir().unwrap();
    let target = outside.path().join("target");
    std::fs::write(&target, b"").unwrap();
    std::os::unix::fs::symlink(&target, dir.path().join(".slot.json.lock")).unwrap();
    let error = Kept::<u64>::keeping(&dir.path().join("slot.json")).expect_err("a link");
    assert_eq!(
        crate::rooted::refusal(&error),
        Some(crate::rooted::Refusal::NotRegular)
    );
}

#[test]
fn a_group_and_a_file_never_share_a_keeper_whatever_they_are_named() {
    let dir = crate::scratch::tempdir().unwrap();
    let registry: super::Registry<u64> = super::Registry::new();
    let file = registry.at(&dir.path().join("slot.json")).unwrap();
    file.advance("orders", &partition("p0"), 7).unwrap();
    // A group named as a file's key was once spelled.
    let (device, inode) = crate::rooted::Dir::ambient(dir.path())
        .unwrap()
        .identity()
        .unwrap();
    let group = registry.named(Some(&format!("file:{device}:{inode}:slot.json")));
    assert_eq!(group.position("orders", &partition("p0")), None);
    assert!(!std::sync::Arc::ptr_eq(&file, &group));
}

#[test]
fn a_keeper_file_whose_name_is_no_text_is_refused() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt as _;
    // Its temporaries and its lock are named after it, as text: two such names could not be
    // told apart there.
    let dir = crate::scratch::tempdir().unwrap();
    let registry: super::Registry<u64> = super::Registry::new();
    let named = dir.path().join(OsStr::from_bytes(b"slot\xff.json"));
    let Err(error) = registry.at(&named) else {
        panic!("a name that is no text was taken");
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}
