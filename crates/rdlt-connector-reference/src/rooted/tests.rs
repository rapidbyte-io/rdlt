use std::cell::RefCell;
use std::ffi::OsStr;
use std::io::{ErrorKind, Write as _};
use std::os::unix::fs::{PermissionsExt as _, symlink};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use super::{Dir, Kind, Limit, Refusal, component, components, private, refusal, unique};

thread_local! {
    /// Every directory this thread synced, in order.
    pub(crate) static SYNCED: RefCell<Vec<PathBuf>> = const { RefCell::new(Vec::new()) };
}

const LIMIT: Limit = Limit {
    name: "test bytes",
    bytes: 8,
};

fn mode(path: &Path) -> u32 {
    std::fs::symlink_metadata(path)
        .unwrap()
        .permissions()
        .mode()
        & 0o7777
}

fn mkfifo(path: &Path) {
    let made = std::process::Command::new("mkfifo")
        .arg(path)
        .status()
        .unwrap();
    assert!(made.success());
}

/// A directory holding a file, a directory, a pipe, and links to a file and to a directory
/// outside it.
fn tree() -> (tempfile::TempDir, Dir) {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    std::fs::create_dir_all(root.join("inner")).unwrap();
    std::fs::create_dir_all(base.path().join("outside").join("below")).unwrap();
    std::fs::write(base.path().join("outside").join("secret"), b"secret").unwrap();
    std::fs::write(root.join("file"), b"12345678").unwrap();
    std::fs::write(root.join("inner").join("file"), b"inner").unwrap();
    symlink(
        base.path().join("outside").join("secret"),
        root.join("link"),
    )
    .unwrap();
    symlink(base.path().join("outside"), root.join("dirlink")).unwrap();
    symlink(base.path().join("nowhere"), root.join("dangling")).unwrap();
    mkfifo(&root.join("pipe"));
    let dir = Dir::ambient(&root).unwrap();
    (base, dir)
}

#[test]
fn a_name_is_one_normal_component() {
    let long = "x".repeat(255);
    for name in ["a", "a.b", " ", "..a", "a..", "...", long.as_str(), "ünï"] {
        assert!(component(OsStr::new(name)).is_ok(), "{name:?}");
    }
    let longer = "x".repeat(256);
    for name in [
        "",
        ".",
        "..",
        "a/b",
        "/",
        "/a",
        "a/",
        "a\0b",
        longer.as_str(),
    ] {
        let error = component(OsStr::new(name)).unwrap_err();
        assert_eq!(refusal(&error), Some(Refusal::Name), "{name:?}");
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    assert_eq!(components("a/b/c").unwrap(), ["a", "b", "c"]);
    assert_eq!(components("a").unwrap(), ["a"]);
    for path in ["", "/a", "a/", "a//b", "a/./b", "a/../b", "..", "a/.."] {
        let error = components(path).unwrap_err();
        assert_eq!(refusal(&error), Some(Refusal::Name), "{path:?}");
    }
}

#[test]
fn every_call_refuses_a_name_that_is_no_component() {
    let (_base, dir) = tree();
    for name in ["", ".", "..", "inner/file", "../outside", "/etc"] {
        let refused = |result: std::io::Result<()>| {
            let error = result.expect_err(name);
            assert_eq!(refusal(&error), Some(Refusal::Name), "{name:?}");
        };
        refused(dir.dir(name).map(drop));
        refused(dir.dir_created(name).map(drop));
        refused(dir.walk(["inner", name]).map(drop));
        refused(dir.walk_created(["inner", name]).map(drop));
        refused(dir.file(name).map(drop));
        refused(dir.read(name, LIMIT).map(drop));
        refused(dir.create(name).map(drop));
        refused(dir.kind(name).map(drop));
        refused(dir.remove_file(name));
        refused(dir.remove_dir(name));
        refused(dir.remove_tree(name));
        refused(dir.rename(name, &dir, "to"));
        refused(dir.rename("file", &dir, name));
        let mut temporary = dir.temporary().unwrap();
        temporary.file().write_all(b"x").unwrap();
        refused(temporary.publish(name).map(drop));
        refused(dir.temporary().unwrap().replace(name));
    }
    let empty: [&str; 0] = [];
    assert!(dir.walk(empty).is_err() && dir.walk_created(empty).is_err());
    // Nothing was made or removed.
    assert_eq!(dir.entries().unwrap().len(), 6);
}

#[test]
fn a_link_is_never_followed_as_a_directory_or_a_file() {
    let (base, dir) = tree();
    for name in ["dirlink", "link", "dangling"] {
        assert!(dir.dir(name).is_err(), "{name}");
        assert!(dir.dir_created(name).is_err(), "{name}");
        assert!(dir.walk([name, "below"]).is_err(), "{name}");
        assert!(dir.walk_created([name, "made"]).is_err(), "{name}");
        let error = dir.file(name).unwrap_err();
        assert_eq!(refusal(&error), Some(Refusal::NotRegular), "{name}");
        let error = dir.read(name, LIMIT).unwrap_err();
        assert_eq!(refusal(&error), Some(Refusal::NotRegular), "{name}");
        let error = dir.create(name).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::AlreadyExists, "{name}");
        assert_eq!(dir.kind(name).unwrap(), Some(Kind::Other), "{name}");
    }
    assert!(!base.path().join("nowhere").exists());
    assert!(!base.path().join("outside").join("made").exists());
    assert_eq!(dir.kind("gone").unwrap(), None);
    assert_eq!(dir.kind("inner").unwrap(), Some(Kind::Dir));
    assert_eq!(dir.kind("file").unwrap(), Some(Kind::File));
}

#[test]
fn a_pipe_a_directory_and_a_device_are_not_files() {
    let (_base, dir) = tree();
    for name in ["pipe", "inner"] {
        // A pipe nobody writes is refused at once, never waited on.
        let (ended, heard) = std::sync::mpsc::channel();
        let opened = Dir::ambient(dir.path()).unwrap();
        std::thread::spawn(move || ended.send(opened.file(name).map(drop)));
        let error = heard
            .recv_timeout(Duration::from_secs(20))
            .expect("the open ends")
            .unwrap_err();
        assert_eq!(refusal(&error), Some(Refusal::NotRegular), "{name}");
        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }
    let devices = Dir::ambient(Path::new("/dev")).unwrap();
    let error = devices.read("zero", LIMIT).unwrap_err();
    assert_eq!(refusal(&error), Some(Refusal::NotRegular));
    assert_eq!(dir.file("gone").unwrap_err().kind(), ErrorKind::NotFound);
    assert!(dir.file("file").is_ok());
}

#[test]
fn a_file_beyond_the_limit_is_refused_unread() {
    let (base, dir) = tree();
    assert_eq!(dir.read("file", LIMIT).unwrap(), b"12345678");
    std::fs::write(base.path().join("root").join("file"), b"123456789").unwrap();
    let error = dir.read("file", LIMIT).unwrap_err();
    let too_large = Refusal::TooLarge {
        name: "test bytes",
        limit: 8,
        actual: 9,
    };
    assert_eq!(refusal(&error), Some(too_large));
    // A sparse file larger than any memory is refused by its size alone.
    let huge = std::fs::File::create(base.path().join("root").join("huge")).unwrap();
    huge.set_len(1 << 44).unwrap();
    let error = dir.read("huge", LIMIT).unwrap_err();
    assert!(matches!(
        refusal(&error),
        Some(Refusal::TooLarge { actual, .. }) if actual == 1 << 44
    ));
    assert_eq!(
        dir.read("inner", LIMIT).unwrap_err().kind(),
        ErrorKind::InvalidData
    );
    let empty = Limit { bytes: 0, ..LIMIT };
    std::fs::write(base.path().join("root").join("empty"), b"").unwrap();
    assert_eq!(dir.read("empty", empty).unwrap(), b"");
}

#[test]
fn created_directories_and_files_are_their_owner_s_alone() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("a").join("b");
    SYNCED.with(|synced| synced.borrow_mut().clear());
    let dir = Dir::ambient_created(&root).unwrap();
    assert_eq!(dir.path(), root);
    let synced = SYNCED.with(|synced| synced.borrow().clone());
    assert_eq!(synced, [base.path().to_owned(), base.path().join("a")]);
    assert_eq!(mode(&base.path().join("a")), 0o700);
    assert_eq!(mode(&root), 0o700);
    // What exists is opened as it is, and nothing is synced for it.
    SYNCED.with(|synced| synced.borrow_mut().clear());
    Dir::ambient_created(&root).unwrap();
    assert!(SYNCED.with(|synced| synced.borrow().is_empty()));
    let made = dir.dir_created("made").unwrap();
    assert_eq!(made.path(), root.join("made"));
    assert_eq!(dir.at("made"), root.join("made"));
    assert_eq!(mode(&root.join("made")), 0o700);
    assert_eq!(
        SYNCED.with(|synced| synced.borrow().clone()),
        std::slice::from_ref(&root)
    );
    dir.dir_created("made").unwrap();
    assert_eq!(SYNCED.with(|synced| synced.borrow().len()), 1);
    let deep = dir.walk_created(["made", "deeper", "deepest"]).unwrap();
    assert_eq!(deep.path(), root.join("made/deeper/deepest"));
    assert_eq!(mode(&root.join("made/deeper/deepest")), 0o700);
    assert_eq!(
        dir.walk(["made", "deeper"]).unwrap().path(),
        root.join("made/deeper")
    );
    assert_eq!(
        dir.walk(["made", "gone"]).unwrap_err().kind(),
        ErrorKind::NotFound
    );
    let mut file = deep.create("file").unwrap();
    file.write_all(b"x").unwrap();
    assert_eq!(mode(&root.join("made/deeper/deepest/file")), 0o600);
    assert_eq!(
        deep.create("file").unwrap_err().kind(),
        ErrorKind::AlreadyExists
    );
    assert_eq!(deep.read("file", LIMIT).unwrap(), b"x");
    assert!(private(&deep.file("file").unwrap()).is_ok());
}

#[test]
fn a_relative_directory_whose_parent_is_the_working_directory_is_created() {
    let scratch = tempfile::Builder::new().tempdir_in(".").unwrap();
    let name = scratch.path().file_name().unwrap().to_string_lossy();
    // One relative component, whose parent is the empty path.
    let relative = PathBuf::from(format!("{name}-root"));
    let created = Dir::ambient_created(&relative);
    let exists = relative.is_dir();
    drop(std::fs::remove_dir_all(&relative));
    created.unwrap();
    assert!(exists);
}

#[test]
fn a_directory_others_may_write_is_not_private() {
    let base = tempfile::tempdir().unwrap();
    let path = base.path().join("shared");
    std::fs::create_dir(&path).unwrap();
    let set = |mode| std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode));
    for (mode, held) in [
        (0o700, true),
        (0o755, true),
        (0o775, false),
        (0o757, false),
        (0o722, false),
    ] {
        set(mode).unwrap();
        let checked = Dir::ambient(&path).unwrap().private();
        assert_eq!(checked.is_ok(), held, "{mode:o}");
        if let Err(error) = checked {
            assert_eq!(refusal(&error), Some(Refusal::Shared));
            assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        }
    }
    // What another user owns is not private either: the root user's directory, unless the
    // test runs as that user.
    let roots = Dir::ambient(Path::new("/")).unwrap().private();
    assert_eq!(roots.is_ok(), rustix::process::geteuid().is_root());
}

#[test]
fn entries_are_listed_with_what_they_are_in_name_order() {
    let (_base, dir) = tree();
    let entries: Vec<(String, Kind)> = dir
        .entries()
        .unwrap()
        .into_iter()
        .map(|(name, kind)| (name.into_string().unwrap(), kind))
        .collect();
    let expected = [
        ("dangling", Kind::Other),
        ("dirlink", Kind::Other),
        ("file", Kind::File),
        ("inner", Kind::Dir),
        ("link", Kind::Other),
        ("pipe", Kind::Other),
    ];
    let expected: Vec<(String, Kind)> = expected
        .into_iter()
        .map(|(name, kind)| (name.to_owned(), kind))
        .collect();
    assert_eq!(entries, expected);
    assert_eq!(dir.entries().unwrap().len(), 6, "a listing can be repeated");
}

#[test]
fn a_tree_is_removed_without_following_links() {
    let (base, dir) = tree();
    let root = base.path().join("root");
    symlink(base.path().join("outside"), root.join("inner").join("out")).unwrap();
    std::fs::create_dir(root.join("inner").join("deeper")).unwrap();
    std::fs::write(root.join("inner").join("deeper").join("file"), b"x").unwrap();
    assert!(dir.remove_dir("inner").is_err(), "it is not empty");
    assert!(dir.remove_file("inner").is_err(), "it is a directory");
    for name in [
        "inner", "dirlink", "link", "dangling", "pipe", "file", "gone",
    ] {
        dir.remove_tree(name).unwrap();
        assert_eq!(dir.kind(name).unwrap(), None, "{name}");
    }
    assert!(dir.entries().unwrap().is_empty());
    assert_eq!(
        std::fs::read(base.path().join("outside").join("secret")).unwrap(),
        b"secret"
    );
    assert!(base.path().join("outside").join("below").is_dir());
    assert_eq!(
        dir.remove_file("gone").unwrap_err().kind(),
        ErrorKind::NotFound
    );
    assert_eq!(
        dir.remove_dir("gone").unwrap_err().kind(),
        ErrorKind::NotFound
    );
}

#[test]
fn a_rename_moves_an_entry_itself_between_directories() {
    let (base, dir) = tree();
    let inner = dir.dir("inner").unwrap();
    dir.rename("dirlink", &inner, "moved").unwrap();
    assert_eq!(dir.kind("dirlink").unwrap(), None);
    assert_eq!(inner.kind("moved").unwrap(), Some(Kind::Other));
    dir.rename("file", &inner, "file").unwrap();
    assert_eq!(inner.read("file", LIMIT).unwrap(), b"12345678");
    assert!(base.path().join("outside").join("secret").exists());
    assert_eq!(
        dir.rename("gone", &inner, "x").unwrap_err().kind(),
        ErrorKind::NotFound
    );
}

#[test]
fn a_temporary_is_removed_unless_published() {
    let (base, dir) = tree();
    let root = base.path().join("root");
    let names = |dir: &Dir| -> Vec<String> {
        dir.entries()
            .unwrap()
            .into_iter()
            .map(|(name, _)| name.into_string().unwrap())
            .collect()
    };
    let before = names(&dir);
    // Dropped unpublished, as every failing path drops it.
    let mut temporary = dir.temporary().unwrap();
    temporary.file().write_all(b"half").unwrap();
    let during = names(&dir);
    assert_eq!(during.len(), before.len() + 1);
    let name = during.iter().find(|name| !before.contains(name)).unwrap();
    assert!(name.starts_with(".tmp-") && name.len() == 5 + 32, "{name}");
    assert_eq!(mode(&root.join(name)), 0o600);
    drop(temporary);
    assert_eq!(names(&dir), before);
    // Published under a free name: the name holds the bytes, the temporary is gone, the
    // directory synced.
    SYNCED.with(|synced| synced.borrow_mut().clear());
    let mut temporary = dir.temporary().unwrap();
    temporary.file().write_all(b"whole").unwrap();
    assert!(temporary.publish("published").unwrap());
    assert_eq!(dir.read("published", LIMIT).unwrap(), b"whole");
    assert_eq!(names(&dir).len(), before.len() + 1);
    assert_eq!(
        SYNCED.with(|synced| synced.borrow().clone()),
        std::slice::from_ref(&root)
    );
    // A name that exists, a link included, wins: nothing is replaced or followed.
    for taken in ["published", "link", "dangling", "inner"] {
        let mut temporary = dir.temporary().unwrap();
        temporary.file().write_all(b"late").unwrap();
        assert!(!temporary.publish(taken).unwrap(), "{taken}");
    }
    assert_eq!(dir.read("published", LIMIT).unwrap(), b"whole");
    assert!(!base.path().join("nowhere").exists());
    assert_eq!(names(&dir).len(), before.len() + 1);
    // Renamed over a name: the name holds the bytes, a link there is replaced itself.
    SYNCED.with(|synced| synced.borrow_mut().clear());
    for replaced in ["published", "link", "fresh"] {
        let mut temporary = dir.temporary().unwrap();
        temporary.file().write_all(b"next").unwrap();
        temporary.replace(replaced).unwrap();
        assert_eq!(dir.read(replaced, LIMIT).unwrap(), b"next", "{replaced}");
    }
    assert_eq!(SYNCED.with(|synced| synced.borrow().len()), 3);
    assert_eq!(
        std::fs::read(base.path().join("outside").join("secret")).unwrap(),
        b"secret"
    );
    assert_eq!(names(&dir).len(), before.len() + 2);
}

#[test]
fn temporaries_are_named_apart_and_those_of_one_file_are_told_from_others() {
    let first = unique("p-").unwrap().into_string().unwrap();
    let second = unique("p-").unwrap().into_string().unwrap();
    assert!(first.starts_with("p-") && first.len() == 34 && first != second);
    assert!(first[2..].bytes().all(|byte| byte.is_ascii_hexdigit()));
    let base = tempfile::tempdir().unwrap();
    let dir = Dir::ambient(base.path()).unwrap();
    let own = dir.temporary_of("keeper.json").unwrap();
    let other = dir.temporary_of("other.json").unwrap();
    let plain = dir.temporary().unwrap();
    let names: Vec<String> = dir
        .entries()
        .unwrap()
        .into_iter()
        .map(|(name, _)| name.into_string().unwrap())
        .collect();
    assert!(
        names
            .iter()
            .any(|name| name.starts_with(".keeper.json.tmp-"))
    );
    // Sweeping one file's temporaries leaves every other's.
    std::mem::forget((own, other, plain));
    dir.sweep_of("keeper.json", Duration::ZERO).unwrap();
    assert_eq!(dir.entries().unwrap().len(), 2);
    dir.sweep(Duration::ZERO).unwrap();
    let left = dir.entries().unwrap();
    assert_eq!(left.len(), 1);
    assert!(left[0].0.to_string_lossy().starts_with(".other.json.tmp-"));
}

#[test]
fn a_sweep_removes_only_temporaries_old_enough() {
    let base = tempfile::tempdir().unwrap();
    let dir = Dir::ambient(base.path()).unwrap();
    let age = Duration::from_secs(60);
    let written = |name: &str, ago: Duration| {
        let path = base.path().join(name);
        std::fs::write(&path, b"x").unwrap();
        let file = std::fs::File::options().write(true).open(&path).unwrap();
        file.set_modified(SystemTime::now() - ago).unwrap();
    };
    written(".tmp-old", Duration::from_secs(61));
    written(".tmp-young", Duration::from_secs(30));
    written("tmp-not-one", Duration::from_secs(600));
    written(".other", Duration::from_secs(600));
    // What no writer of this connector makes under a temporary's name is not waited for.
    symlink(
        base.path().join("tmp-not-one"),
        base.path().join(".tmp-link"),
    )
    .unwrap();
    std::fs::create_dir(base.path().join(".tmp-dir")).unwrap();
    std::fs::write(base.path().join(".tmp-dir").join("file"), b"x").unwrap();
    dir.sweep(age).unwrap();
    let left: Vec<String> = dir
        .entries()
        .unwrap()
        .into_iter()
        .map(|(name, _)| name.into_string().unwrap())
        .collect();
    assert_eq!(left, [".other", ".tmp-young", "tmp-not-one"]);
}

#[test]
fn a_limit_admits_sizes_up_to_it() {
    for (actual, admitted) in [
        (0, true),
        (7, true),
        (8, true),
        (9, false),
        (u64::MAX, false),
    ] {
        let refused = LIMIT.admit(actual).err();
        let too_large = Refusal::TooLarge {
            name: "test bytes",
            limit: 8,
            actual,
        };
        assert_eq!(refused, (!admitted).then_some(too_large), "{actual}");
    }
}

#[test]
fn what_a_descriptor_shows_to_be_no_file_is_refused_though_its_name_was_one() {
    // As when the name is replaced between being inspected and being opened.
    let (_base, dir) = tree();
    for name in ["link", "dirlink", "dangling", "pipe", "inner"] {
        let error = dir.opened(OsStr::new(name)).unwrap_err();
        assert_eq!(refusal(&error), Some(Refusal::NotRegular), "{name}");
    }
    assert!(dir.opened(OsStr::new("file")).is_ok());
    let gone = dir.opened(OsStr::new("gone")).unwrap_err();
    assert_eq!(gone.kind(), ErrorKind::NotFound);
}

#[cfg(target_os = "linux")]
#[test]
fn a_file_larger_than_it_was_measured_is_refused_as_it_is_read() {
    // A file of the process file system measures no bytes and reads many.
    let process = Dir::ambient(Path::new("/proc/self")).unwrap();
    assert_eq!(process.file("status").unwrap().metadata().unwrap().len(), 0);
    let error = process.read("status", LIMIT).unwrap_err();
    let too_large = Refusal::TooLarge {
        name: "test bytes",
        limit: 8,
        actual: 9,
    };
    assert_eq!(refusal(&error), Some(too_large));
    let whole = Limit {
        bytes: 1 << 20,
        ..LIMIT
    };
    assert!(process.read("status", whole).unwrap().len() > 9);
}
