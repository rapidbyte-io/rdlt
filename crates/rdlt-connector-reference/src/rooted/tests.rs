use std::ffi::OsStr;
use std::io::{ErrorKind, Write as _};
use std::os::unix::fs::{PermissionsExt as _, symlink};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use super::{Dir, Kind, Limit, Refusal, component, components, private, refusal, trace, unique};

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
    let base = crate::scratch::tempdir().unwrap();
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
        // Whatever error the platform answers such an open with, it is no directory.
        for entered in [
            dir.dir(name),
            dir.dir_created(name),
            dir.walk([name, "below"]),
            dir.walk_created([name, "made"]),
        ] {
            let error = entered.unwrap_err();
            assert_eq!(refusal(&error), Some(Refusal::NotDirectory), "{name}");
            assert_eq!(error.kind(), ErrorKind::NotADirectory, "{name}");
        }
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
    for name in ["file", "pipe"] {
        let error = dir.dir(name).unwrap_err();
        assert_eq!(refusal(&error), Some(Refusal::NotDirectory), "{name}");
    }
    assert_eq!(dir.dir("gone").unwrap_err().kind(), ErrorKind::NotFound);
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
    let devices = Dir::trusted(Path::new("/dev")).unwrap();
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
    huge.set_len(1 << 40).unwrap();
    let error = dir.read("huge", LIMIT).unwrap_err();
    assert!(matches!(
        refusal(&error),
        Some(Refusal::TooLarge { actual, .. }) if actual == 1 << 40
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
    let base = crate::scratch::tempdir().unwrap();
    let root = base.path().join("a").join("b");
    trace::clear();
    let dir = Dir::ambient_created(&root).unwrap();
    assert_eq!(dir.path(), root);
    let synced = trace::synced();
    assert_eq!(synced, [base.path().to_owned(), base.path().join("a")]);
    assert_eq!(mode(&base.path().join("a")), 0o700);
    assert_eq!(mode(&root), 0o700);
    // What exists is opened as it is, and nothing is synced for it.
    trace::clear();
    Dir::ambient_created(&root).unwrap();
    assert!(trace::synced().is_empty());
    let made = dir.dir_created("made").unwrap();
    assert_eq!(made.path(), root.join("made"));
    assert_eq!(dir.at("made"), root.join("made"));
    assert_eq!(mode(&root.join("made")), 0o700);
    assert_eq!(trace::synced(), std::slice::from_ref(&root));
    dir.dir_created("made").unwrap();
    assert_eq!(trace::synced().len(), 1);
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
fn a_root_and_every_directory_entered_beneath_it_are_their_user_s_alone() {
    let base = crate::scratch::tempdir().unwrap();
    let path = base.path().join("shared");
    std::fs::create_dir(&path).unwrap();
    let parent = Dir::ambient(base.path()).unwrap();
    let set = |mode| std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode));
    let ours = rustix::process::geteuid().as_raw();
    for (mode, held) in [
        (0o700, true),
        (0o755, true),
        (0o1755, true),
        (0o775, false),
        (0o757, false),
        (0o722, false),
        (0o1777, false),
    ] {
        set(mode).unwrap();
        // As a root, as a directory entered beneath one, and as one that would be created.
        let opened = [
            Dir::ambient(&path),
            Dir::ambient_created(&path),
            parent.dir("shared"),
            parent.dir_created("shared"),
            parent.walk(["shared"]),
        ];
        for entered in opened {
            assert_eq!(entered.is_ok(), held, "{mode:o}");
            if let Err(error) = entered {
                let shared = Refusal::Shared { owner: ours, mode };
                assert_eq!(refusal(&error), Some(shared));
                assert_eq!(error.kind(), ErrorKind::PermissionDenied);
            }
        }
    }
    set(0o700).unwrap();
    // What another user owns is not private either: the root user's directory, unless the
    // test runs as that user.
    let roots = Dir::ambient(Path::new("/"));
    assert_eq!(roots.is_ok(), rustix::process::geteuid().is_root());
    if let Err(error) = roots {
        assert!(matches!(
            refusal(&error),
            Some(Refusal::Shared { owner: 0, .. })
        ));
    }
    // A file's privacy is its own: its owner and its mode.
    let file = base.path().join("file");
    std::fs::write(&file, b"").unwrap();
    for (mode, held) in [(0o600, true), (0o644, true), (0o664, false), (0o646, false)] {
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(mode)).unwrap();
        let checked = private(&std::fs::File::open(&file).unwrap());
        assert_eq!(checked.is_ok(), held, "{mode:o}");
    }
}

#[test]
fn a_mount_point_beneath_a_root_is_not_entered() {
    // The device file system is mounted beneath the root directory on every Unix.
    let root = Dir::trusted(Path::new("/")).unwrap();
    let error = root.dir("dev").unwrap_err();
    assert_eq!(refusal(&error), Some(Refusal::Mounted));
    assert_eq!(error.kind(), ErrorKind::NotADirectory);
}

#[test]
fn a_tree_deeper_than_any_the_connectors_make_is_not_removed() {
    use crate::limits::TREE_DEPTH;
    let base = crate::scratch::tempdir().unwrap();
    let dir = Dir::ambient(base.path()).unwrap();
    let nested = |levels: usize| vec!["d"; levels].join("/");
    for (name, levels, removed) in [("fits", TREE_DEPTH, true), ("deep", TREE_DEPTH + 1, false)] {
        std::fs::create_dir_all(base.path().join(name).join(nested(levels - 1))).unwrap();
        let outcome = dir.remove_tree(name);
        assert_eq!(outcome.is_ok(), removed, "{name}");
        assert_eq!(dir.kind(name).unwrap().is_none(), removed, "{name}");
        if let Err(error) = outcome {
            let too_deep = Refusal::TooDeep { limit: TREE_DEPTH };
            assert_eq!(refusal(&error), Some(too_deep));
        }
    }
}

#[test]
fn bytes_beyond_what_a_file_was_measured_to_hold_refuse_it_as_it_is_read() {
    // As a file that grows between being measured and being read.
    for (held, admitted) in [(0_u64, true), (8, true), (9, false), (1 << 20, false)] {
        let reader = std::io::Read::take(std::io::repeat(b'x'), held);
        let read = super::within(reader, LIMIT);
        let expected = admitted.then(|| usize::try_from(held).unwrap());
        assert_eq!(read.as_ref().ok().map(Vec::len), expected, "{held}");
        if let Err(error) = read {
            let too_large = Refusal::TooLarge {
                name: "test bytes",
                limit: 8,
                actual: 9,
            };
            assert_eq!(refusal(&error), Some(too_large), "{held}");
        }
    }
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
    trace::clear();
    let mut temporary = dir.temporary().unwrap();
    temporary.file().write_all(b"whole").unwrap();
    assert!(temporary.publish("published").unwrap());
    assert_eq!(dir.read("published", LIMIT).unwrap(), b"whole");
    assert_eq!(names(&dir).len(), before.len() + 1);
    assert_eq!(trace::synced(), std::slice::from_ref(&root));
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
    trace::clear();
    for replaced in ["published", "link", "fresh"] {
        let mut temporary = dir.temporary().unwrap();
        temporary.file().write_all(b"next").unwrap();
        temporary.replace(replaced).unwrap();
        assert_eq!(dir.read(replaced, LIMIT).unwrap(), b"next", "{replaced}");
    }
    assert_eq!(trace::synced().len(), 3);
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
    let base = crate::scratch::tempdir().unwrap();
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
    let base = crate::scratch::tempdir().unwrap();
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
    // Written after now, as a clock that stepped back shows it: not old.
    written(".tmp-future", Duration::ZERO);
    let ahead = SystemTime::now() + Duration::from_hours(24);
    let future = std::fs::File::options()
        .write(true)
        .open(base.path().join(".tmp-future"))
        .unwrap();
    future.set_modified(ahead).unwrap();
    // One that cannot be opened is as old as its name says: it is never opened.
    written(".tmp-closed", Duration::from_secs(600));
    let closed = std::fs::Permissions::from_mode(0o000);
    std::fs::set_permissions(base.path().join(".tmp-closed"), closed).unwrap();
    // What no writer of this connector makes under a temporary's name is left where it is,
    // never followed and never entered.
    symlink(
        base.path().join("tmp-not-one"),
        base.path().join(".tmp-link"),
    )
    .unwrap();
    std::fs::create_dir(base.path().join(".tmp-dir")).unwrap();
    std::fs::write(base.path().join(".tmp-dir").join("file"), b"x").unwrap();
    mkfifo(&base.path().join(".tmp-pipe"));
    dir.sweep(age).unwrap();
    let left: Vec<String> = dir
        .entries()
        .unwrap()
        .into_iter()
        .map(|(name, _)| name.into_string().unwrap())
        .collect();
    let expected = [
        ".other",
        ".tmp-dir",
        ".tmp-future",
        ".tmp-link",
        ".tmp-pipe",
        ".tmp-young",
        "tmp-not-one",
    ];
    assert_eq!(left, expected);
    assert!(base.path().join(".tmp-dir").join("file").exists());
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
        // On a thread of its own, so an open that waited on the pipe fails the test.
        let (ended, heard) = std::sync::mpsc::channel();
        let opening = Dir::ambient(dir.path()).unwrap();
        std::thread::spawn(move || ended.send(opening.opened(OsStr::new(name)).map(drop)));
        let opened = heard.recv_timeout(Duration::from_secs(20));
        let error = opened.expect("the open ends").unwrap_err();
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
    let process = Dir::trusted(Path::new("/proc/self")).unwrap();
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

#[test]
fn every_durable_step_is_recorded_in_order_and_a_fault_refuses_its_step() {
    use trace::Step;
    let base = crate::scratch::tempdir().unwrap();
    let dir = Dir::ambient(base.path()).unwrap();
    let at = |name: &str| base.path().join(name);
    trace::clear();
    let made = dir.dir_created("made").unwrap();
    dir.dir_created("made").unwrap();
    let mut temporary = made.temporary().unwrap();
    temporary.file().write_all(b"x").unwrap();
    assert!(temporary.publish("file").unwrap());
    made.rename("file", &dir, "moved").unwrap();
    dir.remove_file("moved").unwrap();
    dir.remove_dir("made").unwrap();
    let steps = trace::steps();
    let (Step::Create(temporary) | Step::MakeDir(temporary)) = steps[2].clone() else {
        panic!("{steps:?}");
    };
    let expected = [
        Step::MakeDir(at("made")),
        Step::SyncDir(base.path().to_owned()),
        Step::Create(temporary.clone()),
        Step::SyncFile(temporary.clone()),
        Step::Link(at("made").join("file")),
        Step::SyncDir(at("made")),
        Step::Remove(temporary),
        Step::Rename(at("moved")),
        Step::Remove(at("moved")),
        Step::RemoveDir(at("made")),
    ];
    assert_eq!(steps, expected);
    // A fault refuses its step alone; a crash refuses it and every step after it.
    trace::fail_at(1);
    dir.create("a").unwrap();
    assert!(dir.create("b").is_err() && !at("b").exists());
    dir.create("c").unwrap();
    trace::crash_at(1);
    dir.create("d").unwrap();
    assert!(dir.create("e").is_err() && dir.create("f").is_err());
    assert!(dir.remove_file("d").is_err() && at("d").exists());
    trace::clear();
    dir.remove_file("d").unwrap();
}

#[test]
fn a_limited_reader_refuses_the_first_byte_beyond_its_limit() {
    use std::io::Read as _;
    for (held, chunk) in [(8_u64, 3), (8, 8), (8, 64), (0, 4)] {
        let mut reader = super::Limited::new(std::io::repeat(b'x').take(held), LIMIT);
        let mut bytes = Vec::new();
        let mut buffer = vec![0; chunk];
        loop {
            let read = reader.read(&mut buffer).unwrap();
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..read]);
        }
        assert_eq!(bytes.len(), usize::try_from(held).unwrap(), "{chunk}");
    }
    // One byte more: the read fails, by however many bytes it went beyond, and never ends as
    // though the file did.
    for chunk in [1, 3, 9, 64] {
        let mut reader = super::Limited::new(std::io::repeat(b'x').take(1 << 20), LIMIT);
        let mut buffer = vec![0; chunk];
        let error = loop {
            match reader.read(&mut buffer) {
                Ok(read) => assert!(read > 0, "the reader ended"),
                Err(error) => break error,
            }
        };
        let too_large = Refusal::TooLarge {
            name: "test bytes",
            limit: 8,
            actual: 9,
        };
        assert_eq!(refusal(&error), Some(too_large), "{chunk}");
    }
}

#[test]
fn a_sync_that_fails_fails_its_caller_and_is_not_recorded_as_made() {
    // A pipe is a file no system makes durable: its sync fails as a disk's may.
    let (reader, _writer) = std::io::pipe().unwrap();
    let unsyncable = std::fs::File::from(std::os::fd::OwnedFd::from(reader));
    trace::clear();
    let failed = super::sync_file(&unsyncable, Path::new("pipe"));
    assert!(failed.is_err(), "a sync that failed was taken as made");
    assert_eq!(trace::steps(), [], "a sync that failed was recorded");
    // A file's sync is made and recorded.
    let base = crate::scratch::tempdir().unwrap();
    let file = std::fs::File::create(base.path().join("file")).unwrap();
    super::sync_file(&file, Path::new("file")).unwrap();
    assert_eq!(trace::steps(), [trace::Step::SyncFile("file".into())]);
}

#[test]
fn an_entry_s_status_is_asked_of_the_directory_and_follows_no_link() {
    let (base, dir) = tree();
    let path = base.path().join("root").join("entry");
    std::fs::write(&path, b"x").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
    let found = dir.status("entry").unwrap().unwrap();
    let ours = rustix::process::geteuid().as_raw();
    assert_eq!(
        (found.kind, found.owner, found.mode, found.links),
        (Kind::File, ours, 0o640, 1)
    );
    assert!(found.private_of(0o022).is_ok());
    assert!(matches!(
        found.private_of(0o077),
        Err(Refusal::Shared { mode: 0o640, .. })
    ));
    std::fs::hard_link(&path, base.path().join("root").join("entry-again")).unwrap();
    assert_eq!(dir.status("entry").unwrap().unwrap().links, 2);
    assert_eq!(dir.status("link").unwrap().unwrap().kind, Kind::Other);
    assert_eq!(dir.status("missing").unwrap(), None);
    assert_eq!(dir.kind("entry").unwrap(), Some(Kind::File));
}

#[test]
fn a_lock_on_a_lock_file_outlives_every_other_open_and_close_of_it_in_the_process() {
    let (base, dir) = tree();
    let held = dir.lock_file("table.lock").unwrap();
    held.try_lock().unwrap();
    // The lock is its descriptor's: closing another descriptor of the file releases nothing.
    drop(dir.lock_file("table.lock").unwrap());
    drop(dir.file("table.lock").unwrap());
    drop(std::fs::File::open(base.path().join("root").join("table.lock")).unwrap());
    let other = dir.lock_file("table.lock").unwrap();
    assert!(matches!(
        other.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    drop(held);
    other.try_lock().unwrap();
}
