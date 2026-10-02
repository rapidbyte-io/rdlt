use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use sha2::Digest as _;

use super::{Binary, Unfit};

/// A file at `path` holding `bytes`, with `mode`.
fn file(path: &Path, bytes: &[u8], mode: u32) {
    // One written before may not be written again.
    std::fs::remove_file(path).ok();
    std::fs::write(path, bytes).expect("the file writes");
    std::fs::set_permissions(path, PermissionsExt::from_mode(mode)).expect("its mode is set");
}

/// A directory of this user's alone, in `root`.
fn dir(root: &Path, name: &str) -> PathBuf {
    let dir = root.join(name);
    std::fs::create_dir(&dir).expect("the directory is made");
    std::fs::set_permissions(&dir, PermissionsExt::from_mode(0o755)).expect("its mode is set");
    dir
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    sha2::Sha256::digest(bytes).into()
}

#[test]
fn a_binary_named_without_a_path_is_found_only_in_the_directories_given_in_their_order() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let (first, second) = (dir(root.path(), "first"), dir(root.path(), "second"));
    file(&second.join("rdlt-connector-x"), b"second", 0o755);
    let dirs = [first.clone(), root.path().join("absent"), second.clone()];
    let found = Binary::named(&dirs, "rdlt-connector-x").expect("it is found");
    assert_eq!(found.path(), second.join("rdlt-connector-x"));
    file(&first.join("rdlt-connector-x"), b"first", 0o755);
    let found = Binary::named(&dirs, "rdlt-connector-x").expect("it is found");
    assert_eq!(found.path(), first.join("rdlt-connector-x"));
    assert_eq!(found.digest().expect("it hashes").0, sha256(b"first"));
    // No directory given: nothing is searched, whatever `PATH` or the working directory hold.
    assert!(matches!(Binary::named(&[], "sh"), Err(Unfit::Absent(None))));
    assert!(matches!(
        Binary::named(&dirs, "sh"),
        Err(Unfit::Absent(None))
    ));
}

#[test]
fn a_directory_that_is_not_an_absolute_path_is_refused_and_never_searched() {
    for relative in ["", ".", "bin", "./bin", "../bin"] {
        let refused = Binary::named(&[PathBuf::from(relative)], "sh");
        assert!(
            matches!(refused, Err(Unfit::Absent(Some(_)))),
            "{relative:?}"
        );
    }
    // An executable of the name in the working directory is not found through one.
    let cwd = std::env::current_dir().expect("a working directory");
    let name = "rdlt-connector-planted-in-the-working-directory";
    file(&cwd.join(name), b"#!/bin/sh\n", 0o755);
    let found = Binary::named(&[PathBuf::new()], name).map(|_| ());
    std::fs::remove_file(cwd.join(name)).expect("it is removed");
    assert!(matches!(found, Err(Unfit::Absent(Some(_)))));
}

/// Makes something at a path.
type Made = Box<dyn Fn(&Path)>;

#[test]
fn what_a_directory_holds_by_the_name_is_taken_only_as_an_executable_regular_file_no_link() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let (first, second) = (dir(root.path(), "first"), dir(root.path(), "second"));
    file(&second.join("real"), b"real", 0o755);
    let unfit: [(&str, Made); 4] = [
        (
            "not executable",
            Box::new(|path| file(path, b"data", 0o644)),
        ),
        (
            "a directory",
            Box::new(|path| std::fs::create_dir(path).expect("a directory")),
        ),
        (
            "a link out of the directory",
            Box::new(|path| {
                std::os::unix::fs::symlink("../second/real", path).expect("a link");
            }),
        ),
        (
            "a link within the directory",
            Box::new(|path| {
                file(&path.with_file_name("target"), b"target", 0o755);
                std::os::unix::fs::symlink("target", path).expect("a link");
            }),
        ),
    ];
    for (what, make) in unfit {
        let path = first.join("real");
        make(&path);
        // Not taken where it is unfit, and found in the directory after.
        let found = Binary::named(&[first.clone(), second.clone()], "real").expect(what);
        assert_eq!(found.path(), second.join("real"), "{what}");
        assert!(matches!(
            Binary::named(std::slice::from_ref(&first), "real"),
            Err(Unfit::Absent(None))
        ));
        if path.is_dir() && !path.is_symlink() {
            std::fs::remove_dir(&path).expect("it is removed");
        } else {
            std::fs::remove_file(&path).expect("it is removed");
        }
    }
}

#[test]
fn a_binary_or_a_directory_another_user_may_write_is_refused() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let private = dir(root.path(), "private");
    for mode in [0o775, 0o757, 0o777, 0o722] {
        let path = private.join("writable");
        file(&path, b"x", mode);
        for found in [
            Binary::at(&path),
            Binary::named(std::slice::from_ref(&private), "writable"),
        ] {
            let shared = matches!(&found, Err(Unfit::Shared { mode: seen, path: at, .. }) if *seen == mode && *at == path);
            assert!(shared, "{mode:o}: {found:?}");
        }
    }
    for mode in [0o775, 0o757, 0o1777] {
        let shared = dir(root.path(), &format!("shared-{mode:o}"));
        file(&shared.join("binary"), b"x", 0o755);
        std::fs::set_permissions(&shared, PermissionsExt::from_mode(mode))
            .expect("its mode is set");
        let found = Binary::named(std::slice::from_ref(&shared), "binary");
        assert!(
            matches!(&found, Err(Unfit::Shared { path, .. }) if *path == shared),
            "{mode:o}"
        );
        // Named by its path, it is refused too, unless another user may only add entries to
        // the directory, which is sticky, and may change none they do not own.
        let at = Binary::at(&shared.join("binary"));
        if mode & 0o1000 == 0 {
            assert!(
                matches!(&at, Err(Unfit::Shared { path, .. }) if *path == shared),
                "{mode:o}"
            );
        } else {
            assert!(at.is_ok(), "{mode:o}");
        }
    }
    for mode in [0o755, 0o700, 0o500, 0o555, 0o711] {
        let path = private.join("fit");
        file(&path, b"x", mode);
        assert!(Binary::at(&path).is_ok(), "{mode:o}");
    }
}

#[test]
fn a_binary_named_by_its_path_must_be_an_executable_file_there() {
    let root = tempfile::tempdir().expect("a temporary directory");
    file(&root.path().join("data"), b"x", 0o644);
    for unfit in [
        root.path().join("absent"),
        root.path().join("data"),
        root.path().to_owned(),
    ] {
        assert!(
            matches!(Binary::at(&unfit), Err(Unfit::Absent(Some(_)))),
            "{unfit:?}"
        );
    }
    file(&root.path().join("binary"), b"x", 0o755);
    std::os::unix::fs::symlink("binary", root.path().join("link")).expect("a link");
    // A path its operator wrote is resolved as written, links included.
    let found = Binary::at(&root.path().join("link")).expect("it opens");
    assert_eq!(found.digest().expect("it hashes").0, sha256(b"x"));
}

#[test]
fn what_was_opened_is_what_is_hashed_whatever_its_path_names_afterwards() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let path = root.path().join("connector");
    file(&path, b"the binary that was checked", 0o755);
    let binary = Binary::at(&path).expect("it opens");
    let checked = sha256(b"the binary that was checked");
    assert_eq!(binary.digest().expect("it hashes").0, checked);
    // Another file takes the name, as a rename over it does, and then the name is gone.
    file(
        &root.path().join("other"),
        b"another binary altogether",
        0o755,
    );
    std::fs::rename(root.path().join("other"), &path).expect("it is renamed");
    assert_eq!(binary.digest().expect("it hashes").0, checked);
    std::fs::remove_file(&path).expect("it is removed");
    assert_eq!(binary.digest().expect("it hashes").0, checked);
    assert_eq!(binary.path(), path);
    // Hashed again from its start each time.
    assert_eq!(binary.digest().expect("it hashes").0, checked);
}

#[test]
fn a_digest_is_of_every_byte_of_a_file_of_any_size() {
    let root = tempfile::tempdir().expect("a temporary directory");
    for size in [0, 1, 64 * 1024 - 1, 64 * 1024, 64 * 1024 + 1, 200_000] {
        let bytes: Vec<u8> = (0..size)
            .map(|byte| u8::try_from(byte % 251).expect("fits"))
            .collect();
        let path = root.path().join("sized");
        file(&path, &bytes, 0o755);
        let binary = Binary::at(&path).expect("it opens");
        assert_eq!(
            binary.digest().expect("it hashes").0,
            sha256(&bytes),
            "{size}"
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn a_script_is_told_from_a_binary_by_its_first_two_bytes() {
    let root = tempfile::tempdir().expect("a temporary directory");
    for (bytes, script) in [
        (&b"#!/bin/sh\nexit 0\n"[..], true),
        (b"#!", true),
        (b"#", false),
        (b"", false),
        (b"\x7fELF", false),
        (b" #!/bin/sh", false),
    ] {
        let path = root.path().join("program");
        file(&path, bytes, 0o755);
        let binary = Binary::at(&path).expect("it opens");
        assert_eq!(binary.is_script().expect("it reads"), script, "{bytes:?}");
    }
}

#[test]
fn a_binary_by_path_is_refused_where_a_directory_above_it_or_on_its_link_is_another_users() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let outer = dir(root.path(), "outer");
    let inner = dir(&outer, "inner");
    file(&inner.join("binary"), b"x", 0o755);
    assert!(Binary::at(&inner.join("binary")).is_ok());
    // A directory two levels up that another user may write.
    std::fs::set_permissions(&outer, PermissionsExt::from_mode(0o777)).expect("its mode is set");
    let refused = Binary::at(&inner.join("binary"));
    assert!(
        matches!(&refused, Err(Unfit::Shared { path, .. }) if *path == outer),
        "{refused:?}"
    );
    std::fs::set_permissions(&outer, PermissionsExt::from_mode(0o755)).expect("its mode is set");
    // A link in a directory another user may write, to a binary in one they may not.
    let open = dir(root.path(), "open");
    std::os::unix::fs::symlink(inner.join("binary"), open.join("link")).expect("a link");
    std::fs::set_permissions(&open, PermissionsExt::from_mode(0o777)).expect("its mode is set");
    let refused = Binary::at(&open.join("link"));
    assert!(
        matches!(&refused, Err(Unfit::Shared { path, .. }) if *path == open),
        "{refused:?}"
    );
}

#[test]
fn a_connector_directory_below_one_another_user_may_write_is_refused() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let outer = dir(root.path(), "outer");
    let bin = dir(&outer, "bin");
    file(&bin.join("rdlt-connector-x"), b"x", 0o755);
    std::fs::set_permissions(&outer, PermissionsExt::from_mode(0o777)).expect("its mode is set");
    let refused = Binary::named(std::slice::from_ref(&bin), "rdlt-connector-x");
    assert!(
        matches!(&refused, Err(Unfit::Shared { path, .. }) if *path == outer),
        "{refused:?}"
    );
}

#[test]
fn a_file_renamed_over_or_removed_is_no_longer_linked() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let path = root.path().join("binary");
    file(&path, b"x", 0o755);
    let binary = Binary::at(&path).expect("it opens");
    assert!(binary.linked().expect("it is asked"));
    file(&root.path().join("other"), b"y", 0o755);
    std::fs::rename(root.path().join("other"), &path).expect("it is renamed over");
    assert!(!binary.linked().expect("it is asked"));
}
