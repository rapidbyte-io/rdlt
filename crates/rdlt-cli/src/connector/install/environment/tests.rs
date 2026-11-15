use std::path::Path;

use rdlt_connector::text::CUT;

use super::{Own, shown};
use crate::output::LINE_BYTES;

#[tokio::test]
async fn what_a_child_prints_is_shown_a_line_at_a_time_and_the_end_of_a_long_line_dropped() {
    let long = "x".repeat(LINE_BYTES * 2 + 10);
    let printed = format!("plain\ncoloured \u{1b}[31mred\n{long}\nafter\nunended");
    let mut written = Vec::new();
    shown(Some(printed.as_bytes()), &mut written).await;
    let written = String::from_utf8(written).unwrap();
    let lines: Vec<&str> = written.lines().collect();
    let cut = format!("{}{CUT}", "x".repeat(LINE_BYTES - CUT.len()));
    let expected = [
        "plain",
        "coloured \\u{1b}[31mred",
        cut.as_str(),
        "after",
        "unended",
    ];
    assert_eq!(lines, expected);
    assert!(written.ends_with('\n'), "every line is ended: {written:?}");
}

/// A tree as venv makes one at `tree`: links of its `bin/` to an interpreter, and its
/// `pyvenv.cfg`.
fn made(tree: &Path) {
    std::fs::create_dir_all(tree.join("bin")).unwrap();
    std::os::unix::fs::symlink("/usr/bin/python3", tree.join("bin/python3")).unwrap();
    std::os::unix::fs::symlink("python3", tree.join("bin/python")).unwrap();
    std::fs::write(tree.join("bin/activate"), "a script").unwrap();
    std::fs::write(tree.join("pyvenv.cfg"), "home = /usr/bin\n").unwrap();
}

/// What replaces a file of a tree's own, at the path it is given.
type Replacing = fn(&Path);

#[test]
fn a_tree_whose_interpreter_or_its_configuration_was_replaced_is_refused() {
    let replacing: [(&str, Replacing); 4] = [
        ("bin/python", |path| {
            std::fs::remove_file(path).unwrap();
            std::fs::write(path, "#!/bin/sh\n").unwrap();
        }),
        ("bin/python3", |path| {
            std::fs::remove_file(path).unwrap();
            std::os::unix::fs::symlink("/elsewhere/python3", path).unwrap();
        }),
        ("pyvenv.cfg", |path| {
            std::fs::write(path, "home = /elsewhere\n").unwrap();
        }),
        ("pyvenv.cfg", |path| std::fs::remove_file(path).unwrap()),
    ];
    for (file, replace) in replacing {
        let dir = tempfile::tempdir().unwrap();
        let tree = dir.path().join("tree");
        made(&tree);
        let own = Own::of(&tree).unwrap();
        own.kept().expect("nothing was replaced");
        // What a package may add or change of the rest.
        std::fs::write(tree.join("bin/activate"), "another script").unwrap();
        std::fs::write(tree.join("bin/tool"), "a script").unwrap();
        own.kept().expect("the environment's own is kept");
        replace(&tree.join(file));
        let refused = own.kept().unwrap_err();
        assert_eq!(refused.code(), "replaces_environment", "{file}");
        assert!(
            matches!(&refused, super::InstallError::ReplacesEnvironment { path } if path == &tree.join(file)),
            "{file}: {refused:?}"
        );
    }
}

#[test]
fn a_configuration_a_package_adds_where_venv_made_none_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let own = Own::of(dir.path()).unwrap();
    own.kept().expect("nothing is there, and nothing was added");
    std::fs::write(dir.path().join("pyvenv.cfg"), "home = /elsewhere\n").unwrap();
    assert_eq!(own.kept().unwrap_err().code(), "replaces_environment");
}

#[test]
fn a_tree_whose_own_files_cannot_be_read_is_refused_as_io() {
    let unreadable: [(&str, Replacing); 2] = [
        // Its `bin/` a file, which no directory can be read of.
        ("bin", |path| {
            std::fs::remove_dir_all(path).unwrap();
            std::fs::write(path, "").unwrap();
        }),
        // Its `pyvenv.cfg` a directory, which no bytes can be read of.
        ("pyvenv.cfg", |path| {
            std::fs::remove_file(path).unwrap();
            std::fs::create_dir(path).unwrap();
        }),
    ];
    for (file, replace) in unreadable {
        let dir = tempfile::tempdir().unwrap();
        let tree = dir.path().join("tree");
        made(&tree);
        replace(&tree.join(file));
        let Err(refused) = Own::of(&tree) else {
            panic!("{file}: a tree that cannot be read is read");
        };
        assert_eq!(refused.code(), "io", "{file}");
    }
}
