use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use super::{launcher, locked, private, replaceable, replaced, swept, target};
use crate::connector::install::InstallError;

fn mode(path: &Path) -> u32 {
    path.symlink_metadata().unwrap().permissions().mode() & 0o7777
}

fn chmod(path: &Path, mode: u32) {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

#[test]
fn a_launcher_imports_its_entry_point_and_serves_it_with_the_tree_s_interpreter() {
    let tree = Path::new("/connectors/rdlt-connector-tickets");
    let (module, attribute) = target("tickets.source:Sources.Tickets").unwrap();
    let written = launcher(tree, module, attribute).unwrap();
    let expected = "#!/connectors/rdlt-connector-tickets/bin/python -I\n\
                    from rdlt.connector import serve\n\
                    from tickets.source import Sources\n\
                    raise SystemExit(serve(Sources.Tickets))\n";
    assert_eq!(String::from_utf8(written).unwrap(), expected);
}

#[test]
fn an_entry_point_that_names_no_attribute_of_a_module_is_refused() {
    for refused in [
        "tickets",
        "tickets:",
        ":Tickets",
        "tickets:Tickets [extra]",
        "tickets:Tickets\nimport os",
        "tickets; import os:Tickets",
        "1tickets:Tickets",
        "tickets..source:Tickets",
        "tickets:Tickets.",
        "tickets:Tic-kets",
    ] {
        let error = target(refused).unwrap_err();
        assert_eq!(error.code(), "entry_points", "{refused:?}");
    }
}

#[test]
fn a_launcher_whose_first_line_the_kernel_would_not_take_whole_is_refused() {
    // `#!`, the tree, `/bin/python` and ` -I`: 255 bytes before its newline, and then 256.
    let fits = PathBuf::from(format!("/{}", "c".repeat(238)));
    let written = launcher(&fits, "tickets", "Tickets").expect("a first line of 255 bytes fits");
    assert_eq!(written.iter().position(|byte| *byte == b'\n'), Some(255));
    let long = PathBuf::from(format!("/{}", "c".repeat(239)));
    let refused = launcher(&long, "tickets", "Tickets").unwrap_err();
    assert!(
        matches!(refused, InstallError::ShebangTooLong { length: 256 }),
        "{refused:?}"
    );
    for spaced in ["/my connectors/tickets", "/tabbed\tconnectors/tickets"] {
        let refused = launcher(Path::new(spaced), "tickets", "Tickets").unwrap_err();
        assert_eq!(refused.code(), "shebang_whitespace", "{spaced:?}");
    }
}

#[test]
fn a_tree_is_made_its_operator_s_alone_and_a_link_and_what_it_leads_to_are_left_as_they_are() {
    let root = tempfile::tempdir().unwrap();
    let tree = root.path().join("tree");
    std::fs::create_dir(&tree).unwrap();
    let file = tree.join("file");
    std::fs::write(&file, "").unwrap();
    let directory = tree.join("directory");
    std::fs::create_dir(&directory).unwrap();
    let outside = root.path().join("outside");
    std::fs::write(&outside, "").unwrap();
    let link = tree.join("link");
    std::os::unix::fs::symlink(&outside, &link).unwrap();
    for path in [&tree, &file, &directory, &outside] {
        chmod(path, 0o777);
    }
    let held = mode(&link);
    private(&tree).unwrap();
    for path in [&tree, &file, &directory] {
        assert_eq!(mode(path), 0o755, "{}", path.display());
    }
    assert_eq!(
        mode(&outside),
        0o777,
        "what a link leads to is not the tree's"
    );
    assert_eq!(mode(&link), held);
}

#[test]
fn an_old_tree_that_cannot_be_removed_is_left_once_the_new_one_is_in_place() {
    let connectors = tempfile::tempdir().unwrap();
    let tree = connectors.path().join("rdlt-connector-tickets");
    let held = tree.join("held");
    std::fs::create_dir_all(&held).unwrap();
    std::fs::write(held.join("file"), "").unwrap();
    // Its file cannot be removed from it.
    chmod(&held, 0o555);
    let staged = connectors.path().join(".rdlt-install-new");
    std::fs::create_dir(&staged).unwrap();
    std::fs::write(staged.join("new"), "").unwrap();
    let left = replaced(&staged, &tree, connectors.path()).unwrap();
    assert!(tree.join("new").is_file(), "the new tree is in place");
    let entries: Vec<PathBuf> = std::fs::read_dir(connectors.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    let named = |path: &&PathBuf| {
        let name = path.file_name().unwrap().to_string_lossy();
        name.starts_with(".rdlt-replaced-")
    };
    match entries.iter().find(named) {
        Some(at) => {
            assert!(left.is_some(), "the tree left is said");
            chmod(&at.join("tree/held"), 0o755);
        }
        // The superuser removes it all the same.
        None => assert!(left.is_none(), "{left:?}"),
    }
}

#[test]
fn one_install_at_a_time_holds_a_connector_directory() {
    let connectors = tempfile::tempdir().unwrap();
    let held = locked(connectors.path()).unwrap();
    let refused = locked(connectors.path()).unwrap_err();
    assert_eq!(refused.code(), "install_running");
    drop(held);
    locked(connectors.path()).expect("free once the first is done");
}

#[tokio::test]
async fn what_a_killed_install_left_is_swept_and_nothing_else() {
    let connectors = tempfile::tempdir().unwrap();
    let kept = [
        "rdlt-connector-tickets",
        ".rdlt-install.lock",
        "rdlt-connector-.rdlt-install-x",
    ];
    for name in kept {
        std::fs::write(connectors.path().join(name), "").unwrap();
    }
    std::fs::create_dir_all(connectors.path().join(".rdlt-install-a/bin")).unwrap();
    std::fs::create_dir_all(connectors.path().join(".rdlt-replaced-b/tree")).unwrap();
    std::fs::write(connectors.path().join(".rdlt-install-c"), "").unwrap();
    swept(connectors.path()).await;
    let mut left: Vec<String> = std::fs::read_dir(connectors.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    left.sort_unstable();
    let mut kept = kept.map(str::to_owned).to_vec();
    kept.sort_unstable();
    assert_eq!(left, kept);
}

#[test]
fn only_an_earlier_install_of_the_same_connector_may_be_replaced() {
    let connectors = tempfile::tempdir().unwrap();
    let tree = connectors.path().join("rdlt-connector-tickets");
    replaceable(&tree, "io.example.tickets").expect("nothing is there");
    std::fs::create_dir(&tree).unwrap();
    let id = tree.join("rdlt-connector.id");
    let refused = replaceable(&tree, "io.example.tickets").unwrap_err();
    assert_eq!(refused.code(), "name_taken", "a tree that records no id");
    std::fs::write(&id, "com.acme.tickets\n").unwrap();
    let refused = replaceable(&tree, "io.example.tickets").unwrap_err();
    assert_eq!(refused.code(), "name_taken", "a tree of another connector");
    std::fs::write(&id, "io.example.tickets\n").unwrap();
    replaceable(&tree, "io.example.tickets").expect("an earlier install of the connector");
    std::fs::remove_file(&id).unwrap();
    std::os::unix::fs::symlink(connectors.path().join("elsewhere"), &id).unwrap();
    std::fs::write(connectors.path().join("elsewhere"), "io.example.tickets\n").unwrap();
    let refused = replaceable(&tree, "io.example.tickets").unwrap_err();
    assert_eq!(
        refused.code(),
        "name_taken",
        "an id recorded through a link"
    );
    std::fs::remove_dir_all(&tree).unwrap();
    std::fs::write(&tree, "a native connector's binary").unwrap();
    let refused = replaceable(&tree, "io.example.tickets").unwrap_err();
    assert_eq!(refused.code(), "name_taken", "a binary of the name");
}

#[test]
fn a_tree_that_cannot_be_looked_at_is_no_tree_that_is_not_there() {
    let connectors = tempfile::tempdir().unwrap();
    let shut = connectors.path().join("shut");
    std::fs::create_dir(&shut).unwrap();
    chmod(&shut, 0o000);
    let looked = replaceable(&shut.join("rdlt-connector-tickets"), "io.example.tickets");
    // The superuser looks all the same, and finds nothing there.
    let superuser = std::fs::read_dir(&shut).is_ok();
    chmod(&shut, 0o755);
    if !superuser {
        assert_eq!(looked.unwrap_err().code(), "io");
    }
}
