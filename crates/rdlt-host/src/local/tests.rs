use std::os::unix::fs::PermissionsExt as _;

use super::{Binary, programs};

#[test]
fn a_program_with_a_second_name_is_held_as_linked_a_script_and_its_interpreter_alike() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let interpreter = root.path().join("sh");
    std::fs::copy("/bin/sh", &interpreter).expect("the shell copies");
    let script = root.path().join("rdlt-connector-script");
    let line = format!("#!{}\nexit 3\n", interpreter.display());
    std::fs::write(&script, line).expect("the script writes");
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
        .expect("the script is executable");
    let linked = || {
        let binary = Binary::at(&script).expect("the script opens");
        let programs = programs(&binary).expect("its programs");
        programs
            .iter()
            .map(|program| program.linked)
            .collect::<Vec<_>>()
    };
    assert_eq!(linked(), [false, false]);
    std::fs::hard_link(&interpreter, root.path().join("another-sh")).expect("a second name");
    assert_eq!(linked(), [false, true]);
    std::fs::hard_link(&script, root.path().join("another-script")).expect("a second name");
    assert_eq!(linked(), [true, true]);
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn a_binary_not_hashed_within_the_connect_deadline_is_not_launched() {
    use std::time::Duration;

    use rdlt_connector::ConnectorId;

    use super::Local;
    use crate::provider::{ConnectorRef, ProviderError};
    use crate::remote::{Deadlines, Options};

    let root = tempfile::tempdir().expect("a temporary directory");
    let script = root.path().join("rdlt-connector-large");
    std::fs::write(&script, "#!/bin/sh\nexit 3\n").expect("the script writes");
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
        .expect("the script is executable");
    // Sparse, so it takes no room, and long, so hashing it outlasts the deadline.
    let file = std::fs::OpenOptions::new().write(true).open(&script);
    file.expect("the script opens")
        .set_len(256 << 20)
        .expect("the script grows");
    let options = Options {
        deadlines: Deadlines {
            connect: Duration::from_millis(1),
            ..Deadlines::default()
        },
        ..Options::default()
    };
    let local = Local::trusting_binaries().options(options);
    let mut reference = ConnectorRef::new(ConnectorId::parse("test.large").expect("a valid id"));
    reference.path = Some(script);
    let Err(refused) = local.launch(&reference).await else {
        panic!("it was launched");
    };
    let ProviderError::SpawnFailed { source, .. } = refused else {
        panic!("{refused}");
    };
    assert_eq!(source.kind(), std::io::ErrorKind::TimedOut, "{source}");
}

// The real clock: a hash sent to a blocking thread is what it rules out, and a paused clock does
// not move while a blocking task runs.
#[cfg(all(target_os = "linux", feature = "sim"))]
#[tokio::test(flavor = "multi_thread")]
async fn a_played_placement_hashes_its_binary_where_it_is_placed() {
    use std::time::Duration;

    use rdlt_connector::ConnectorId;

    use super::Local;
    use crate::provider::ConnectorRef;
    use crate::remote::{Deadlines, Options};

    let root = tempfile::tempdir().expect("a temporary directory");
    let script = root.path().join("rdlt-connector-large");
    std::fs::write(&script, "#!/bin/sh\nexit 3\n").expect("the script writes");
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
        .expect("the script is executable");
    // Long enough that a hash on a thread of its own outlasts the deadline.
    let file = std::fs::OpenOptions::new().write(true).open(&script);
    file.expect("the script opens")
        .set_len(256 << 20)
        .expect("the script grows");
    let options = Options {
        deadlines: Deadlines {
            connect: Duration::from_millis(1),
            ..Deadlines::default()
        },
        ..Options::default()
    };
    let local = Local::trusting_binaries()
        .options(options)
        .played(|_| Err(std::io::Error::other("nothing is played")));
    let mut reference = ConnectorRef::new(ConnectorId::parse("test.large").expect("a valid id"));
    reference.path = Some(script.clone());
    let launch = local
        .launch(&reference)
        .await
        .expect("it is hashed in place");
    let expected = Binary::at(&script)
        .expect("it opens")
        .digest()
        .expect("it hashes");
    assert_eq!(launch.digest, Some(expected));
}

#[tokio::test]
async fn a_tree_put_where_a_checked_one_was_is_never_bound() {
    use std::os::unix::fs::MetadataExt as _;

    use rdlt_connector::ConnectorId;

    use super::Local;
    use crate::provider::{ConnectorRef, ProviderError};

    let temporary = tempfile::tempdir().expect("a temporary directory");
    let root = std::fs::canonicalize(temporary.path()).expect("it resolves");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).expect("its mode");
    let tree = |at: &std::path::Path| {
        std::fs::create_dir_all(at.join("bin")).expect("a tree");
        for directory in [at.to_owned(), at.join("bin")] {
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o755))
                .expect("its mode is set");
        }
        let launcher = at.join("bin/rdlt-connector");
        std::fs::write(&launcher, "#!/bin/sh\n").expect("the launcher writes");
        std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o755))
            .expect("the launcher is executable");
    };
    let identity = |metadata: std::fs::Metadata| (metadata.dev(), metadata.ino());
    let path = root.join("rdlt-connector-tree");
    tree(&path);
    let checked = identity(std::fs::metadata(&path).expect("the tree is there"));
    let binary = Binary::at(&path).expect("the tree opens");
    // Another tree takes its path once it was checked.
    std::fs::rename(&path, root.join("moved")).expect("the tree moves");
    tree(&path);
    let replacing = identity(std::fs::metadata(&path).expect("the other tree is there"));
    assert_ne!(replacing, checked, "another tree is at the path");
    let reference = ConnectorRef::new(ConnectorId::parse("test.tree").expect("a valid id"));
    let lease = Local::trusting_binaries()
        .lease(&reference, &binary, true)
        .await;
    // Linux finds the launcher where the kernel says the open file lies, so the tree checked is
    // held and bound, wherever it went; elsewhere the launcher is found again by its path, which
    // names another tree's now, and the placement is refused.
    if cfg!(target_os = "linux") {
        let lease = lease.expect("it is held");
        let bound = lease.bound.iter().find(|bound| bound.at == path);
        let bound = bound.expect("the tree is bound at its path");
        let held = identity(bound.file.metadata().expect("what is bound"));
        assert_eq!(held, checked, "what is bound is the tree checked");
    } else {
        let refused = lease.expect_err("a placement of a replaced tree is refused");
        assert!(
            matches!(
                &refused,
                ProviderError::SpawnFailed { source, .. } if source.to_string().contains("changed")
            ),
            "{refused:?}"
        );
    }
}
