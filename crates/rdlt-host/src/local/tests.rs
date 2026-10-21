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
