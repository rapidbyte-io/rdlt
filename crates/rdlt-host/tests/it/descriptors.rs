//! What a spawned connector holds open: its standard streams and its socket, and nothing else
//! of its host's.

use rdlt_host::Provider as _;

use crate::process::{local, scripted};

/// The script of a connector whose check fails unless it started with descriptors 0 to 3 alone.
fn only_its_descriptors() -> serde_json::Value {
    serde_json::json!({ "only_its_descriptors": true })
}

#[tokio::test]
async fn a_spawned_connector_starts_with_its_streams_and_its_socket_alone() {
    let source = local()
        .source(&scripted(), &only_its_descriptors())
        .await
        .expect("the connector starts")
        .connector;
    source.check().await.expect("it holds no other descriptor");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn connectors_spawned_at_once_hold_none_of_each_others_descriptors() {
    let spawning = (0..16).map(|_| {
        tokio::spawn(async move {
            let source = local()
                .source(&scripted(), &only_its_descriptors())
                .await
                .expect("the connector starts")
                .connector;
            source.check().await
        })
    });
    for spawned in spawning.collect::<Vec<_>>() {
        let checked = spawned.await.expect("the task ends");
        checked.expect("it holds no descriptor of another connector");
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_descriptor_the_host_holds_to_be_inherited_reaches_no_connector() {
    use std::io::{Seek as _, Write as _};
    use std::os::fd::AsRawFd as _;

    // A file the host was started with, as a supervisor's log is: open, and inherited by
    // whatever the host starts.
    let mut held = tempfile::tempfile().expect("a temporary file");
    held.write_all(b"the host's own").expect("it writes");
    held.rewind().expect("it rewinds");
    rdlt_host_inheritable(&held);
    let dir = tempfile::tempdir().expect("a temporary directory");
    let (seen, done) = (dir.path().join("seen"), dir.path().join("done"));
    // The connector starts a shell that copies what it finds at the descriptor's number, and
    // then says it has.
    let copy = format!(
        "cat <&{} > '{}' 2>/dev/null; touch '{}'",
        held.as_raw_fd(),
        seen.display(),
        done.display()
    );
    let script = serde_json::json!({ "starts": copy });
    let source = local()
        .grace(std::time::Duration::from_millis(200))
        .source(&scripted(), &script)
        .await
        .expect("the connector starts")
        .connector;
    source.check().await.expect("the connector answers");
    for _ in 0..2000 {
        if done.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(done.exists(), "the shell never ran");
    let found = std::fs::read(&seen).expect("the shell ran");
    // Nothing of the host's: the number leads to the null device, or to nothing.
    assert_eq!(String::from_utf8_lossy(&found), "");
    // And nothing on the connector's side read the host's file, which would have moved it.
    assert_eq!(held.stream_position().expect("its position"), 0);
}

/// Lets what this process starts inherit `file`, as a descriptor it was itself started with is.
#[cfg(target_os = "linux")]
fn rdlt_host_inheritable(file: &std::fs::File) {
    rustix::io::fcntl_setfd(file, rustix::io::FdFlags::empty()).expect("the flag clears");
}
