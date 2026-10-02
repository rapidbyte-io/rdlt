//! What a spawned connector holds open: its standard streams and its socket, and nothing else
//! of its host's.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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
    // Nothing of the host's: the number leads to nothing, so the copy fails before it starts.
    let found = std::fs::read(&seen).unwrap_or_default();
    assert_eq!(String::from_utf8_lossy(&found), "");
    // And nothing on the connector's side read the host's file, which would have moved it.
    assert_eq!(held.stream_position().expect("its position"), 0);
}

/// Lets what this process starts inherit `file`, as a descriptor it was itself started with is.
#[cfg(target_os = "linux")]
fn rdlt_host_inheritable(file: &std::fs::File) {
    rustix::io::fcntl_setfd(file, rustix::io::FdFlags::empty()).expect("the flag clears");
}

/// Opens and closes a file without close-on-exec, again and again, until `stop` is set, as a
/// library that knows nothing of the flag does on a thread of its own.
fn opening(stop: Arc<AtomicBool>) -> std::thread::JoinHandle<u64> {
    std::thread::spawn(move || {
        let mut opened = 0;
        while !stop.load(Ordering::Relaxed) {
            let flags = rustix::fs::OFlags::RDONLY;
            if let Ok(file) = rustix::fs::open("/dev/null", flags, rustix::fs::Mode::empty()) {
                opened += 1;
                std::thread::sleep(std::time::Duration::from_micros(200));
                drop(file);
            }
        }
        opened
    })
}

/// Spawns `count` connectors with `local`, eight at a time, while another thread opens files
/// without close-on-exec: what each connector that did not start with its own descriptors
/// alone said, or why it did not start.
async fn leaked(local: rdlt_host::Local, count: usize) -> Vec<String> {
    let stop = Arc::new(AtomicBool::new(false));
    let opener = opening(Arc::clone(&stop));
    let mut leaked = Vec::new();
    for _ in 0..count / 8 {
        let spawning = (0..8).map(|_| {
            let local = local.clone();
            tokio::spawn(async move {
                let placed = local.source(&scripted(), &only_its_descriptors()).await;
                let source = placed.map_err(|error| format!("{error:?}"))?.connector;
                source.check().await.map_err(|error| format!("{error:?}"))
            })
        });
        for spawned in spawning.collect::<Vec<_>>() {
            leaked.extend(spawned.await.expect("the task ends").err());
        }
    }
    stop.store(true, Ordering::Relaxed);
    assert!(
        opener.join().expect("the opener ends") > 0,
        "the opener opened nothing"
    );
    leaked
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_trusted_connector_inherits_a_descriptor_another_thread_opens_as_it_is_spawned() {
    assert_eq!(leaked(local(), 304).await, Vec::<String>::new());
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_sandboxed_connector_inherits_a_descriptor_another_thread_opens_as_it_is_spawned() {
    let sandbox = rdlt_host::Bubblewrap::new();
    if let Err(unusable) = sandbox.usable() {
        rdlt_testkit::process::without_sandbox(&unusable);
        return;
    }
    let leaked = leaked(rdlt_host::Local::sandboxed(sandbox), 304).await;
    assert_eq!(leaked, Vec::<String>::new());
}
