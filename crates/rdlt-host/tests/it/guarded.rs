//! What the tests of this suite start ends with the test that started it, even one a runner
//! kills outright: the host example, the connectors it spawns, and a listening connector.

use std::time::Duration;

use rdlt_testkit::process::{outliving, ready, started};
use rdlt_testkit::tls::Pki;

use crate::network::listening;
use crate::process::example;

/// A stand-in for a test: it starts a host of two connectors and a listening connector, as
/// the suite's helpers start them, says which processes those are, and waits to be killed.
#[tokio::test]
#[ignore = "run by the test below, as the process it kills"]
async fn stand_in_starting_a_host_and_a_listening_connector() {
    use tokio::io::AsyncBufReadExt as _;
    let mut host = tokio::process::Command::new(example("connector_host"))
        .arg(example("scripted_connector"))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .process_group(0)
        .spawn()
        .expect("the host starts");
    crate::process::guarded(&host);
    let stdout = host.stdout.take().expect("its output is piped");
    let mut lines = tokio::io::BufReader::new(stdout).lines();
    let said = lines.next_line().await.expect("it reads");
    assert_eq!(said.as_deref(), Some("ready"));
    let host = host.id().expect("the host runs");
    started(host);
    let connectors = std::process::Command::new("pgrep")
        .args(["-P", &host.to_string()])
        .output()
        .expect("pgrep runs");
    let connectors = String::from_utf8_lossy(&connectors.stdout).into_owned();
    let connectors: Vec<u32> = connectors
        .lines()
        .filter_map(|pid| pid.trim().parse().ok())
        .collect();
    assert_eq!(connectors.len(), 2, "{connectors:?}");
    connectors.into_iter().for_each(started);
    let pki = Pki::new("ca");
    let server = pki.server("server", &["localhost"]);
    let (connector, _) = listening(&pki, &server, "127.0.0.1:0").await;
    started(connector.id().expect("the connector runs"));
    // Held, not dropped: a test that is killed drops nothing.
    std::mem::forget(connector);
    ready()
}

#[test]
fn what_a_test_of_this_suite_started_is_gone_once_the_test_is_killed() {
    let test = "guarded::stand_in_starting_a_host_and_a_listening_connector";
    let left = outliving(test, Duration::from_secs(30));
    assert_eq!(left, Vec::<u32>::new(), "processes outlived their test");
}
