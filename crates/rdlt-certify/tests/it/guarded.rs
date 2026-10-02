//! What the tests of this suite start ends with the test that started it, even one a runner
//! kills outright: the certifier, the connector it spawned, and a listening connector.

use std::time::Duration;

use rdlt_testkit::process::{outliving, ready, started};
use rdlt_testkit::tls::Pki;

use crate::listening::listening;
use crate::spawned::example;

/// A stand-in for a test: it starts a certification of a connector that never answers, and
/// a listening connector, as the suite's helpers start them, says which processes those are,
/// and waits to be killed.
#[tokio::test]
#[ignore = "run by the test below, as the process it kills"]
async fn stand_in_starting_a_certification_and_a_listening_connector() {
    let hanging = example("serve_hang");
    let certifying = tokio::process::Command::new(env!("CARGO_BIN_EXE_rdlt-certify"))
        .arg(&hanging)
        .args(["--role", "destination", "--trusted"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .process_group(0)
        .spawn()
        .expect("rdlt-certify runs");
    crate::guarded(&certifying);
    let certifier = certifying.id().expect("the certifier runs");
    started(certifier);
    // The connector it spawned, once it has.
    let mut connectors = Vec::new();
    for _ in 0..600 {
        let children = std::process::Command::new("pgrep")
            .args(["-P", &certifier.to_string()])
            .output()
            .expect("pgrep runs");
        let children = String::from_utf8_lossy(&children.stdout).into_owned();
        connectors = children
            .lines()
            .filter_map(|pid| pid.trim().parse().ok())
            .collect();
        if !connectors.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!connectors.is_empty(), "the certifier spawned no connector");
    connectors.into_iter().for_each(started);
    let (connector, _) = listening(&Pki::new("ca")).await;
    started(connector.id().expect("the connector runs"));
    // Held, not dropped: a test that is killed drops nothing.
    std::mem::forget((certifying, connector));
    ready()
}

#[test]
fn what_a_test_of_this_suite_started_is_gone_once_the_test_is_killed() {
    let test = "guarded::stand_in_starting_a_certification_and_a_listening_connector";
    let left = outliving(test, Duration::from_secs(30));
    assert_eq!(left, Vec::<u32>::new(), "processes outlived their test");
}
