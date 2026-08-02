#![cfg(feature = "failpoints")]
//! The crash sweep: every fail point × 3 actions through the read
//! path against the live database — armed twice, recovered disarmed,
//! with the resumed run reaching the same rows exactly once. Its own
//! binary, selected by name from `make test TARGET=sweep`;
//! skip-not-fail without a container runtime.

#[path = "cases/common.rs"]
mod common;

use common::{OracleFixture, incremental};
use rdlt_connector_oracle::source::{FAIL_POINTS, Shell};
use rdlt_connector_sdk::spi::core::failpoint::fail;
use rdlt_connector_sdk::spi::{PushPayload, ReadRequest, Source, StreamSpec};

const TOTAL_ROWS: usize = 300;
const ACTIONS: [&str; 3] = ["return", "panic", "1*off->return"];

/// Read one stream, returning the rows delivered and the last cursor.
///
/// The read runs in its own task: the `panic` fail-point action
/// panics inside it, and a panic must be an ATTEMPT failure to
/// observe, not the death of the sweep.
async fn attempt(
    shell: Shell,
    since: Option<rdlt_connector_sdk::spi::core::Cursor>,
) -> Result<(usize, Option<rdlt_connector_sdk::spi::core::Cursor>), String> {
    let (out, mut incoming) = rdlt_connector_sdk::spi::records_channel(32 << 20);
    let reader = tokio::spawn(async move {
        shell
            .read(ReadRequest {
                stream: StreamSpec::new("sweep"),
                since,
                out,
            })
            .await
            .map_err(|e| e.to_string())
    });
    let collect = async {
        let (mut rows, mut cursor) = (0usize, None);
        while let Some(push) = incoming.recv().await {
            match push.payload {
                PushPayload::RawJson(bytes) => {
                    rows += bytes.iter().filter(|b| **b == b'\n').count();
                }
                PushPayload::Checkpoint(c) => cursor = Some(c),
                _ => {}
            }
        }
        (rows, cursor)
    };
    let (joined, (rows, cursor)) = tokio::join!(reader, collect);
    match joined {
        Ok(Ok(())) => Ok((rows, cursor)),
        Ok(Err(e)) => Err(e),
        Err(join) => Err(format!("panicked: {join}")),
    }
}

/// Every point × action: armed twice (a crash during recovery too),
/// then disarmed — and the resumed read delivers the remaining rows
/// so the run as a whole sees each row exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn every_fail_point_recovers_exactly_once() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE SWEEP_T (ID NUMBER(8) PRIMARY KEY, V VARCHAR2(30))",
            &format!(
                "INSERT INTO SWEEP_T SELECT LEVEL, 'r'||LEVEL FROM DUAL \
                 CONNECT BY LEVEL <= {TOTAL_ROWS}"
            ),
        ])
        .await;
    let shell = fixture.shell(&[incremental("sweep", "SWEEP_T", "ID")]);

    let mut fired = std::collections::BTreeSet::new();
    for &point in FAIL_POINTS {
        for action in ACTIONS {
            fail::cfg(point, action).expect("configure fail point");
            let armed1 = attempt(shell.clone(), None).await;
            let armed2 = attempt(shell.clone(), None).await;
            fail::remove(point);
            if armed1.is_err() || armed2.is_err() {
                fired.insert((point, action));
            }

            // Recovery from wherever the armed attempts left off: a
            // crashed read must never cost rows, and a resumed one
            // must never repeat what it already delivered.
            let (mut seen, mut cursor) = (0usize, None);
            for _ in 0..3 {
                let (rows, next) = attempt(shell.clone(), cursor.clone())
                    .await
                    .unwrap_or_else(|e| panic!("[{point} / {action}] recovery failed: {e}"));
                seen += rows;
                if next.is_none() || rows == 0 {
                    break;
                }
                cursor = next;
            }
            assert_eq!(
                seen, TOTAL_ROWS,
                "[{point} / {action}] the recovered run must see every row exactly once"
            );
        }
    }
    let expected: std::collections::BTreeSet<_> = FAIL_POINTS
        .iter()
        .flat_map(|p| ACTIONS.iter().map(move |a| (*p, *a)))
        .collect();
    assert_eq!(fired, expected, "the armed-fire matrix must be complete");
}

/// The registry names exactly the points armed in the sources — the
/// self-check before container minutes are spent (the ungated twin
/// lives in cases/test_gating.rs).
#[test]
fn the_registry_matches_the_sources() {
    rdlt_testkit::assert_registry_matches_sources(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .as_path(),
        &[FAIL_POINTS],
    );
}
