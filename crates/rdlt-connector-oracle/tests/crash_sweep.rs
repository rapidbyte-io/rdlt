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
                PushPayload::Arrow(batch) => rows += batch.num_rows(),
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
    // Small batches on purpose: the crash points fire PER BATCH, and
    // an action like `1*off->return` needs more than one to reach.
    // With one batch per read the second cell never armed at all.
    let shell = fixture.shell_tuned(
        &[incremental("sweep", "SWEEP_T", "ID")],
        serde_json::json!({"batch_rows": 25}),
    );

    let mut fired = std::collections::BTreeSet::new();
    for &point in FAIL_POINTS {
        for action in ACTIONS {
            fail::cfg(point, action).expect("configure fail point");
            // The armed attempts CARRY THEIR CURSOR forward.
            //
            // Discarding it made the whole sweep vacuous: recovery
            // restarted from `None` with the fail point already
            // removed, so `seen == TOTAL_ROWS` was satisfied by a
            // plain uncrashed full read no matter what the read path
            // did. The property these cells exist to prove — that a
            // crash costs no rows and a resume repeats none — was the
            // one thing untested.
            let mut crashed_seen = 0usize;
            let mut cursor = None;
            let mut any_err = false;
            for _ in 0..2 {
                match attempt(shell.clone(), cursor.clone()).await {
                    Ok((rows, next)) => {
                        crashed_seen += rows;
                        if next.is_some() {
                            cursor = next;
                        }
                    }
                    Err(_) => any_err = true,
                }
            }
            fail::remove(point);
            if any_err {
                fired.insert((point, action));
            }

            // Recovery resumes FROM the crashed run's checkpoint.
            let mut seen = crashed_seen;
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
                "[{point} / {action}] a crash must cost no rows and a resume must repeat \
                 none — {crashed_seen} delivered before recovery"
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
