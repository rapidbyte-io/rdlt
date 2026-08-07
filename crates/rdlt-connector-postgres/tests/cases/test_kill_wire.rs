//! THE KILL MATRIX (041 Task 4): the spawned pg bin SIGKILLed at every
//! K boundary against a LIVE server — the first kill matrix ever run
//! against a real database (the file cell is hermetic on a tempdir).
//! Both halves of the K-vocabulary: K-S1/K-S2/K-S3 on the read wire
//! (typed-error-not-hang within the kit's window), K-D1..K-D6 on the
//! session wire (typed error on the dead wire, then exactly-once
//! convergence: a FRESH spawn re-runs the load and the read-back probe
//! must count the fixture rows EXACTLY — here that read-back is real
//! SQL over a separate connection, not a directory listing).
//!
//! THE FIXTURE-SIZE OBLIGATION this cell carries: the kit's source arms
//! dial with a floored 64 KiB h2 window, and a stream the window can
//! swallow WHOLE before the SIGKILL ends cleanly — which the kit
//! reports as an honest Skip, never a vacuous Pass. Task 3's
//! `small_config` (five rows) does exactly that — proven red while this
//! cell was written — so the source half rides [`large_config`], sized
//! well past the window with checkpoints early enough that K-S3's
//! post-checkpoint kill still has most of the stream in flight. The
//! no-Skip assertion below is therefore load-bearing: it is what makes
//! an under-sized fixture a FAILURE of this cell rather than a quiet
//! narrowing of the matrix.
//!
//! The destination arms mint their own identities (`k_d1`..`k_d6`
//! tables, per-arm pipelines, sibling `-r` reruns — `rdlt-certify`'s
//! kill module owns the names), all landing in this cell's scratch
//! dataset, so the probe is rooted at that one schema and collides with
//! nothing a sibling suite writes. Convergence leans on the pg
//! destination's OWN receipts table: the receipt is keyed by
//! `(load_id, commit_seq)`, so the sibling-pipeline rerun finds the
//! killed run's receipt and replays instead of double-publishing.
//!
//! Skip-not-fail without a container runtime, like every container
//! suite in this crate.

use rdlt_certify::{Entry, Target, Verdict, kill_matrix_destination, kill_matrix_source};
use rdlt_connector_postgres::fixtures::PostgresContainer;
use serde_json::json;

use super::common::Probe;
use super::support::spawn::built_bin;

/// The LARGE source config: ONE cursor stream over `k_rows`, sized so
/// the read is provably in flight when the SIGKILL lands. 5,000 rows of
/// ~100 bytes is ~500 KB of row bytes — nearly 8x the kit's 64 KiB
/// window — and `batch_max_rows: 200` cuts it into 25 batches whose
/// per-batch checkpoints put K-S3's boundary (the FIRST checkpoint)
/// ~4% into the stream, leaving ~480 KB still to flow at its kill.
fn large_config(conn: &str) -> serde_json::Value {
    json!({
        "conn": conn,
        "tables": [
            {"name": "k_rows", "cursor": {"column": "id"}},
        ],
        "batch_max_rows": 200,
    })
}

/// Seed the large source fixture — the row count and payload width
/// [`large_config`]'s sizing math is stated in.
async fn seed_source_fixture(container: &PostgresContainer) {
    container
        .seed(
            "CREATE TABLE public.k_rows (id int8 PRIMARY KEY, payload text); \
             INSERT INTO public.k_rows SELECT g, repeat('x', 96) FROM \
             generate_series(1, 5000) g;",
        )
        .await;
}

/// Render entries the report way, for failure messages.
fn render(entries: &[Entry]) -> String {
    let mut out = String::new();
    for entry in entries {
        out.push_str(&match &entry.verdict {
            Verdict::Pass => format!("PASS {}\n", entry.clause),
            Verdict::Fail(why) => format!("FAIL {}: {why}\n", entry.clause),
            Verdict::Skip(why) => format!("SKIP {}: {why}\n", entry.clause),
        });
    }
    out
}

/// Every arm must be a real Pass. A Skip gets its own diagnosis before
/// the Pass sweep: on this matrix a Skip means the fixture failed the
/// cell (the stream was swallowed whole before the kill), not that the
/// connector did — name that separately from a genuine clause failure.
#[track_caller]
fn assert_all_pass(entries: &[Entry], fixture_advice: &str) {
    for entry in entries {
        if let Verdict::Skip(why) = &entry.verdict {
            panic!(
                "{} skipped — {fixture_advice}: {why}\n{}",
                entry.clause,
                render(entries)
            );
        }
    }
    assert!(
        entries
            .iter()
            .all(|entry| matches!(entry.verdict, Verdict::Pass)),
        "every kill arm must Pass:\n{}",
        render(entries)
    );
}

/// THE SOURCE HALF: every boundary in K order, every arm a real Pass —
/// the killed pg bin's read wire fails typed within the kit's window,
/// never hangs, and never "completes cleanly despite the kill" (the
/// under-sized-fixture Skip [`large_config`] exists to defeat).
#[tokio::test(flavor = "multi_thread")]
async fn the_source_kill_matrix_passes_at_every_boundary() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    seed_source_fixture(&container).await;
    let target = Target::resolve_path(built_bin(), large_config(&container.connection_string));

    let entries = kill_matrix_source(&target).await;

    let clauses: Vec<&str> = entries.iter().map(|entry| entry.clause).collect();
    assert_eq!(
        clauses,
        ["K-S1", "K-S2", "K-S3"],
        "the K-vocabulary is fixed, in order"
    );
    assert_all_pass(
        &entries,
        "the large fixture must keep the read in flight at the SIGKILL, and a Skip here \
         means it no longer does (enlarge `k_rows` past the kit's read window)",
    );
}

/// THE DESTINATION HALF: every boundary in K order, every arm a real
/// Pass — typed error on the dead session wire, then exactly-once
/// convergence proven by a fresh spawn's re-run with the probe counting
/// the kit's `k_d*` tables over real SQL (K-D6's no-op arm doubles as
/// the kill-can-fail proof: a receipt that did not survive the killed
/// process, or a rerun that duplicated rows, breaks its exact count).
#[tokio::test(flavor = "multi_thread")]
async fn the_destination_kill_matrix_passes_at_every_boundary() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    // The scratch discipline (the certification cell's): a dataset of
    // this matrix's own — the destination creates the schema, every
    // per-arm table the kit mints lands inside it, and the probe reads
    // back from exactly there.
    let dataset = "kill_scratch";
    let config = json!({
        "conn": container.connection_string,
        "dataset": dataset,
    });
    let probe = Probe {
        connection_string: container.connection_string.clone(),
        schema: dataset.into(),
    };

    let entries =
        kill_matrix_destination(&Target::resolve_path(built_bin(), config), Some(&probe)).await;

    let clauses: Vec<&str> = entries.iter().map(|entry| entry.clause).collect();
    assert_eq!(
        clauses,
        ["K-D1", "K-D2", "K-D3", "K-D4", "K-D5", "K-D6"],
        "the K-vocabulary is fixed, in order"
    );
    assert_all_pass(
        &entries,
        "a probe was supplied and the boundaries are all reachable on this destination, so \
         no destination arm has a legitimate Skip",
    );
}
