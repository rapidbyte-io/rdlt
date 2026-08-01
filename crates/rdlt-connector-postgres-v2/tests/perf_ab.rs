//! Wall-clock A/B: generation 1 vs generation 2, same engine, same
//! containers, same data — the end-to-end complement to the iai
//! instruction-count parity comparison.
//!
//! Run BY HAND with a release build (a debug build distorts the ratio
//! between client work and server work):
//!
//! ```text
//! RDLT_PERF_AB=1 cargo nextest run --cargo-profile release \
//!     -p rdlt-connector-postgres-v2 -E 'binary(perf_ab)'
//! ```
//!
//! Protocol, matching the recorded measurement discipline: one discarded
//! warmup pair, then interleaved A/B pairs (never A A A B B B — machine
//! drift lands on both arms), medians reported over all pairs. Each run
//! loads into a FRESH destination schema under a FRESH pipeline id, so
//! every run is a full cold load and no state or reclaim scan crosses
//! runs. This binary reports; it does not gate — wall clock on a shared
//! machine is evidence for a human, not a pass/fail bar.

use std::time::{Duration, Instant};

use rdlt_connector::WriteMode;
use rdlt_connector_postgres_v2::fixtures::PostgresContainer;
use rdlt_engine::{Engine, EngineConfig};

/// Rows for the append cell — big enough that the COPY decode/encode hot
/// paths dominate the client side.
const APPEND_ROWS: u64 = 1_000_000;
/// Rows for the merge cell — the stage + dedup + arm publish machinery.
const MERGE_ROWS: u64 = 250_000;
/// Measured pairs per cell (after one discarded warmup pair).
const PAIRS: usize = 5;

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort();
    samples[samples.len() / 2]
}

fn render(cell: &str, generation_1: &[Duration], generation_2: &[Duration]) {
    let median_1 = median(generation_1.to_vec());
    let median_2 = median(generation_2.to_vec());
    let delta = (median_2.as_secs_f64() / median_1.as_secs_f64() - 1.0) * 100.0;
    eprintln!("== {cell} ==");
    eprintln!(
        "  gen1 samples: {:?}",
        generation_1
            .iter()
            .map(Duration::as_millis)
            .collect::<Vec<_>>()
    );
    eprintln!(
        "  gen2 samples: {:?}",
        generation_2
            .iter()
            .map(Duration::as_millis)
            .collect::<Vec<_>>()
    );
    eprintln!(
        "  gen1 median {} ms | gen2 median {} ms | gen2 vs gen1: {delta:+.1}%",
        median_1.as_millis(),
        median_2.as_millis()
    );
}

/// One generation-1 run: seeded source table → fresh destination schema.
async fn run_generation_1(
    source_connection: &str,
    destination_connection: &str,
    schema: &str,
    pipeline: &str,
    table: &str,
    write_mode: WriteMode,
) -> Duration {
    let source = rdlt_connector_postgres::source::PostgresSource::from_yaml(&format!(
        "conn: \"{source_connection}\"\ntables:\n  - name: {table}\n"
    ))
    .expect("gen1 source config");
    let destination =
        rdlt_connector_postgres::dest::Postgres::connect(destination_connection).dataset(schema);
    let config = EngineConfig::new(pipeline).with_write_mode(write_mode);
    let started = Instant::now();
    Engine::new(config, source, destination)
        .run()
        .await
        .expect("gen1 run");
    started.elapsed()
}

/// One generation-2 run: identical shape through the v2 crate.
async fn run_generation_2(
    source_connection: &str,
    destination_connection: &str,
    schema: &str,
    pipeline: &str,
    table: &str,
    write_mode: WriteMode,
) -> Duration {
    let source = rdlt_connector_postgres_v2::source::Postgres::from_yaml(&format!(
        "conn: \"{source_connection}\"\ntables:\n  - name: {table}\n"
    ))
    .expect("gen2 source config");
    let destination =
        rdlt_connector_postgres_v2::destination::Postgres::new(destination_connection)
            .schema(schema);
    let config = EngineConfig::new(pipeline).with_write_mode(write_mode);
    let started = Instant::now();
    Engine::new(config, source, destination)
        .run()
        .await
        .expect("gen2 run");
    started.elapsed()
}

/// Interleaved pairs over one cell; returns (gen1 samples, gen2 samples).
#[allow(clippy::too_many_arguments)]
async fn cell(
    source_connection: &str,
    destination_connection: &str,
    label: &str,
    table: &str,
    write_mode: &WriteMode,
) -> (Vec<Duration>, Vec<Duration>) {
    let mut generation_1 = Vec::with_capacity(PAIRS);
    let mut generation_2 = Vec::with_capacity(PAIRS);
    // Pair 0 is the discarded warmup (connection pools, page cache, JIT-ish
    // server warm paths); pairs 1..=PAIRS are measured.
    for pair in 0..=PAIRS {
        let sample_1 = run_generation_1(
            source_connection,
            destination_connection,
            &format!("ab_{label}_g1_{pair}"),
            &format!("ab-{label}-g1-{pair}"),
            table,
            write_mode.clone(),
        )
        .await;
        let sample_2 = run_generation_2(
            source_connection,
            destination_connection,
            &format!("ab_{label}_g2_{pair}"),
            &format!("ab-{label}-g2-{pair}"),
            table,
            write_mode.clone(),
        )
        .await;
        if pair > 0 {
            generation_1.push(sample_1);
            generation_2.push(sample_2);
        }
    }
    (generation_1, generation_2)
}

#[tokio::test(flavor = "multi_thread")]
async fn generation_2_matches_generation_1_wall_clock() {
    if std::env::var("RDLT_PERF_AB").is_err() {
        eprintln!("SKIP: RDLT_PERF_AB not set — hand-run measurement, see module docs");
        return;
    }
    let Some(source_fixture) = PostgresContainer::start().await else {
        return;
    };
    let Some(destination_fixture) = PostgresContainer::start().await else {
        return;
    };
    // The representative row mix the bench cells use: every hot decode and
    // encode family, uuid and jsonb included so their per-cell work is in
    // the wall time on both sides.
    source_fixture
        .seed(&format!(
            "CREATE TABLE events (
                 id bigint PRIMARY KEY,
                 small int4,
                 ratio float8,
                 name text,
                 at timestamptz,
                 ok bool,
                 token uuid,
                 doc jsonb
             );
             INSERT INTO events
             SELECT g,
                    (g % 100000)::int4,
                    g * 0.5,
                    'user-' || g,
                    TIMESTAMPTZ 'epoch' + g * INTERVAL '1 second',
                    g % 2 = 0,
                    gen_random_uuid(),
                    jsonb_build_object('city', 'NYC', 'zip', 10001 + g % 100)
             FROM generate_series(1, {APPEND_ROWS}) g;
             CREATE TABLE events_small AS SELECT * FROM events LIMIT {MERGE_ROWS};
             ALTER TABLE events_small ADD PRIMARY KEY (id);"
        ))
        .await;

    let (append_1, append_2) = cell(
        &source_fixture.connection_string,
        &destination_fixture.connection_string,
        "append",
        "events",
        &WriteMode::Append,
    )
    .await;
    render(
        &format!("append {APPEND_ROWS} rows (full load, direct-to-target)"),
        &append_1,
        &append_2,
    );

    let merge = WriteMode::Merge {
        key: vec!["id".into()],
    };
    let (merge_1, merge_2) = cell(
        &source_fixture.connection_string,
        &destination_fixture.connection_string,
        "merge",
        "events_small",
        &merge,
    )
    .await;
    render(
        &format!("merge {MERGE_ROWS} rows (stage + dedup + arm publish)"),
        &merge_1,
        &merge_2,
    );
}
