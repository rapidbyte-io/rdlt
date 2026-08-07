//! The 041 spawn-latency instrument, run BY HAND in the recorded
//! session (Task 7): spawn the postgres connector bin N times
//! sequentially through the provider and report min/median/p90 of
//! spawn → handshake-complete (process spawn, handshake line, socket
//! dial, wire handshake with the connector validating its config —
//! everything a `connector:` pipeline pays before the first byte).
//!
//! The config is syntactically valid and validated LOCALLY by the
//! connector's own gate, so NO live postgres server is needed. Each
//! spawned child is dropped before the next spawn — sequential cold
//! spawns, no pooling.
//!
//! Usage (bin path is argv[1]; omit it to discover
//! `rdlt-connector-postgres` on PATH; N is argv[2], default 20):
//!
//! ```sh
//! cargo build -p rdlt-connector-postgres --features bin-serve --bin rdlt-connector-postgres
//! cargo run -p rdlt-runtime --features spawn-bins --example spawn_latency -- \
//!     target/debug/rdlt-connector-postgres [N]
//! ```

use std::time::Instant;

use rdlt_runtime::{ConnectorProvider, ConnectorRequirement, LocalBinaryConnectorProvider};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut args = std::env::args().skip(1);
    let bin = args.next();
    let n: usize = args
        .next()
        .map(|raw| raw.parse().expect("N (argv[2]) must be a positive integer"))
        .unwrap_or(20);

    let mut requirement = ConnectorRequirement::new("io.rapidbyte.postgres");
    if let Some(bin) = &bin {
        requirement = requirement.with_path(bin);
    }
    // Validated by the connector's LOCAL config gate at the handshake;
    // nothing connects to this address.
    let config = serde_json::json!({
        "conn": "host=127.0.0.1 port=5439 user=postgres password=postgres dbname=src",
        "tables": [{"name": "events"}],
    });
    let provider = LocalBinaryConnectorProvider::new();

    let mut samples_ms: Vec<f64> = Vec::with_capacity(n);
    for spawn in 1..=n {
        let started = Instant::now();
        let managed = provider
            .source(&requirement, &config)
            .await
            .unwrap_or_else(|error| panic!("spawn {spawn}/{n} failed: {error}"));
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        // Kill the child (and unlink its socket) before the next spawn.
        drop(managed);
        eprintln!("spawn {spawn:>3}/{n}: {elapsed_ms:8.2} ms");
        samples_ms.push(elapsed_ms);
    }

    samples_ms.sort_by(f64::total_cmp);
    // Nearest-rank on the sorted samples; N=20 puts p90 at index 18.
    let rank = |q: f64| samples_ms[(q * (samples_ms.len() - 1) as f64).round() as usize];
    println!(
        "spawn -> handshake-complete, io.rapidbyte.postgres (source role), \
         {n} sequential spawns ({}):",
        bin.as_deref().unwrap_or("discovered on PATH"),
    );
    println!("  min    {:8.2} ms", samples_ms[0]);
    println!("  median {:8.2} ms", rank(0.5));
    println!("  p90    {:8.2} ms", rank(0.9));
}
