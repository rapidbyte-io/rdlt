//! A host of two spawned connectors, for the tests of what a host that ends leaves running:
//! `connector_host <connector binary> [mode] [configuration]`.
//!
//! - `wait`, or no mode: it runs until it is killed or hears a signal, and then stops what it spawned
//!   before it exits, as a host does.
//! - `leave`: it drops its connectors and returns, without waiting to see them stop.
//! - `panic`: it panics on its main thread, and stops what it spawned as it unwinds.
//!
//! The configuration is the JSON each connector is configured with, an empty object when
//! absent.

#![forbid(unsafe_code)]

use std::io::Write as _;
use std::path::PathBuf;
use std::time::Duration;

use rdlt_connector::ConnectorId;
use rdlt_host::{ConnectorRef, Local, Placed, Provider as _};

/// How long what the host spawned has to stop.
const STOPPING: Duration = Duration::from_secs(20);

type Source = Placed<Box<dyn rdlt_connector::Source>>;

/// Two connectors of `binary`, spawned with `config`, each given `grace` to stop.
async fn connectors(
    binary: PathBuf,
    grace: Duration,
    config: serde_json::Value,
) -> (Source, Source) {
    let id = ConnectorId::parse("test.scripted").expect("a valid id");
    let reference = ConnectorRef::new(id).path(binary);
    let local = Local::new()
        .env_passthrough("LLVM_PROFILE_FILE")
        .grace(grace);
    let first = local.source(&reference, &config).await;
    let second = local.source(&reference, &config).await;
    (
        first.expect("the first connector starts"),
        second.expect("the second connector starts"),
    )
}

fn main() {
    let mut args = std::env::args().skip(1);
    let binary = PathBuf::from(args.next().expect("the connector's binary"));
    let mode = args.next().unwrap_or_else(|| "wait".to_owned());
    let config = args.next().map_or_else(
        || serde_json::json!({}),
        |config| serde_json::from_str(&config).expect("a configuration in JSON"),
    );
    // A host that leaves is gone long before a grace it never waits out: nothing it stopped
    // is killed for having outlasted it.
    let grace = match mode.as_str() {
        "leave" => Duration::from_secs(1000),
        _ => Duration::from_millis(500),
    };
    let stops = rdlt_host::StopsSpawned::within(STOPPING);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let code = runtime.block_on(async {
        let mut signals = rdlt_host::Interrupts::listen().expect("the signals are heard");
        let connectors = connectors(binary, grace, config).await;
        let mut stdout = std::io::stdout();
        writeln!(stdout, "ready").expect("the test reads this");
        stdout.flush().expect("the test reads this");
        let code = match mode.as_str() {
            "wait" => signals.heard().await,
            "leave" => 0,
            _ => panic!("the host panics"),
        };
        drop(connectors);
        code
    });
    if mode == "leave" {
        // Gone at once: nothing waits for what it dropped.
        std::mem::forget(stops);
        std::process::exit(code);
    }
    drop(runtime);
    if let Err(lingering) = stops.stop() {
        writeln!(std::io::stderr(), "{lingering}").ok();
        std::process::exit(74);
    }
    std::process::exit(code);
}
