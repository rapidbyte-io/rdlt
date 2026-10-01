//! A host of two spawned connectors that runs until it is killed or interrupted, for the tests
//! of what a host that ends leaves running: `connector_host <connector binary>`.
//!
//! Interrupted or asked to terminate, it stops what it spawned before it exits, as a host does.

#![forbid(unsafe_code)]

use std::io::Write as _;
use std::path::PathBuf;
use std::time::Duration;

use rdlt_connector::ConnectorId;
use rdlt_host::{ConnectorRef, Local, Provider as _};

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let code = runtime.block_on(async {
        let mut interrupts = rdlt_host::Interrupts::listen().expect("the signals are heard");
        let binary = PathBuf::from(std::env::args().nth(1).expect("the connector's binary"));
        let id = ConnectorId::parse("test.scripted").expect("a valid id");
        let reference = ConnectorRef::new(id).path(binary);
        let config = serde_json::json!({});
        let local = Local::new()
            .env_passthrough("LLVM_PROFILE_FILE")
            .grace(Duration::from_millis(500));
        let first = local
            .source(&reference, &config)
            .await
            .expect("the first connector starts");
        let second = local
            .source(&reference, &config)
            .await
            .expect("the second connector starts");
        let mut stdout = std::io::stdout();
        writeln!(stdout, "ready").expect("the test reads this");
        stdout.flush().expect("the test reads this");
        let code = interrupts.heard().await;
        drop((first, second));
        code
    });
    drop(runtime);
    rdlt_host::stop_spawned(Duration::from_secs(20)).expect("every connector's group ends");
    std::process::exit(code);
}
