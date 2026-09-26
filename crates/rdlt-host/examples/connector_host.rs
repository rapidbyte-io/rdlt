//! A host of two spawned connectors that runs until it is killed, for the test that a killed host
//! leaves no connector running: `connector_host <connector binary>`.

use std::io::Write as _;
use std::path::PathBuf;

use rdlt_connector::ConnectorId;
use rdlt_host::{ConnectorRef, Local, Provider as _};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let binary = PathBuf::from(std::env::args().nth(1).expect("the connector's binary"));
    let id = ConnectorId::parse("test.scripted").expect("a valid id");
    let reference = ConnectorRef::new(id).path(binary);
    let config = serde_json::json!({});
    let local = Local::new().env_passthrough("LLVM_PROFILE_FILE");
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
    std::future::pending::<()>().await;
    drop((first, second));
}
