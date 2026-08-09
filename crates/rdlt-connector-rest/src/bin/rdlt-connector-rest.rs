//! `rdlt-connector-rest` — the rest connector as an out-of-process
//! protocol server (ADR 0001). D-039-1's discovery convention resolves
//! the connector id `io.rapidbyte.rest` to THIS binary name on PATH; a
//! provider spawns it with `--role=source`, reads the one stdout
//! handshake line, and everything after is the wire protocol.
//!
//! SOURCE-ONLY: this crate has no destination half, so `ServeRole`
//! carries the one variant and `--role=destination` fails as an
//! unrecognized value (clap's stderr + exit 2) — an arg error before
//! any serve machinery, never a half-served role.
//!
//! Behavior contract (pinned in this crate's spawn-bins suite, not
//! clap's usage text): missing/invalid args → clap's stderr + exit 2;
//! `--version` prints the crate version; a serve error → one stderr
//! line + exit 1.

use clap::{Parser, ValueEnum};
use rdlt_connector_rest::source::Rest;

#[derive(Parser)]
#[command(version, about = "rdlt rest connector — a protocol server (ADR 0001)")]
struct Args {
    /// Which half of the connector to serve on this process.
    #[arg(long, value_enum)]
    role: ServeRole,
}

#[derive(Clone, Copy, ValueEnum)]
enum ServeRole {
    Source,
}

#[tokio::main]
async fn main() {
    let args = Args::parse(); // clap: bad args → its stderr + exit 2
    let outcome = match args.role {
        ServeRole::Source => rdlt_connector_sdk::serve::source::source::<Rest>().await,
    };
    if let Err(error) = outcome {
        eprintln!("rdlt-connector-rest: {error}");
        std::process::exit(1);
    }
}
