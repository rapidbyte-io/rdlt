//! `rdlt-connector-postgres` — the postgres connector as an
//! out-of-process protocol server (ADR 0001). D-039-1's discovery
//! convention resolves the connector id `io.rapidbyte.postgres` to THIS
//! binary name on PATH; a provider spawns it with
//! `--role=<source|destination>`, reads the one stdout handshake line,
//! and everything after is the wire protocol.
//!
//! Behavior contract (pinned in this crate's spawn-bins suite, not
//! clap's usage text): missing/invalid args → clap's stderr + exit 2;
//! `--version` prints the crate version; a serve error → one stderr
//! line + exit 1.

use clap::{Parser, ValueEnum};
use rdlt_connector_postgres::destination::Postgres as PgDestination;
use rdlt_connector_postgres::source::Postgres as PgSource;

#[derive(Parser)]
#[command(
    version,
    about = "rdlt postgres connector — a protocol server (ADR 0001)"
)]
struct Args {
    /// Which half of the connector to serve on this process.
    #[arg(long, value_enum)]
    role: ServeRole,
}

#[derive(Clone, Copy, ValueEnum)]
enum ServeRole {
    Source,
    Destination,
}

#[tokio::main]
async fn main() {
    let args = Args::parse(); // clap: bad args → its stderr + exit 2
    let outcome = match args.role {
        ServeRole::Source => rdlt_connector_sdk::serve::source::source::<PgSource>().await,
        ServeRole::Destination => {
            rdlt_connector_sdk::serve::destination::destination::<PgDestination>().await
        }
    };
    if let Err(error) = outcome {
        eprintln!("rdlt-connector-postgres: {error}");
        std::process::exit(1);
    }
}
