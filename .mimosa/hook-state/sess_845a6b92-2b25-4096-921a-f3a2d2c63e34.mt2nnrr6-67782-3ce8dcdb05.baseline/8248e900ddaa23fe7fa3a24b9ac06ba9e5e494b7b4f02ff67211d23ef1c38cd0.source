//! Serving an sdk connector as an out-of-process protocol server.
//! Behind the `serve` feature, OFF by default — a connector that never
//! runs out-of-process pays nothing for this module, not even tonic in
//! its dependency tree.
//!
//! [`wire`] is the plumbing both roles share (the socket, [`Error`], the
//! handshake, the two refusal shapes); the private `gate` module is the
//! serve-side gate family's one seat; [`source`] serves a
//! [`crate::source::SourceConnector`] and [`destination`] a
//! [`crate::destination::DestinationConnector`] — each with `run` (what
//! a spawned connector process runs) and `run_on` (the seam under it,
//! bound at an explicit path without printing, for tests and embedders).

pub mod destination;
mod gate;
pub mod source;
pub mod wire;

pub use wire::Error;

/// A connector binary's WHOLE `main`, stated once instead of copied
/// into every served connector. The expansion is a behavior contract:
/// missing/invalid args → clap's stderr + exit 2 (a role the crate does
/// not list is an unrecognized VALUE, refused before any serve
/// machinery); `--version` prints the crate version; a serve error →
/// one stderr line, `<bin name>: <error>`, and exit 1.
///
/// The bin name is cargo's own (`env!("CARGO_PKG_NAME")` at the
/// expansion site, so it cannot drift); the caller supplies the clap
/// `about` line, optionally a `preflight` expression (run after arg
/// parsing, before any serve — a client-library probe that exits with
/// its own typed refusal, say), and one arm per served role mapping the
/// `--role` value to a serve future:
///
/// ```ignore
/// rdlt_connector_sdk::serve_main! {
///     about: "my connector — a protocol server",
///     roles: {
///         Source => rdlt_connector_sdk::serve::source::run::<MySource>(),
///         Destination => rdlt_connector_sdk::serve::destination::run::<MyDestination>(),
///     }
/// }
/// ```
///
/// A macro rather than a generic fn deliberately: the role ENUM differs
/// per crate (source-only, destination-only, both) and clap derives it,
/// so the variants must exist as items in the bin crate.
#[macro_export]
macro_rules! serve_main {
    (
        about: $about:literal,
        $(preflight: $preflight:expr,)?
        roles: { $($variant:ident => $serve:expr),+ $(,)? }
    ) => {
        #[derive(::clap::Parser)]
        #[command(version, about = $about)]
        struct Args {
            /// Which half of the connector to serve on this process.
            #[arg(long, value_enum)]
            role: ServeRole,
        }

        #[derive(Clone, Copy, ::clap::ValueEnum)]
        enum ServeRole {
            $($variant,)+
        }

        #[::tokio::main]
        async fn main() {
            use ::clap::Parser as _;
            let args = Args::parse(); // clap: bad args → its stderr + exit 2
            $($preflight;)?
            let outcome = match args.role {
                $(ServeRole::$variant => $serve.await,)+
            };
            if let Err(error) = outcome {
                eprintln!(concat!(env!("CARGO_PKG_NAME"), ": {}"), error);
                ::std::process::exit(1);
            }
        }
    };
}
