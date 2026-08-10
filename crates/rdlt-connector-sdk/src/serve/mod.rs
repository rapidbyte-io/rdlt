//! `serve()`: turning an sdk connector into an out-of-process protocol
//! server (038). Behind the `serve` feature, OFF by default — a
//! connector that never runs out-of-process pays nothing for this
//! module, not even tonic in its dependency tree (`cargo tree -i tonic`
//! against a connector crate at default features stays clean).
//!
//! [`common`] is the plumbing every service shares (the UDS bind, the
//! [`common::ServeError`] taxonomy, the `common::error_frame`
//! builder). [`source`] is the [`crate::source::SourceConnector`] half —
//! [`source::source`] is the entry a spawned connector process actually
//! runs. [`destination`] is the [`crate::destination::DestinationConnector`]
//! half — one bidi stream IS the session; [`destination::destination`]
//! is its entry point.
//!
//! ## THE STATUS-VS-ERRORFRAME RULE (038 T6; recorded once here, once
//! more on the crate's own contract page — never a third time)
//!
//! Two distinct wire shapes answer a refusal, and which one a given
//! refusal takes is NOT arbitrary: it tracks whether the thing that went
//! wrong is a violation of the PROTOCOL's own state machine or an
//! outcome the CONNECTOR itself decided.
//!
//! A protocol-state violation — any [`crate::source::SourceConnector`]/
//! [`crate::destination::DestinationConnector`] RPC other than
//! `Handshake` arriving before a handshake has completed (the
//! config-free `Spec` RPC is the one other exemption: it answers
//! before the handshake — it carries no session state, only the
//! connector's static identity, so arriving early is not a violation
//! at all), or a second concurrent `OpenSession` while one is already
//! active on the same served process — answers as a raw gRPC `Status`
//! (`Code::FailedPrecondition`), ending the RPC outright: there was no
//! valid session for a payload-shaped outcome to be reported INTO, so
//! there is nothing to carry one. Pinned on the source side by
//! `test_serve_source`'s `streams_before_a_handshake_refuses_as_a_status`/
//! `read_before_a_handshake_refuses_as_a_status`, and on the destination
//! side by `test_serve_destination`'s
//! `a_second_concurrent_open_session_refuses_while_the_first_is_active`.
//!
//! Everything else — including a `Handshake` RPC's OWN refusals (bad
//! role, out-of-range version, undecodable or invalid config) and every
//! refusal reachable ONCE a `DestinationService` session is open
//! (write-before-`Open`, write-before-`Ensure`, a second `Open` frame, an
//! empty request frame, a connector's own classified failure) — answers
//! as a [`rdlt_connector_protocol::proto::ErrorFrame`] carried as
//! reply-payload state (`Classification::{Transient,RateLimited,Fatal}`),
//! inside a stream/RPC that itself completes normally. A `Read` request
//! whose `stream_spec_json`/`since_cursor_json` fails to decode rides
//! this shape too: the refusal is the response stream's first and only
//! frame, a terminal `Error` — never a `Status`. This is
//! deliberate, not an inconsistency to unify away: a `Handshake` refusal
//! and every in-session refusal are DATA a caller is meant to inspect
//! uniformly (a bad config is not a protocol bug, and neither is a
//! transient write failure) — the RPC layer itself has no reason to
//! reject the call that reported them.
//!
//! A caller therefore has to know both shapes exist and check the right
//! one: `Status` for "you broke the protocol's own sequencing," a typed
//! `ErrorFrame` for "the connector is telling you something."

pub mod common;
pub mod destination;
pub mod source;

/// The connector bin's WHOLE `main` — the template every served
/// connector carried as a verbatim ~48-line copy (round-5 fix), stated
/// once. The expansion is the behavior contract the spawn suites pin:
/// missing/invalid args → clap's stderr + exit 2 (a role the crate
/// does not list is an unrecognized VALUE — an arg error before any
/// serve machinery); `--version` prints the crate version; a serve
/// error → one stderr line, `<bin name>: <error>`, and exit 1.
///
/// The caller supplies its bin name, the clap `about` line, optionally
/// a `preflight` expression (run after arg parsing, before any serve —
/// oracle's client probe exits there with its own typed refusal), and
/// one arm per served role mapping the `--role` value to a serve
/// future:
///
/// ```ignore
/// rdlt_connector_sdk::serve_main! {
///     bin: "rdlt-connector-duckdb",
///     about: "rdlt duckdb connector — a protocol server (ADR 0001)",
///     roles: {
///         Destination => rdlt_connector_sdk::serve::destination::destination::<DuckDb>(),
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
        bin: $bin:literal,
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
                eprintln!(concat!($bin, ": {}"), error);
                ::std::process::exit(1);
            }
        }
    };
}
