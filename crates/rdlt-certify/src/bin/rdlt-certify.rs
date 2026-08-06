//! The certifier CLI — the third-party seam's front door: point it at
//! any connector executable (a path, or a connector id resolved on
//! PATH by the provider's convention) and it certifies the binary
//! against the same clauses first-party connectors answer to, one
//! titled verdict line per clause on stdout.
//!
//! The exit-code vocabulary: 0 all-pass (skips are honest non-verdicts
//! and do not refuse), 1 clause failures (listed on stdout), 2 when
//! the run was REFUSED before certification could judge anything — a
//! resolution/spawn refusal (the runtime's frozen spelling verbatim on
//! stderr), an unusable `--config`, or bad arguments (clap's default).
//!
//! v0 carries NO `--probe`: the destination read-back clauses and the
//! kill matrix's convergence render Skip with the reason — the library
//! API (`certify_destination`, `kill_matrix_destination`) takes a
//! `TableProbe`; the bin gains `--probe` when a portable probe format
//! exists.
//!
//! The config document is read, parsed, and CARRIED — never printed:
//! no path through this bin echoes config bytes onto either stream
//! (the report names clauses; refusal messages name files and causes).

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use rdlt_certify::{
    CLAUSES, Target, certify_destination, certify_source, kill_matrix_destination,
    kill_matrix_source,
};
use rdlt_runtime::{LocalBinaryConnectorProvider, ProviderError};
use serde_json::Value;

/// Clause failures: the report on stdout names every one.
const EXIT_CLAUSE_FAILURES: u8 = 1;

/// Refused before certification: resolution/spawn refusals and an
/// unusable `--config`. Bad arguments share this code via clap's own
/// default exit.
const EXIT_REFUSED: u8 = 2;

/// How long the pre-flight Spec probe may take before the bin stops
/// waiting and lets certification (whose every clause is itself
/// timeout-bounded) render the outcome — the certifier never hangs,
/// its own front door included.
const PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(30);

/// Which SPI half to certify the connector as.
#[derive(Clone, Copy, ValueEnum)]
enum CertifyRole {
    Source,
    Destination,
}

/// Where the report lands on stdout.
#[derive(Clone, Copy, ValueEnum)]
enum ReportFormat {
    /// One titled verdict line per clause.
    Text,
    /// One JSON document (`{"entries": [...]}`), stdout kept pure —
    /// diagnostics stay on stderr.
    Json,
}

/// Certify a connector executable against the conformance clauses.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Which SPI half to certify the connector as
    #[arg(long, value_enum, required_unless_present = "explain")]
    role: Option<CertifyRole>,

    /// Connector config JSON file (defaults to `{}`)
    #[arg(long)]
    config: Option<PathBuf>,

    /// Run the SIGKILL boundary matrix too
    #[arg(long)]
    kill_matrix: bool,

    /// Report format on stdout
    #[arg(long, value_enum, default_value = "text")]
    report: ReportFormat,

    /// Print every clause id, title and definition, then exit
    #[arg(long)]
    explain: bool,

    /// Path to a connector binary, or a connector id resolved on PATH
    #[arg(required_unless_present = "explain")]
    target: Option<String>,
}

fn main() -> ExitCode {
    let args = Args::parse();

    if args.explain {
        print!("{}", explain());
        return ExitCode::SUCCESS;
    }
    // clap enforced both through `required_unless_present`.
    let role = args.role.expect("clap requires --role unless --explain");
    let named = args
        .target
        .expect("clap requires a target unless --explain");

    let config = match load_config(args.config.as_deref()) {
        Ok(config) => config,
        Err(why) => {
            eprintln!("{why}");
            return ExitCode::from(EXIT_REFUSED);
        }
    };
    let target = resolve(&named, config);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread tokio runtime builds");
    runtime.block_on(run(role, args.kill_matrix, args.report, &target))
}

/// The clause vocabulary, one block per clause — id, title, and the
/// self-contained definition, straight from the library's own table
/// (the one place they live).
fn explain() -> String {
    let mut out = String::new();
    for clause in CLAUSES {
        out.push_str(&format!(
            "{} ({})\n  {}\n\n",
            clause.id, clause.title, clause.definition
        ));
    }
    out
}

/// The config document: `--config`'s file parsed as one JSON document,
/// or `{}` when the flag is absent. Refusal messages name the file and
/// the cause — a serde_json parse error carries line/column, never the
/// document's bytes, so nothing here can echo config content.
fn load_config(path: Option<&Path>) -> Result<Value, String> {
    let Some(path) = path else {
        return Ok(serde_json::json!({}));
    };
    let text = std::fs::read_to_string(path)
        .map_err(|error| format!("reading the config file `{}`: {error}", path.display()))?;
    serde_json::from_str(&text).map_err(|error| {
        format!(
            "the config file `{}` is not one JSON document: {error}",
            path.display()
        )
    })
}

/// Path or id: anything with a path separator — or naming an existing
/// file — certifies by explicit path (its identity learned from the
/// connector's own Spec reply); anything else is a connector id for
/// the provider's PATH convention.
fn resolve(named: &str, config: Value) -> Target {
    if named.contains(std::path::MAIN_SEPARATOR) || Path::new(named).is_file() {
        Target::resolve_path(PathBuf::from(named), config)
    } else {
        Target::resolve_id(named, config)
    }
}

/// Certification proper: pre-flight the target, run the role's
/// certifier (plus the kill matrix when asked), render the one report
/// to stdout, and speak the exit-code vocabulary.
async fn run(
    role: CertifyRole,
    kill_matrix: bool,
    format: ReportFormat,
    target: &Target,
) -> ExitCode {
    if let Some(refused) = preflight(target).await {
        eprintln!("{refused}");
        return ExitCode::from(EXIT_REFUSED);
    }

    let mut report = match role {
        CertifyRole::Source => certify_source(target).await,
        CertifyRole::Destination => certify_destination(target, None).await,
    };
    if kill_matrix {
        let entries = match role {
            CertifyRole::Source => kill_matrix_source(target).await,
            CertifyRole::Destination => kill_matrix_destination(target, None).await,
        };
        report.entries.extend(entries);
    }

    match format {
        ReportFormat::Text => print!("{}", report.render_text()),
        ReportFormat::Json => println!("{}", report.render_json()),
    }
    if report.passed() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(EXIT_CLAUSE_FAILURES)
    }
}

/// The exit-2 gate: one Spec probe through the provider. A refusal
/// that means "there is no connector process to certify" — discovery
/// found nothing, the OS refused the spawn, or the process never spoke
/// a handshake line — surfaces the runtime's frozen spelling verbatim
/// and refuses the run. Anything the SERVED wire itself did wrong (a
/// refused handshake, a dead socket) is certification's subject: the
/// clauses will name it and the run exits with the failure code
/// instead. A pre-flight that stalls past its budget also falls
/// through — every clause is timeout-bounded on its own.
async fn preflight(target: &Target) -> Option<String> {
    let provider = LocalBinaryConnectorProvider::new();
    let outcome = tokio::time::timeout(PREFLIGHT_TIMEOUT, provider.spec(&target.requirement)).await;
    match outcome {
        Ok(Err(
            error @ (ProviderError::NotFound { .. }
            | ProviderError::Spawn { .. }
            | ProviderError::HandshakeLine { .. }
            | ProviderError::HandshakeLineOverflow { .. }
            | ProviderError::Timeout { .. }),
        )) => Some(error.to_string()),
        // A good Spec, a served-wire error, or a stalled pre-flight:
        // certification judges it.
        _ => None,
    }
}
