//! The certifier CLI: point it at any connector executable (a path, or
//! a connector id resolved on PATH by the provider's convention) and it
//! certifies the binary against the same clauses first-party connectors
//! answer to, one titled verdict line per clause on stdout. Exit codes:
//! 0 all-pass (skips are honest non-verdicts and do not refuse), 1
//! clause failures (listed on stdout), 2 when the run was REFUSED
//! before certification could judge anything — a resolution/spawn
//! refusal (the runtime's own spelling on stderr), an unusable
//! `--config`, or bad arguments.
//!
//! Two things are never echoed onto either stream: the `--probe-cmd`
//! line (it may carry credentials — no report line, refusal or
//! probe-failure message repeats it) and the config document, which is
//! read, parsed and CARRIED (the report names clauses; refusal messages
//! name files and causes).

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::{CommandFactory as _, Parser, ValueEnum};
use rdlt_certify::clause::{c, d, k, s};
use rdlt_certify::probe::Shell;
use rdlt_certify::report::CLAUSES;
use rdlt_certify::target::Target;
use rdlt_connector_client::handshake::Role;
use rdlt_runtime::local::Local;
use rdlt_runtime::provider;
use rdlt_testkit::conformance::destination::TableProbe;
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

impl CertifyRole {
    fn runtime_role(self) -> Role {
        match self {
            Self::Source => Role::Source,
            Self::Destination => Role::Destination,
        }
    }
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

    /// Shell line counting reader-visible rows in one destination
    /// table: `{{table}}` is substituted, the line runs via `sh -c`,
    /// and its stdout must be one number. Destinations only. May carry
    /// credentials — it is never echoed by any report or failure text
    #[arg(long, value_name = "SH_LINE")]
    probe_cmd: Option<String>,

    /// A MISCONFIGURED config JSON file for the honest-check clause
    /// (D7/S5): a document the connector's gate accepts but its
    /// operations must refuse — a file behind a trailing slash, a
    /// directory where a file is expected. Absent, the clause skips
    /// with the reason
    #[arg(long, value_name = "FILE")]
    hostile_config: Option<PathBuf>,

    /// Report format on stdout
    #[arg(long, value_enum, default_value = "text")]
    report: ReportFormat,

    /// Acknowledge skipped SOURCE-suite clauses for the NAMED streams
    /// (comma-separated): an honest snapshot source (no cursor field,
    /// never checkpoints) skips S2, but a source that merely FORGOT
    /// resume looks identical — so any S1/S2/S4 skip whose stream is
    /// not named here fails certification. Naming is the
    /// acknowledgment: a blanket form would fold a regressed stream
    /// green beside a genuine snapshot one. Kill matrix skips (fixture
    /// sizing) and destination probe skips are unaffected
    #[arg(long, value_name = "STREAM[,STREAM]", value_delimiter = ',')]
    accept_skips: Vec<String>,

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

    // `--probe-cmd` is a destination read-back seam; beside `--role
    // source` it is a usage error, spoken in clap's own voice (exit 2)
    // WITHOUT echoing the command line the flag carries.
    if args.probe_cmd.is_some() && matches!(role, CertifyRole::Source) {
        Args::command()
            .error(
                clap::error::ErrorKind::ArgumentConflict,
                "--probe-cmd is a destination read-back probe and cannot be combined with \
                 --role source",
            )
            .exit();
    }

    // A template without the placeholder would count ONE fixed target
    // for every clause — wrong verdicts in both directions (a merge
    // count off the wrong table fails a conformant connector; a
    // permanently-empty wrong table false-passes invisibility) with no
    // error naming the real mistake. Refused at argument time; the
    // message names ONLY the placeholder — the line may carry
    // credentials and is never echoed.
    if args
        .probe_cmd
        .as_deref()
        .is_some_and(|template| !template.contains("{{table}}"))
    {
        Args::command()
            .error(
                clap::error::ErrorKind::ValueValidation,
                "--probe-cmd must contain the `{{table}}` placeholder — the line runs once \
                 per counted table with `{{table}}` substituted for the table name",
            )
            .exit();
    }

    let config = match load_config(args.config.as_deref()) {
        Ok(config) => config,
        Err(why) => {
            eprintln!("{why}");
            return ExitCode::from(EXIT_REFUSED);
        }
    };
    let hostile_target = match args.hostile_config.as_deref() {
        None => None,
        Some(path) => match load_config(Some(path)) {
            Ok(hostile) => Some(resolve(&named, hostile)),
            Err(why) => {
                eprintln!("{why}");
                return ExitCode::from(EXIT_REFUSED);
            }
        },
    };
    let target = resolve(&named, config);
    let probe = args
        .probe_cmd
        .map(|line| Shell::new(line).expect("the argument check above required the placeholder"));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread tokio runtime builds");
    runtime.block_on(run(
        role,
        args.kill_matrix,
        args.report,
        &args.accept_skips,
        &target,
        hostile_target.as_ref(),
        probe.as_ref().map(|shell| shell as &dyn TableProbe),
    ))
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
/// to stdout, and speak the exit-code vocabulary. `probe` is `Some`
/// exactly when `--probe-cmd` was given — destination-only, enforced
/// at argument time.
async fn run(
    role: CertifyRole,
    kill_matrix: bool,
    format: ReportFormat,
    accept_skips: &[String],
    target: &Target,
    hostile_target: Option<&Target>,
    probe: Option<&dyn TableProbe>,
) -> ExitCode {
    if let Some(refused) = preflight(target, role.runtime_role()).await {
        eprintln!("{refused}");
        return ExitCode::from(EXIT_REFUSED);
    }

    let mut report = match role {
        CertifyRole::Source => {
            let streams: Vec<&str> = accept_skips.iter().map(String::as_str).collect();
            s::certify(target, &streams).await
        }
        CertifyRole::Destination => d::certify(target, probe).await,
    };
    // The honest-check clause rides its OWN spawn with the
    // misconfigured document; without one it skips with the reason.
    match (role, hostile_target) {
        (CertifyRole::Source, Some(hostile)) => {
            c::source(&mut report, hostile).await;
            c::source_read(&mut report, hostile).await;
        }
        (CertifyRole::Destination, Some(hostile)) => c::destination(&mut report, hostile).await,
        (CertifyRole::Source, None) => c::skip_source(&mut report),
        (CertifyRole::Destination, None) => c::skip_destination(&mut report),
    }
    if kill_matrix {
        let entries = match role {
            CertifyRole::Source => k::source(target).await,
            CertifyRole::Destination => k::destination(target, probe).await,
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
async fn preflight(target: &Target, role: Role) -> Option<String> {
    let provider = Local::new();
    let outcome = tokio::time::timeout(
        PREFLIGHT_TIMEOUT,
        provider.spec(&target.requirement, Some(role)),
    )
    .await;
    match outcome {
        Ok(Err(
            error @ (provider::Error::NotFound { .. }
            | provider::Error::Spawn { .. }
            | provider::Error::ExitedBeforeHandshake { .. }
            | provider::Error::HandshakeLine { .. }
            | provider::Error::HandshakeLineOverflow { .. }
            | provider::Error::Timeout { .. }),
        )) => Some(error.to_string()),
        // A good Spec, a served-wire error, or a stalled pre-flight:
        // certification judges it.
        _ => None,
    }
}
