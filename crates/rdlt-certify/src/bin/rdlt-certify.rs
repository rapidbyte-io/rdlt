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
//! Destination read-back rides `--probe-cmd '<sh line>'`: the line
//! runs via `sh -c` once per count with `{{table}}` substituted and
//! must print one number — the reader-visible row count in that table.
//! The command line may carry credentials, so it is NEVER echoed: no
//! report line, refusal or probe-failure message repeats it. Without
//! the flag the read-back clauses and the kill matrix's convergence
//! render Skip with the reason; the library API
//! (`certify_destination`, `kill_matrix_destination`) takes a
//! `TableProbe` directly.
//!
//! The config document is read, parsed, and CARRIED — never printed:
//! no path through this bin echoes config bytes onto either stream
//! (the report names clauses; refusal messages name files and causes).

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use async_trait::async_trait;
use clap::{CommandFactory as _, Parser, ValueEnum};
use rdlt_certify::{
    CLAUSES, Report, SOURCE_CLAUSES, Target, Verdict, certify_destination, certify_source,
    kill_matrix_destination, kill_matrix_source,
};
use rdlt_connector::core::TableName;
use rdlt_runtime::{LocalBinaryConnectorProvider, ProviderError};
use rdlt_testkit::conformance::destination::{ProbeError, TableProbe};
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

    /// Shell line counting reader-visible rows in one destination
    /// table: `{{table}}` is substituted, the line runs via `sh -c`,
    /// and its stdout must be one number. Destinations only. May carry
    /// credentials — it is never echoed by any report or failure text
    #[arg(long, value_name = "SH_LINE")]
    probe_cmd: Option<String>,

    /// Report format on stdout
    #[arg(long, value_enum, default_value = "text")]
    report: ReportFormat,

    /// Accept skipped SOURCE-suite clauses as acknowledged: an honest
    /// snapshot source (no cursor field, never checkpoints) skips S2,
    /// but a source that merely FORGOT resume looks identical — so
    /// without this flag any S1/S2/S4 skip fails certification. Kill
    /// matrix skips (fixture sizing) and destination probe skips are
    /// unaffected
    #[arg(long)]
    accept_skips: bool,

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
    let target = resolve(&named, config);
    let probe = args.probe_cmd.map(|template| ShellProbe { template });

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread tokio runtime builds");
    runtime.block_on(run(
        role,
        args.kill_matrix,
        args.report,
        args.accept_skips,
        &target,
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

/// How long one probe command may run before its count fails.
/// Deliberately inside the library's 30 s clause budget: a hanging
/// probe fails naming ITSELF, before the clause it serves times out
/// and the evidence blames the connector.
const PROBE_TIMEOUT: Duration = Duration::from_secs(20);

/// The `--probe-cmd` read-back: one shell line, run per count with
/// `{{table}}` substituted. The template may carry credentials, so no
/// error below ever repeats it — failures name what happened, never
/// the command.
struct ShellProbe {
    /// The operator's shell line, `{{table}}` and all.
    template: String,
}

#[async_trait]
impl TableProbe for ShellProbe {
    async fn count(&self, table: &TableName) -> Result<u64, ProbeError> {
        let name = table.as_str();
        // The substitution guard: only `[A-Za-z0-9_]+` names are ever
        // spliced into a shell line (every conformance-kit table name
        // qualifies) — anything else is refused as a probe error, not
        // handed to the shell and not silently passed.
        if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            return Err(ProbeError {
                message: format!(
                    "table name `{name}` is outside [A-Za-z0-9_]+ — refusing to substitute \
                     it into the probe command"
                ),
            });
        }
        let line = self.template.replace("{{table}}", name);
        // `kill_on_drop`: tokio only kills a child on drop when asked,
        // so without it the timeout arm below — which drops the
        // `output()` future — would leak the hung sh process for the
        // rest of the certification run.
        let run = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(line)
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output();
        let output = match tokio::time::timeout(PROBE_TIMEOUT, run).await {
            Err(_elapsed) => {
                return Err(ProbeError {
                    message: format!(
                        "the probe command did not finish within {}s",
                        PROBE_TIMEOUT.as_secs()
                    ),
                });
            }
            Ok(Err(error)) => {
                return Err(ProbeError {
                    message: format!("the probe command could not run: {error}"),
                });
            }
            Ok(Ok(output)) => output,
        };
        if !output.status.success() {
            return Err(ProbeError {
                message: format!("the probe command failed: {}", output.status),
            });
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let count = stdout.trim();
        count.parse::<u64>().map_err(|_unparseable| ProbeError {
            message: format!("the probe command printed `{count}`, not one u64 row count"),
        })
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
    accept_skips: bool,
    target: &Target,
    probe: Option<&dyn TableProbe>,
) -> ExitCode {
    if let Some(refused) = preflight(target).await {
        eprintln!("{refused}");
        return ExitCode::from(EXIT_REFUSED);
    }

    let mut report = match role {
        CertifyRole::Source => certify_source(target).await,
        CertifyRole::Destination => certify_destination(target, probe).await,
    };
    if kill_matrix {
        let entries = match role {
            CertifyRole::Source => kill_matrix_source(target).await,
            CertifyRole::Destination => kill_matrix_destination(target, probe).await,
        };
        report.entries.extend(entries);
    }

    match format {
        ReportFormat::Text => print!("{}", report.render_text()),
        ReportFormat::Json => println!("{}", report.render_json()),
    }
    if !report.passed() {
        return ExitCode::from(EXIT_CLAUSE_FAILURES);
    }
    // A SOURCE-suite skip is not certified evidence: S2's skip is
    // reachable by DEFAULT (a stream that never declares a cursor and
    // never checkpoints), so a source that merely forgot resume would
    // otherwise certify exit 0. The skip still renders above — this
    // gate decides only whether it refuses; the operator acknowledges
    // the snapshot trade with --accept-skips.
    let skipped = unaccepted_source_skips(&report, accept_skips);
    if !skipped.is_empty() {
        eprintln!(
            "source clauses skipped: {} — a skip is not certified evidence (a source that \
             never checkpoints looks identical to one that forgot resume); pass \
             --accept-skips to acknowledge the source as a snapshot source",
            skipped.join(", ")
        );
        return ExitCode::from(EXIT_CLAUSE_FAILURES);
    }
    ExitCode::SUCCESS
}

/// The skipped source-suite clauses the operator has not acknowledged —
/// non-empty refuses certification. ONLY the S-clauses: kill-matrix
/// skips (fixture sizing) and the destination's no-probe/no-merge skips
/// carry reasons the operator already chose, and refusing them would
/// break the report's deliberate skip-not-vacuous design.
fn unaccepted_source_skips(report: &Report, accept_skips: bool) -> Vec<&'static str> {
    if accept_skips {
        return Vec::new();
    }
    report
        .entries
        .iter()
        .filter(|entry| {
            matches!(entry.verdict, Verdict::Skip(_)) && SOURCE_CLAUSES.contains(&entry.clause)
        })
        .map(|entry| entry.clause)
        .collect()
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

#[cfg(test)]
mod skip_gate_tests {
    //! The skip-acknowledgment gate, pinned on constructed reports —
    //! the CLI arm proves it end to end against the real file bin.

    use super::*;
    use rdlt_certify::Entry;

    fn report_with(entries: Vec<Entry>) -> Report {
        let mut report = Report::default();
        report.entries = entries;
        report
    }

    fn skip(clause: &'static str) -> Entry {
        Entry {
            clause,
            verdict: Verdict::Skip("why".to_owned()),
        }
    }

    fn pass(clause: &'static str) -> Entry {
        Entry {
            clause,
            verdict: Verdict::Pass,
        }
    }

    /// Only S-suite skips refuse — and only unacknowledged: the
    /// kill matrix's fixture skips and the destination's probe skips
    /// keep the report's skip-not-vacuous design.
    #[test]
    fn only_unacknowledged_source_suite_skips_refuse() {
        let report = report_with(vec![
            pass("S1"),
            skip("S2"),
            pass("S4"),
            skip("K-S2"),
            skip("D1"),
        ]);
        assert_eq!(
            unaccepted_source_skips(&report, false),
            vec!["S2"],
            "S2's skip refuses; kill-matrix and destination skips never do"
        );
        assert!(
            unaccepted_source_skips(&report, true).is_empty(),
            "--accept-skips acknowledges the source-suite skips"
        );
        assert!(
            unaccepted_source_skips(&report_with(vec![pass("S1"), pass("S2")]), false).is_empty(),
            "an all-pass report has nothing to acknowledge"
        );
    }
}
