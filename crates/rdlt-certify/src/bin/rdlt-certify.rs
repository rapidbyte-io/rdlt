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
    CLAUSES, Target, certify_destination, certify_source, kill_matrix_destination,
    kill_matrix_source,
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

/// How long one probe command may run before its count fails. The
/// library stops the clause clock while a count runs (its budget
/// covers SPI traffic alone), so a hanging probe fails naming ITSELF
/// and can never exhaust a clause budget whose evidence would blame
/// the connector. Not the only bound: the certifier additionally
/// bounds EVERY probe — this shell one included — at the library's
/// own 30s `PROBE_BOUND` (clock.rs), the no-hang backstop for probes
/// it does not spawn; for CLI runs this tighter 20s fires first.
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
        // `process_group(0)` puts sh at the head of its OWN process
        // group (pgid = its pid), so a timed-out probe can kill the
        // WHOLE group: a piped or compound line forks grandchildren
        // the direct SIGKILL misses — the real store-reader would
        // survive the probe's own death and could keep a single-writer
        // store open. `kill_on_drop` stays as the direct child's net.
        let mut child = match tokio::process::Command::new("sh")
            .arg("-c")
            .arg(line)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            // Discarded, as `output()` effectively did before: the
            // probe's own stderr may repeat pieces of its command
            // line, which no certifier stream may echo.
            .stderr(std::process::Stdio::null())
            .process_group(0)
            .kill_on_drop(true)
            .spawn()
        {
            Ok(child) => child,
            Err(error) => {
                return Err(ProbeError {
                    message: format!("the probe command could not run: {error}"),
                });
            }
        };
        let pgid = child.id();
        match tokio::time::timeout(PROBE_TIMEOUT, drain(&mut child)).await {
            Err(_elapsed) => {
                // The group kill: `kill -- -<pgid>` signals every
                // process in sh's group, grandchildren included; std
                // exposes no group-kill call, and /bin/kill with the
                // NEGATIVE pgid is the portable spelling. A kill spawn
                // failure is NOT swallowed: the direct child still
                // dies below either way, but the caller must know the
                // grandchildren may have survived. The direct child is
                // AWAITED so it is dead, not dying, on return, and one
                // signal-0 reading then reports possible residue —
                // possible, because a recycled group id is
                // indistinguishable from a survivor (see `group_kill`).
                let group_note = match pgid {
                    None => {
                        let _ = child.start_kill();
                        let _ = child.wait().await;
                        Some("the probe's pid was unknown, so only the direct child was killed")
                    }
                    Some(pgid) => group_kill(pgid, &mut child).await,
                };
                let mut message = format!(
                    "the probe command did not finish within {}s",
                    PROBE_TIMEOUT.as_secs()
                );
                if let Some(note) = group_note {
                    message.push_str(" (");
                    message.push_str(note);
                    message.push(')');
                }
                Err(ProbeError { message })
            }
            Ok(Err(error)) => {
                // Best-effort group kill on this exit too (round-11 —
                // only the timeout path swept grandchildren, so a
                // compound probe line's forks could outlive a count
                // that failed or finished): the note is discarded,
                // nothing here failed FOR that reason.
                match pgid {
                    Some(pgid) => {
                        let _ = group_kill(pgid, &mut child).await;
                    }
                    None => {
                        let _ = child.start_kill();
                        let _ = child.wait().await;
                    }
                }
                Err(ProbeError {
                    message: format!("the probe command could not run: {error}"),
                })
            }
            Ok(Ok((status, stdout))) => {
                // Same sweep on the normal exit — `drain` reaped the
                // direct sh child, but a fork it left behind holds
                // whatever the probe line opened (a single-writer
                // store, for one) into the next clause.
                if let Some(pgid) = pgid {
                    let _ = group_kill(pgid, &mut child).await;
                }
                if !status.success() {
                    return Err(ProbeError {
                        message: format!("the probe command failed: {status}"),
                    });
                }
                let stdout = String::from_utf8_lossy(&stdout);
                let count = stdout.trim();
                count.parse::<u64>().map_err(|_unparseable| ProbeError {
                    message: format!("the probe command printed `{count}`, not one u64 row count"),
                })
            }
        }
    }
}

/// SIGKILL the probe's whole process group, then take ONE `kill -0`
/// reading (round-9 honesty fix — the old 20-poll, 1s drain window
/// could spend its whole second observing a pgid RECYCLED to an
/// unrelated live group, then state phantom survivors as FACT): the
/// direct sh child is reaped BEFORE the reading (round-6 fix — its
/// zombie would answer signal 0 forever), and a positive answer is
/// then reported as POSSIBLE residue, because signal 0 cannot
/// distinguish the probe's own stragglers from an unrelated group
/// that inherited the recycled id. Returns the degradation note for
/// the failure message when the group could not be killed or may not
/// have drained — never a command echo, and never a silent swallow.
async fn group_kill(pgid: u32, child: &mut tokio::process::Child) -> Option<&'static str> {
    let target = format!("-{pgid}");
    let signalled = tokio::process::Command::new("kill")
        .args(["-KILL", "--", &target])
        .status()
        .await;
    // The direct child is reaped FIRST either way (round-6 fix): a
    // zombie still answers signal 0 as a group member and would turn
    // every reading below into a phantom note.
    let _ = child.start_kill();
    let _ = child.wait().await;
    if !matches!(signalled, Ok(status) if status.success()) {
        return Some(
            "the group kill could not run — processes the probe line forked may have \
             survived; only the direct child was killed",
        );
    }
    let survivors = tokio::process::Command::new("kill")
        .args(["-0", "--", &target])
        .stderr(std::process::Stdio::null())
        .status()
        .await;
    match survivors {
        Ok(status) if status.success() => Some(
            "processes may remain in the probe's process group (or the group id was \
             recycled)",
        ),
        // The check failed to run, or no member answers signal 0: the
        // group is gone.
        _ => None,
    }
}

/// Read the probe's stdout to EOF, then reap its exit status — split
/// from the timeout wrapper so the `Child` handle stays usable in the
/// timeout arm (the group kill needs its pgid, and the post-kill wait
/// needs the handle).
async fn drain(
    child: &mut tokio::process::Child,
) -> std::io::Result<(std::process::ExitStatus, Vec<u8>)> {
    use tokio::io::AsyncReadExt as _;
    let mut stdout = Vec::new();
    if let Some(pipe) = child.stdout.as_mut() {
        pipe.read_to_end(&mut stdout).await?;
    }
    let status = child.wait().await?;
    Ok((status, stdout))
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
        CertifyRole::Source => certify_source(target, accept_skips).await,
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
