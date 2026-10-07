//! The command line: which connector, how to reach it and with what configuration, and how to
//! print what it met.

mod configured;
mod panics;
mod session;
#[cfg(test)]
mod tests;

use std::io::Write as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use rdlt_certify::{
    CREDIT_WATCH, Observed, Outcome, Probe, RUN_TIMEOUT, Report, Target, Unprobed, Verdict,
    certify_destination_observed, certify_source_observed, json, markdown, plain, read_back,
};
use rdlt_connector::ConnectorId;
use rdlt_host::{
    Bubblewrap, ConnectorRef, Endpoint, Identity, Local, Redactions, Remote, StopsSpawned,
};
use session::{STOPPING, Session, ending, signalled};

/// Every clause that applies to the connector was seen to be met.
const PASSED: u8 = 0;
/// A clause failed, or none could run: the connector serves none of the roles asked.
const FINDINGS: u8 = 1;
/// No clause failed, yet one that applies was not observed.
const INCOMPLETE: u8 = 2;
/// The command line was wrong.
const USAGE: u8 = 64;
/// The connector's binary, or a file named, could not be read.
const IO: u8 = 74;

/// Certifies a connector against the protocol's conformance clauses.
#[derive(Debug, Parser)]
#[command(name = "rdlt-certify", version)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each is a switch of the command line, independent of the others"
)]
struct Args {
    /// The connector: its binary's path, or the `grpcs://host:port` endpoint it listens at.
    #[arg(required_unless_present = "clauses")]
    target: Option<String>,
    /// Certify this role alone; by default, every role the connector serves.
    #[arg(long, value_enum)]
    role: Option<Role>,
    /// A file holding the connector's configuration, as JSON, or `-` to read it from standard
    /// input; an empty object without it; a text value may refer to a secret as `${env:NAME}`
    /// or `${file:/absolute/path}`, where `--secret-env` and `--secret-dir` allow it, and no
    /// report then shows the secret.
    #[arg(long, value_name = "PATH")]
    config_file: Option<PathBuf>,
    /// An environment variable the configuration may refer to as `${env:NAME}`.
    #[arg(long, value_name = "NAME")]
    secret_env: Vec<String>,
    /// A private directory the configuration may refer to files beneath as `${file:...}`.
    #[arg(long, value_name = "DIR")]
    secret_dir: Vec<PathBuf>,
    /// Runs the connector's binary with your own access to files, the network and other
    /// processes, in no sandbox: for a binary you trust as you trust this command.
    #[arg(long, conflicts_with_all = ["grant_read", "grant_write", "grant_network"])]
    trusted: bool,
    /// A path the sandboxed connector may read.
    #[arg(long, value_name = "PATH")]
    grant_read: Vec<PathBuf>,
    /// A path the sandboxed connector may read and write.
    #[arg(long, value_name = "PATH")]
    grant_write: Vec<PathBuf>,
    /// Lets the sandboxed connector reach the network.
    #[arg(long)]
    grant_network: bool,
    /// An environment variable a spawned connector keeps; it otherwise starts with none.
    #[arg(long = "env", value_name = "NAME")]
    env: Vec<String>,
    /// The host's certificate chain, in PEM, for an endpoint.
    #[arg(long, requires_all = ["tls_key", "tls_ca"])]
    tls_cert: Option<PathBuf>,
    /// The host's private key, in PEM, for an endpoint.
    #[arg(long, requires = "tls_cert")]
    tls_key: Option<PathBuf>,
    /// The CA bundle that issued the connector's certificate, in PEM, for an endpoint.
    #[arg(long, requires = "tls_cert")]
    tls_ca: Option<PathBuf>,
    /// Kills the connector at the points this seed draws, as a failed kill clause reports, to
    /// reproduce the failure.
    #[arg(long, value_name = "SEED")]
    kill_seed: Option<u64>,
    /// Gives each kill clause this many seconds for all its loads, rather than 300: a connector
    /// slower than about two seconds a commit needs more.
    #[arg(long, value_name = "SECONDS")]
    kill_timeout: Option<u64>,
    /// Watches a read whose credit is spent this many milliseconds after each grant, from 1 to
    /// 1000, rather than 1000: `P-CREDIT` then takes four times as long, and its pass notes the
    /// watch.
    #[arg(long, value_name = "MS", value_parser = clap::value_parser!(u64).range(1..))]
    credit_watch: Option<u64>,
    /// Ends the certification after this many seconds, rather than 3600, failing every clause
    /// of a role still certifying then.
    #[arg(long, value_name = "SECONDS", conflicts_with = "no_timeout")]
    timeout: Option<u64>,
    /// Lets the certification take as long as its clauses' own bounds allow.
    #[arg(long)]
    no_timeout: bool,
    /// What exits 0: `complete`, every clause that applies seen to be met; `partial`, none
    /// failed and one passed, whatever was not observed.
    #[arg(long, value_enum, default_value_t = Require::Complete)]
    require: Require,
    /// How to print the reports.
    #[arg(long, value_enum, default_value_t = Output::Plain)]
    output: Output,
    /// Prints every clause, in Markdown, and exits.
    #[arg(long)]
    clauses: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Role {
    Source,
    Destination,
}

/// What a certification must show to exit 0: every clause that applies seen to be met, or none
/// failed and one passed, whatever was not observed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Require {
    Complete,
    Partial,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Output {
    Plain,
    Json,
}

/// Why the command ended without certifying: its exit code and message.
struct Ended(u8, String);

/// Runs the command line.
pub(crate) fn main() -> ExitCode {
    panics::contain();
    let args = match Args::try_parse() {
        Ok(args) => args,
        Err(error) => {
            let code = if error.use_stderr() { USAGE } else { PASSED };
            error.print().ok();
            return ExitCode::from(code);
        }
    };
    let ran = if args.clauses {
        print(&markdown()).map(|()| PASSED)
    } else {
        run(&args)
    };
    match ran {
        Ok(code) => ExitCode::from(code),
        Err(Ended(code, message)) => {
            let message = redactions().scrubbed(message);
            writeln!(std::io::stderr(), "rdlt-certify: {message}").ok();
            ExitCode::from(code)
        }
    }
}

/// The secrets the configuration's references resolved to: nothing this command prints holds
/// one.
pub(crate) fn redactions() -> &'static Redactions {
    static REDACTIONS: std::sync::OnceLock<Redactions> = std::sync::OnceLock::new();
    REDACTIONS.get_or_init(Redactions::new)
}

fn run(args: &Args) -> Result<u8, Ended> {
    let watch = args.credit_watch.map(credit_watch).transpose()?;
    let config = configured::config(args)?;
    let target = target(args)?;
    let target = match args.kill_seed {
        Some(seed) => target.kill_seed(seed),
        None => target,
    };
    let target = match args.kill_timeout {
        Some(seconds) => target.kill_timeout(Duration::from_secs(seconds)),
        None => target,
    };
    let target = match watch {
        Some(watch) => target.credit_watch(watch),
        None => target,
    };
    // Held from before anything is spawned: however the certification ends, a panic of this
    // thread included, each connector it spawned is stopped with its whole process group.
    let stops = StopsSpawned::within(STOPPING);
    let mut session = Session::start(until(args)?)?;
    let certified = certified(args, &target, &config, &mut session);
    let heard = certified.as_ref().err().and_then(signalled);
    let (stopped, heard) = session.stopped(stops, heard);
    let reports = ending(certified, stopped, heard)?;
    let verdict = verdict(&reports);
    if matches!(args.output, Output::Json) {
        let passed = verdict == Verdict::Passed;
        let reports: Vec<_> = reports.iter().map(json).collect();
        let document = serde_json::json!({
            "verdict": verdict.as_str(),
            "passed": passed,
            "reports": reports,
        });
        print(&format!("{document}\n"))?;
    }
    if !reports.iter().any(ran) {
        return Err(Ended(
            FINDINGS,
            "the connector serves none of the roles asked, so nothing was certified".to_owned(),
        ));
    }
    Ok(code(verdict, args.require, &reports))
}

/// When the certification's bound passes, when it has one: whoever asks for a bound gets one, so
/// a timeout the clock cannot hold is refused.
fn until(args: &Args) -> Result<Option<Instant>, Ended> {
    let Some(bound) = bound(args.timeout, args.no_timeout) else {
        return Ok(None);
    };
    let until = Instant::now().checked_add(bound).ok_or_else(|| {
        let message = "--timeout is further ahead than the clock holds; see --no-timeout";
        Ended(USAGE, message.to_owned())
    })?;
    Ok(Some(until))
}

/// A watch of `millis` milliseconds, which is no longer than `P-CREDIT`'s own.
fn credit_watch(millis: u64) -> Result<Duration, Ended> {
    let watch = Duration::from_millis(millis);
    if watch > CREDIT_WATCH {
        let most = CREDIT_WATCH.as_millis();
        return Err(Ended(USAGE, format!("--credit-watch is at most {most} ms")));
    }
    Ok(watch)
}

/// The report of each role asked that the connector serves, or of every role when it serves
/// none, each printed as text as its certification ends.
fn certified(
    args: &Args,
    target: &Target,
    config: &serde_json::Value,
    session: &mut Session,
) -> Result<Vec<Report>, Ended> {
    let mut printed = Printed {
        output: args.output,
        held: Vec::new(),
    };
    let mut reports = Vec::new();
    if !matches!(args.role, Some(Role::Destination)) {
        let observed = Observed::new();
        let certifying = std::pin::pin!(certify_source_observed(target, config.clone(), &observed));
        let report = session.ran_to(target, rdlt_connector::Role::Source, &observed, certifying)?;
        // No clause of a role the connector does not serve applies: unless the role was asked
        // for, or no role is served, its report is left out.
        if args.role.is_some() || ran(&report) {
            printed.report(&report)?;
            reports.push(report);
        } else {
            printed.held.push(report);
        }
    }
    if !matches!(args.role, Some(Role::Source)) {
        let observed = Observed::new();
        let certifying = std::pin::pin!(async {
            // What the destination published is read back when it can be; else the clauses
            // that read it are not observed.
            let read_back = read_back(target, config).await;
            let probe: &dyn Probe = match &read_back {
                Some(read_back) => read_back,
                None => &Unprobed,
            };
            certify_destination_observed(target, config.clone(), probe, &observed).await
        });
        let role = rdlt_connector::Role::Destination;
        let report = session.ran_to(target, role, &observed, certifying)?;
        let served = ran(&report);
        if served || reports.is_empty() {
            if !served {
                reports.append(&mut printed.held);
                for held in &reports {
                    printed.report(held)?;
                }
            }
            printed.report(&report)?;
            reports.push(report);
        }
    }
    Ok(reports)
}

/// How long the certification may take: the `timeout` chosen, in seconds, else
/// [`RUN_TIMEOUT`]; no bound when `unbounded`.
fn bound(timeout: Option<u64>, unbounded: bool) -> Option<Duration> {
    if unbounded {
        return None;
    }
    Some(timeout.map_or(RUN_TIMEOUT, Duration::from_secs))
}

/// The reports printed as text as each role ends, so a certification stopped from outside has
/// said what it found; as JSON they are one document, printed last.
struct Printed {
    output: Output,
    /// Reports of roles the connector does not serve, printed only when it serves none.
    held: Vec<Report>,
}

impl Printed {
    fn report(&self, report: &Report) -> Result<(), Ended> {
        match self.output {
            Output::Plain => print(&plain(report)),
            Output::Json => Ok(()),
        }
    }
}

/// What `reports` amount to together: failed when one failed, else incomplete when one is.
fn verdict(reports: &[Report]) -> Verdict {
    let any = |verdict| reports.iter().any(|report| report.verdict() == verdict);
    if any(Verdict::Failed) {
        Verdict::Failed
    } else if any(Verdict::Incomplete) {
        Verdict::Incomplete
    } else {
        Verdict::Passed
    }
}

/// The exit code of a certification whose reports amount to `verdict`, when `require` is asked.
fn code(verdict: Verdict, require: Require, reports: &[Report]) -> u8 {
    let passed = |report: &Report| {
        report
            .results
            .iter()
            .any(|result| result.outcome == Outcome::Passed)
    };
    match verdict {
        Verdict::Passed => PASSED,
        Verdict::Failed => FINDINGS,
        // A report none of whose clauses passed certified nothing, whatever is required.
        Verdict::Incomplete if require == Require::Partial && reports.iter().all(passed) => PASSED,
        Verdict::Incomplete => INCOMPLETE,
    }
}

/// Whether a clause of `report` applies to the connector, rather than none.
fn ran(report: &Report) -> bool {
    report
        .results
        .iter()
        .any(|result| !matches!(result.outcome, Outcome::Inapplicable(_)))
}

/// The connector the command line names, and how to reach it.
fn target(args: &Args) -> Result<Target, Ended> {
    let named = args.target.as_deref().unwrap_or_default();
    // Its id is learned from its handshake: the reference's is only a name for its output.
    let id = ConnectorId::parse("rdlt.certify.target")
        .map_err(|error| Ended(USAGE, error.to_string()))?;
    let usage = |message: &str| Ended(USAGE, message.to_owned());
    if named.starts_with("grpcs://") {
        Endpoint::parse(named).map_err(|error| {
            Ended(
                USAGE,
                format!("{error}: an endpoint is `grpcs://host:port`"),
            )
        })?;
        if !args.env.is_empty() {
            return Err(usage("--env is for a spawned connector, not an endpoint"));
        }
        let (Some(cert), Some(key), Some(ca)) = (&args.tls_cert, &args.tls_key, &args.tls_ca)
        else {
            return Err(Ended(
                USAGE,
                "an endpoint needs --tls-cert, --tls-key and --tls-ca".to_owned(),
            ));
        };
        let identity = Identity {
            cert: cert.clone(),
            key: key.clone(),
        };
        let reference = ConnectorRef::new(id).endpoint(named);
        return Ok(Target::listening(Remote::new(identity, ca), reference));
    }
    if named.contains("://") {
        return Err(usage("an endpoint is `grpcs://host:port`"));
    }
    // The command line takes the TLS flags all together or none.
    if args.tls_cert.is_some() {
        return Err(usage(
            "--tls-cert, --tls-key and --tls-ca are for an endpoint",
        ));
    }
    let path = Path::new(named);
    if !path.is_file() {
        // Not repeated: what was typed may be an endpoint mistyped, with a credential in it.
        let message = "the connector named is neither a binary's path nor an endpoint, \
                       `grpcs://host:port`";
        return Err(Ended(IO, message.to_owned()));
    }
    let executable = path
        .metadata()
        .is_ok_and(|metadata| metadata.permissions().mode() & 0o111 != 0);
    if !executable {
        return Err(Ended(IO, format!("{named} is not executable")));
    }
    Ok(Target::spawned(
        local(args)?,
        granted(args, ConnectorRef::new(id).path(path)),
    ))
}

/// `reference` with what the command line grants its connector, shared among its spawns: a
/// certification spawns its connector many times, at once too.
fn granted(args: &Args, reference: ConnectorRef) -> ConnectorRef {
    let reference = args
        .grant_read
        .iter()
        .fold(reference, ConnectorRef::grant_read);
    let reference = args
        .grant_write
        .iter()
        .fold(reference, ConnectorRef::grant_write);
    let reference = if args.grant_network {
        reference.grant_network()
    } else {
        reference
    };
    reference.share_grants()
}

/// What spawns the connector's binary: inside a sandbox, unless the command line states the
/// binary is trusted.
fn local(args: &Args) -> Result<Local, Ended> {
    let local = if args.trusted {
        Local::trusting_binaries()
    } else {
        let sandbox = Bubblewrap::new();
        sandbox.usable().map_err(|error| {
            let message = format!("{error}; --trusted runs a binary you trust in no sandbox");
            Ended(IO, message)
        })?;
        Local::sandboxed(sandbox)
    };
    // What the command line grants is where it lets grants be made, and where no secret
    // directory it names may be written.
    let local = args.grant_read.iter().fold(local, Local::grantable_read);
    let local = args.grant_write.iter().fold(local, Local::grantable_write);
    let local = args.secret_dir.iter().fold(local, Local::guarded_dir);
    Ok(args.env.iter().fold(local, Local::env_passthrough))
}

/// Prints `text` on standard output, scrubbed of the configuration's secrets: a closed pipe
/// ends quietly, and any other failure is an I/O error.
fn print(text: &str) -> Result<(), Ended> {
    let text = redactions().scrubbed(text.to_owned());
    let mut stdout = std::io::stdout().lock();
    match stdout
        .write_all(text.as_bytes())
        .and_then(|()| stdout.flush())
    {
        Err(error) if error.kind() != std::io::ErrorKind::BrokenPipe => Err(Ended(
            IO,
            format!("writing to standard output failed: {error}"),
        )),
        _ => Ok(()),
    }
}
