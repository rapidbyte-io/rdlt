//! The command line: which connector, how to reach it and with what configuration, and how to
//! print what it met.

use std::io::Write as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use rdlt_certify::{
    Outcome, Probe, Report, Target, Unprobed, Verdict, certify_destination, certify_source, json,
    markdown, plain, read_back,
};
use rdlt_connector::ConnectorId;
use rdlt_host::{ConnectorRef, Endpoint, Identity, Local, Remote};

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
struct Args {
    /// The connector: its binary's path, or the `grpcs://host:port` endpoint it listens at.
    #[arg(required_unless_present = "clauses")]
    target: Option<String>,
    /// Certify this role alone; by default, every role the connector serves.
    #[arg(long, value_enum)]
    role: Option<Role>,
    /// The connector's configuration, as JSON.
    #[arg(long, default_value = "{}", conflicts_with = "config_file")]
    config: String,
    /// A file holding the connector's configuration, as JSON.
    #[arg(long)]
    config_file: Option<PathBuf>,
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
            writeln!(std::io::stderr(), "rdlt-certify: {message}").ok();
            ExitCode::from(code)
        }
    }
}

fn run(args: &Args) -> Result<u8, Ended> {
    let config = config(args)?;
    let target = target(args)?;
    let target = match args.kill_seed {
        Some(seed) => target.kill_seed(seed),
        None => target,
    };
    let target = match args.kill_timeout {
        Some(seconds) => target.kill_timeout(Duration::from_secs(seconds)),
        None => target,
    };
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|error| Ended(IO, format!("starting the runtime failed: {error}")))?;
    let mut reports = runtime.block_on(async {
        let mut reports = Vec::new();
        if !matches!(args.role, Some(Role::Destination)) {
            reports.push(certify_source(&target, config.clone()).await);
        }
        if !matches!(args.role, Some(Role::Source)) {
            // What the destination published is read back when it can be; else the clauses
            // that read it are not observed.
            let read_back = read_back(&target, &config).await;
            let probe: &dyn Probe = match &read_back {
                Some(read_back) => read_back,
                None => &Unprobed,
            };
            reports.push(certify_destination(&target, config, probe).await);
        }
        reports
    });
    // No clause of a role the connector does not serve applies: unless it was asked for, its
    // report is left out.
    if args.role.is_none() && reports.iter().any(ran) {
        reports.retain(ran);
    }
    let verdict = verdict(&reports);
    let text = match args.output {
        Output::Plain => reports.iter().map(plain).collect::<String>(),
        Output::Json => {
            let passed = verdict == Verdict::Passed;
            let reports: Vec<_> = reports.iter().map(json).collect();
            let document = serde_json::json!({
                "verdict": verdict.as_str(),
                "passed": passed,
                "reports": reports,
            });
            format!("{document}\n")
        }
    };
    print(&text)?;
    if !reports.iter().any(ran) {
        return Err(Ended(
            FINDINGS,
            "the connector serves none of the roles asked, so nothing was certified".to_owned(),
        ));
    }
    Ok(code(verdict, args.require, &reports))
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

/// The configuration the command line gives, as JSON.
fn config(args: &Args) -> Result<serde_json::Value, Ended> {
    let (text, source) = match &args.config_file {
        Some(path) => {
            let text = std::fs::read_to_string(path).map_err(|error| {
                Ended(IO, format!("reading {} failed: {error}", path.display()))
            })?;
            (text, path.display().to_string())
        }
        None => (args.config.clone(), "--config".to_owned()),
    };
    serde_json::from_str(&text)
        .map_err(|error| Ended(USAGE, format!("{source} is no JSON configuration: {error}")))
}

/// The connector the command line names, and how to reach it.
fn target(args: &Args) -> Result<Target, Ended> {
    let named = args.target.as_deref().unwrap_or_default();
    // Its id is learned from its handshake: the reference's is only a name for its output.
    let id = ConnectorId::parse("rdlt.certify.target")
        .map_err(|error| Ended(USAGE, error.to_string()))?;
    let usage = |message: &str| Ended(USAGE, message.to_owned());
    if named.starts_with("grpcs://") {
        Endpoint::parse(named).map_err(|error| Ended(USAGE, error.to_string()))?;
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
        return Err(Ended(IO, format!("{named} is no connector binary")));
    }
    let executable = path
        .metadata()
        .is_ok_and(|metadata| metadata.permissions().mode() & 0o111 != 0);
    if !executable {
        return Err(Ended(IO, format!("{named} is not executable")));
    }
    let local = args.env.iter().fold(Local::new(), Local::env_passthrough);
    Ok(Target::spawned(local, ConnectorRef::new(id).path(path)))
}

/// Prints `text` on standard output: a closed pipe ends quietly, and any other failure is an I/O
/// error.
fn print(text: &str) -> Result<(), Ended> {
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
