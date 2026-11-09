//! `rdlt certify`: which connector, how to reach it and with what configuration, and how to
//! print what it met.

mod configured;
pub(crate) mod panics;
mod session;
#[cfg(test)]
mod tests;

use std::io::Write as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::ValueEnum;
use rdlt_certify::{
    CREDIT_WATCH, Observed, Outcome, Probe, RUN_TIMEOUT, Report, Target, Unprobed, Verdict,
    certify_destination_observed, certify_source_observed, json, markdown, plain, read_back,
};
use rdlt_connector::ConnectorId;
use rdlt_host::{
    Bubblewrap, ConnectorRef, Endpoint, Identity, Local, Redactions, Remote, StopsSpawned,
};
use session::{Session, ending, signalled};

use crate::Exit;
use crate::args::HostArgs;
use crate::output::{Mode, failure_json};
use crate::stopping::STOPPING;

/// Certifies a connector against the protocol's conformance clauses.
#[derive(Debug, clap::Args)]
pub(crate) struct CertifyArgs {
    /// The connector: its binary's path, or the `grpcs://host:port` endpoint it listens at.
    #[arg(required_unless_present = "clauses")]
    target: Option<String>,
    /// Certify this role alone; by default, every role the connector serves.
    #[arg(long, value_enum)]
    role: Option<Role>,
    /// A file holding the connector's configuration, as JSON, or `-` to read it from standard
    /// input; an empty object without it; a text value may refer to a secret as `${env:NAME}`
    /// or `${file:/absolute/path}`, where `--allow-env` and `--allow-files` allow it, and no
    /// report then shows the secret.
    #[arg(long, value_name = "PATH")]
    config_file: Option<PathBuf>,
    /// Lets the sandboxed connector reach the network.
    #[arg(long)]
    network: bool,
    /// An environment variable a spawned connector keeps; it otherwise starts with glibc's
    /// malloc tunables alone, and in a sandbox `TMPDIR` names its scratch directory, whatever it
    /// was.
    #[arg(long = "env", value_name = "NAME")]
    env: Vec<String>,
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

/// Why the command ended without certifying: how it exits and its message.
struct Ended(Exit, String);

/// Certifies the connector `args` names, as `host` lets it be reached, printing what it met as
/// `mode` says, and answers how the command exits.
pub(crate) fn certify(args: &CertifyArgs, host: &HostArgs, mode: Mode) -> Exit {
    let ran = if args.clauses {
        print(&markdown()).map(|()| Exit::Success)
    } else {
        run(args, host, mode)
    };
    match ran {
        Ok(exit) => exit,
        Err(Ended(exit, message)) => {
            let message = redactions().scrubbed(message);
            if mode == Mode::Json {
                print(&failure_json(exit, &message)).ok();
            } else {
                writeln!(std::io::stderr(), "rdlt certify: {message}").ok();
            }
            exit
        }
    }
}

/// The secrets the configuration's references resolved to: nothing this command prints holds
/// one.
pub(crate) fn redactions() -> &'static Redactions {
    static REDACTIONS: std::sync::OnceLock<Redactions> = std::sync::OnceLock::new();
    REDACTIONS.get_or_init(Redactions::new)
}

fn run(args: &CertifyArgs, host: &HostArgs, mode: Mode) -> Result<Exit, Ended> {
    if host.file.is_some() || !host.connectors.is_empty() || host.allow_network {
        return Err(Ended(
            Exit::Usage,
            "--host, --connectors and --allow-network are for a pipeline's host: \
             certify takes what its connector may reach from its own flags"
                .to_owned(),
        ));
    }
    let watch = args.credit_watch.map(credit_watch).transpose()?;
    let config = configured::config(args, host)?;
    let target = target(args, host)?;
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
    let certified = certified(args, mode, &target, &config, &mut session);
    let heard = certified.as_ref().err().and_then(signalled);
    let (stopped, heard) = session.stopped(stops, heard);
    let reports = ending(certified, stopped, heard)?;
    let verdict = verdict(&reports);
    if mode == Mode::Json {
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
        // Told on standard error: JSON's one value, the reports, is written and says it already.
        let message = "the connector serves none of the roles asked, so nothing was certified";
        writeln!(std::io::stderr(), "rdlt certify: {message}").ok();
        return Ok(Exit::Findings);
    }
    Ok(code(verdict, args.require, &reports))
}

/// When the certification's bound passes, when it has one: whoever asks for a bound gets one, so
/// a timeout the clock cannot hold is refused.
fn until(args: &CertifyArgs) -> Result<Option<Instant>, Ended> {
    let Some(bound) = bound(args.timeout, args.no_timeout) else {
        return Ok(None);
    };
    let until = Instant::now().checked_add(bound).ok_or_else(|| {
        let message = "--timeout is further ahead than the clock holds; see --no-timeout";
        Ended(Exit::Usage, message.to_owned())
    })?;
    Ok(Some(until))
}

/// A watch of `millis` milliseconds, which is no longer than `P-CREDIT`'s own.
fn credit_watch(millis: u64) -> Result<Duration, Ended> {
    let watch = Duration::from_millis(millis);
    if watch > CREDIT_WATCH {
        let most = CREDIT_WATCH.as_millis();
        return Err(Ended(
            Exit::Usage,
            format!("--credit-watch is at most {most} ms"),
        ));
    }
    Ok(watch)
}

/// The report of each role asked that the connector serves, or of every role when it serves
/// none, each printed as text as its certification ends.
fn certified(
    args: &CertifyArgs,
    mode: Mode,
    target: &Target,
    config: &serde_json::Value,
    session: &mut Session,
) -> Result<Vec<Report>, Ended> {
    let mut printed = Printed {
        mode,
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
    mode: Mode,
    /// Reports of roles the connector does not serve, printed only when it serves none.
    held: Vec<Report>,
}

impl Printed {
    fn report(&self, report: &Report) -> Result<(), Ended> {
        match self.mode {
            Mode::Plain => print(&plain(report)),
            Mode::Json => Ok(()),
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
fn code(verdict: Verdict, require: Require, reports: &[Report]) -> Exit {
    let passed = |report: &Report| {
        report
            .results
            .iter()
            .any(|result| result.outcome == Outcome::Passed)
    };
    match verdict {
        Verdict::Passed => Exit::Success,
        // A report none of whose clauses passed certified nothing, whatever is required.
        Verdict::Incomplete if require == Require::Partial && reports.iter().all(passed) => {
            Exit::Success
        }
        Verdict::Failed | Verdict::Incomplete => Exit::Findings,
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
fn target(args: &CertifyArgs, host: &HostArgs) -> Result<Target, Ended> {
    let named = args.target.as_deref().unwrap_or_default();
    // Its id is learned from its handshake: the reference's is only a name for its output.
    let id = ConnectorId::parse("rdlt.certify.target")
        .map_err(|error| Ended(Exit::Usage, error.to_string()))?;
    let usage = |message: &str| Ended(Exit::Usage, message.to_owned());
    if named.starts_with("grpcs://") {
        Endpoint::parse(named).map_err(|error| {
            Ended(
                Exit::Usage,
                format!("{error}: an endpoint is `grpcs://host:port`"),
            )
        })?;
        if !args.env.is_empty() {
            return Err(usage("--env is for a spawned connector, not an endpoint"));
        }
        let (Some(cert), Some(key), Some(ca)) = (&host.tls_cert, &host.tls_key, &host.tls_ca)
        else {
            return Err(usage(
                "an endpoint needs --tls-cert, --tls-key and --tls-ca",
            ));
        };
        let opening = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(|error| Ended(Exit::Io, format!("starting the runtime failed: {error}")))?;
        let opened = opening.block_on(rdlt::Tls::from_files(cert, key, ca));
        opening.shutdown_background();
        opened.map_err(|error| Ended(Exit::Io, error.to_string()))?;
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
    if host.tls_cert.is_some() {
        return Err(usage(
            "--tls-cert, --tls-key and --tls-ca are for an endpoint",
        ));
    }
    let path = Path::new(named);
    if !path.is_file() {
        // Not repeated: what was typed may be an endpoint mistyped, with a credential in it.
        let message = "the connector named is neither a binary's path nor an endpoint, \
                       `grpcs://host:port`";
        return Err(Ended(Exit::Io, message.to_owned()));
    }
    let executable = path
        .metadata()
        .is_ok_and(|metadata| metadata.permissions().mode() & 0o111 != 0);
    if !executable {
        return Err(Ended(Exit::Io, format!("{named} is not executable")));
    }
    Ok(Target::spawned(
        local(args, host)?,
        granted(args, host, ConnectorRef::new(id).path(path)),
    ))
}

/// `reference` with what the command line grants its connector, shared among its spawns: a
/// certification spawns its connector many times, at once too.
fn granted(args: &CertifyArgs, host: &HostArgs, reference: ConnectorRef) -> ConnectorRef {
    let reference = host.readable.iter().fold(reference, ConnectorRef::read);
    let reference = host.writable.iter().fold(reference, ConnectorRef::write);
    let reference = if args.network {
        reference.network()
    } else {
        reference
    };
    reference.share_access()
}

/// What spawns the connector's binary: inside a sandbox, unless the command line states the
/// binary is trusted.
fn local(args: &CertifyArgs, host: &HostArgs) -> Result<Local, Ended> {
    let granting = args.network || !host.readable.is_empty() || !host.writable.is_empty();
    if host.trust_binaries && granting {
        let message = "--trust-binaries gives a connector your own access: grant it nothing";
        return Err(Ended(Exit::Usage, message.to_owned()));
    }
    let local = if host.trust_binaries {
        Local::trusting_binaries()
    } else {
        let sandbox = Bubblewrap::new();
        sandbox.usable().map_err(|error| {
            let message =
                format!("{error}; --trust-binaries runs a binary you trust in no sandbox");
            Ended(Exit::Io, message)
        })?;
        Local::sandboxed(sandbox)
    };
    // What the command line grants is where it lets grants be made, and where no secret
    // directory it names may be written.
    let local = host.readable.iter().fold(local, Local::readable);
    let local = host.writable.iter().fold(local, Local::writable);
    let local = host.allow_files.iter().fold(local, Local::guarded_dir);
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
            Exit::Io,
            format!("writing to standard output failed: {error}"),
        )),
        _ => Ok(()),
    }
}
