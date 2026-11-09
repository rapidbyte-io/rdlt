//! The command line's words: the verbs, the host's policy in the operator's words, and output.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

/// Write, check, run, inspect and certify rdlt pipelines and connectors.
#[derive(Debug, Parser)]
#[command(name = "rdlt", version)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) verb: Verb,
    #[command(flatten)]
    pub(crate) host: HostArgs,
    /// How results print: plain on a terminal and JSON elsewhere, or as named.
    #[arg(long, value_enum, default_value_t = Output::Auto, global = true)]
    pub(crate) output: Output,
    /// The least level logged to standard error; `RUST_LOG`, where set, wins.
    #[arg(long, value_name = "LEVEL", default_value = "warn", global = true)]
    pub(crate) log_level: String,
}

/// The host's policy: the operator's file, then these flags over it, a flag given standing in
/// place of what the file says for that setting.
#[derive(Clone, Debug, Default, Args)]
pub(crate) struct HostArgs {
    /// The operator's host file; by default
    /// `$XDG_CONFIG_HOME/rdlt/host.yaml`, where it exists.
    #[arg(long = "host", value_name = "PATH", global = true)]
    pub(crate) file: Option<PathBuf>,
    /// A directory local connectors are found in; by default
    /// `$XDG_DATA_HOME/rdlt/connectors`.
    #[arg(long, value_name = "DIR", global = true)]
    pub(crate) connectors: Vec<PathBuf>,
    /// Runs local connectors with your own access, in no sandbox.
    #[arg(long, global = true)]
    pub(crate) trust_binaries: bool,
    /// A root a document may let its connectors write beneath.
    #[arg(long, value_name = "PATH", global = true)]
    pub(crate) writable: Vec<PathBuf>,
    /// A root a document may let its connectors read beneath.
    #[arg(long, value_name = "PATH", global = true)]
    pub(crate) readable: Vec<PathBuf>,
    /// Lets a document let its sandboxed connectors share this host's network, its loopback
    /// services included.
    #[arg(long, global = true)]
    pub(crate) allow_network: bool,
    /// An environment variable a configuration's `${env:NAME}` may reach.
    #[arg(long, value_name = "NAME", global = true)]
    pub(crate) allow_env: Vec<String>,
    /// A private directory a configuration's `${file:/path}` may reach beneath.
    #[arg(long, value_name = "DIR", global = true)]
    pub(crate) allow_files: Vec<PathBuf>,
    /// The host's certificate chain, in PEM, for remote connectors.
    #[arg(long, value_name = "PATH", global = true, requires_all = ["tls_key", "tls_ca"])]
    pub(crate) tls_cert: Option<PathBuf>,
    /// The host's private key, in PEM.
    #[arg(long, value_name = "PATH", global = true, requires = "tls_cert")]
    pub(crate) tls_key: Option<PathBuf>,
    /// The CA bundle remote connectors' certificates chain to, in PEM.
    #[arg(long, value_name = "PATH", global = true, requires = "tls_cert")]
    pub(crate) tls_ca: Option<PathBuf>,
}

/// How results print.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum Output {
    Auto,
    Plain,
    Json,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Verb {
    /// Writes a starter document.
    Init {
        /// Where the document is written; it must not exist.
        #[arg(default_value = "rdlt.yaml")]
        path: PathBuf,
    },
    /// Builds the pipeline a document describes: validates it, places and handshakes its
    /// connectors, checks its streams, modes and transforms; no data moves.
    Check {
        /// The pipeline's document.
        document: PathBuf,
    },
    /// Prints the source's catalog.
    Discover {
        /// The pipeline's document.
        document: PathBuf,
    },
    /// Runs the pipeline a document describes; the first SIGINT or SIGTERM stops it after its
    /// next commit, a second at once.
    Run {
        /// The pipeline's document.
        document: PathBuf,
        /// Writes each event as a line of JSON to PATH, or to standard output for `-`.
        #[arg(long, value_name = "PATH|-")]
        events: Option<PathBuf>,
        /// Writes the run's report, as JSON, to PATH.
        #[arg(long, value_name = "PATH")]
        report: Option<PathBuf>,
    },
    /// Resets what a pipeline committed; it opens a session, which fences a run of the pipeline
    /// in progress, as any write does.
    State {
        #[command(subcommand)]
        action: StateAction,
    },
    /// Prints the document's JSON Schema, or a connector's configuration schema.
    Schema {
        /// The document's schema.
        #[arg(long, conflicts_with = "connector")]
        document: bool,
        /// A connector's id, placed as a pipeline would place it.
        #[arg(required_unless_present = "document")]
        connector: Option<String>,
        /// The role the connector is asked for.
        #[arg(long, value_enum, default_value_t = Side::Source)]
        role: Side,
    },
    /// Lists what the host can place.
    Connectors,
    /// Certifies a connector, from its binary or at its endpoint, against the protocol's clauses.
    Certify(crate::certify::CertifyArgs),
    /// Makes connector projects.
    Connector {
        #[command(subcommand)]
        action: ConnectorAction,
    },
    /// Prints shell completions.
    Completions {
        /// The shell.
        shell: clap_complete::Shell,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum StateAction {
    /// Resets streams: their positions, or with --tables their tables too.
    Reset {
        /// The pipeline's document.
        document: PathBuf,
        /// The streams reset.
        #[arg(required = true)]
        streams: Vec<String>,
        /// Drops the streams' tables too.
        #[arg(long)]
        tables: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum Side {
    Source,
    Destination,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ConnectorAction {
    /// Writes a connector project whose certify test passes on its first run.
    New {
        /// The connector's name: its id is io.example.<name>.
        name: String,
        /// The project's language.
        #[arg(long, value_enum, default_value_t = Lang::Rust)]
        lang: Lang,
        /// The directory the project's directory is made in.
        #[arg(long, value_name = "DIR", default_value = ".")]
        into: PathBuf,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum Lang {
    Rust,
}
