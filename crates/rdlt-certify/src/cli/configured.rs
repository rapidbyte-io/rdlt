//! The connector's configuration, as the command line names it: read from a file or standard
//! input, never the command line, with the secrets it refers to resolved where allowed.

use std::io::Read as _;

use super::{Args, Ended, IO, USAGE, redactions};

/// Bytes read of a configuration at most, one beyond what a configuration may hold.
const CONFIG_READ: u64 = rdlt_wire::limits::CONFIG_BYTES + 1;

/// The configuration the command line names, from its file or from standard input, with each
/// secret it refers to resolved, and kept from everything printed from here on.
///
/// It is never taken from the command line itself, which other users of the machine can read.
pub(super) fn config(args: &Args) -> Result<serde_json::Value, Ended> {
    let Some(path) = &args.config_file else {
        return Ok(serde_json::json!({}));
    };
    let (read, source) = if path.as_os_str() == "-" {
        let mut text = String::new();
        let read = std::io::stdin()
            .lock()
            .take(CONFIG_READ)
            .read_to_string(&mut text);
        (read.map(|_| text), "standard input".to_owned())
    } else {
        let opened = std::fs::File::open(path);
        let read = opened.and_then(|file| {
            let mut text = String::new();
            file.take(CONFIG_READ)
                .read_to_string(&mut text)
                .map(|_| text)
        });
        (read, path.display().to_string())
    };
    let text = read.map_err(|error| Ended(IO, format!("reading {source} failed: {error}")))?;
    // What is wrong with it is said without quoting it.
    let unusable = |error: rdlt_host::SecretError| {
        let cause = std::error::Error::source(&error);
        let cause = cause.map(|cause| format!(": {cause}")).unwrap_or_default();
        let message = format!("the configuration in {source} cannot be used: {error}{cause}");
        Ended(USAGE, message)
    };
    let held = rdlt_host::Config::parse(text).map_err(&unusable)?;
    let resolving = tokio::runtime::Builder::new_current_thread()
        .build()
        .map_err(|error| Ended(IO, format!("starting the runtime failed: {error}")))?;
    let secrets = rdlt_host::Secrets::new()
        .env(rdlt_host::EnvSecrets::allowing(
            args.secret_env.iter().cloned(),
        ))
        .files(rdlt_host::FileSecrets::within(
            args.secret_dir.iter().cloned(),
        ));
    let resolved = resolving
        .block_on(held.resolved(&secrets, redactions()))
        .map_err(&unusable)?;
    serde_json::from_str(&resolved).map_err(|_| unusable(rdlt_host::SecretError::NotJson))
}
