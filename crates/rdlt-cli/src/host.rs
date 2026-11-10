//! The host the command line places pipelines by: the operator's host file, then the flags.

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};

use crate::args::HostArgs;
use crate::output::Failure;

/// The policy `args` and the operator's file say, a flag given standing in place of the file's
/// setting; with neither, the default host: sandboxed, connectors from
/// `$XDG_DATA_HOME/rdlt/connectors`, no secret allowed.
pub(crate) async fn policy(args: &HostArgs) -> Result<rdlt::HostFile, Failure> {
    let file = match &args.file {
        Some(path) => Some(rdlt::HostFile::from_path(path).await?),
        None => match xdg("XDG_CONFIG_HOME", ".config") {
            Some(dir) => {
                let path = dir.join("rdlt/host.yaml");
                // A missing file is the default host; anything else that cannot be read fails.
                match tokio::fs::metadata(&path).await {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                    _ => Some(rdlt::HostFile::from_path(&path).await?),
                }
            }
            None => None,
        },
    };
    let mut policy = file.unwrap_or_default();
    replaced(&mut policy.connectors, &args.connectors);
    replaced(&mut policy.writable, &args.writable);
    replaced(&mut policy.readable, &args.readable);
    replaced(&mut policy.allow_env, &args.allow_env);
    replaced(&mut policy.allow_files, &args.allow_files);
    if policy.connectors.is_empty() {
        policy
            .connectors
            .extend(xdg("XDG_DATA_HOME", ".local/share").map(|dir| dir.join("rdlt/connectors")));
    }
    policy.trust_binaries |= args.trust_binaries;
    policy.allow_network |= args.allow_network;
    if let (Some(cert), Some(key), Some(ca)) = (&args.tls_cert, &args.tls_key, &args.tls_ca) {
        policy.remote = Some(rdlt::RemoteFiles::new(cert, key, ca));
    }
    Ok(policy)
}

/// The host `args` and the operator's file allow.
pub(crate) async fn host(args: &HostArgs) -> Result<rdlt::Host, Failure> {
    Ok(policy(args).await?.host().await?)
}

/// The pipeline the document at `path` describes, read by the facade, which refuses a path that
/// is no regular file and whose errors start with the path; one that cannot be read exits 74.
pub(crate) async fn document(path: &Path) -> Result<rdlt::PipelineBuilder, Failure> {
    rdlt::Pipeline::from_path(path)
        .await
        .map_err(|error| Failure::of_document(&error))
}

/// `$name`, or `$HOME/<fallback>` where it is unset or not absolute; none without a home.
fn xdg(name: &str, fallback: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .filter(|dir| dir.is_absolute())
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(fallback)))
}

/// `flags`, where any are given, in place of `setting`.
fn replaced<T: Clone>(setting: &mut Vec<T>, flags: &[T]) {
    if !flags.is_empty() {
        setting.clear();
        setting.extend_from_slice(flags);
    }
}
