//! The OS seam: spawn a connector binary under the frozen contract and
//! read its one stdout handshake line. Nothing here dials or speaks
//! RPC — the caller gets a [`Guard`] owning the new process group and
//! the parsed [`Line`] saying where to dial.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use rdlt_connector_protocol::PROTOCOL_VERSION;
use rdlt_connector_protocol::handshake::Line;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;

use crate::managed::Guard;
use crate::provider::Error;

/// How long a spawned binary gets to write its handshake line. Generous
/// on purpose: the line is the FIRST thing a served connector writes,
/// before any config work, so ten seconds is not a performance budget
/// but a "this is not a connector" detector — and it equals the
/// client's per-await RPC deadline, so a dead or silent connector
/// yields a typed error within ten seconds at every stage.
pub(crate) const LINE_TIMEOUT: Duration = Duration::from_secs(10);

/// The most bytes the one handshake line may occupy, terminator
/// included. A conforming line is tens of bytes, so 64 KiB is a flood
/// detector: without a cap, a binary spewing newline-less stdout would
/// grow the line buffer unboundedly until the timeout.
pub(crate) const MAX_LINE_BYTES: u64 = 64 * 1024;

/// How long after stdout EOF to wait for the child to EXIT before
/// refusing on the empty line. EOF means no handshake can ever arrive,
/// so this is not a handshake budget: it exists so the common shape —
/// the process died and EOF is how we learn — reports the exit status
/// rather than an empty-line parse. Generous relative to the instant
/// the common case needs, deliberately: a role refusal whose exit
/// status is reaped AFTER the grace would misreport as a malformed
/// handshake, and the flagless spec probe would skip its destination
/// retry keyed to the exit code.
pub(crate) const EOF_EXIT_GRACE: Duration = Duration::from_secs(2);

/// Observe child exit without reaping it. `WNOWAIT` keeps the direct
/// pid anchored until the process-group kill has run, so the group id
/// cannot be reused before descendants are swept.
async fn exited_without_reaping(pid: u32, deadline: tokio::time::Instant) -> bool {
    let Some(pid) = i32::try_from(pid)
        .ok()
        .and_then(rustix::process::Pid::from_raw)
    else {
        return false;
    };
    loop {
        let options = rustix::process::WaitIdOptions::EXITED
            | rustix::process::WaitIdOptions::NOHANG
            | rustix::process::WaitIdOptions::NOWAIT;
        if matches!(
            rustix::process::waitid(rustix::process::WaitId::Pid(pid), options),
            Ok(Some(_))
        ) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Spawn `path` for `role` and read EXACTLY ONE stdout line under
/// `line_timeout`. A guard owns the new process group from the moment
/// the child exists, so every error below kills the direct child and
/// anything it forked even before a socket path is known. `binary` is
/// the label the errors name — the conventional binary name, or the
/// override path as given.
///
/// `quiet_stderr` nulls the child's stderr instead of inheriting it —
/// for the spec probe, where a wrong-role attempt against a
/// single-role connector would otherwise print that bin's usage
/// refusal into the caller's terminal.
pub(crate) async fn spawn(
    path: &Path,
    binary: &str,
    role: &str,
    quiet_stderr: bool,
    line_timeout: Duration,
) -> Result<(Guard, Line), Error> {
    let spawn_error = |source| Error::Spawn {
        binary: binary.to_string(),
        source,
    };
    let mut command = Command::new(path);
    command
        .arg(format!("--role={role}"))
        // stdout is the machine channel (the one handshake line);
        // stderr stays the connector's human log channel unless the
        // caller asked for quiet; stdin is nothing's channel.
        .stdout(Stdio::piped())
        .stderr(if quiet_stderr {
            Stdio::null()
        } else {
            Stdio::inherit()
        })
        .stdin(Stdio::null())
        // Connector configuration is explicit; the host process's
        // ambient credentials are not an implicit second config
        // channel. Retain only locale/temp/process-discovery data and
        // the dynamic-linker search path below.
        .env_clear()
        // Belt beside the guard's group kill.
        .process_group(0)
        .kill_on_drop(true);
    for key in [
        "PATH",
        "TMPDIR",
        "TMP",
        "TEMP",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "TZ",
        // Connectors that load a native client library at runtime (the
        // documented mechanism for e.g. Oracle Instant Client) resolve
        // it through the host's dynamic-linker search path, so the
        // linker variables pass through; the clean-room stays for
        // everything else. The DYLD names are macOS's spellings —
        // absent keys, forwarded as nothing, on other platforms.
        "LD_LIBRARY_PATH",
        "DYLD_LIBRARY_PATH",
        "DYLD_FALLBACK_LIBRARY_PATH",
    ] {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    let child = command.spawn().map_err(spawn_error)?;
    let mut guard = Guard::for_process_group(child);

    let stdout = guard
        .child_mut()
        .stdout
        .take()
        .expect("stdout was piped at spawn, so the child carries it");
    // `take` caps the read at [`MAX_LINE_BYTES`]: a binary flooding
    // stdout without a newline hits EOF at the cap instead of growing
    // the buffer until the timeout — detected below as a full buffer
    // with no line terminator.
    let mut reader = BufReader::new(stdout.take(MAX_LINE_BYTES));
    let mut line = String::new();
    let deadline = tokio::time::Instant::now() + line_timeout;
    match tokio::time::timeout_at(deadline, reader.read_line(&mut line)).await {
        Err(_elapsed) => Err(Error::Timeout {
            binary: binary.to_string(),
        }),
        Ok(Err(source)) => Err(spawn_error(source)),
        Ok(Ok(bytes)) => {
            if bytes == 0 {
                // EOF: no handshake can ever arrive, so never wait out
                // the full line deadline here — a child that closed
                // stdout but kept running would convert this typed
                // refusal into a Timeout. [`EOF_EXIT_GRACE`] bounds the
                // wait for an exit status; on elapse the empty line
                // falls through to the parse refusal below (the
                // still-running child dies with its kill_on_drop
                // handle).
                let grace = deadline.min(tokio::time::Instant::now() + EOF_EXIT_GRACE);
                if let Some(pid) = guard.pid()
                    && exited_without_reaping(pid, grace).await
                {
                    guard.kill_process_group();
                    let status = guard.child_mut().wait().await.map_err(spawn_error)?;
                    if !status.success() {
                        return Err(Error::ExitedBeforeHandshake {
                            binary: binary.to_string(),
                            role: role.to_string(),
                            status,
                        });
                    }
                }
            }
            if !line.ends_with('\n') && line.len() as u64 >= MAX_LINE_BYTES {
                return Err(Error::HandshakeLineOverflow {
                    binary: binary.to_string(),
                    limit: MAX_LINE_BYTES,
                });
            }
            let parsed = Line::parse(line.trim_end_matches(['\n', '\r'])).map_err(|source| {
                Error::HandshakeLine {
                    binary: binary.to_string(),
                    source,
                }
            })?;
            // The range the line advertises is HONORED here, before any
            // dial: this host's protocol version must sit inside it.
            // The gRPC handshake would refuse a mismatch too, but only
            // after the socket exchange — late, and in whatever shape a
            // version-skewed exchange produces.
            if !(parsed.protocol_min..=parsed.protocol_max).contains(&PROTOCOL_VERSION) {
                return Err(Error::ProtocolRange {
                    binary: binary.to_string(),
                    min: parsed.protocol_min,
                    max: parsed.protocol_max,
                    ours: PROTOCOL_VERSION,
                });
            }
            guard.set_socket_path(parsed.socket_path.clone());
            Ok((guard, parsed))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One law, pinned from this side: the ten seconds a spawned
    /// binary gets to write its handshake line is the same ten seconds
    /// the client gives every wire await after it — a dead OR silent
    /// connector yields a typed error within ten seconds, never a
    /// hang. Change either constant and this pin names the fracture.
    #[test]
    fn the_line_timeout_is_the_client_rpc_deadline() {
        assert_eq!(LINE_TIMEOUT, rdlt_connector_client::wire::DEFAULT_DEADLINE);
    }
}
