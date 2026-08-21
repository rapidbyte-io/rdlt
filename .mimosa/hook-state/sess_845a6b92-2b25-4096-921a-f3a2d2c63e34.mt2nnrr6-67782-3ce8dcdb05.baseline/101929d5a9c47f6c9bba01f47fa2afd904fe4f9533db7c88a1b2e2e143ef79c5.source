//! The `--probe-cmd` table probe: one operator shell line, run via
//! `sh -c` once per count with `{{table}}` substituted, whose stdout
//! must be one u64 row count. The line may carry credentials, so it is
//! NEVER echoed — no failure message repeats it, its stderr is
//! discarded, and its stdout is never embedded. Every count is bounded
//! in time and in bytes, and the whole process group the line forked
//! is swept when a count ends.

use std::time::Duration;

use async_trait::async_trait;
use rdlt_connector::core::id::TableName;
use rdlt_testkit::conformance::destination::{ProbeError, TableProbe};

/// How long one probe command may run before its count fails. The
/// clause clock stops while a count runs (its budget covers SPI
/// traffic alone), so a hanging probe fails naming ITSELF and can never
/// exhaust a clause budget whose evidence would blame the connector.
/// Not the only bound: [`crate::clock`] additionally bounds EVERY probe
/// at its own 30s backstop; this tighter 20s fires first.
const PROBE_TIMEOUT: Duration = Duration::from_secs(20);

/// The most probe stdout the certifier will buffer. A conforming probe
/// prints ONE u64 row count, so a megabyte is orders of magnitude of
/// headroom — and without a cap, [`drain_output`]'s read-to-EOF would
/// buffer whatever an arbitrary operator command emits until
/// [`PROBE_TIMEOUT`], which bounds time but not memory.
const MAX_PROBE_STDOUT_BYTES: u64 = 1024 * 1024;

/// The shell-line read-back: one shell line, run per count with
/// `{{table}}` substituted. The template may carry credentials, so no
/// error below ever repeats it — failures name what happened, never
/// the command.
pub struct Shell {
    /// The operator's shell line, `{{table}}` and all.
    template: String,
}

/// The spelling a placeholder-less template is refused with; the
/// message names the placeholder alone, never the line.
pub const MISSING_PLACEHOLDER: &str = "the probe command must contain the `{{table}}` \
                                        placeholder — the line runs once per counted table \
                                        with `{{table}}` substituted for the table name";

impl Shell {
    /// Wrap `command`, the operator's shell line. It must contain the
    /// `{{table}}` placeholder: without one the line would count ONE
    /// fixed target for every clause — wrong verdicts in both
    /// directions with no error naming the mistake — so it is refused
    /// here with [`MISSING_PLACEHOLDER`], never accepted and run.
    pub fn new(command: String) -> Result<Self, String> {
        if !command.contains("{{table}}") {
            return Err(MISSING_PLACEHOLDER.to_string());
        }
        Ok(Self { template: command })
    }
}

/// Manual, not derived: the shell line may carry credentials, and a
/// derived `Debug` would print it into whatever log or panic message
/// renders the probe.
impl std::fmt::Debug for Shell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shell")
            .field("template", &format_args!("<elided>"))
            .finish()
    }
}

#[async_trait]
impl TableProbe for Shell {
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
        // `drain_output` deliberately does NOT reap: the sweep on
        // every arm below runs while the direct sh child is
        // still unreaped — zombie included — so the group id is
        // ANCHORED and the group SIGKILL can never land on a recycled
        // group (measured: a zombie-only group still answers `kill
        // -0`, and its id cannot be reused until the reap; a pre-kill
        // reading therefore cannot distinguish "only the zombie" from
        // "grandchildren exist", so the ORDERING is the safety, not a
        // conditional). `sweep_probe_exit` signals, reaps, then takes
        // the one post-reap reading; its degradation note is surfaced
        // on EVERY arm — a fork the probe line left behind holds
        // whatever it opened (a single-writer store, for one) into the
        // next clause, so silence would blame the connector.
        match tokio::time::timeout(PROBE_TIMEOUT, drain_output(&mut child)).await {
            Err(_elapsed) => {
                let group_note = sweep_probe_exit(pgid, &mut child).await;
                let message = format!(
                    "the probe command did not finish within {}s",
                    PROBE_TIMEOUT.as_secs()
                );
                Err(ProbeError {
                    message: with_note(message, group_note),
                })
            }
            Ok(Err(error)) => {
                let group_note = sweep_probe_exit(pgid, &mut child).await;
                Err(ProbeError {
                    message: with_note(
                        format!("the probe command could not run: {error}"),
                        group_note,
                    ),
                })
            }
            Ok(Ok(stdout)) => {
                let group_note = sweep_probe_exit(pgid, &mut child).await;
                // The cap verdict comes BEFORE the exit-status one: a
                // probe still writing past the cap blocks on its full
                // pipe and dies under the sweep, and its kill signal
                // would otherwise mask the actual problem.
                if stdout.len() as u64 > MAX_PROBE_STDOUT_BYTES {
                    return Err(ProbeError {
                        message: with_note(
                            format!(
                                "the probe command printed more than {MAX_PROBE_STDOUT_BYTES} \
                                 bytes of stdout — a row-count probe answers one small line"
                            ),
                            group_note,
                        ),
                    });
                }
                let status = match child.wait().await {
                    Ok(status) => status,
                    Err(error) => {
                        return Err(ProbeError {
                            message: with_note(
                                format!("the probe command could not run: {error}"),
                                group_note,
                            ),
                        });
                    }
                };
                if !status.success() {
                    return Err(ProbeError {
                        message: with_note(
                            format!("the probe command failed: {status}"),
                            group_note,
                        ),
                    });
                }
                // A degraded sweep FAILS the count even though the
                // command succeeded: the count's
                // evidence is only as clean as the store it read, and
                // possible residue holds that store into the next
                // clause — the failure names the probe, never the
                // connector.
                if let Some(note) = group_note {
                    return Err(ProbeError {
                        message: format!(
                            "the probe command completed but its process sweep degraded \
                             ({note}) — residue could hold the destination into the next \
                             clause"
                        ),
                    });
                }
                let stdout = String::from_utf8_lossy(&stdout);
                let count = stdout.trim();
                // The output is NEVER embedded (a usage-printing
                // wrapper would leak the credential-bearing probe line
                // into the report): the byte count locates the problem
                // without repeating the bytes.
                count.parse::<u64>().map_err(|_unparseable| ProbeError {
                    message: format!(
                        "the probe command printed output that is not one u64 row count \
                         ({} bytes)",
                        count.len()
                    ),
                })
            }
        }
    }
}

/// Append a sweep degradation note to a probe failure message — the
/// one spelling every arm folds it in with.
fn with_note(mut message: String, note: Option<&'static str>) -> String {
    if let Some(note) = note {
        message.push_str(" (");
        message.push_str(note);
        message.push(')');
    }
    message
}

/// The one exit sweep every probe arm runs BEFORE the reap: SIGKILL the
/// whole group when its id is known (grandchildren included — safe
/// because the unreaped direct child still anchors the id), else kill
/// and reap the direct child alone. The returned degradation note joins
/// every arm's failure message, and on an otherwise-successful count it
/// fails the count naming the probe.
async fn sweep_probe_exit(
    pgid: Option<u32>,
    child: &mut tokio::process::Child,
) -> Option<&'static str> {
    match pgid {
        Some(pgid) => group_kill(pgid, child).await,
        None => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            Some("the probe's pid was unknown, so only the direct child was killed")
        }
    }
}

/// SIGKILL the probe's whole process group, reap the direct sh child
/// (its zombie would answer signal 0 forever), then take ONE `kill -0`
/// reading — a polling window could spend its length observing a pgid
/// RECYCLED to an unrelated group and state phantom survivors as fact,
/// so a positive answer is reported as POSSIBLE residue. Returns the
/// degradation note for the failure message when the group could not
/// be killed or may not have drained — never a command echo, never a
/// silent swallow.
async fn group_kill(pgid: u32, child: &mut tokio::process::Child) -> Option<&'static str> {
    let target = format!("-{pgid}");
    let signalled = tokio::process::Command::new("kill")
        .args(["-KILL", "--", &target])
        .status()
        .await;
    // The direct child is reaped FIRST either way: a zombie still
    // answers signal 0 as a group member and would turn
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

/// Read the probe's stdout to EOF WITHOUT reaping — the reap belongs to
/// the arms AFTER their sweep, while the unreaped child still anchors
/// the group id (a reap here would let the sweep's SIGKILL land on a
/// recycled group). The read is capped one byte past
/// [`MAX_PROBE_STDOUT_BYTES`] so the caller can tell at-the-cap from
/// past-it; a probe still writing beyond the cap blocks on its full
/// pipe until the sweep kills the group.
async fn drain_output(child: &mut tokio::process::Child) -> std::io::Result<Vec<u8>> {
    use tokio::io::AsyncReadExt as _;
    let mut stdout = Vec::new();
    if let Some(pipe) = child.stdout.as_mut() {
        pipe.take(MAX_PROBE_STDOUT_BYTES + 1)
            .read_to_end(&mut stdout)
            .await?;
    }
    Ok(stdout)
}

#[cfg(test)]
mod tests {
    //! The shell probe's byte bound — probe stdout is an arbitrary
    //! operator command's output, so the drain is CAPPED in bytes, not
    //! just in time (a read-to-EOF would buffer whatever the command
    //! emitted until the 20s timeout, which bounds patience but not
    //! memory) — and the placeholder precondition.

    use super::*;

    /// A probe flooding stdout past [`MAX_PROBE_STDOUT_BYTES`] is
    /// refused NAMING THE CAP — the kill signal its blocked pipe earns
    /// under the sweep must not mask the actual problem — and the cap
    /// itself is pinned so drifting it is a deliberate act.
    #[tokio::test]
    async fn probe_stdout_flood_is_refused_at_the_cap() {
        let probe = Shell::new(format!(
            ": {{{{table}}}}; head -c {} /dev/zero",
            4 * MAX_PROBE_STDOUT_BYTES
        ))
        .expect("the template carries the placeholder");
        let error = probe
            .count(&TableName::new("t"))
            .await
            .expect_err("an over-cap stdout must be refused");
        assert!(
            error.message.starts_with(
                "the probe command printed more than 1048576 bytes of stdout — a \
                 row-count probe answers one small line"
            ),
            "{}",
            error.message
        );
        assert_eq!(MAX_PROBE_STDOUT_BYTES, 1024 * 1024);
    }

    /// The control: a conforming one-number probe still counts under
    /// the capped drain.
    #[tokio::test]
    async fn probe_stdout_one_number_still_counts_under_the_cap() {
        let probe = Shell::new(": {{table}}; echo 42".to_string())
            .expect("the template carries the placeholder");
        assert_eq!(probe.count(&TableName::new("t")).await.expect("counts"), 42);
    }

    /// A template without `{{table}}` is refused at construction with
    /// the pinned spelling — the line is never run, so no fixed table
    /// can be counted for every clause — and the refusal repeats the
    /// placeholder only, never the line.
    #[test]
    fn a_template_without_the_placeholder_is_refused_at_construction() {
        let error = Shell::new("echo 42 --password=hunter2".to_string())
            .expect_err("a placeholder-less template is refused");
        assert_eq!(error, MISSING_PLACEHOLDER);
        assert!(
            !error.contains("hunter2"),
            "the refusal must not echo the line: {error}"
        );
    }
}
