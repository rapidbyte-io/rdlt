//! Safe-Rust process resource sampler (research R3, contract BH3).
//!
//! `getrusage`/`wait4` are libc FFI (unsafe) — rejected. Instead a thread polls
//! `/proc/<pid>/status` (`VmHWM`: the kernel's own peak-RSS high-water mark, so
//! transient spikes between samples are captured by construction) and
//! `/proc/<pid>/stat` (utime/stime). The final CPU sample undercounts by at
//! most one polling interval; recorded honestly, never fabricated.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const POLL: Duration = Duration::from_millis(50);

/// One time-series point (raw; written to results/raw/, never committed).
#[derive(Debug, Clone, serde::Serialize)]
pub struct SamplePoint {
    pub at_ms: u64,
    pub rss_kb: u64,
    pub cpu_ms: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ResourceUsage {
    /// VmHWM at the last successful poll (bytes). None: process exited before
    /// the first poll, or /proc unreadable — reported null with this reason.
    pub peak_rss_bytes: Option<u64>,
    /// user+sys CPU time (ms) at the last successful poll.
    pub cpu_ms: Option<u64>,
    pub series: Vec<SamplePoint>,
    /// Why a metric is None, when it is.
    pub note: Option<String>,
}

/// Kernel clock ticks per second. `sysconf(_SC_CLK_TCK)` is libc; `getconf
/// CLK_TCK` is the safe route, 100 the universal Linux default fallback.
fn clk_tck() -> u64 {
    std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(100)
}

fn read_vmhwm_kb(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status.lines().find_map(|l| {
        l.strip_prefix("VmHWM:")
            .and_then(|rest| rest.trim().strip_suffix("kB"))
            .and_then(|n| n.trim().parse().ok())
    })
}

fn read_cpu_ticks(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // comm can contain spaces/parens: fields count from after the LAST ')'.
    let after = &stat[stat.rfind(')')? + 1..];
    let fields: Vec<&str> = after.split_whitespace().collect();
    // after ')' the next field is state (index 0); utime/stime are fields
    // 14/15 of the full line = indices 11/12 here.
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some(utime + stime)
}

/// Samples a child process until [`Sampler::stop`]. The child must outlive the
/// first poll for any data to exist (sub-50ms processes report null + note).
#[derive(Debug)]
pub struct Sampler {
    stop: Arc<AtomicBool>,
    handle: std::thread::JoinHandle<ResourceUsage>,
}

impl Sampler {
    pub fn spawn(pid: u32) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();
        let handle = std::thread::spawn(move || {
            let tck = clk_tck();
            let started = Instant::now();
            let mut usage = ResourceUsage::default();
            loop {
                match (read_vmhwm_kb(pid), read_cpu_ticks(pid)) {
                    (Some(rss_kb), Some(ticks)) => {
                        let cpu_ms = ticks * 1000 / tck;
                        usage.peak_rss_bytes = Some(rss_kb * 1024);
                        usage.cpu_ms = Some(cpu_ms);
                        usage.series.push(SamplePoint {
                            at_ms: started.elapsed().as_millis() as u64,
                            rss_kb,
                            cpu_ms,
                        });
                    }
                    _ if usage.series.is_empty() => {
                        // not readable yet (or already gone) — keep trying
                        // until stop; the note lands only if it never worked.
                    }
                    _ => {} // process exited; keep the last-seen values
                }
                if stop2.load(Ordering::Relaxed) {
                    break;
                }
                std::thread::sleep(POLL);
            }
            if usage.series.is_empty() {
                usage.note = Some(format!(
                    "no /proc/{pid} sample succeeded (process exited before first poll?)"
                ));
            } else {
                usage.note = Some(format!(
                    "cpu from last /proc poll — undercounts up to {}ms",
                    POLL.as_millis()
                ));
            }
            usage
        });
        Self { stop, handle }
    }

    pub fn stop(self) -> ResourceUsage {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.join().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn samples_a_real_child_and_reports_peak_rss() {
        // Python holds 50MB in the sampled process, then sleeps across polls.
        let mut child = std::process::Command::new("python3")
            .args(["-c", "import time; x = bytearray(50_000_000); time.sleep(0.4)"])
            .stdout(std::process::Stdio::null())
            .spawn()
            .expect("spawn");
        let sampler = Sampler::spawn(child.id());
        child.wait().unwrap();
        let usage = sampler.stop();
        let peak = usage.peak_rss_bytes.expect("peak rss sampled");
        assert!(peak > 10 * 1024 * 1024, "peak {peak} should exceed 10MB");
        assert!(!usage.series.is_empty());
        assert!(usage.cpu_ms.is_some());
    }

    #[test]
    fn vanished_pid_reports_null_with_reason() {
        // A PID that (almost certainly) doesn't exist.
        let sampler = Sampler::spawn(u32::MAX - 7);
        std::thread::sleep(Duration::from_millis(80));
        let usage = sampler.stop();
        assert!(usage.peak_rss_bytes.is_none());
        assert!(usage.note.unwrap().contains("no /proc"));
    }

    #[test]
    fn stat_parser_survives_hostile_comm_names() {
        // field layout after the last ')' — synthetic line, utime=7 stime=11.
        let line = "1234 (a b) c) R 1 1 1 0 -1 4194304 100 0 0 0 7 11 0 0 20 0 1 0 100 1000 200";
        let after = &line[line.rfind(')').unwrap() + 1..];
        let fields: Vec<&str> = after.split_whitespace().collect();
        assert_eq!(fields[11], "7");
        assert_eq!(fields[12], "11");
    }
}
