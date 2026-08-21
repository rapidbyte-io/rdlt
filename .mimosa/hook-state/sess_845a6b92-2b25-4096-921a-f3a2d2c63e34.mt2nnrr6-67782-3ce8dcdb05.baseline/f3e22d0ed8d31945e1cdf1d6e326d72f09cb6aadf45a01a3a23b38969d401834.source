//! Safe-Rust process resource sampler.
//!
//! `getrusage`/`wait4` are libc FFI (unsafe) — rejected. Instead a thread polls
//! procfs. The sampled unit is the child's whole PROCESS TREE, not just the
//! spawned pid: `command` cells run through wrappers (`sh -c`, `cargo run`),
//! and the wrapper's own /proc numbers would silently describe the wrapper
//! instead of the workload. Per poll the tree is rebuilt from /proc ppid links;
//! peak RSS is max(root VmHWM — kernel-exact for the root — , max sampled
//! tree RSS sum); CPU sums the members' utime+stime plus the root's
//! cutime+cstime (reaped children land there, so finished workloads keep
//! their time). Tree spikes between polls can be missed and the final CPU
//! sample undercounts by at most one interval — recorded honestly in the
//! note, never fabricated.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const POLL: Duration = Duration::from_millis(50);

/// One time-series point (raw; written to results/raw/, never committed).
#[derive(Debug, Clone, serde::Serialize)]
pub struct SamplePoint {
    pub at_ms: u64,
    /// Instantaneous RSS sum across the process tree.
    pub rss_kb: u64,
    pub cpu_ms: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ResourceUsage {
    /// max(root VmHWM, max sampled tree-RSS sum), bytes. None: nothing
    /// sampled before exit, or /proc unreadable — reported null with reason.
    pub peak_rss_bytes: Option<u64>,
    /// Tree user+sys CPU time (ms), running max across polls.
    pub cpu_ms: Option<u64>,
    pub series: Vec<SamplePoint>,
    /// Why a metric is None (or how it was sampled), when it is.
    pub note: Option<String>,
}

fn getconf(name: &str, fallback: u64) -> u64 {
    std::process::Command::new("getconf")
        .arg(name)
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(fallback)
}

fn read_vmhwm_kb(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status.lines().find_map(|l| {
        l.strip_prefix("VmHWM:")
            .and_then(|rest| rest.trim().strip_suffix("kB"))
            .and_then(|n| n.trim().parse().ok())
    })
}

/// The /proc/<pid>/stat fields the tree walk needs, indexed after the last
/// `)` (comm can contain spaces/parens).
#[derive(Debug, Clone, Copy)]
struct StatLine {
    ppid: u32,
    utime: u64,
    stime: u64,
    cutime: u64,
    cstime: u64,
    rss_pages: u64,
}

fn parse_stat(stat: &str) -> Option<StatLine> {
    let after = &stat[stat.rfind(')')? + 1..];
    let fields: Vec<&str> = after.split_whitespace().collect();
    // after ')' : state=0, ppid=1, …, utime=11, stime=12, cutime=13,
    // cstime=14, …, rss(pages)=21.
    Some(StatLine {
        ppid: fields.get(1)?.parse().ok()?,
        utime: fields.get(11)?.parse().ok()?,
        stime: fields.get(12)?.parse().ok()?,
        cutime: fields.get(13)?.parse().ok()?,
        cstime: fields.get(14)?.parse().ok()?,
        rss_pages: fields.get(21)?.parse().ok()?,
    })
}

fn read_stat(pid: u32) -> Option<StatLine> {
    parse_stat(&std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?)
}

/// One snapshot of the root's process tree: (members' stat lines, root line).
fn scan_tree(root: u32) -> Option<(Vec<StatLine>, StatLine)> {
    let root_line = read_stat(root)?;
    let mut all: BTreeMap<u32, StatLine> = BTreeMap::new();
    if let Ok(entries) = std::fs::read_dir("/proc") {
        for entry in entries.flatten() {
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|n| n.parse::<u32>().ok())
            else {
                continue;
            };
            if let Some(line) = read_stat(pid) {
                all.insert(pid, line);
            }
        }
    }
    // Membership: walk ppid chains up to the root (chains are short; /proc is
    // a snapshot so cycles cannot occur, but cap the walk defensively).
    let mut members = vec![root_line];
    for (&pid, line) in &all {
        if pid == root {
            continue;
        }
        let mut cursor = line.ppid;
        for _ in 0..64 {
            if cursor == root {
                members.push(*line);
                break;
            }
            match all.get(&cursor) {
                Some(parent) if parent.ppid != cursor => cursor = parent.ppid,
                _ => break,
            }
        }
    }
    Some((members, root_line))
}

/// Samples a child's process tree until [`Sampler::stop`]. Sub-interval
/// processes report null + note.
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
            let tck = getconf("CLK_TCK", 100);
            let page = getconf("PAGESIZE", 4096);
            let started = Instant::now();
            let mut usage = ResourceUsage::default();
            let mut peak_tree_rss = 0u64;
            let mut peak_cpu_ms = 0u64;
            let mut root_vmhwm_kb = 0u64;
            loop {
                if let Some((members, root_line)) = scan_tree(pid) {
                    let rss: u64 = members.iter().map(|m| m.rss_pages * page).sum();
                    let live_ticks: u64 = members.iter().map(|m| m.utime + m.stime).sum();
                    let reaped_ticks = root_line.cutime + root_line.cstime;
                    let cpu_ms = (live_ticks + reaped_ticks) * 1000 / tck;
                    peak_tree_rss = peak_tree_rss.max(rss);
                    peak_cpu_ms = peak_cpu_ms.max(cpu_ms);
                    if let Some(kb) = read_vmhwm_kb(pid) {
                        root_vmhwm_kb = root_vmhwm_kb.max(kb);
                    }
                    usage.peak_rss_bytes = Some(peak_tree_rss.max(root_vmhwm_kb * 1024));
                    usage.cpu_ms = Some(peak_cpu_ms);
                    usage.series.push(SamplePoint {
                        at_ms: started.elapsed().as_millis() as u64,
                        rss_kb: rss / 1024,
                        cpu_ms,
                    });
                }
                // else: process gone (or not yet visible) — keep last values.
                if stop2.load(Ordering::Relaxed) {
                    break;
                }
                std::thread::sleep(POLL);
            }
            usage.note = Some(if usage.series.is_empty() {
                format!("no /proc/{pid} sample succeeded (process exited before first poll?)")
            } else {
                format!(
                    "process TREE sampled at {}ms (root VmHWM exact; tree spikes between polls can be missed; cpu undercounts up to one interval)",
                    POLL.as_millis()
                )
            });
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
            .args([
                "-c",
                "import time; x = bytearray(50_000_000); time.sleep(0.4)",
            ])
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
    fn wrapper_children_are_charged_to_the_tree() {
        // The workload hides behind an `sh -c` wrapper: the tree walk must
        // attribute the grandchild's 50MB, not sh's ~2MB.
        let mut child = std::process::Command::new("sh")
            .args([
                "-c",
                "python3 -c 'import time; x = bytearray(50_000_000); time.sleep(0.5)'",
            ])
            .stdout(std::process::Stdio::null())
            .spawn()
            .expect("spawn");
        let sampler = Sampler::spawn(child.id());
        child.wait().unwrap();
        let usage = sampler.stop();
        let peak = usage.peak_rss_bytes.expect("tree rss sampled");
        assert!(
            peak > 10 * 1024 * 1024,
            "tree peak {peak} should exceed 10MB (wrapper alone is ~2MB)"
        );
        assert!(usage.note.unwrap().contains("TREE"));
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
        // synthetic line: utime=7 stime=11 cutime=1 cstime=2 rss(pages)=200.
        let line = "1234 (a b) c) R 1 1 1 0 -1 4194304 100 0 0 0 7 11 1 2 20 0 1 0 100 1000 200 18446744073709551615";
        let parsed = parse_stat(line).unwrap();
        assert_eq!(parsed.ppid, 1);
        assert_eq!(parsed.utime, 7);
        assert_eq!(parsed.stime, 11);
        assert_eq!(parsed.cutime, 1);
        assert_eq!(parsed.cstime, 2);
        assert_eq!(parsed.rss_pages, 200);
    }
}
