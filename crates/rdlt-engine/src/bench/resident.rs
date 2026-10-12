//! What a run holds resident against what its budget bounds: the peak of the process's resident
//! memory, reset once its inputs are made, and the limit ADR 0039's bound gives it.

#[cfg(test)]
mod tests;

/// Bytes of stack each thread is given room for in the limit.
const STACK: u64 = 2 << 20;

/// A run's budget, the push it holds before admitting it, and its threads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Resident {
    /// Bytes: the run's memory budget.
    pub memory: u64,
    /// Bytes: the largest push or frame a partition holds before it is admitted.
    pub push: u64,
    /// The process's threads: its runtime workers, its compute threads and its main thread.
    pub threads: u64,
}

impl Resident {
    /// Bytes: the most a run may raise the process's resident memory by: the budget, a fifth
    /// of it and 32 MiB (ADR 0039), the push it holds unadmitted, and a stack a thread.
    pub fn limit(&self) -> u64 {
        self.memory
            .saturating_add(self.memory / 5)
            .saturating_add(32 << 20)
            .saturating_add(self.push)
            .saturating_add(self.threads.saturating_mul(STACK))
    }
}

/// Kibibytes: the value of the field `field` of a `/proc/<pid>/status` text.
pub fn resident_kib(status: &str, field: &str) -> Option<u64> {
    status
        .lines()
        .find_map(|line| line.strip_prefix(field)?.strip_prefix(':'))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|kib| kib.parse().ok())
}

/// Resets the process's resident peak to what it holds now (Linux).
///
/// # Errors
///
/// Where the system has no such file, as only Linux has.
pub fn reset_peak() -> std::io::Result<()> {
    std::fs::write("/proc/self/clear_refs", b"5")
}
