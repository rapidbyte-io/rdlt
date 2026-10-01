//! Whether a process group has a member that has not ended.
//!
//! The null signal answers for a group as long as any process is in it. On Linux a process
//! that ended and that nothing has reaped is still in its group, so where nothing reaps
//! orphans, as in a container whose first process is the host, a killed group answers for as
//! long as the host lives. There the members' states are read from `/proc`.

use rustix::process::{Pid, test_kill_process_group};

/// Whether `group` has a member that has not ended.
///
/// The null signal sends nothing: it asks whether a process is in the group. Where it answers
/// for a group and the platform says which of its members have ended, a group of those alone
/// has no living member.
pub(super) fn living(group: Pid) -> bool {
    test_kill_process_group(group).is_ok() && unended(group).unwrap_or(true)
}

/// Whether a member of `group` has not ended, where the platform says: nowhere but Linux.
#[cfg(not(target_os = "linux"))]
fn unended(_group: Pid) -> Option<bool> {
    None
}

/// Whether a member of `group` has not ended, as `/proc` says, when it is this process's
/// namespace's or one above it.
#[cfg(target_os = "linux")]
fn unended(group: Pid) -> Option<bool> {
    let own = std::fs::read_to_string("/proc/self/status").ok()?;
    // An id for each namespace from that which `/proc` shows down to this process's own.
    let depth = ids(&own, "NSpid:")?.count();
    let group = group.as_raw_nonzero().get();
    for entry in std::fs::read_dir("/proc").ok()? {
        let name = entry.ok()?.file_name();
        if !name.as_encoded_bytes().iter().all(u8::is_ascii_digit) {
            continue;
        }
        // A process that is gone by now has no status, and is no member.
        let path = std::path::Path::new("/proc").join(name).join("status");
        let Ok(status) = std::fs::read_to_string(path) else {
            continue;
        };
        if member(&status, depth) == Some((group, true)) {
            return Some(true);
        }
    }
    Some(false)
}

/// The ids on the line of `status` that starts with `field`, one for each namespace from that
/// which `/proc` shows down to the process's own.
#[cfg(target_os = "linux")]
fn ids<'a>(status: &'a str, field: &str) -> Option<impl Iterator<Item = &'a str>> {
    let line = status.lines().find_map(|line| line.strip_prefix(field))?;
    Some(line.split_ascii_whitespace())
}

/// The group of the process whose `status` this is, as the namespace numbers it that is the
/// `depth`th from that which `/proc` shows, and whether the process has not ended; `None` for a
/// process no namespace that deep holds.
#[cfg(target_os = "linux")]
pub(super) fn member(status: &str, depth: usize) -> Option<(i32, bool)> {
    let state = status
        .lines()
        .find_map(|line| line.strip_prefix("State:"))?;
    // A zombie, and a process being released, have ended.
    let ended = matches!(state.trim_start().chars().next(), Some('Z' | 'X') | None);
    let group = ids(status, "NSpgid:")?.nth(depth.checked_sub(1)?)?;
    Some((group.parse().ok()?, !ended))
}
