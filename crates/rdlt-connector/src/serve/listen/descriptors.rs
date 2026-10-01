//! The file descriptors a listening connector needs, held against what its process may open.

#[cfg(test)]
mod tests;

use nix::sys::resource::{Resource, getrlimit, setrlimit};

use crate::limits::{ListenLimits, TooFewDescriptors};

/// Makes sure this process may open the file descriptors `limits` need, raising its soft limit
/// as far as they need where the hard limit allows.
///
/// # Errors
///
/// [`TooFewDescriptors`] when the process may not open that many, or its limits cannot be read.
pub(super) fn reserve(limits: &ListenLimits) -> Result<(), TooFewDescriptors> {
    let needed = limits.descriptors();
    let Ok((soft, hard)) = getrlimit(Resource::RLIMIT_NOFILE) else {
        return Err(TooFewDescriptors { limit: 0, needed });
    };
    if limits.admit_descriptors(soft).is_ok() {
        return Ok(());
    }
    limits.admit_descriptors(hard)?;
    // No further than needed: some platforms report a hard limit they refuse to grant.
    setrlimit(Resource::RLIMIT_NOFILE, needed, hard).map_err(|_| TooFewDescriptors {
        limit: soft,
        needed,
    })
}
