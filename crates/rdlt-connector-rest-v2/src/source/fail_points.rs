//! Fail-point registry: the read path's protocol boundaries. The IDs are
//! FROZEN (shared with generation 1's sweeps and any operator tooling);
//! the sweep's registry-vs-sources check verifies every armed site is
//! declared here and every declared name is armed.

pub const FAIL_POINTS: &[&str] = &["rest.request", "rest.decode", "rest.checkpoint"];
