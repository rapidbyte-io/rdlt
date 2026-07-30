//! WAL resume: single forward scan of the manifest, replay of the uncommitted span,
//! degradation to re-extraction on any damage (slower, never wrong).

mod blocking;
mod replay;
mod scan;

pub(crate) use replay::replay;
pub(crate) use scan::{RecoverySpan, ScanOutcome, scan_off_runtime};
