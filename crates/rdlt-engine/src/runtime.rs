//! Run orchestration, bounded channels, retry driver, cancellation.
//! All `pub(crate)` — the engine is one deep module whose only public surface
//! is `Engine`/`EngineConfig` at the crate root.

mod classify;
mod drain;
mod extract;
pub(crate) mod lock;
mod recover;
pub(crate) mod run;
mod validate;

pub(crate) use classify::classify_dest_error;

/// Secondary message-count bound on a stage channel. The byte budget is the primary
/// backpressure; this hard cap keeps zero-byte items (markers) from queueing without
/// limit when the budget alone would never park them.
///
/// Runtime rule: stage channels are bounded by BYTES, not batch count — peak memory
/// stays capped regardless of row width; a batch-count bound would silently scale RSS
/// with schema size. A slow consumer exhausts the byte budget and the producer parks
/// on it: that *is* the backpressure. The byte-bounded channel itself is the SPI's
/// (`rdlt_connector::channel`) — one implementation serves both the engine's stages
/// and the source-push path.
pub(crate) const STAGE_MSG_CAPACITY: usize = 256;
