//! Every limit the facade enforces.

/// Events: how far each subscriber of a run may fall behind before it misses some.
pub const EVENT_BUFFER: usize = 4_096;
