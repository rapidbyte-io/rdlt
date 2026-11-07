//! Every limit the facade enforces.

/// Events: how far each subscriber of a run may fall behind before it misses some.
pub const EVENT_BUFFER: usize = 4_096;

/// Events: the most a run keeps for its subscribers, however many a pipeline asks for.
pub const EVENT_BUFFER_MAX: usize = 65_536;
