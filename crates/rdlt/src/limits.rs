//! Every limit the facade enforces.

use std::time::Duration;

/// How long building a pipeline waits for each thing it asks of something else: a connector's
/// placement, a mask key's secret, the TLS files it names; a host may set its own.
pub const BUILD_WAIT: Duration = Duration::from_mins(10);

/// Events: how far each subscriber of a run may fall behind before it misses some.
pub const EVENT_BUFFER: usize = 4_096;

/// Events: the most a run keeps for its subscribers, however many a pipeline asks for.
pub const EVENT_BUFFER_MAX: usize = 65_536;
