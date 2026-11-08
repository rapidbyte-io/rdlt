//! Every limit the facade enforces.

use std::time::Duration;

/// How long building a pipeline waits for each thing it asks of something else: a connector's
/// placement, a mask key's secret, the TLS files it names; a host may set its own.
pub const BUILD_WAIT: Duration = Duration::from_mins(10);

/// Events: how far each subscriber of a run may fall behind before it misses some.
pub const EVENT_BUFFER: usize = 4_096;

/// Events: the most a run keeps for its subscribers, however many a pipeline asks for.
pub const EVENT_BUFFER_MAX: usize = 65_536;

/// Bytes: the largest document read.
pub const DOCUMENT_BYTES: usize = 1_048_576;

/// Levels: how deep a document's mappings and lists may nest.
pub const DOCUMENT_DEPTH: usize = 32;

/// Nodes: the most mappings, lists and scalars a document may hold.
pub const DOCUMENT_NODES: usize = 100_000;

/// Bytes: how much of the name of a document's origin its errors show.
pub(crate) const ORIGIN_BYTES: usize = 1_024;

/// Bytes: how much of a key path a document's error shows.
pub(crate) const PATH_BYTES: usize = 256;
