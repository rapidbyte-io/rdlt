//! Limits on what a spawned connector's output costs its host.

use std::time::Duration;

/// Bytes: the end of a connector's standard error kept for the errors of its transport.
pub const TAIL_BYTES: usize = 8 * 1024;

/// Bytes: bounds a connector's last words as an error shows them, the end of its standard
/// error with every character a reader could be deceived by escaped.
pub const LAST_WORDS_BYTES: usize = 3 * 1024;

/// Bytes: bounds one line of a connector's output as the log is given it; the rest of a
/// longer line is dropped.
pub const OUTPUT_LINE_BYTES: usize = 1024;

/// Lines: how many lines of one stream of a connector's output the log is given at once.
pub const OUTPUT_LINES_BURST: u64 = 256;

/// Lines: how many lines of one stream of a connector's output the log is given each second,
/// once [`OUTPUT_LINES_BURST`] are spent; the lines beyond are counted and dropped.
pub const OUTPUT_LINES_PER_SECOND: u64 = 32;

/// Bytes: how much of one stream of a connector's output is read at once.
pub const OUTPUT_BYTES_BURST: u64 = 4 * 1024 * 1024;

/// Bytes: how much of one stream of a connector's output is read each second, once
/// [`OUTPUT_BYTES_BURST`] are spent; a connector that writes more waits to write.
pub const OUTPUT_BYTES_PER_SECOND: u64 = 1024 * 1024;

/// How long the errors of a lost connector wait for its standard error to close.
pub const LAST_WORDS: Duration = Duration::from_secs(1);

/// References: bounds the secret references one configuration holds.
pub const SECRET_REFERENCES: usize = 1024;

/// Bytes: bounds one resolved secret.
pub const SECRET_BYTES: u64 = 64 * 1024;

/// Bytes: bounds the name of a secret reference, a path among them.
pub const SECRET_NAME_BYTES: usize = 4096;

#[cfg(test)]
mod tests;
