//! What a world's connectors were told by their configurations, which the faults they raise quote,
//! as a connector that writes its configuration into its errors does.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::Deserialize;

/// How many ways a fault quotes what its connector was told, each in turn.
pub(crate) const FORMS: u64 = 4;

/// The secrets a connector's configuration referred to, as its host resolved them.
#[derive(Clone, Default, Deserialize, JsonSchema)]
pub struct Told {
    /// A secret that ends in whitespace.
    pub canary: String,
    /// A secret of several lines, as a key read from a file holds.
    pub lines: String,
    /// A secret shaped as a version, as an access token may be.
    pub token: String,
}

impl fmt::Debug for Told {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Told(***)")
    }
}

/// What a world's connectors last heard, and how many faults have quoted it.
#[derive(Default)]
pub(crate) struct Heard {
    told: Mutex<Option<Told>>,
    quoted: AtomicU64,
}

impl fmt::Debug for Heard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Heard(***)")
    }
}

impl Heard {
    /// Keeps `told`, where a connector was told anything.
    pub(crate) fn hear(&self, told: Option<&Told>) {
        if let Some(told) = told {
            *self.told.lock() = Some(told.clone());
        }
    }

    /// `message` and a cause, quoting what was heard in the next of the [`FORMS`]; `message` as
    /// it is where nothing was heard.
    pub(crate) fn quoted(&self, message: String) -> (String, Option<String>) {
        let told = self.told.lock().clone();
        match told {
            Some(told) => quoting(&told, self.quoted.fetch_add(1, Ordering::SeqCst), message),
            None => (message, None),
        }
    }
}

/// `message` quoting `told` in form `turn` of [`FORMS`]: the secret that ends in whitespace at the
/// message's end; a long line of the secret of several lines alone; that secret whole; and the
/// first as the message's cause.
pub(crate) fn quoting(told: &Told, turn: u64, message: String) -> (String, Option<String>) {
    match turn % FORMS {
        0 => (format!("{message}; told {}", told.canary), None),
        1 => {
            let line = told.lines.lines().nth(2).unwrap_or_default();
            (format!("{message}; told {line} of a key"), None)
        }
        2 => (format!("{message}; told {}", told.lines), None),
        _ => (message, Some(format!("told {} as a cause", told.canary))),
    }
}
