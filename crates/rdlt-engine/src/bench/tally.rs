//! What the bench's tallying sinks staged, kept by the name their configuration gives, for a
//! workload to take after its run.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use parking_lot::Mutex;

static TALLIES: LazyLock<Mutex<BTreeMap<String, Arc<Mutex<Staged>>>>> =
    LazyLock::new(Mutex::default);

/// What the sinks tallying under one name staged: rows by table, and the commits that published
/// them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Staged {
    /// Rows staged, by the name of their table.
    pub rows: BTreeMap<String, u64>,
    /// Commits made.
    pub commits: u64,
}

impl Staged {
    /// Notes `rows` rows staged in the table `table`.
    pub(super) fn stage(&mut self, table: &str, rows: u64) {
        match self.rows.get_mut(table) {
            Some(staged) => *staged += rows,
            None => {
                self.rows.insert(table.to_owned(), rows);
            }
        }
    }
}

/// The tally `name`, made empty where there is none.
pub(super) fn tally(name: &str) -> Arc<Mutex<Staged>> {
    Arc::clone(TALLIES.lock().entry(name.to_owned()).or_default())
}

/// What the sinks tallying under `name` have staged since it was last taken, taken.
pub fn staged(name: &str) -> Staged {
    std::mem::take(&mut *tally(name).lock())
}
