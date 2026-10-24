//! A table's rows by their lineage ids: no two share one, but a merge stream's roots, which share
//! their key's.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

/// The identity of each row of one table, by its lineage id.
pub(super) struct Ids {
    rows: BTreeMap<String, String>,
    /// Whether rows of one key share an id, as a merge stream's roots do.
    keyed: bool,
}

impl Ids {
    /// No row yet, of a table whose rows of one key share an id where `keyed`.
    pub(super) fn new(keyed: bool) -> Self {
        Self {
            rows: BTreeMap::new(),
            keyed,
        }
    }

    /// Records the row `ident`, whose id is `id`; the finding where another row has it.
    pub(super) fn record(&mut self, id: String, ident: &str) -> Option<String> {
        let other = self.rows.insert(id, ident.to_owned())?;
        (!self.keyed).then(|| format!("rows {other} and {ident} share an id"))
    }

    /// Each row's id and identity.
    pub(super) fn into_rows(self) -> impl Iterator<Item = (String, String)> {
        self.rows.into_iter()
    }
}
