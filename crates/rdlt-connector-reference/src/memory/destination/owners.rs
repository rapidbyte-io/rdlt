//! Who owns the store's tables: claims, and the commits that swap or drop them.

use rdlt_connector::prelude::*;
use rdlt_connector::{Epoch, PipelineId, TableRef};

use super::Store;
use super::table::Table;

impl Store {
    /// The table `table` refers to, claimed for `pipeline`'s session at `epoch` where no pipeline
    /// owns it yet, recording its name for its path and how it merges; another pipeline's table
    /// is refused, and so is a claim by a session a newer one fenced, which a drop may have
    /// released the table from.
    pub(super) fn table(
        &mut self,
        pipeline: &PipelineId,
        epoch: Epoch,
        table: &TableRef,
    ) -> Result<&mut Table> {
        let unclaimed = self
            .tables
            .get(&*table.name)
            .is_none_or(|table| table.owner.is_none());
        let current = self.pipelines.get(pipeline).map(|store| store.epoch);
        if unclaimed && current != Some(epoch) {
            return Err(ConnectorError::fenced(format!(
                "pipeline {pipeline} has a session newer than epoch {epoch}"
            )));
        }
        let entry = self.tables.entry(table.name.to_string()).or_default();
        let owner = entry.owner.get_or_insert_with(|| pipeline.clone());
        if owner != pipeline {
            return Err(ConnectorError::table_owned(&table.name, owner.as_str()));
        }
        entry.merge.clone_from(&table.merge);
        self.names
            .insert(table.path.clone(), table.name.to_string());
        Ok(entry)
    }

    /// Refuses `meta` where a table whose generation it swaps in, or which it drops, belongs to
    /// another pipeline.
    pub(super) fn owned(&self, pipeline: &PipelineId, meta: &CommitMeta) -> Result<()> {
        let swapped = meta
            .finish_generations
            .iter()
            .filter_map(|(path, _)| self.names.get(path).map(String::as_str));
        let dropped = meta.drop_tables.iter().map(|dropped| &*dropped.name);
        for name in swapped.chain(dropped) {
            if let Some(owner) = self.tables.get(name).and_then(|table| table.owner.as_ref())
                && owner != pipeline
            {
                return Err(ConnectorError::table_owned(name, owner.as_str()));
            }
        }
        Ok(())
    }
}
