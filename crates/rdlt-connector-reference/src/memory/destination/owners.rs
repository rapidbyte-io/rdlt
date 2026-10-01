//! Who owns the store's tables: claims, and the owner check every table a commit touches passes.

use rdlt_connector::prelude::*;
use rdlt_connector::{Epoch, PipelineId, TablePath, TableRef};

use super::Store;
use super::table::Table;

impl Store {
    /// The table `table` refers to, claimed for `pipeline`'s session at `epoch` where no pipeline
    /// owns it yet, recording its name for the pipeline's path and how it merges; another
    /// pipeline's table is refused, and so is a claim by a session a newer one fenced, which a
    /// drop may have released the table from.
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
        self.names.insert(
            (pipeline.clone(), table.path.clone()),
            table.name.to_string(),
        );
        Ok(entry)
    }

    /// The table `name`, which `pipeline` owns; another pipeline's is refused as `table_owned`,
    /// and one no pipeline owns, as a table dropped since, as `table_unowned`.
    pub(super) fn owned(&self, pipeline: &PipelineId, name: &str) -> Result<&Table> {
        match self.tables.get(name) {
            Some(table) if table.owner.as_ref() == Some(pipeline) => Ok(table),
            Some(Table {
                owner: Some(owner), ..
            }) => Err(ConnectorError::table_owned(name, owner.as_str())),
            _ => Err(ConnectorError::config(format!(
                "table {name} belongs to no pipeline, so no pipeline's session changes it"
            ))
            .with_code("table_unowned")),
        }
    }

    /// The name of the table `pipeline` registered for `path`, where it still exists.
    pub(super) fn named(&self, pipeline: &PipelineId, path: &TablePath) -> Option<&str> {
        self.names
            .get(&(pipeline.clone(), path.clone()))
            .map(String::as_str)
            .filter(|name| self.tables.contains_key(*name))
    }

    /// Refuses `meta` where a table whose generation it swaps in, or which it drops, is not
    /// `pipeline`'s; a table that does not exist is neither swapped nor dropped.
    pub(super) fn owns(&self, pipeline: &PipelineId, meta: &CommitMeta) -> Result<()> {
        let swapped = meta
            .finish_generations
            .iter()
            .filter_map(|(path, _)| self.named(pipeline, path));
        let dropped = meta
            .drop_tables
            .iter()
            .map(|dropped| &*dropped.name)
            .filter(|name| self.tables.contains_key(*name));
        for name in swapped.chain(dropped) {
            self.owned(pipeline, name)?;
        }
        Ok(())
    }
}
