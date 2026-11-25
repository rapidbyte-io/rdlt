//! Who owns the store's tables: claims, and the owner check every table a commit touches passes.

use rdlt_connector::prelude::*;
use rdlt_connector::{Epoch, PipelineId, TablePath, TableRef};

use super::Store;
use super::table::Table;

impl Store {
    /// Applies `change` for `pipeline`'s session at `epoch`: only `TableChange::Create` makes a
    /// table and claims it, and any other change of a table no pipeline owns is refused as
    /// `table_unowned`, as [`Store::unclaimed`] refuses.
    ///
    /// A change refused leaves the store as it was: a refused creation claims nothing.
    pub(super) fn change(
        &mut self,
        pipeline: &PipelineId,
        epoch: Epoch,
        change: &TableChange,
    ) -> Result<()> {
        let table = change.table();
        let creates = matches!(change, TableChange::Create { .. });
        if self.unclaimed(pipeline, epoch, &table.name)? && !creates {
            return Err(unowned(&table.name));
        }
        if let Some(entry) = self.tables.get_mut(&*table.name) {
            entry.change(change)?;
        } else {
            let mut fresh = Table {
                owner: Some(pipeline.clone()),
                ..Table::default()
            };
            fresh.change(change)?;
            self.tables.insert(table.name.to_string(), fresh);
        }
        self.register(pipeline, table);
        Ok(())
    }

    /// Readies `pipeline`'s table `table` for a table writer of its session at `epoch`,
    /// recording how it merges and its name for the pipeline's path; a table no pipeline owns is
    /// refused as `table_unowned`, as [`Store::unclaimed`] refuses.
    pub(super) fn writes(
        &mut self,
        pipeline: &PipelineId,
        epoch: Epoch,
        table: &TableRef,
    ) -> Result<()> {
        if self.unclaimed(pipeline, epoch, &table.name)? {
            return Err(unowned(&table.name));
        }
        self.register(pipeline, table);
        Ok(())
    }

    /// Whether no pipeline owns the table `name`: another pipeline's table is refused as
    /// `table_owned`, and one no pipeline owns, to a session a newer one fenced, as fenced,
    /// since a drop may have released the table from that session.
    fn unclaimed(&self, pipeline: &PipelineId, epoch: Epoch, name: &str) -> Result<bool> {
        match self.tables.get(name).and_then(|table| table.owner.as_ref()) {
            Some(owner) if owner == pipeline => Ok(false),
            Some(owner) => Err(ConnectorError::table_owned(name, owner.as_str())),
            None if self.pipelines.get(pipeline).map(|store| store.epoch) != Some(epoch) => {
                Err(ConnectorError::fenced(format!(
                    "pipeline {pipeline} has a session newer than epoch {epoch}"
                )))
            }
            None => Ok(true),
        }
    }

    /// Records how the table `table`, which `pipeline` owns, merges, and its name for the
    /// pipeline's path.
    fn register(&mut self, pipeline: &PipelineId, table: &TableRef) {
        if let Some(entry) = self.tables.get_mut(&*table.name) {
            entry.merge.clone_from(&table.merge);
        }
        self.names.insert(
            (pipeline.clone(), table.path.clone()),
            table.name.to_string(),
        );
    }

    /// The table `name`, which `pipeline` owns; another pipeline's is refused as `table_owned`,
    /// and one no pipeline owns, as a table dropped since, as `table_unowned`.
    pub(super) fn owned(&self, pipeline: &PipelineId, name: &str) -> Result<&Table> {
        match self.tables.get(name) {
            Some(table) if table.owner.as_ref() == Some(pipeline) => Ok(table),
            Some(Table {
                owner: Some(owner), ..
            }) => Err(ConnectorError::table_owned(name, owner.as_str())),
            _ => Err(unowned(name)),
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

/// The error for a table writer or a change of the table `name`, which no pipeline owns.
fn unowned(name: &str) -> ConnectorError {
    ConnectorError::config(format!(
        "table {name} belongs to no pipeline, so no pipeline's session changes it"
    ))
    .with_code("table_unowned")
}
