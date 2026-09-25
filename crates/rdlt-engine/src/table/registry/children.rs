//! The child tables of normalized streams: added as their first rows arrive, admitted by the
//! stream's policy, and listed for the commits of merge streams.

use std::sync::Arc;

use rdlt_connector::{ChildTable, ColumnPath, RootKey, SchemaVersion, TablePath, TableRef};

use super::super::model::Model;
use super::Tables;
use crate::error::Error;
use crate::policy::SchemaPolicy;

/// What becomes of rows for a child table that may be new.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Admission {
    /// The table is added, or exists, and takes them.
    Add,
    /// The stream's schema is frozen: the rows fail the stream.
    Refuse,
    /// The stream discards values that would change its schema: the rows are dropped, and
    /// counted as discarded values.
    Discard,
    /// The stream discards rows that would change its schema: the rows are dropped with the
    /// parent rows holding them, and those parents' other descendants.
    DiscardParents,
}

impl Tables {
    /// The child table at `path` below the table `root`, added at its committed schema, with its
    /// replace generation, the first time it is asked for.
    pub(crate) async fn child(&self, root: usize, path: &[Arc<str>]) -> Result<usize, Error> {
        let key = (root, path.to_vec());
        if let Some(index) = self.children.lock().get(&key) {
            return Ok(*index);
        }
        let _adding = self.adding.lock().await;
        if let Some(index) = self.children.lock().get(&key) {
            return Ok(*index);
        }
        let parent = self.slot(root);
        let root_view = self.view(root);
        let base = &root_view.table;
        let table_path = TablePath::new(base.path.segments().chain(path.iter().map(AsRef::as_ref)))
            .map_err(|error| {
                Error::internal(format!("a child table has no valid path: {error}"))
            })?;
        let root_key = match (&root_view.table.merge, &root_view.meta.id) {
            (Some(key), Some(id)) => Some(RootKey {
                table: Arc::clone(&root_view.table.name),
                id: Arc::clone(id),
                seq: Arc::clone(&key.seq),
            }),
            _ => None,
        };
        let resolver = parent.resolver.child(root_key)?;
        let model = Model::from_state(self.committed.get(&table_path))?;
        let table = TableRef {
            name: self.name(&table_path, &resolver.naming)?,
            path: table_path,
            version: SchemaVersion(model.version),
            generation: base.generation,
            merge: None,
        };
        let index = self.add(resolver, &table, model);
        self.create_generation(index).await?;
        self.children.lock().insert(key, index);
        Ok(index)
    }

    /// What becomes of rows for the child table at `path` below `root`, which may be new.
    ///
    /// A child table that exists, or that state records, takes its rows, as does a new one while
    /// the stream's table is being created: before a unit that `existed` says found it created,
    /// as a table being created takes every column. After that, a new child table is a change to
    /// the stream's schema, which its policy for the array's column decides.
    pub(crate) fn admit_child(&self, root: usize, path: &[Arc<str>], existed: bool) -> Admission {
        if self.children.lock().contains_key(&(root, path.to_vec())) || !existed {
            return Admission::Add;
        }
        let base = self.view(root).table.path.clone();
        let child = base.segments().chain(path.iter().map(AsRef::as_ref));
        if TablePath::new(child).is_ok_and(|child| self.committed.contains_key(&child)) {
            return Admission::Add;
        }
        let Ok(column) = ColumnPath::new(path.to_vec()) else {
            return Admission::Add;
        };
        match self.slot(root).resolver.settings.column(&column).policy {
            SchemaPolicy::Freeze => Admission::Refuse,
            SchemaPolicy::DiscardValue => Admission::Discard,
            SchemaPolicy::DiscardRow => Admission::DiscardParents,
            SchemaPolicy::Evolve => Admission::Add,
        }
    }

    /// The paths below `root` of the child tables state records for it.
    pub(crate) fn recorded_children(&self, root: usize) -> Vec<Vec<Arc<str>>> {
        let base = self.view(root).table.path.clone();
        let depth = base.segments().count();
        self.committed
            .keys()
            .filter(|path| {
                path.segments().count() > depth && path.segments().take(depth).eq(base.segments())
            })
            .map(|path| path.segments().skip(depth).map(Arc::from).collect())
            .collect()
    }

    /// Every child table of a merge stream, as commits list them: each follows its root.
    pub(crate) fn child_tables(&self) -> Vec<ChildTable> {
        let slots = self.slots.read().clone();
        slots
            .iter()
            .filter_map(|slot| {
                let view = slot.current.lock();
                let merge = view.table.merge.clone().filter(|key| key.root.is_some())?;
                Some(ChildTable {
                    table: Arc::clone(&view.table.name),
                    merge,
                })
            })
            .collect()
    }

    /// The paths of `root` and of every child table added below it.
    pub(crate) fn family(&self, root: usize) -> Vec<TablePath> {
        let mut paths = vec![self.view(root).table.path.clone()];
        let children: Vec<usize> = self
            .children
            .lock()
            .iter()
            .filter(|((parent, _), _)| *parent == root)
            .map(|(_, index)| *index)
            .collect();
        paths.extend(
            children
                .into_iter()
                .map(|index| self.view(index).table.path.clone()),
        );
        paths
    }
}
