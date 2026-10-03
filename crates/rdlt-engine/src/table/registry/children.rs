//! The child tables of normalized streams: added as their first rows arrive, admitted by the
//! stream's policy, and listed for the commits of merge streams.

use std::sync::Arc;

use rdlt_connector::{
    ChildTable, ColumnPath, RootKey, SchemaVersion, StreamName, TablePath, TableRef,
};

use super::super::model::Model;
use super::Tables;
use crate::error::Error;
use crate::limits::CHILD_TABLES_EXCEEDED;
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
    /// No table can be named after the array's path, a key of the source's being empty, too long
    /// or holding a control character: the rows fail the stream.
    Unnamable,
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
        let parent = self.resolver(root);
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
        let owner = path.first().map_or_else(
            || Err(Error::internal("a child table's path is empty")),
            |column| Ok(ColumnPath::from(column.as_ref())),
        )?;
        let recorded = self.committed.get(&table_path);
        if recorded.is_none() {
            self.admit_another(root, &parent.stream)?;
        }
        let resolver = parent.child(root_key, owner);
        let model = Model::from_state(recorded)?;
        let table = TableRef {
            name: self.name(&table_path, &resolver.naming)?,
            path: table_path,
            version: SchemaVersion(model.version),
            generation: base.generation,
            merge: None,
        };
        let index = self.add(resolver, &table, model)?;
        self.create_generation(index).await?;
        self.children.lock().insert(key, index);
        Ok(index)
    }

    /// Admits a child table state does not record below the table `root`, of `stream`, if the
    /// table has fewer than its limit; recorded ones are added first, so they count.
    fn admit_another(&self, root: usize, stream: &StreamName) -> Result<(), Error> {
        let below = self
            .children
            .lock()
            .range((root, Vec::new())..)
            .take_while(|((parent, _), _)| *parent == root)
            .count();
        let limit = self.children_limit;
        if below < limit {
            return Ok(());
        }
        Err(Error::schema(format!(
            "stream {stream}: a child table more would pass the limit of {limit}"
        ))
        .with_code(CHILD_TABLES_EXCEEDED)
        .with_stream(stream))
    }

    /// Records `paths`, the arrays below the table `root` that its stream's declared schema
    /// holds: their rows change no schema.
    pub(crate) fn declare_children(&self, root: usize, paths: Vec<Vec<Arc<str>>>) {
        self.declared
            .lock()
            .extend(paths.into_iter().map(|path| (root, path)));
    }

    /// What becomes of rows for the child table at `path` below `root`, which may be new.
    ///
    /// A child table that exists, that state records or whose array the stream declares takes
    /// its rows, as does a new one while the stream's table is being created: before a unit that
    /// `existed` says found it created, as a table being created takes every column. After that,
    /// a new child table is a change to the stream's schema, which its policy for the array's
    /// column decides. So is an array no table can be named after, whenever it arrives: a stream
    /// that discards changes drops it, and any other is refused.
    pub(crate) fn admit_child(&self, root: usize, path: &[Arc<str>], existed: bool) -> Admission {
        let key = (root, path.to_vec());
        let base = self.view(root).table.path.clone();
        let child = TablePath::new(base.segments().chain(path.iter().map(AsRef::as_ref)));
        if let Ok(child) = &child
            && (!existed
                || self.children.lock().contains_key(&key)
                || self.declared.lock().contains(&key)
                || self.committed.contains_key(child))
        {
            return Admission::Add;
        }
        // A child table's settings are those of the stream's column whose arrays it holds.
        let column = path.first().map_or_else(
            || ColumnPath::from(""),
            |column| ColumnPath::from(column.as_ref()),
        );
        match self.resolver(root).settings.column(&column).policy {
            SchemaPolicy::Freeze => Admission::Refuse,
            SchemaPolicy::DiscardValue => Admission::Discard,
            SchemaPolicy::DiscardRow => Admission::DiscardParents,
            SchemaPolicy::Evolve if child.is_ok() => Admission::Add,
            SchemaPolicy::Evolve => Admission::Unnamable,
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
