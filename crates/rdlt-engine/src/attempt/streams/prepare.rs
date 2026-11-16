//! Preparing a stream's table at the destination before anything is read.

use rdlt_connector::TableSchema;

use crate::error::Error;
use crate::normalize::{self, Shape};
use crate::table::{Incoming, Tables};

/// Changes the stream's table at `index` at the destination for its `declared` schema, normalized
/// as `shape` says: fitted to it, its generation created, its meta columns added, and its child
/// tables, declared and recorded, fitted and opened.
pub(super) async fn prepare(
    tables: &Tables,
    index: usize,
    declared: Option<&TableSchema>,
    shape: Option<&Shape>,
) -> Result<(), Error> {
    let mut children = Vec::new();
    if let Some(declared) = declared {
        let incoming = match shape {
            Some(shape) => {
                let made = normalize::declared::children(declared, shape)?;
                children = tables.declared_children(index, made)?;
                normalize::declared::root_columns(declared, shape)?
            }
            None => Incoming::declared(declared.clone()),
        };
        tables.fit(index, &incoming).await?;
    }
    tables.create_generation(index).await?;
    tables.add_meta_columns(index).await?;
    for (path, incoming) in children {
        let child = tables.child(index, &path).await?;
        tables.fit(child, &incoming).await?;
    }
    for child in tables.recorded_children(index) {
        tables.child(index, &child).await?;
    }
    Ok(())
}
