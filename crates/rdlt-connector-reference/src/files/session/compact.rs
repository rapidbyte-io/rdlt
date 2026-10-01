//! Merging the files at the end of a table's list into one, so the list stays short however
//! small the batches a table is written in.
//!
//! A file is merged with those after it while it holds at most twice their rows, so the files
//! of a list at least halve in rows from each to the next: their number grows with the
//! logarithm of the table's rows, and each row is rewritten as often. Row order is kept.

#[cfg(test)]
mod tests;

use arrow_schema::{DataType, SchemaRef};
use rdlt_connector::prelude::*;
use rdlt_connector::{CommitMeta, GenerationId};

use super::Location;
use crate::files::format::plain::plain;
use crate::files::format::{FileFormat, Reader, Writer};
use crate::files::io;
use crate::files::manifest::{self, Listed};
use crate::limits::COMPACT_BYTES;

/// Merges the files at the end of `files`, the list of the table `name` or of its `generation`,
/// into one file of commit `meta` where the list's rule asks for it; `created` gains the file.
///
/// A merge that fails changes nothing and leaves nothing: the list stays as it is, the commit
/// goes on, and the next commit tries again.
pub(super) fn compact(
    location: &Location,
    name: &str,
    generation: Option<GenerationId>,
    files: &mut Vec<Listed>,
    meta: &CommitMeta,
    created: &mut Vec<String>,
) {
    let Some(from) = tail(location, files) else {
        return;
    };
    let written = created.len();
    if let Ok(merged) = merged(location, name, generation, &files[from..], meta, created) {
        files.truncate(from);
        files.push(merged);
    } else {
        // Nothing of the try stays: neither a part of its file nor the directories made for it.
        super::commit::remove(&location.dir, created[written..].iter());
        created.truncate(written);
    }
}

/// Writes the rows of `files`, in order, as one file of commit `meta` staged for the table `name`
/// or its `generation`; `created` gains the file's path before it is written.
fn merged(
    location: &Location,
    name: &str,
    generation: Option<GenerationId>,
    files: &[Listed],
    meta: &CommitMeta,
    created: &mut Vec<String>,
) -> Result<Listed> {
    // Every file to merge is opened before anything is written: one that cannot be read costs
    // the try nothing.
    let mut opened = Vec::with_capacity(files.len());
    for merging in files {
        let (parent, name) = manifest::located(&location.dir, &merging.path)?;
        let reached = parent.at(name);
        let file = parent.file(name).map_err(io::listed("opening", &reached))?;
        opened.push((file, reached));
    }
    let schema = schema_of(location, &files[0])?
        .unwrap_or_else(|| SchemaRef::new(arrow_schema::Schema::empty()));
    let segment = Location::written_by("compacted", meta)?;
    let (names, file) = location.staged(&segment, name, generation, 0);
    let path = format!("{}/{file}", names.join("/"));
    created.push(path.clone());
    let dir = location.staging(&names)?;
    let mut writer = Writer::create(location.format, &dir, &file, &schema)?;
    for (file, reached) in opened {
        writer.append(file, reached)?;
    }
    let written = writer.finish()?;
    Ok(Listed {
        path,
        rows: written.rows,
        bytes: written.bytes,
    })
}

/// Where the files to merge start in `files`, if more than the last is to be merged.
///
/// Only files of the session's format merge, an Arrow file only with files of its schema, a
/// dictionary column counting as the values it stands for, and never into a file larger than
/// [`COMPACT_BYTES`]. A file whose schema cannot be read merges with none.
fn tail(location: &Location, files: &[Listed]) -> Option<usize> {
    let last = files.len().checked_sub(1)?;
    let format = location.format;
    let formatted = |file: &Listed| FileFormat::named(&file.path) == Some(format);
    if !formatted(&files[last]) {
        return None;
    }
    let (mut from, mut rows, mut bytes) = (last, files[last].rows, files[last].bytes);
    let mut schema = None;
    while from > 0 {
        let before = &files[from - 1];
        let sized = before.rows <= rows.saturating_mul(2)
            && before.bytes.saturating_add(bytes) <= COMPACT_BYTES;
        if !sized || !formatted(before) {
            break;
        }
        if format == FileFormat::Arrow {
            if schema.is_none() {
                schema = schema_of(location, &files[from]).ok().flatten();
            }
            let same = schema_of(location, before).ok().flatten() == schema;
            if !same || schema.as_ref().is_none_or(|schema| keyed(schema)) {
                break;
            }
        }
        rows = rows.saturating_add(before.rows);
        bytes = bytes.saturating_add(before.bytes);
        from -= 1;
    }
    (from < last).then_some(from)
}

/// The schema the Arrow file `file` lists holds its batches in, its dictionaries as the values
/// they stand for, which is what a file merged from it holds; none for JSON lines.
fn schema_of(location: &Location, file: &Listed) -> Result<Option<SchemaRef>> {
    let format = manifest::format_of(&location.dir, &file.path)?;
    if format != FileFormat::Arrow {
        return Ok(None);
    }
    let (parent, name) = manifest::located(&location.dir, &file.path)?;
    let empty = SchemaRef::new(arrow_schema::Schema::empty());
    let reader = Reader::open(format, &parent, name, &empty)?;
    Ok(reader.schema().map(|schema| plain(schema)))
}

/// Whether a column of `schema` is dictionary-encoded at any depth: one file holds one
/// dictionary per such column, which files written apart do not share, so a dictionary that
/// cannot be written as its values keeps its file from merging.
fn keyed(schema: &arrow_schema::Schema) -> bool {
    fn holds(data_type: &DataType) -> bool {
        match data_type {
            DataType::Dictionary(..) => true,
            DataType::List(item)
            | DataType::LargeList(item)
            | DataType::ListView(item)
            | DataType::LargeListView(item)
            | DataType::FixedSizeList(item, _)
            | DataType::Map(item, _) => holds(item.data_type()),
            DataType::Struct(fields) => fields.iter().any(|field| holds(field.data_type())),
            DataType::Union(fields, _) => fields.iter().any(|(_, field)| holds(field.data_type())),
            DataType::RunEndEncoded(_, values) => holds(values.data_type()),
            _ => false,
        }
    }
    schema.fields().iter().any(|field| holds(field.data_type()))
}
