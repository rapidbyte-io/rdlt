//! Merging the files at the end of a table's list into one, so the list stays short however
//! small the batches a table is written in.
//!
//! The files a commit adds are merged with each other, and a file listed before them with those
//! after it while it holds at most twice their rows. The files of a list then at least halve
//! in rows from each to the next, unless two of them together would be larger than a full file:
//! a table smaller than a full file lists as many files as the logarithm of its rows, and a row
//! is rewritten about as often, since a merge leaves it in a file half as large again. Row
//! order is kept.

#[cfg(test)]
mod tests;

use std::ops::Range;

use arrow_schema::{DataType, SchemaRef};
use rdlt_connector::prelude::*;
use rdlt_connector::{CommitMeta, GenerationId};

use super::Location;
use crate::files::format::plain::plain;
use crate::files::format::{FileFormat, Reader, Writer};
use crate::files::io;
use crate::files::manifest::{self, Listed};
use crate::limits::COMPACT_BYTES;

/// Merges runs of the files at the end of `files`, the list of the table `name` or of its
/// `generation`, each into one file of commit `meta`, where the list's rule asks for it.
///
/// The last `added` files are those the commit adds; `created` gains each file written. A merge
/// that fails changes nothing and leaves nothing: its files stay listed as they are, the commit
/// goes on, and the next commit tries again.
pub(super) fn compact(
    location: &Location,
    name: &str,
    generation: Option<GenerationId>,
    files: &mut Vec<Listed>,
    added: usize,
    meta: &CommitMeta,
    created: &mut Vec<String>,
) {
    // The runs come latest first, so merging one moves no run still to merge.
    for run in runs(location, files, added) {
        let written = created.len();
        let merging = &files[run.clone()];
        if let Ok(merged) = merged(location, name, generation, merging, meta, created) {
            files.splice(run, [merged]);
        } else {
            // Nothing of the try stays: neither a part of its file nor the directories made
            // for it.
            super::commit::remove(&location.dir, created[written..].iter());
            created.truncate(written);
        }
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

/// The runs of `files` to merge, each into one file, the latest first; the last `added` files
/// are those the commit adds.
///
/// The files a commit adds merge with each other whatever rows they hold, since a writer's
/// batches come in any order of sizes; a file listed before them joins the run that reaches it
/// while it holds at most twice the run's rows. Every two files next to each other then either
/// at least halve in rows or are together larger than [`COMPACT_BYTES`].
pub(super) fn runs(location: &Location, files: &[Listed], added: usize) -> Vec<Range<usize>> {
    let fresh = files.len().saturating_sub(added);
    let mut runs = Vec::new();
    let mut end = files.len();
    while end > 0 {
        let start = run(location, files, end, fresh);
        if end - start > 1 {
            runs.push(start..end);
        }
        if start <= fresh {
            break;
        }
        end = start;
    }
    runs
}

/// Where the run of files to merge that ends before `end` starts in `files`, of which those
/// from `fresh` on are the commit's own.
///
/// Only files of the session's format merge, an Arrow file only with files of its schema, a
/// dictionary column counting as the values it stands for, and never into a file larger than
/// [`COMPACT_BYTES`]. A file whose schema cannot be read merges with none.
fn run(location: &Location, files: &[Listed], end: usize, fresh: usize) -> usize {
    let last = end - 1;
    let format = location.format;
    let formatted = |file: &Listed| FileFormat::named(&file.path) == Some(format);
    if !formatted(&files[last]) {
        return last;
    }
    let (mut from, mut rows, mut bytes) = (last, files[last].rows, files[last].bytes);
    let mut schema = None;
    while from > 0 {
        let before = &files[from - 1];
        let joins = from > fresh || before.rows <= rows.saturating_mul(2);
        let sized = before.bytes.saturating_add(bytes) <= COMPACT_BYTES;
        if !joins || !sized || !formatted(before) {
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
    from
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
