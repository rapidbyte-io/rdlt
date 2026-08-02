//! Listing: turning a stream's path-or-glob into a COMPLETE, sorted
//! file list — or failing, never returning a partial one.
//!
//! One rule governs ambiguity on both location kinds: a path naming an
//! EXISTING file is always literal, even when it contains glob
//! metacharacters — `events[prod].jsonl` is a file, not a character
//! class. The listing is snapshotted once per run; files created after
//! it arrive next run.

use rdlt_connector_sdk::spi::SourceError;

use super::cursor::FileMeta;

/// Resolve a local path-or-glob. A named missing file is an error; an
/// empty glob is an empty stream; a directory is a typed suggestion,
/// never an implicit expansion — which files a stream reads is the
/// operator's declaration, not a guess.
pub(crate) fn local_listing(pattern: &str) -> Result<Vec<FileMeta>, SourceError> {
    let mut matched: Vec<String> = if std::path::Path::new(pattern).is_file() {
        vec![pattern.to_owned()]
    } else if pattern.contains(['*', '?', '[']) {
        let mut paths = Vec::new();
        for entry in glob::glob(pattern)
            .map_err(|e| SourceError::fatal(format!("invalid glob `{pattern}`: {e}")))?
        {
            let path = entry.map_err(|e| {
                SourceError::fatal(format!(
                    "expanding glob `{pattern}`: {e}; refusing to load a partial file list"
                ))
            })?;
            if path.is_file() {
                paths.push(path.to_string_lossy().into_owned());
            }
        }
        paths
    } else if std::path::Path::new(pattern).is_dir() {
        return Err(SourceError::fatal(format!(
            "`{pattern}` is a directory, not a file — name a file, or use a pattern such as \
             `{pattern}/*.jsonl`"
        )));
    } else {
        return Err(SourceError::fatal(format!(
            "file `{pattern}` does not exist"
        )));
    };
    matched.sort();
    matched
        .into_iter()
        .map(|path| {
            let meta = std::fs::metadata(&path)
                .map_err(|e| SourceError::fatal(format!("stat `{path}`: {e}")))?;
            let mtime_ms = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as u64);
            Ok(FileMeta {
                path,
                size_units: meta.len(),
                mtime_ms,
                etag: None,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ambiguity rule: an existing file with metacharacters in its
    /// name is LITERAL, never a character class.
    #[test]
    fn an_existing_file_with_metacharacters_is_literal() {
        let dir = tempfile::tempdir().expect("dir");
        let tricky = dir.path().join("events[prod].jsonl");
        std::fs::write(&tricky, b"{}\n").expect("write");
        let listing = local_listing(tricky.to_str().expect("utf8")).expect("lists");
        assert_eq!(listing.len(), 1);
        assert_eq!(listing[0].size_units, 3);
        assert!(listing[0].mtime_ms.is_some() && listing[0].etag.is_none());
    }

    /// Globs list only FILES, sorted; an empty glob is an empty
    /// stream, not an error.
    #[test]
    fn globs_list_only_files_sorted_and_empty_is_empty() {
        let dir = tempfile::tempdir().expect("dir");
        std::fs::write(dir.path().join("b.jsonl"), b"x").expect("write");
        std::fs::write(dir.path().join("a.jsonl"), b"x").expect("write");
        std::fs::create_dir(dir.path().join("c.jsonl")).expect("dir entry");
        let pattern = dir.path().join("*.jsonl");
        let listing = local_listing(pattern.to_str().expect("utf8")).expect("lists");
        let names: Vec<&str> = listing
            .iter()
            .map(|m| m.path.rsplit('/').next().expect("name"))
            .collect();
        assert_eq!(names, vec!["a.jsonl", "b.jsonl"], "sorted, files only");

        let empty = dir.path().join("*.csv");
        assert!(
            local_listing(empty.to_str().expect("utf8"))
                .expect("empty ok")
                .is_empty()
        );
    }

    /// The refusals: a directory suggests the pattern; a named missing
    /// file is an error.
    #[test]
    fn directories_and_missing_files_refuse_with_the_frozen_spellings() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().to_str().expect("utf8").to_owned();
        let err = local_listing(&path).expect_err("directory refused");
        assert!(
            format!("{err}").contains(&format!(
                "`{path}` is a directory, not a file — name a file, or use a pattern such as \
                 `{path}/*.jsonl`"
            )),
            "{err}"
        );
        let missing = format!("{path}/nope.jsonl");
        let err = local_listing(&missing).expect_err("missing refused");
        assert!(
            format!("{err}").contains(&format!("file `{missing}` does not exist")),
            "{err}"
        );
    }
}
