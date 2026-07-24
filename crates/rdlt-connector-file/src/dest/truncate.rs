//! Replace truncation: clear a table's data files before republishing. Two
//! ownership rules, ONE per-backend implementation (they run over the shared
//! `Location::keys_of_table` listing, so local and S3 can never diverge):
//!
//! - The FROZEN plain-parquet rule (local parquet, no partitioning): TOP-LEVEL
//!   `*.parquet` of ANY name, matching the exact pre-015 behavior.
//! - The owned-parts rule (everything else): only `part-*.<ext>` files this
//!   destination writes — top level, plus one level into partition dirs when
//!   partitioning is declared. User files are never ours to delete.

use rdlt_connector::DestinationError;

use crate::location::Location;

/// The FROZEN rule: a top-level `*.parquet` of any name (pre-015 compatibility).
fn frozen_owns(tail: &str) -> bool {
    !tail.contains('/') && tail.ends_with(".parquet")
}

/// The owned-parts rule: `part-*.<ext>` at depth 1, or `<partition>/part-*.<ext>`
/// at depth 2 when partitioning is declared.
fn owns_tail(tail: &str, ext: &str, partitioned: bool) -> bool {
    let is_part = |file: &str| file.starts_with("part-") && file.ends_with(&format!(".{ext}"));
    let segments: Vec<&str> = tail.split('/').collect();
    match segments.as_slice() {
        [file] => is_part(file),
        [_partition, file] if partitioned => is_part(file),
        _ => false,
    }
}

/// Truncate one table's owned files. `frozen_plain_parquet` selects the frozen
/// rule; otherwise the owned-parts rule (parameterized by extension and whether
/// partitioning is declared).
pub(crate) async fn truncate_table(
    location: &Location,
    table: &str,
    ext: &str,
    partitioned: bool,
    frozen_plain_parquet: bool,
) -> Result<(), DestinationError> {
    for tail in location.keys_of_table(table).await? {
        let owned = if frozen_plain_parquet {
            frozen_owns(&tail)
        } else {
            owns_tail(&tail, ext, partitioned)
        };
        if owned {
            location.delete_table_file(table, &tail).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_rule_takes_any_top_level_parquet_but_nothing_nested() {
        assert!(frozen_owns("stray.parquet"));
        assert!(frozen_owns("part-load-1-0.parquet"));
        assert!(!frozen_owns("user-subdir/data.parquet"));
        assert!(!frozen_owns("user.jsonl"));
    }

    #[test]
    fn owned_rule_takes_only_part_files_at_the_right_depth() {
        assert!(owns_tail("part-load-1-0.jsonl", "jsonl", false));
        assert!(!owns_tail("user.jsonl", "jsonl", false));
        // Partition depth is honored only when partitioning is declared.
        assert!(owns_tail("d1/part-load-1-0.jsonl", "jsonl", true));
        assert!(!owns_tail("d1/part-load-1-0.jsonl", "jsonl", false));
        // Wrong extension is never ours.
        assert!(!owns_tail("part-load-1-0.parquet", "jsonl", false));
        // Deeper than one partition level is never ours.
        assert!(!owns_tail("a/b/part-load-1-0.jsonl", "jsonl", true));
    }
}
