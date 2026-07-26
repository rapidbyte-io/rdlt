//! Replace truncation: clear a table's data files before republishing. Two
//! ownership rules, ONE per-backend implementation (they run over the shared
//! `Location::keys_of_table` listing, so local and S3 can never diverge):
//!
//! - The owned-parts rule (ALWAYS applied): files whose names have the exact
//!   shape this destination writes, `part-<load>-<seq>-<index>.<ext>` for any
//!   `<ext>` it can write, at top level or one level into a partition
//!   directory. It reads no configuration, so a load that switched format or
//!   dropped `partition_by` still clears what its predecessors wrote.
//! - The FROZEN plain-parquet rule (ADDED for local parquet with no
//!   partitioning): TOP-LEVEL `*.parquet` of ANY name, matching the exact
//!   pre-015 behavior.
//!
//! The two are a UNION. User files are never ours to delete: a bare `part-`
//! prefix is not enough, because that is the default output basename of pyarrow,
//! Spark, Hive and Delta.

use rdlt_connector::DestinationError;

use crate::dest::config::DestFormat;
use crate::location::Location;

/// The FROZEN rule: a top-level `*.parquet` of any name (pre-015 compatibility).
fn frozen_owns(tail: &str) -> bool {
    !tail.contains('/') && tail.ends_with(".parquet")
}

/// Does this file name have the exact shape `publish` writes —
/// `part-<load>-<seq>-<index>.<ext>` with numeric `seq` and `index`, and `<ext>`
/// one of this destination's output formats?
///
/// A bare `part-` prefix is NOT enough and must never be used: `part-0.parquet`
/// and `part-00000-<uuid>-c000.snappy.parquet` are the DEFAULT output basenames
/// of pyarrow, Spark, Hive and Delta, so a prefix test would claim a user's own
/// dataset sitting under the table directory and a Replace would delete it.
fn is_written_part(file: &str) -> bool {
    let Some(rest) = file.strip_prefix("part-") else {
        return false;
    };
    let Some(stem) = DestFormat::ALL
        .iter()
        .find_map(|f| rest.strip_suffix(&format!(".{}", f.extension())))
    else {
        return false;
    };
    // `<load>-<seq>-<index>`: the last two hyphen-separated components are the
    // sequence and index this destination stamps, and a load id remains before
    // them. Anything else was written by something that is not us.
    let mut parts = stem.rsplit('-');
    let (Some(index), Some(seq)) = (parts.next(), parts.next()) else {
        return false;
    };
    let load_remains = parts.next().is_some_and(|first| !first.is_empty());
    load_remains
        && !seq.is_empty()
        && !index.is_empty()
        && seq.bytes().all(|b| b.is_ascii_digit())
        && index.bytes().all(|b| b.is_ascii_digit())
}

/// The owned-parts rule: a file this destination WROTE, at depth 1 or inside one
/// partition directory.
///
/// Ownership depends on the name shape this destination writes — never on the
/// configuration in effect right now. A load that switches format, or drops
/// `partition_by`, still has to clear what earlier loads wrote, or a Replace
/// silently leaves them behind and the table becomes a mixture of two loads.
fn owns_part(tail: &str) -> bool {
    match tail.split('/').collect::<Vec<_>>().as_slice() {
        [file] => is_written_part(file),
        [_partition, file] => is_written_part(file),
        _ => false,
    }
}

/// Truncate one table's owned files. `frozen_plain_parquet` ADDS the frozen
/// rule on top of the always-applied owned-parts rule.
pub(crate) async fn truncate_table(
    location: &Location,
    table: &str,
    frozen_plain_parquet: bool,
) -> Result<(), DestinationError> {
    for tail in location.keys_of_table(table).await? {
        // UNION, not either/or. The frozen rule is an ADDITION for the local
        // plain-parquet case (it also takes top-level parquet this destination
        // did not name), never a replacement: making them exclusive is what let
        // a load that switched format or dropped partitioning skip its own
        // predecessors, because the frozen rule sees neither nested tails nor
        // any extension but `.parquet`.
        let owned = owns_part(&tail) || (frozen_plain_parquet && frozen_owns(&tail));
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
        assert!(owns_part("part-load-1-0.jsonl"));
        assert!(!owns_part("user.jsonl"));
        assert!(owns_part("d1/part-load-1-0.jsonl"));
        // Deeper than one partition level is never ours.
        assert!(!owns_part("a/b/part-load-1-0.jsonl"));
    }

    /// Ownership must not depend on the configuration in effect NOW. A load that
    /// switched format, or dropped `partition_by`, still has to clear what its
    /// earlier loads wrote — otherwise a Replace leaves them beside the new data
    /// and the table silently holds two loads at once.
    #[test]
    fn ownership_survives_a_format_or_partitioning_change() {
        // Written as parquet, now configured jsonl (and vice versa).
        assert!(owns_part("part-load-1-0.parquet"));
        assert!(owns_part("part-load-1-0.jsonl"));
        // Written partitioned, now configured unpartitioned.
        assert!(owns_part("eu/part-load-1-0.parquet"));
        assert!(owns_part("eu/part-load-1-0.jsonl"));
    }

    /// The rule must never claim a file this destination did not write.
    ///
    /// `part-<n>.parquet` and `part-00000-<uuid>-c000.snappy.parquet` are the
    /// DEFAULT output basenames of pyarrow, Spark, Hive and Delta — the single
    /// most likely foreign shape to sit under a data directory. A bare `part-`
    /// prefix test claims them and Replace deletes the user's dataset.
    #[test]
    fn a_foreign_dataset_is_never_ours_to_delete() {
        assert!(!owns_part("part-0.parquet"));
        assert!(!owns_part("spark-export/part-0.parquet"));
        assert!(!owns_part("part-00000-8f3a1c2e-c000.snappy.parquet"));
        assert!(!owns_part("hive/part-00000-8f3a1c2e-c000.snappy.parquet"));
        assert!(!owns_part("part-of-my-notes.txt"));
        assert!(!owns_part("report.parquet"));
        assert!(!owns_part("eu/report.jsonl"));
        assert!(!owns_part("partition-summary.parquet"));
        // Our prefix and extension, but not our shape.
        assert!(!owns_part("part-load.parquet"));
        assert!(!owns_part("part-load-1.parquet"));
        assert!(!owns_part("part--1-0.parquet"));
    }

    /// `ALL` must name every variant, or that format's files stop being cleared.
    /// `in_all` is an exhaustive match, so a new variant cannot compile until it
    /// is listed — this asserts the listing, not the array's length.
    #[test]
    fn every_output_format_appears_in_all() {
        for format in DestFormat::ALL {
            assert!(format.in_all(), "{format:?} must appear in DestFormat::ALL");
        }
    }

    /// Every name `final_tail` can produce must be owned, at both depths. Built
    /// from the writer itself so the two cannot drift.
    #[test]
    fn every_name_this_destination_writes_is_owned() {
        use crate::dest::layout::final_tail;
        use rdlt_connector::core::{LoadId, TableName};

        let table = TableName::new("events");
        let load = LoadId::new("2026-07-26T00-00-00Z-abcdef");
        for format in DestFormat::ALL {
            for partition in [None, Some("eu")] {
                let full = final_tail(&table, partition, &load, 3, 7, format.extension());
                let tail = full
                    .strip_prefix("events/")
                    .expect("final_tail is table-rooted");
                assert!(
                    owns_part(tail),
                    "`{tail}` is written by us and must be owned"
                );
            }
        }
    }
}
