//! The destination clause registry.

use crate::testing::Clause;

/// The clauses [`certify_destination`](super::certify_destination) checks, in order.
pub const DESTINATION_CLAUSES: &[Clause] = &[
    Clause {
        id: "D-CHECK",
        statement: "check succeeds exactly when opening a session does, and both do for a valid \
                    configuration",
    },
    Clause {
        id: "D-EPOCH",
        statement: "each open returns a higher epoch than the last",
    },
    Clause {
        id: "D-STAGING",
        statement: "staged segments are invisible until committed",
    },
    Clause {
        id: "D-COMMIT",
        statement: "a commit publishes exactly its segments and reports their rows",
    },
    Clause {
        id: "D-IDEMPOTENT",
        statement: "re-committing a commit returns its receipt and publishes nothing",
    },
    Clause {
        id: "D-STATE",
        statement: "committed state records are returned by the next open",
    },
    Clause {
        id: "D-DISCARD",
        statement: "segments staged by an earlier session are never published",
    },
    Clause {
        id: "D-REPLACE",
        statement: "a replace generation stays hidden until the commit that finishes it swaps it in",
    },
    Clause {
        id: "D-SCHEMA",
        statement: "every declared schema change applies, and applying it again changes nothing",
    },
    Clause {
        id: "D-MERGE",
        statement: "a merge keeps one row per key: the newest commit's, and within a commit the \
                    greatest sequence's",
    },
    Clause {
        id: "D-CHILDREN",
        statement: "a child table of a merge table holds the children of each root's winning row \
                    only, whatever it held before",
    },
    Clause {
        id: "D-ENCODING",
        statement: "dictionary-encoded columns publish the values they encode",
    },
    Clause {
        id: "D-TABLES",
        statement: "a segment may hold rows for several tables, and its commit publishes each \
                    table's rows",
    },
    Clause {
        id: "D-NAMES",
        statement: "identifiers at the edges of the destination's own rules are published under \
                    their names",
    },
    Clause {
        id: "D-LANES",
        statement: "writers of one table, as many as the destination runs at once, stage at the \
                    same time, and a commit publishes what each staged",
    },
    Clause {
        id: "D-OWNED",
        statement: "a table belongs to the pipeline that created it: another pipeline's schema \
                    change or writer is refused as table_owned and its rows stay",
    },
    Clause {
        id: "D-FENCE",
        statement: "a session opened before the latest one cannot commit",
    },
];

/// The clauses that read what the destination published, which a probe that reads nothing
/// cannot check.
pub(super) const PROBED: [&str; 14] = [
    "D-STAGING",
    "D-COMMIT",
    "D-IDEMPOTENT",
    "D-DISCARD",
    "D-REPLACE",
    "D-SCHEMA",
    "D-MERGE",
    "D-CHILDREN",
    "D-ENCODING",
    "D-TABLES",
    "D-NAMES",
    "D-LANES",
    "D-OWNED",
    "D-FENCE",
];
