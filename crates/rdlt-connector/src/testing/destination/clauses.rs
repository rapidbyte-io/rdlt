//! The destination clause registry.

use crate::testing::Clause;

/// The clauses [`certify_destination`](super::certify_destination) checks, in order.
pub const DESTINATION_CLAUSES: &[Clause] = &[
    Clause {
        id: "D-CHECK",
        statement: "check succeeds for a valid configuration",
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
        id: "D-FENCE",
        statement: "a session opened before the latest one cannot commit",
    },
];
