//! The destination clause registry.

use crate::testing::Clause;

/// The clauses [`certify_destination`](super::certify_destination) checks, in order.
pub const DESTINATION_CLAUSES: &[Clause] = &[
    Clause {
        id: "D-CHECK",
        statement: "check succeeds exactly when opening a session does, and both do for a valid \
                    configuration",
        unless: "",
    },
    Clause {
        id: "D-EPOCH",
        statement: "each open returns a higher epoch than the last",
        unless: "",
    },
    Clause {
        id: "D-STAGING",
        statement: "staged segments are invisible until committed",
        unless: "",
    },
    Clause {
        id: "D-COMMIT",
        statement: "a commit publishes exactly its segments and reports their rows",
        unless: "",
    },
    Clause {
        id: "D-IDEMPOTENT",
        statement: "re-committing a commit no horizon of its pipeline has passed returns its receipt and publishes nothing",
        unless: "",
    },
    Clause {
        id: "D-STATE",
        statement: "committed state records are returned by the next open",
        unless: "",
    },
    Clause {
        id: "D-DISCARD",
        statement: "segments staged by an earlier session are never published",
        unless: "",
    },
    Clause {
        id: "D-REPLACE",
        statement: "a replace generation stays hidden until the commit that finishes it swaps it in",
        unless: "the destination cannot replace",
    },
    Clause {
        id: "D-SCHEMA",
        statement: "every declared schema change applies, and applying it again changes nothing",
        unless: "the destination adds no column and widens no 32-bit integer",
    },
    Clause {
        id: "D-MERGE",
        statement: "a merge keeps one row per key: the newest commit's, and within a commit the \
                    greatest sequence's; a change stream's change applies only past the sequence \
                    of the row its key holds",
        unless: "the destination cannot merge",
    },
    Clause {
        id: "D-DELETE",
        statement: "a change stream's delete removes its key's row, or marks it deleted and keeps \
                    its values; no change sequenced before a hard delete, even of a key no row \
                    held, brings the row back in any later session, until the table is replaced \
                    whole",
        unless: "the destination merges no change stream, or neither removes nor marks deleted rows",
    },
    Clause {
        id: "D-PARTIAL",
        statement: "a change stream's update keeps the published value of each column it flags \
                    unchanged",
        unless: "the destination merges no change stream, or takes no partial update",
    },
    Clause {
        id: "D-TRUNCATE",
        statement: "a change stream's truncate removes, or marks deleted, every row sequenced \
                    before it and none after, and no change from before it, sent again, brings a \
                    row back",
        unless: "the destination merges no change stream, or neither removes nor marks deleted rows",
    },
    Clause {
        id: "D-HIST",
        statement: "a history table keeps every version of each key: a change equal to the \
                    current version changes nothing, any other closes it where the next begins, \
                    a delete closes it, or opens a deleted version keeping its data, and a \
                    truncate does so to every version sequenced before it; a change stream's \
                    change applies only past its key's newest version, tombstone and bound",
        unless: "the destination keeps no history",
    },
    Clause {
        id: "D-CHILDREN",
        statement: "a child table of a merge table holds the children of each root's winning row \
                    only, whatever it held before",
        unless: "the destination cannot merge",
    },
    Clause {
        id: "D-ENCODING",
        statement: "dictionary-encoded columns publish the values they encode",
        unless: "",
    },
    Clause {
        id: "D-TABLES",
        statement: "a segment may hold rows for several tables, and its commit publishes each \
                    table's rows",
        unless: "",
    },
    Clause {
        id: "D-NAMES",
        statement: "identifiers at the edges of the destination's own rules are published under \
                    their names",
        unless: "",
    },
    Clause {
        id: "D-LANES",
        statement: "writers of one table, as many as the destination runs at once, stage at the \
                    same time, and a commit publishes what each staged",
        unless: "the destination runs one writer at a time",
    },
    Clause {
        id: "D-OWNED",
        statement: "a table belongs to the pipeline that created it: another pipeline's schema \
                    change or writer is refused as table_owned, its generation swap too unless it \
                    changes nothing, and the owner keeps loading it",
        unless: "",
    },
    Clause {
        id: "D-DROP",
        statement: "a commit drops the tables it names, leaving nothing of them, and releases them: \
                    dropping again changes nothing, a session fenced before cannot claim them \
                    again, another pipeline may create a table of the name, and dropping another \
                    pipeline's table is refused as table_owned",
        unless: "the destination drops no tables",
    },
    Clause {
        id: "D-FENCE",
        statement: "a session opened before the latest one cannot commit",
        unless: "",
    },
];

/// The clauses that read what the destination published, which a probe that reads nothing
/// cannot check.
pub(super) const PROBED: [&str; 17] = [
    "D-STAGING",
    "D-COMMIT",
    "D-IDEMPOTENT",
    "D-DISCARD",
    "D-REPLACE",
    "D-SCHEMA",
    "D-MERGE",
    "D-DELETE",
    "D-PARTIAL",
    "D-TRUNCATE",
    "D-HIST",
    "D-CHILDREN",
    "D-ENCODING",
    "D-TABLES",
    "D-NAMES",
    "D-LANES",
    "D-FENCE",
];
