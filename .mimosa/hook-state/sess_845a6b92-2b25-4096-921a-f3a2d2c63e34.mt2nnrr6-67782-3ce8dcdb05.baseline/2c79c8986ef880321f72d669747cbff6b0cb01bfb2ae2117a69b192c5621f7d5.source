//! Shared support for the certification cases.

pub(crate) mod bins;
pub(crate) mod probe;

/// The skip reason a probe-less run stamps on every read-back clause
/// (the D-clauses and the destination K-clauses) —
/// `clause::d::NO_PROBE_SKIP`'s spelling, restated here byte-identical
/// as a deliberate outside pin: importing the const would compare it
/// to itself.
pub(crate) const NO_PROBE_REASON: &str = "no table probe supplied — read-back clauses need one; pass --probe-cmd '<sh line>' \
     (the library API takes a TableProbe directly). Single-writer stores (duckdb) refuse \
     every open beside the live connector, a read-only one included — probe a COPY: copy \
     the store file plus its WAL sidecar, then count in the copy";
