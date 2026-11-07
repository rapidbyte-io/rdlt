//! Limits on what a SQL transform's text may ask for: it is a tenant's input, which the engine
//! parses and plans.

/// Bytes: the longest text a SQL transform may have.
pub const SQL_BYTES: usize = 64 << 10;

/// How deep a SQL transform's expressions may nest: each parenthesis, call or `CASE` is a level,
/// and so is each operator of a chain, so `a + b + c` takes two.
pub const SQL_DEPTH: usize = 64;

/// Bytes: the longest literal a SQL transform may hold.
pub const SQL_LITERAL_BYTES: usize = 4 << 10;

/// Bytes: the most a SQL transform's regular expression may compile to.
pub const REGEX_BYTES: usize = 1 << 20;
