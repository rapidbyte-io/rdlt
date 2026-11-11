//! Limits on what a SQL transform's text may ask for: it is a tenant's input, which the engine
//! parses and plans.

/// Bytes: the longest text a SQL transform may have.
pub const SQL_BYTES: usize = 64 << 10;

/// How deep a SQL transform's expressions may nest, each expression within as many as lie
/// above it in the parsed query: a parenthesis, a call, a `CASE` and each operator of a chain
/// are a level each, so `lower(t)` is one deep and `a + b + c` two.
pub const SQL_DEPTH: usize = 64;

/// How many conditions a SQL transform's `CASE` may lie within: the `WHEN`s of the `CASE`s
/// around it, and the arguments but the last of the calls of `coalesce` around it, which is
/// planned as a `CASE` testing each, so a `coalesce` lies within them as a `CASE` does.
///
/// The planner reads a `CASE` in a condition again for each operator above it there, so its
/// work multiplies with each condition a `CASE` lies within.
pub const SQL_CONDITION_DEPTH: usize = 1;

/// How deep the parser may recurse, growing its stack as it goes: beyond what any query the
/// depth limit admits takes, so text deeper still is refused as the limit refuses it.
pub(crate) const PARSER_DEPTH: usize = 4 * SQL_DEPTH;

/// Bytes: the longest literal a SQL transform may hold.
pub const SQL_LITERAL_BYTES: usize = 4 << 10;

/// Bytes: the most a SQL transform's regular expression may compile to.
pub const REGEX_BYTES: usize = 1 << 20;

/// Plans a SQL transform keeps, one for each of the input schemas asked for most recently.
pub(crate) const PLANS: usize = 8;
