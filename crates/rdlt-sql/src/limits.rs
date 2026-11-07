//! Limits on what a SQL transform's text may ask for: it is a tenant's input, which the engine
//! parses and plans.

/// Bytes: the longest text a SQL transform may have.
pub const SQL_BYTES: usize = 64 << 10;

/// How deep a SQL transform's expressions may nest, each expression within as many as lie
/// above it in the parsed query: a parenthesis, a call, a `CASE` and each operator of a chain
/// are a level each, so `lower(t)` is one deep and `a + b + c` two.
pub const SQL_DEPTH: usize = 64;

/// How deep the parser may recurse, growing its stack as it goes: beyond what any query the
/// depth limit admits takes, so text deeper still is refused as the limit refuses it.
pub(crate) const PARSER_DEPTH: usize = 4 * SQL_DEPTH;

/// Bytes: the longest literal a SQL transform may hold.
pub const SQL_LITERAL_BYTES: usize = 4 << 10;

/// Bytes: the most a SQL transform's regular expression may compile to.
pub const REGEX_BYTES: usize = 1 << 20;

/// Plans a SQL transform keeps, one for each input schema it met last.
pub(crate) const PLANS: usize = 8;
