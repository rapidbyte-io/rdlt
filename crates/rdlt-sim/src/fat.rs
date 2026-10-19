//! Fat values: in a world of them, a wide object of JSON on every eighth record a stream that
//! pushes JSON sends, so most of what its runs hold is data; the destination keeps a short form
//! of each, which the model expects.

#[cfg(test)]
mod tests;

use std::fmt::Write as _;
use std::sync::LazyLock;

use rdlt_engine::EngineConfig;
use rdlt_wire::Limits;

use crate::rng::SplitMix64;
use crate::workload::Row;

/// The column fat values arrive in, which a stream pushing JSON declares as JSON.
pub(crate) const COLUMN: &str = "fat";

/// What a stream's draw of its fat values mixes into its world's salt: "fat" in ASCII.
const FAT: u64 = 0x0066_6174;

/// Rows apart that carry a fat value: no push of eight rows or fewer holds two.
const EVERY: i64 = 8;

static LEAST: LazyLock<Limits> = LazyLock::new(|| {
    EngineConfig::builder()
        .memory(EngineConfig::least_memory(1).expect("a memory admits one partition"))
        .partitions(1)
        .build()
        .expect("the least memory makes a configuration")
        .limits()
});

/// Bytes: the most JSON text one push may take under the least budget an engine is given.
pub(crate) fn least_push() -> u64 {
    LEAST.json_push_bytes
}

/// Columns: the most a push's records may hold together, nested keys counting, under the least
/// budget an engine is given.
pub(crate) fn least_columns() -> u64 {
    LEAST.schema_columns
}

/// Keys of the widest fat value: a thirty-second of the least columns, which its keys count
/// against beside the rest of its record, its bytes coming from long strings instead.
pub(crate) fn widest() -> u64 {
    least_columns() / 32
}

/// Bytes of the key and punctuation around each digit string of a fat value, with room to spare.
const FRAME: u64 = 16;

/// What share of the least push the widest fat value takes: the most that every budget a world
/// draws still builds the batches of, as wider ones were refused at the smallest.
const SHARE: u64 = 16;

/// Digits each key of a fat value holds: as many as bring the widest fat value to a sixteenth of
/// the least push, so the text of a fat value is data, not keys.
fn span() -> u64 {
    (least_push() / SHARE / widest()).saturating_sub(FRAME)
}

/// Keys of each fat value of stream `index` of a workload salted `salt`: the widest, or a half
/// or a quarter of it.
pub(crate) fn keys(salt: u64, index: u64) -> u64 {
    widest() >> SplitMix64::new(salt ^ FAT ^ index).below(3)
}

/// Rows apart that a push holds one of for it to carry spaces: every fourth carrier.
const SPACED: i64 = 32;

/// Whether a push holding `row` carries spaces.
pub(crate) fn spaced(row: &Row) -> bool {
    row.offset.rem_euclid(SPACED) == 0
}

/// Whether `row` carries a fat value.
pub(crate) fn carried(row: &Row) -> bool {
    row.offset.rem_euclid(EVERY) == 0
}

/// Writes key `key` of a fat value whose first digit is `first` onto `text`, after a comma
/// where it is not the first key: a string of `span` digits, each one more than the last, modulo
/// ten.
fn entry(text: &mut String, first: i64, key: u64) {
    if key > 0 {
        text.push(',');
    }
    write!(text, "\"f{key}\":\"").expect("a string takes what is written");
    let start = (first + i64::try_from(key % 10).unwrap_or(0)) % 10;
    let mut digit = start;
    for _ in 0..span() {
        text.push(char::from(b'0' + u8::try_from(digit).unwrap_or(0)));
        digit = (digit + 1) % 10;
    }
    text.push('"');
}

/// The fat value of `row` as JSON text: an object of `keys` keys `f0`, `f1` and on, each holding
/// a string of digits, the first digit of the first key the last digit of `row`'s id and each
/// next key's one more, modulo ten.
pub(crate) fn value(row: &Row, keys: u64) -> String {
    let first = row.id.rem_euclid(10);
    let mut text = String::from("{");
    for key in 0..keys {
        entry(&mut text, first, key);
    }
    text.push('}');
    text
}

/// The short form the destination keeps of a fat value of `keys` keys whose first digit is
/// `first`.
pub(crate) fn condensed(first: i64, keys: u64) -> String {
    format!("{{\"{COLUMN}\":[{first},{keys}]}}")
}

/// The short form of the stored JSON `text` where it is a fat value, byte for byte; `None` where
/// it is anything else, which the destination keeps as it is.
pub(crate) fn condense(text: &str) -> Option<String> {
    let first = text
        .strip_prefix("{\"f0\":\"")?
        .bytes()
        .next()?
        .checked_sub(b'0')?;
    if first > 9 {
        return None;
    }
    let first = i64::from(first);
    let commas = text.bytes().filter(|byte| *byte == b',').count();
    let keys = u64::try_from(commas).ok()? + 1;
    let mut rest = text.strip_prefix('{')?;
    let mut expected = String::new();
    for key in 0..keys {
        expected.clear();
        entry(&mut expected, first, key);
        rest = rest.strip_prefix(expected.as_str())?;
    }
    (rest == "}").then(|| condensed(first, keys))
}

/// The short form the destination keeps of the fat value `row` carries, of `keys` keys.
pub(crate) fn expected(row: &Row, keys: u64) -> String {
    condensed(row.id.rem_euclid(10), keys)
}
