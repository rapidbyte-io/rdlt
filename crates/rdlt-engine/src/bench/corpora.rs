//! Generated JSON lines corpora: what the shred bench shreds and the allocation counts count.

#[cfg(test)]
mod tests;

use std::fmt::Write as _;
use std::num::NonZeroU64;

use bytes::Bytes;

use super::Mix;

/// Bytes each corpus holds in the benches.
pub const CORPUS_BYTES: usize = 32 << 20;
/// Bytes per push, the default coalescing target.
pub const PUSH_BYTES: usize = 8 << 20;
/// Bytes per shredding job, the default chunk size.
pub const CHUNK_BYTES: usize = 1 << 20;

/// A generated corpus of JSON lines, the same bytes on every run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Corpus {
    /// Nested rows of about 180 bytes.
    Nested,
    /// Nested rows of which about one in ten thousand carries an optional key after its name, so
    /// most chunks lack a column the others have, and those that hold it meet it before most
    /// columns.
    Sparse,
    /// Flat rows of three narrow columns.
    FlatNarrow,
    /// Flat rows of this many columns, integers and short strings by turns.
    Wide(u16),
    /// Rows of long strings with escapes.
    StringHeavy,
    /// Nested rows with arrays: `Nested`'s fields and up to three orders of a few tags each.
    WithArrays,
    /// Rows of an object and of as many items as their id modulo four, each item of two tags,
    /// so the rows normalizing them makes follow from the rows' count alone.
    Orders,
    /// Nested rows as the elements of one JSON array a push, rather than lines.
    ArrayForm,
    /// Nested rows with a column of integers that is an object holding a float in one row of
    /// four thousand, so a flush joins a column of JSON over chunks that parsed it as integers,
    /// and parses them again exactly.
    DriftJson,
    /// Nested rows with a column of integers that is a string in one row of four thousand, so a
    /// flush joins a column of JSON over chunks that parsed scalars alone.
    DriftScalar,
    /// Rows of an id and sixty-four one-digit integers, whose builders take more than the text
    /// they come from.
    IntArrays,
    /// Rows of integers beyond 64 bits, beyond a decimal's 38 digits and of floats beyond 2⁶³,
    /// which the fast parse cannot read exactly.
    BigNumbers,
    /// Nested rows printed over several lines each, as the elements of one JSON array a push.
    Pretty,
}

impl Corpus {
    /// The corpora the shred bench shreds on one core, in the order it reports them.
    pub const SHREDDED: [Self; 5] = [
        Self::Nested,
        Self::Sparse,
        Self::FlatNarrow,
        Self::Wide(200),
        Self::StringHeavy,
    ];

    /// The cliff corpora: each meets a path the shredder takes for some data alone.
    pub const CLIFFS: [Self; 6] = [
        Self::ArrayForm,
        Self::DriftJson,
        Self::DriftScalar,
        Self::IntArrays,
        Self::BigNumbers,
        Self::Pretty,
    ];

    /// The corpus's name in benchmark ids.
    pub fn name(self) -> String {
        match self {
            Self::Nested => "nested".to_owned(),
            Self::Sparse => "sparse".to_owned(),
            Self::FlatNarrow => "flat_narrow".to_owned(),
            Self::Wide(columns) => format!("wide_{columns}"),
            Self::StringHeavy => "string_heavy".to_owned(),
            Self::WithArrays => "with_arrays".to_owned(),
            Self::Orders => "orders".to_owned(),
            Self::ArrayForm => "array_form".to_owned(),
            Self::DriftJson => "drift_json".to_owned(),
            Self::DriftScalar => "drift_scalar".to_owned(),
            Self::IntArrays => "int_arrays".to_owned(),
            Self::BigNumbers => "big_numbers".to_owned(),
            Self::Pretty => "pretty".to_owned(),
        }
    }

    /// The corpus's rows until they hold `bytes`, cut into pushes of whole lines of
    /// [`PUSH_BYTES`] or a little more; the array-form corpora give one JSON array a push.
    pub fn pushes(self, bytes: usize) -> Vec<Bytes> {
        match self {
            Self::ArrayForm => arrays(bytes, PUSH_BYTES, |index, mix| self.row(index, mix)),
            Self::Pretty => arrays(bytes, PUSH_BYTES, |index, mix| {
                pretty(&self.row(index, mix))
            }),
            _ => corpus(bytes, PUSH_BYTES, |index, mix| self.row(index, mix)),
        }
    }

    /// The corpus's first `rows` rows, cut into pushes of `per_push` lines, the last of the rest.
    pub fn rows(self, rows: u64, per_push: NonZeroU64) -> Vec<Bytes> {
        let mut mix = Mix::new(SEED);
        let mut pushes = Vec::new();
        let mut first = 0;
        while first < rows {
            let last = rows.min(first.saturating_add(per_push.get()));
            let mut push = String::new();
            for index in first..last {
                push.push_str(&self.row(index, &mut mix));
                push.push('\n');
            }
            pushes.push(Bytes::from(push));
            first = last;
        }
        pushes
    }

    /// The corpus's row `index`, its values drawn from `mix`.
    fn row(self, index: u64, mix: &mut Mix) -> String {
        match self {
            Self::Nested | Self::ArrayForm | Self::Pretty => nested(index, mix),
            Self::Sparse => sparse(index, mix),
            Self::FlatNarrow => flat_narrow(index, mix),
            Self::Wide(columns) => wide(columns, mix),
            Self::StringHeavy => string_heavy(index, mix),
            Self::WithArrays => with_arrays(index, mix),
            Self::Orders => orders(index),
            Self::DriftJson => drift_json(index, mix),
            Self::DriftScalar => drift_scalar(index, mix),
            Self::IntArrays => int_arrays(index, mix),
            Self::BigNumbers => big_numbers(index, mix),
        }
    }
}

/// The seed of every corpus's draws.
const SEED: u64 = 7;

/// Rows from `row` until they hold `bytes`, cut into pushes of whole lines of `push_bytes` or a
/// little more.
fn corpus(
    bytes: usize,
    push_bytes: usize,
    mut row: impl FnMut(u64, &mut Mix) -> String,
) -> Vec<Bytes> {
    let mut mix = Mix::new(SEED);
    let mut pushes = Vec::new();
    let mut push = String::with_capacity(push_bytes + 4096);
    let (mut total, mut index) = (0, 0);
    while total < bytes {
        let line = row(index, &mut mix);
        total += line.len() + 1;
        push.push_str(&line);
        push.push('\n');
        if push.len() >= push_bytes {
            pushes.push(Bytes::from(std::mem::take(&mut push)));
        }
        index += 1;
    }
    if !push.is_empty() {
        pushes.push(Bytes::from(push));
    }
    pushes
}

/// Rows from `row` until they hold `bytes`, as the elements of one JSON array a push of
/// `push_bytes` or a little more.
fn arrays(
    bytes: usize,
    push_bytes: usize,
    mut row: impl FnMut(u64, &mut Mix) -> String,
) -> Vec<Bytes> {
    let mut mix = Mix::new(SEED);
    let mut pushes = Vec::new();
    let mut push = String::from("[");
    let (mut total, mut index) = (0, 0);
    while total < bytes {
        if push.len() > 1 {
            push.push_str(",\n");
        }
        let element = row(index, &mut mix);
        total += element.len() + 2;
        push.push_str(&element);
        if push.len() >= push_bytes {
            push.push(']');
            pushes.push(Bytes::from(std::mem::replace(&mut push, String::from("["))));
        }
        index += 1;
    }
    if push.len() > 1 {
        push.push(']');
        pushes.push(Bytes::from(push));
    }
    pushes
}

/// `row`, a compact JSON object, printed over several lines.
fn pretty(row: &str) -> String {
    let value: serde_json::Value = serde_json::from_str(row).expect("a generated row is JSON");
    serde_json::to_string_pretty(&value).expect("a JSON value prints")
}

/// A nested row whose `payload` is an integer, but in one row of four thousand an object holding
/// a float.
fn drift_json(index: u64, mix: &mut Mix) -> String {
    let row = nested(index, mix);
    let payload = if index % 4_000 == 3_999 {
        format!(r#"{{"ratio":{}.5,"tags":[1,2]}}"#, mix.below(100))
    } else {
        mix.below(1 << 20).to_string()
    };
    format!(r#"{},"payload":{payload}}}"#, &row[..row.len() - 1])
}

/// A nested row whose `code` is an integer, but in one row of four thousand a string.
fn drift_scalar(index: u64, mix: &mut Mix) -> String {
    let row = nested(index, mix);
    let code = if index % 4_000 == 3_999 {
        format!(r#""c{}""#, mix.below(100))
    } else {
        mix.below(1_000).to_string()
    };
    format!(r#"{},"code":{code}}}"#, &row[..row.len() - 1])
}

/// A row of an id and sixty-four one-digit integers.
fn int_arrays(index: u64, mix: &mut Mix) -> String {
    let values: Vec<String> = (0..64).map(|_| mix.below(10).to_string()).collect();
    format!(r#"{{"id":{index},"values":[{}]}}"#, values.join(","))
}

/// A row of numbers the fast parse cannot read exactly: beyond 64 bits, beyond 38 digits, and a
/// whole float beyond 2⁶³.
fn big_numbers(index: u64, mix: &mut Mix) -> String {
    format!(
        r#"{{"id":{index},"wide":{},"huge":{}{:024},"vast":9{:045},"float":{}.5e30}}"#,
        10_000_000_000_000_000_000_u64 + index,
        1 + mix.below(9),
        index,
        index,
        1 + mix.below(9),
    )
}

/// Nested rows of about 170 bytes.
fn nested(index: u64, mix: &mut Mix) -> String {
    let cities = ["Warsaw", "Krakow", "Gdansk", "Wroclaw", "Poznan"];
    format!(
        r#"{{"id":{index},"name":"user-{index:07}","score":{}.{:02},"active":{},"created_at":"2026-09-{:02}T12:{:02}:00Z","profile":{{"city":"{}","zip":"{}","geo":{{"lat":{}.{:05},"lon":{}.{:05}}}}}}}"#,
        mix.below(1000),
        mix.below(100),
        index.is_multiple_of(3),
        1 + index % 28,
        index % 60,
        cities[usize::try_from(mix.below(5)).unwrap_or(0)],
        10_000 + mix.below(90_000),
        49 + mix.below(6),
        mix.below(100_000),
        14 + mix.below(10),
        mix.below(100_000),
    )
}

/// Nested rows with arrays: `nested`'s fields and up to three orders of a few tags each.
fn with_arrays(index: u64, mix: &mut Mix) -> String {
    let row = nested(index, mix);
    let orders: Vec<String> = (0..mix.below(4))
        .map(|order| {
            let tags: Vec<String> = (0..mix.below(3))
                .map(|tag| format!(r#""t{}""#, mix.below(50) + tag))
                .collect();
            format!(
                r#"{{"sku":"s{}","qty":{},"tags":[{}]}}"#,
                mix.below(1000) + order,
                1 + mix.below(9),
                tags.join(",")
            )
        })
        .collect();
    format!(
        r#"{},"orders":[{}]}}"#,
        &row[..row.len() - 1],
        orders.join(",")
    )
}

/// Rows of an object and of as many items as `index` modulo four, each of two tags.
fn orders(index: u64) -> String {
    let items: Vec<String> = (0..index % 4)
        .map(|item| {
            format!(
                r#"{{"sku":"sku-{index}-{item}","qty":{},"price":{}.25,"tags":["t{}","u{item}"]}}"#,
                item + 1,
                index % 97,
                index % 5,
            )
        })
        .collect();
    format!(
        r#"{{"id":{index},"user":{{"name":"user-{index:08}","age":{},"active":{}}},"total":{}.5,"items":[{}]}}"#,
        index % 90,
        index.is_multiple_of(2),
        index % 1000,
        items.join(",")
    )
}

/// Nested rows of which about one in ten thousand carries an optional key after its name.
fn sparse(index: u64, mix: &mut Mix) -> String {
    let row = nested(index, mix);
    if mix.below(10_000) == 0 {
        let name = row.find(r#","score""#).expect("a nested row has a score");
        format!(
            r#"{},"tag":"t{}"{}"#,
            &row[..name],
            mix.below(100),
            &row[name..]
        )
    } else {
        row
    }
}

/// Flat rows of three narrow columns.
fn flat_narrow(index: u64, mix: &mut Mix) -> String {
    format!(
        r#"{{"id":{index},"value":{},"flag":{}}}"#,
        mix.draw() >> 12,
        mix.below(2) == 0
    )
}

/// A flat row of `columns` columns, integers and short strings by turns.
fn wide(columns: u16, mix: &mut Mix) -> String {
    let columns: Vec<String> = (0..columns)
        .map(|column| match column % 2 {
            0 => format!(r#""c{column}":{}"#, mix.below(1 << 20)),
            _ => format!(r#""c{column}":"v{}""#, mix.below(1000)),
        })
        .collect();
    format!("{{{}}}", columns.join(","))
}

/// Rows of long strings with escapes.
fn string_heavy(index: u64, mix: &mut Mix) -> String {
    let mut text = String::new();
    for _ in 0..8 {
        write!(text, "word{} \\\"quoted\\\" é ", mix.below(1000)).expect("writing to a string");
    }
    format!(
        r#"{{"id":{index},"body":"{text}","title":"title {} é"}}"#,
        mix.below(1000)
    )
}
