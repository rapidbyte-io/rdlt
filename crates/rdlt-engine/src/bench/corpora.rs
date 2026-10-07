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
    /// Nested rows of about 170 bytes.
    Nested,
    /// Nested rows of which about one in ten thousand carries an optional key after its name, so
    /// most chunks lack a column the others have, and those that hold it meet it before most
    /// columns.
    Sparse,
    /// Flat rows of three narrow columns.
    FlatNarrow,
    /// Flat rows of 200 columns, integers and short strings.
    FlatWide,
    /// Rows of long strings with escapes.
    StringHeavy,
    /// Nested rows with arrays: `Nested`'s fields and up to three orders of a few tags each.
    WithArrays,
    /// Rows of an object and of as many items as their id modulo four, each item of two tags,
    /// so the rows normalizing them makes follow from the rows' count alone.
    Orders,
}

impl Corpus {
    /// The corpora the shred bench shreds on one core, in the order it reports them.
    pub const SHREDDED: [Self; 5] = [
        Self::Nested,
        Self::Sparse,
        Self::FlatNarrow,
        Self::FlatWide,
        Self::StringHeavy,
    ];

    /// The corpus's name in benchmark ids.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Nested => "nested",
            Self::Sparse => "sparse",
            Self::FlatNarrow => "flat_narrow",
            Self::FlatWide => "flat_wide",
            Self::StringHeavy => "string_heavy",
            Self::WithArrays => "with_arrays",
            Self::Orders => "orders",
        }
    }

    /// The corpus's rows until they hold `bytes`, cut into pushes of whole lines of
    /// [`PUSH_BYTES`] or a little more.
    pub fn pushes(self, bytes: usize) -> Vec<Bytes> {
        corpus(bytes, PUSH_BYTES, self.row())
    }

    /// The corpus's first `rows` rows, cut into pushes of `per_push` lines, the last of the rest.
    pub fn rows(self, rows: u64, per_push: NonZeroU64) -> Vec<Bytes> {
        let row = self.row();
        let mut mix = Mix::new(SEED);
        let mut pushes = Vec::new();
        let mut first = 0;
        while first < rows {
            let last = rows.min(first.saturating_add(per_push.get()));
            let mut push = String::new();
            for index in first..last {
                push.push_str(&row(index, &mut mix));
                push.push('\n');
            }
            pushes.push(Bytes::from(push));
            first = last;
        }
        pushes
    }

    /// The generator of the corpus's rows.
    fn row(self) -> fn(u64, &mut Mix) -> String {
        match self {
            Self::Nested => nested,
            Self::Sparse => sparse,
            Self::FlatNarrow => flat_narrow,
            Self::FlatWide => flat_wide,
            Self::StringHeavy => string_heavy,
            Self::WithArrays => with_arrays,
            Self::Orders => orders,
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
fn orders(index: u64, _: &mut Mix) -> String {
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

/// Flat rows of 200 columns, integers and short strings.
fn flat_wide(_: u64, mix: &mut Mix) -> String {
    let columns: Vec<String> = (0..200)
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
