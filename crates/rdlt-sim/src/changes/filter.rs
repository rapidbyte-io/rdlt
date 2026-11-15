//! A change stream's query: its filter, the column it reads and how it says no, and the column it
//! computes beside those it passes on.

/// The column a change stream's filter reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Filtered {
    /// The key, which a filter of a stream merged by key whose updates leave columns unchanged
    /// reads alone: its answer for a row never flips, so a row it deleted never comes back.
    Key,
    /// The counter each insert and update sets.
    Counter,
}

/// A change stream's filter: an insert or update whose value of the column it reads this
/// leaves one over fails it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Filter {
    /// The column it reads.
    pub reads: Filtered,
    /// The modulus.
    pub modulus: i64,
    /// Whether it answers a failing insert or update with null, and a change lacking the column
    /// it reads with false, rather than false and null.
    pub cased: bool,
}

impl Filter {
    /// Whether it keeps an insert or update of `key` setting counter `n`.
    pub(crate) fn keeps(self, key: Option<i64>, n: Option<i64>) -> bool {
        let value = match self.reads {
            Filtered::Key => key,
            Filtered::Counter => n,
        };
        value.is_none_or(|value| value % self.modulus != 1)
    }

    /// The predicate a plan filters the stream by.
    fn predicate(self) -> String {
        let column = match self.reads {
            Filtered::Key => "id",
            Filtered::Counter => "n",
        };
        let modulus = self.modulus;
        if self.cased {
            format!(
                "CASE WHEN {column} IS NULL THEN FALSE WHEN {column} % {modulus} <> 1 THEN TRUE END"
            )
        } else {
            format!("{column} % {modulus} <> 1")
        }
    }
}

/// The column `extra` a change stream's query computes beside every column it passes on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Projection {
    /// A constant text.
    Constant,
    /// Twice the counter.
    Doubled,
    /// The value, or a text of its own where there is none.
    Coalesced,
}

impl Projection {
    /// The expression the query computes it by.
    fn expression(self) -> &'static str {
        match self {
            Self::Constant => "'c'",
            Self::Doubled => "n * 2",
            Self::Coalesced => "coalesce(value, 'none')",
        }
    }

    /// What it holds for an insert or update of `value` and counter `n`, as text.
    pub(crate) fn computed(self, value: Option<&str>, n: Option<i64>) -> Option<String> {
        match self {
            Self::Constant => Some("c".to_owned()),
            Self::Doubled => n.map(|n| (n * 2).to_string()),
            Self::Coalesced => Some(value.unwrap_or("none").to_owned()),
        }
    }
}

/// The query a plan transforms a change stream with, where it filters or computes: every column
/// passed on, `extra` beside them where it computes one.
pub(crate) fn query(filter: Option<Filter>, projection: Option<Projection>) -> Option<String> {
    if filter.is_none() && projection.is_none() {
        return None;
    }
    let extra = projection.map_or(String::new(), |projection| {
        format!(", {} AS extra", projection.expression())
    });
    let filter = filter.map_or(String::new(), |filter| {
        format!(" WHERE {}", filter.predicate())
    });
    Some(format!("SELECT *{extra} FROM input{filter}"))
}
