//! A change stream's filter: the column it reads, and how it says no.

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

    /// The query a plan filters the stream with.
    pub(crate) fn query(self) -> String {
        let column = match self.reads {
            Filtered::Key => "id",
            Filtered::Counter => "n",
        };
        let modulus = self.modulus;
        if self.cased {
            format!(
                "SELECT * FROM input WHERE CASE WHEN {column} IS NULL THEN FALSE \
                 WHEN {column} % {modulus} <> 1 THEN TRUE END"
            )
        } else {
            format!("SELECT * FROM input WHERE {column} % {modulus} <> 1")
        }
    }
}
