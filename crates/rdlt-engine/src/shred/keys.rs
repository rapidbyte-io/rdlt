//! The keys of the objects open in one JSON value, to refuse a key an object repeats however the
//! key was escaped.

use std::ops::Range;

/// The keys of each object open in a value, as ranges of a text that holds each key written one
/// way only, so that two keys are the same where their text is.
///
/// Kept from value to value, it allocates only to grow.
#[derive(Debug, Default)]
pub(crate) struct OpenKeys {
    /// The keys the open objects have named, the innermost object's last.
    keys: Vec<Range<usize>>,
    /// Where each open object's keys start among `keys`, the innermost last.
    objects: Vec<usize>,
}

impl OpenKeys {
    /// Forgets every open object, for a value that stopped before closing them.
    pub(crate) fn clear(&mut self) {
        self.keys.clear();
        self.objects.clear();
    }

    /// Opens an object inside the innermost one.
    pub(crate) fn open(&mut self) {
        self.objects.push(self.keys.len());
    }

    /// Notes `key`, a range of the text, as the innermost object's next key.
    pub(crate) fn key(&mut self, key: Range<usize>) {
        self.keys.push(key);
    }

    /// Closes the innermost object, whose keys `text` holds: the key it named again first, if it
    /// repeated one.
    pub(crate) fn close(&mut self, text: &str) -> Option<Range<usize>> {
        let first = self.objects.pop().unwrap_or_default();
        let keys = self.keys.get_mut(first..).unwrap_or_default();
        let named = |key: &Range<usize>| text.get(key.clone()).unwrap_or_default();
        keys.sort_unstable_by(|one, other| {
            named(one)
                .cmp(named(other))
                .then(one.start.cmp(&other.start))
        });
        // Sorted so, each key named again follows where it was named before.
        let repeated = keys
            .windows(2)
            .filter(|pair| named(&pair[0]) == named(&pair[1]))
            .map(|pair| pair[1].clone())
            .min_by_key(|key| key.start);
        self.keys.truncate(first);
        repeated
    }
}

#[cfg(test)]
mod tests;
