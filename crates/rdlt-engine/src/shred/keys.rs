//! The keys of the objects open in one JSON value, to refuse a key an object repeats however the
//! key was escaped.

use std::ops::Range;

use rdlt_wire::limits::count;

use super::meter::{Meter, Over};

/// Bytes a key's range among the open keys takes.
pub(crate) const KEY_BYTES: u64 = 16;

/// Bytes an open object's mark of where its keys start takes.
pub(crate) const OBJECT_BYTES: u64 = 8;

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
    /// Opens an object inside the innermost one, charging `meter` first for what that grows
    /// the objects by.
    ///
    /// # Errors
    ///
    /// [`Over`] where the meter has no room for that; no object is opened.
    pub(crate) fn open(&mut self, meter: &Meter) -> Result<(), Over> {
        grow(&mut self.objects, OBJECT_BYTES, meter)?;
        self.objects.push(self.keys.len());
        Ok(())
    }

    /// Notes `key`, a range of the text, as the innermost object's next key, charging `meter`
    /// first for what that grows the keys by.
    ///
    /// # Errors
    ///
    /// [`Over`] where the meter has no room for that; the key is not noted.
    pub(crate) fn key(&mut self, key: Range<usize>, meter: &Meter) -> Result<(), Over> {
        grow(&mut self.keys, KEY_BYTES, meter)?;
        self.keys.push(key);
        Ok(())
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

/// Makes room in `items` for one more, charging `meter` first for what their capacity grows by,
/// `entry` bytes an item: to twice what it was, one at the least.
fn grow<T>(items: &mut Vec<T>, entry: u64, meter: &Meter) -> Result<(), Over> {
    let capacity = items.capacity();
    if items.len() < capacity {
        return Ok(());
    }
    let grown = capacity.saturating_mul(2).max(1);
    let bytes = count(grown - capacity).saturating_mul(entry);
    meter.charge(bytes)?;
    items.reserve_exact(grown - items.len());
    Ok(())
}

#[cfg(test)]
mod tests;
