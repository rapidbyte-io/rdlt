//! The keys of one JSON object, to refuse a key it repeats however the key was escaped.

use std::borrow::Cow;
use std::collections::BTreeSet;

use super::ShredError;
use crate::limits::QUOTED_BYTES;

/// The keys an object held so far, each as its escapes read: borrowed from the text where it had
/// none to read.
#[derive(Debug, Default)]
pub(super) struct ObjectKeys<'a>(BTreeSet<Cow<'a, str>>);

impl<'a> ObjectKeys<'a> {
    /// Notes `key`, the object's next key.
    ///
    /// # Errors
    ///
    /// [`ShredError::DuplicateKey`], the key shown cut to a limit, where the object held it.
    pub(super) fn note(&mut self, key: Cow<'a, str>) -> Result<(), ShredError> {
        if self.0.contains(key.as_ref()) {
            let shown = rdlt_connector::text::shown(&key, QUOTED_BYTES);
            return Err(ShredError::DuplicateKey(shown));
        }
        self.0.insert(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
