//! Reading any text as a pipeline's document never panics, holds a bounded heap and, refusing
//! it, quotes none of its values; what it reads writes back, if it can be written, to an equal
//! document.

#![forbid(unsafe_code)]
#![no_main]

use std::collections::BTreeSet;
use std::fmt::Write as _;

use libfuzzer_sys::fuzz_target;
use peak_alloc::PeakAlloc;
use serde_saphyr::granit_parser::{Event, Parser};

#[global_allocator]
static PEAK: PeakAlloc = PeakAlloc;

/// Bytes: the most reading a document of the largest size may hold at once.
const HELD: usize = 64 << 20;

/// Bytes: how long a value must be for a refusal to be sure not to hold it by chance, as a word
/// of its own.
const LONG: usize = 16;

/// What the walk of a document's events is inside: a list, or a mapping with a key next or not.
enum Open {
    List,
    Mapping { key_next: bool },
}

/// The scalars of `text` at a value's place, each of at least `LONG` bytes, and every key.
fn values_and_keys(text: &str) -> (BTreeSet<String>, BTreeSet<String>) {
    let (mut values, mut keys, mut open) = (BTreeSet::new(), BTreeSet::new(), Vec::new());
    for parsed in Parser::new_from_str(text) {
        let Ok((event, _)) = parsed else {
            break;
        };
        match event {
            Event::Scalar(value, ..) => {
                let key = match open.last_mut() {
                    Some(Open::Mapping { key_next }) => std::mem::replace(key_next, !*key_next),
                    _ => false,
                };
                if key {
                    keys.insert(value.into_owned());
                } else if value.len() >= LONG {
                    values.insert(value.into_owned());
                }
            }
            Event::SequenceStart(..) | Event::MappingStart(..) => {
                if let Some(Open::Mapping { key_next }) = open.last_mut() {
                    *key_next = !*key_next;
                }
                open.push(if matches!(event, Event::SequenceStart(..)) {
                    Open::List
                } else {
                    Open::Mapping { key_next: true }
                });
            }
            Event::SequenceEnd | Event::MappingEnd => {
                open.pop();
            }
            _ => {}
        }
    }
    (values, keys)
}

/// The text of `refusal`, its `Debug`, its report, and each cause's text and `Debug`.
fn said(refusal: &rdlt::Error) -> String {
    let mut said = format!("{refusal} {refusal:?} {:?}", refusal.report());
    let mut cause = std::error::Error::source(refusal);
    while let Some(held) = cause {
        write!(said, " {held} {held:?}").expect("text writes");
        cause = held.source();
    }
    said
}

fuzz_target!(|text: &str| {
    PEAK.reset_peak_usage();
    match rdlt::Document::parse(text) {
        Ok(document) => {
            if let Ok(written) = document.to_yaml() {
                let again = rdlt::Document::parse(&written).expect("a written document reads");
                assert_eq!(again, document);
            }
        }
        Err(refusal) => {
            let said = said(&refusal);
            let (values, keys) = values_and_keys(text);
            for value in &values {
                // A key is shown, and a key may hold a value's text.
                if !keys.iter().any(|key| key.contains(value.as_str())) {
                    assert!(!said.contains(value.as_str()), "{said} quotes {value:?}");
                }
            }
        }
    }
    assert!(
        PEAK.peak_usage() < HELD,
        "reading held {} bytes",
        PEAK.peak_usage()
    );
});
