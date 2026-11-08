//! A document read as hostile input: bounded in size, depth and nodes, with no anchor, alias,
//! tag, repeated key or second document before serde sees any of it, and no merge key read.

#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::collections::HashSet;

use serde::de::DeserializeOwned;
use serde_saphyr::granit_parser::{ErrorKind, Event, Marker, Parser};
use serde_saphyr::{DuplicateKeyPolicy, MergeKeyPolicy, Options};

use super::fault::{Fault, Place, Refused};
use super::read::Mapped;
use crate::limits::{DOCUMENT_BYTES, DOCUMENT_DEPTH, DOCUMENT_NODES};

/// `text` as a `T`, a mapping, read under the document's limits, its faults saying `noun`: a
/// fault is placed and named by its key path, never by a value.
pub(crate) fn read<T: DeserializeOwned>(text: &str, noun: &str) -> Result<T, Fault> {
    reading(text, noun, None)
}

/// As [`read`], a text holding no node at all, as one left empty or commented out, being
/// `T::default()`.
pub(crate) fn read_or_default<T: DeserializeOwned + Default>(
    text: &str,
    noun: &str,
) -> Result<T, Fault> {
    reading(text, noun, Some(T::default()))
}

/// Refuses `text` for what a reader holds against it before serde sees any of it, its faults
/// saying `noun`: its size, and what [`screened`] finds; whether it holds nothing.
pub(crate) fn screen(text: &str, noun: &str) -> Result<bool, Fault> {
    if text.len() > DOCUMENT_BYTES {
        return Err(Fault::too_large(noun));
    }
    screened(text, noun)
}

fn reading<T: DeserializeOwned>(text: &str, noun: &str, default: Option<T>) -> Result<T, Fault> {
    if screen(text, noun)?
        && let Some(default) = default
    {
        return Ok(default);
    }
    let mut path = None;
    let read =
        serde_saphyr::with_deserializer_from_str_with_options(text, options(), |deserializer| {
            // The path is kept aside: the error returned is the reader's, which carries the place.
            serde_path_to_error::deserialize::<_, Mapped<T>>(deserializer).map_err(|error| {
                path = Some(error.path().to_string());
                error.into_inner()
            })
        });
    read.map(|Mapped(value)| value)
        .map_err(|error| Fault::of(&error, path, noun))
}

/// What the screen is inside: a list, or a mapping, the keys it holds and whether a key is next.
enum Open<'a> {
    List,
    Mapping {
        keys: HashSet<Cow<'a, str>>,
        key_next: bool,
    },
}

impl<'a> Open<'a> {
    /// Whether `node`, a scalar's text or none for a list or a mapping, is a key this mapping
    /// holds already, the keys compared as text: a mapping's nodes are a key and a value in turn.
    fn repeats(&mut self, node: Option<Cow<'a, str>>) -> bool {
        let Self::Mapping { keys, key_next } = self else {
            return false;
        };
        let is_key = *key_next;
        *key_next = !is_key;
        is_key && node.is_some_and(|key| !keys.insert(key))
    }
}

/// Opens `new` inside what is open, in which it takes a node's place.
fn opening<'a>(open: &mut Vec<Open<'a>>, new: Open<'a>) {
    if let Some(within) = open.last_mut() {
        within.repeats(None);
    }
    open.push(new);
}

/// Refuses, at the event that holds it, what the reader reports without a place of its own: a
/// tag on any node, an anchor, an alias, a key a mapping already holds, a second document, and
/// nesting or nodes past the limits.
///
/// Whether `text` holds nothing, scanning to its end with no node: a syntax error is no empty
/// text, and is left for the reader to report.
fn screened(text: &str, noun: &str) -> Result<bool, Fault> {
    let events = serde_saphyr::granit_parser::options! {
        emit_unresolved_aliases: true,
        emit_anchor_events: true,
    };
    let mut open = Vec::new();
    let (mut documents, mut nodes) = (0_usize, 0_usize);
    for parsed in Parser::new_from_str_with_options(text, events) {
        let (event, span) = match parsed {
            Ok(parsed) => parsed,
            // Scanning ahead met nesting past the parser's own limit before an event for it.
            Err(error) if matches!(error.kind(), ErrorKind::RecursionLimitExceeded) => {
                return Err(Fault::too_deep(noun, place(error.marker())));
            }
            Err(_) => return Ok(false),
        };
        let at = place(&span.start);
        let refused = match &event {
            Event::DocumentStart(..) => {
                documents += 1;
                (documents > 1).then_some(Refused::Document)
            }
            Event::Alias(..) | Event::UnresolvedAlias(..) => Some(Refused::Alias),
            Event::Anchor(..) => Some(Refused::Anchor),
            Event::Scalar(_, _, _, tag)
            | Event::SequenceStart(_, _, tag)
            | Event::MappingStart(_, _, tag) => {
                nodes += 1;
                tag.is_some().then_some(Refused::Tag)
            }
            _ => None,
        };
        if let Some(refused) = refused {
            return Err(Fault::refused(refused, at));
        }
        match event {
            Event::Scalar(value, ..) => {
                if open
                    .last_mut()
                    .is_some_and(|within: &mut Open<'_>| within.repeats(Some(value)))
                {
                    return Err(Fault::repeated(at));
                }
            }
            Event::SequenceStart(..) => opening(&mut open, Open::List),
            Event::MappingStart(..) => {
                let (keys, key_next) = (HashSet::new(), true);
                opening(&mut open, Open::Mapping { keys, key_next });
            }
            Event::SequenceEnd | Event::MappingEnd => {
                open.pop();
            }
            _ => {}
        }
        if open.len() > DOCUMENT_DEPTH {
            return Err(Fault::too_deep(noun, at));
        }
        if nodes > DOCUMENT_NODES {
            return Err(Fault::too_deep(noun, at));
        }
    }
    Ok(nodes == 0)
}

/// The place `marker` stands at; a parser's column counts from 0.
fn place(marker: &Marker) -> Place {
    Place::new(marker.line(), marker.col() + 1)
}

/// The reader's options: the screen's limits again as its own, and every expansion refused.
fn options() -> Options {
    serde_saphyr::options! {
        budget: serde_saphyr::budget! {
            max_depth: DOCUMENT_DEPTH,
            max_nodes: DOCUMENT_NODES,
            max_anchors: 0,
            max_aliases: 0,
            max_documents: 1,
            max_merge_keys: 0,
            max_total_scalar_bytes: DOCUMENT_BYTES,
        },
        duplicate_keys: DuplicateKeyPolicy::Error,
        merge_keys: MergeKeyPolicy::Error,
        strict_booleans: true,
        reject_unsupported_tags: true,
        // A snippet quotes the document around the fault, which may be a value.
        with_snippet: false,
    }
}
