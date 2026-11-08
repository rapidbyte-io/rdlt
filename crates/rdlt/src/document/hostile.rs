//! A document read as hostile input: bounded in size, depth and nodes, with no anchor, alias,
//! tag or second document before serde sees any of it, and no merge key or repeated key read.

#[cfg(test)]
mod tests;

use serde::de::DeserializeOwned;
use serde_saphyr::granit_parser::{ErrorKind, Event, Marker, Parser};
use serde_saphyr::{DuplicateKeyPolicy, MergeKeyPolicy, Options};

use super::fault::{Fault, Place, Refused};
use super::read::Mapped;
use crate::limits::{DOCUMENT_BYTES, DOCUMENT_DEPTH, DOCUMENT_NODES};

/// `text` as a `T`, a mapping, read under the document's limits: a fault is placed and named by
/// its key path, never by a value.
pub(crate) fn read<T: DeserializeOwned>(text: &str) -> Result<T, Fault> {
    if text.len() > DOCUMENT_BYTES {
        return Err(Fault::too_large());
    }
    screened(text)?;
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
        .map_err(|error| Fault::of(&error, path))
}

/// Refuses, at the event that holds it, what the reader reports without a place of its own: a
/// tag on any node, an anchor, an alias, a second document, and nesting or nodes past the limits.
///
/// A syntax error is left for the reader to report.
fn screened(text: &str) -> Result<(), Fault> {
    let events = serde_saphyr::granit_parser::options! {
        emit_unresolved_aliases: true,
        emit_anchor_events: true,
    };
    let (mut documents, mut depth, mut nodes) = (0_usize, 0_usize, 0_usize);
    for parsed in Parser::new_from_str_with_options(text, events) {
        let (event, span) = match parsed {
            Ok(parsed) => parsed,
            // Scanning ahead met nesting past the parser's own limit before an event for it.
            Err(error) if matches!(error.kind(), ErrorKind::RecursionLimitExceeded) => {
                return Err(Fault::too_deep(place(error.marker())));
            }
            Err(_) => return Ok(()),
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
            Event::SequenceStart(..) | Event::MappingStart(..) => depth += 1,
            Event::SequenceEnd | Event::MappingEnd => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth > DOCUMENT_DEPTH || nodes > DOCUMENT_NODES {
            return Err(Fault::too_deep(at));
        }
    }
    Ok(())
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
