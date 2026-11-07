//! A document's streams: each entry lowered once, the streams the pipeline selects from the
//! source's catalog, and each one's plan.

use std::collections::BTreeSet;

use rdlt_connector::{Catalog, LogicalType, StreamName, StreamSpec};
use rdlt_engine::StreamPlan;
use wildmatch::WildMatch;

use super::{settings, transforms};
use crate::{Defaults, Document, Error, Host, Stream, WriteMode, codes};

/// A stream entry of a document, lowered once: what every stream it selects is planned from.
pub(super) struct Entry {
    /// The name the entry is written with: a stream's, or a pattern.
    name: StreamName,
    /// Each column's fixed type.
    hints: Vec<(String, LogicalType)>,
    /// The transforms, checked once: mask keys resolved, SQL parsed and code found.
    steps: Vec<transforms::Checked>,
}

impl Entry {
    /// `stream` lowered: its name checked, its hints' types and its transforms, which `host`'s
    /// secrets and code complete.
    pub(super) async fn lowered(stream: &Stream, host: &Host) -> Result<Self, Error> {
        let name = StreamName::new(&stream.name).map_err(|error| {
            let message = format!("a stream's name is not one: {error}");
            Error::config_caused(codes::STREAM_INVALID, message, error)
        })?;
        let mut hints = Vec::new();
        for (column, entry) in &stream.columns {
            if let Some(hint) = entry.hint {
                let hinted =
                    settings::logical(hint).map_err(|error| error.in_stream(&stream.name))?;
                hints.push((column.clone(), hinted));
            }
        }
        let steps = transforms::lowered(&stream.transform, host).await;
        let steps = steps.map_err(|error| error.in_stream(&stream.name))?;
        Ok(Self { name, hints, steps })
    }

    /// The name the entry is written with.
    pub(super) fn name(&self) -> &StreamName {
        &self.name
    }

    /// The plan of stream `name`, which the entry `stream` selects, under `defaults`, with a SQL
    /// transform of its own for each SQL step.
    pub(super) fn plan(
        &self,
        name: StreamName,
        stream: &Stream,
        defaults: &Defaults,
    ) -> Result<StreamPlan, Error> {
        let mut plan = StreamPlan::new(name);
        if let Some(read) = stream.read.or(defaults.read) {
            plan = plan.read(settings::read(read));
        }
        plan = match stream.write.as_ref().or(defaults.write.as_ref()) {
            None => plan,
            Some(WriteMode::Append) => plan.write(rdlt_engine::WriteMode::Append),
            Some(WriteMode::Replace) => plan.write(rdlt_engine::WriteMode::Replace),
            Some(WriteMode::Merge { key }) => keyed(plan.write(rdlt_engine::WriteMode::Merge), key),
            Some(WriteMode::History { key }) => {
                keyed(plan.write(rdlt_engine::WriteMode::History), key)
            }
        };
        if let Some(deletes) = stream.deletes {
            plan = plan.deletes(settings::deletes(deletes));
        }
        if let Some(truncates) = stream.on_truncate {
            plan = plan.on_truncate(settings::truncates(truncates));
        }
        if let Some(loss) = stream.on_retention_loss {
            plan = plan.on_retention_loss(settings::retention(loss));
        }
        if stream.schema.is_some() || stream.nested.is_some() {
            plan = plan.schema(settings::schema(stream.schema.as_ref(), stream.nested));
        }
        for (column, entry) in &stream.columns {
            if let Some(set) = settings::column(*entry) {
                plan = plan.column(column.as_str(), set);
            }
        }
        for (column, hinted) in &self.hints {
            plan = plan.hint(column.as_str(), hinted.clone());
        }
        if !stream.included.is_empty() {
            plan = plan.include_only(stream.included.iter().map(String::as_str));
        }
        if !stream.exclude.is_empty() {
            plan = plan.exclude(stream.exclude.iter().map(String::as_str));
        }
        let steps =
            transforms::bound(&self.steps).map_err(|error| error.in_stream(&stream.name))?;
        for step in steps {
            plan = plan.transform(step);
        }
        Ok(plan)
    }
}

/// `plan`, matching rows by `key` where the document names one, and by the source's primary key
/// otherwise.
fn keyed(plan: StreamPlan, key: &[String]) -> StreamPlan {
    if key.is_empty() {
        plan
    } else {
        plan.key(key.iter().map(String::as_str))
    }
}

/// Whether `name` selects by a pattern.
fn is_pattern(name: &str) -> bool {
    name.contains(['*', '?'])
}

/// Whether `stream` is the stream `name` names, as a catalog shows it: its namespace, a dot and
/// its name, or its name alone.
fn displays_as(stream: &StreamName, name: &str) -> bool {
    match stream.namespace() {
        Some(namespace) => name
            .strip_prefix(namespace)
            .and_then(|rest| rest.strip_prefix('.'))
            .is_some_and(|rest| rest == stream.name()),
        None => name == stream.name(),
    }
}

/// The stream of `catalog` that shows as `name`, if one does.
///
/// # Errors
///
/// `stream_ambiguous` where two streams show as `name`, a namespace `a.b` with a name `c` and a
/// namespace `a` with a name `b.c`.
pub(crate) fn resolved(name: &str, catalog: &Catalog) -> Result<Option<StreamName>, Error> {
    let mut showing = catalog
        .iter()
        .map(StreamSpec::name)
        .filter(|stream| displays_as(stream, name));
    match (showing.next(), showing.next()) {
        (Some(_), Some(_)) => {
            let message = format!("stream {name}: two streams of the source's catalog show as it");
            Err(Error::config(rdlt_engine::codes::STREAM_AMBIGUOUS, message).in_stream(name))
        }
        (found, _) => Ok(found.cloned()),
    }
}

/// The streams `document` selects from `catalog`, each with its entry's place in `entries`:
/// those named outright, in the document's order, then each stream of `catalog` a pattern
/// matches and none named, in the catalog's order, under the first pattern that matches it.
///
/// A name is the catalog's stream that shows as it; one the catalog lacks is selected as its
/// entry reads, for the engine's check to refuse.
///
/// # Errors
///
/// `stream_ambiguous` for a name two of the catalog's streams show as, and `pattern_unmatched`
/// for a pattern that matches none of them.
pub(super) fn selected(
    document: &Document,
    entries: &[Entry],
    catalog: &Catalog,
) -> Result<Vec<(StreamName, usize)>, Error> {
    let (mut named, mut patterns) = (Vec::new(), Vec::new());
    for (index, stream) in document.streams.iter().enumerate() {
        if is_pattern(&stream.name) {
            patterns.push((index, stream));
        } else {
            named.push((index, stream));
        }
    }
    let mut chosen = Vec::new();
    for (index, stream) in named {
        let name = resolved(&stream.name, catalog)?;
        chosen.push((name.unwrap_or_else(|| entries[index].name.clone()), index));
    }
    let shown: Vec<(&StreamName, String)> = catalog
        .iter()
        .map(|spec| (spec.name(), spec.name().to_string()))
        .collect();
    let mut matches = Vec::new();
    for (index, stream) in patterns {
        let pattern = WildMatch::new(&stream.name);
        if !shown.iter().any(|(_, text)| pattern.matches(text)) {
            let message = format!(
                "stream pattern {} matches no stream of the source's catalog",
                stream.name
            );
            return Err(Error::config(codes::PATTERN_UNMATCHED, message).in_stream(&stream.name));
        }
        matches.push((pattern, index));
    }
    let named: BTreeSet<StreamName> = chosen.iter().map(|(name, _)| name.clone()).collect();
    for (name, text) in shown {
        if named.contains(name) {
            continue;
        }
        if let Some((_, index)) = matches.iter().find(|(pattern, _)| pattern.matches(&text)) {
            chosen.push((name.clone(), *index));
        }
    }
    Ok(chosen)
}
