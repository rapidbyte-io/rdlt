//! How a pipeline's document becomes what the engine runs: its plan, its configuration, and its
//! two connectors' references, by this path alone.
//!
//! A document is lowered in two passes. [`prepare`] checks and lowers everything that needs no
//! connector, before one is placed; [`Prepared::bind`] selects the streams from the source's
//! catalog, each taking what its entry lowered once.

mod settings;
mod streams;
#[cfg(test)]
mod tests;
mod transforms;

pub(crate) use streams::resolved;

use std::time::Duration;

use rdlt_connector::{Catalog, ConnectorId, PipelineId, Role};
use rdlt_engine::{CommitPolicy, EngineConfig, PipelinePlan, RetryPolicy};
use rdlt_host::{ConnectorRef, Digest};

use crate::error::noun;
use crate::{Connector, Document, Error, Host, Side, codes};

/// What the engine runs of a document: its plan and its configuration.
#[derive(Debug)]
pub(crate) struct Lowered {
    pub(crate) plan: PipelinePlan,
    pub(crate) config: EngineConfig,
}

/// A document checked and lowered as far as it can be without its connectors: its pipeline, the
/// engine's configuration, and each stream entry's transforms and hints, lowered once.
pub(crate) struct Prepared<'a> {
    document: &'a Document,
    pipeline: PipelineId,
    config: EngineConfig,
    entries: Vec<streams::Entry>,
}

/// `document` checked and lowered as far as it can be without its connectors: its version, its
/// pipeline's name, its commit, resources and retry, its connectors' references, and each stream
/// entry's name, hints and transforms, with mask keys resolved through `host`'s secrets, SQL
/// parsed and code transforms found among the host's; then its modes, keys, columns and
/// transforms as the engine checks them, each entry under the name it is written with.
pub(crate) async fn prepare<'a>(
    document: &'a Document,
    host: &Host,
) -> Result<Prepared<'a>, Error> {
    if document.version != 1 {
        let message = format!("a document's version is 1, not {}", document.version);
        return Err(Error::config(codes::DOCUMENT_VERSION, message));
    }
    let pipeline = PipelineId::parse(&document.pipeline).map_err(|error| {
        let message = format!("the pipeline's name is not one: {error}");
        Error::config_caused(codes::REFERENCE_INVALID, message, error)
    })?;
    let config = engine_config(document)?;
    reference(&document.source, Role::Source)?;
    reference(&document.destination, Role::Destination)?;
    #[cfg(feature = "local")]
    {
        host.admits_network(&document.source, Role::Source)?;
        host.admits_network(&document.destination, Role::Destination)?;
    }
    let mut entries = Vec::new();
    for stream in &document.streams {
        entries.push(streams::Entry::lowered(stream, host).await?);
    }
    let mut plans = Vec::new();
    for (stream, entry) in document.streams.iter().zip(&entries) {
        plans.push(entry.plan(entry.name().clone(), stream, &document.defaults)?);
    }
    PipelinePlan::new(pipeline.clone(), plans).map_err(Error::engine)?;
    Ok(Prepared {
        document,
        pipeline,
        config,
        entries,
    })
}

impl Prepared<'_> {
    /// How long a call into a connector may take, as the engine's configuration says.
    pub(crate) fn connector_wait(&self) -> Duration {
        self.config.connector_wait()
    }

    /// What the engine runs: the streams the document selects from `catalog`, the source's,
    /// each planned from its entry.
    pub(crate) fn bind(self, catalog: &Catalog) -> Result<Lowered, Error> {
        let document = self.document;
        let mut plans = Vec::new();
        for (name, index) in streams::selected(document, &self.entries, catalog)? {
            let (stream, entry) = (&document.streams[index], &self.entries[index]);
            plans.push(entry.plan(name, stream, &document.defaults)?);
        }
        let defaults = &document.defaults;
        let plan = PipelinePlan::new(self.pipeline, plans)
            .map_err(Error::engine)?
            .schema(settings::schema(defaults.schema.as_ref(), defaults.nested))
            .with_until(settings::until(document.until));
        Ok(Lowered {
            plan,
            config: self.config,
        })
    }
}

/// The engine's configuration `document`'s commit, resources and retry say.
fn engine_config(document: &Document) -> Result<EngineConfig, Error> {
    let mut builder = EngineConfig::builder();
    let resources = &document.resources;
    if let Some(memory) = resources.memory {
        builder = builder.memory(memory);
    }
    if let Some(writers) = resources.writers {
        builder = builder.writers(writers);
    }
    if let Some(partitions) = resources.partitions {
        builder = builder.partitions(partitions);
    }
    if let Some(commit) = &document.commit {
        let policy = CommitPolicy::new(commit.every, commit.rows, commit.bytes);
        builder = builder.commit(policy.map_err(Error::engine)?);
    }
    if let Some(retry) = &document.retry {
        let mut policy = RetryPolicy::default();
        if let Some(attempts) = retry.max_attempts {
            if attempts == 0 {
                let message = "a retry's max_attempts is at least 1, the run's first attempt";
                return Err(Error::config(rdlt_engine::codes::CONFIG_INVALID, message));
            }
            policy = policy.max_attempts(attempts);
        }
        if let Some(initial) = retry.initial {
            policy = policy.initial(initial);
        }
        if let Some(longest) = retry.max_delay {
            policy = policy.max_delay(longest);
        }
        builder = builder.retry(policy);
    }
    builder.build().map_err(Error::engine)
}

/// The reference `side`, the pipeline's `role`, names: its connector, what its placement must
/// honour, and what it may reach.
pub(crate) fn reference(side: &Side, role: Role) -> Result<ConnectorRef, Error> {
    let noun = noun(role);
    let id = ConnectorId::parse(side.connector.id()).map_err(|error| {
        let message = format!("the {noun} connector's id is not one: {error}");
        Error::config_caused(codes::REFERENCE_INVALID, message, error)
    })?;
    let mut connector = ConnectorRef::new(id);
    if let Connector::Reference(reference) = &side.connector {
        if let Some(version) = &reference.version {
            let required = semver::VersionReq::parse(version).map_err(|error| {
                let message = format!(
                    "the {noun} connector's version is no semver requirement, as ^1.2 is: {error}"
                );
                Error::config_caused(codes::REFERENCE_INVALID, message, error)
            })?;
            connector = connector.version(required);
        }
        if let Some(digest) = &reference.digest {
            let digest = digest_of(digest).ok_or_else(|| {
                let message =
                    format!("the {noun} connector's digest is not a SHA-256: 64 hex digits");
                Error::config(codes::REFERENCE_INVALID, message)
            })?;
            connector = connector.digest(digest);
        }
        if let Some(endpoint) = &reference.endpoint {
            connector = connector.endpoint(endpoint.clone());
        }
    }
    for path in &side.access.read {
        connector = connector.read(path);
    }
    for path in &side.access.write {
        connector = connector.write(path);
    }
    if side.access.network {
        connector = connector.network();
    }
    Ok(connector)
}

/// The SHA-256 `hex` writes as 64 hex digits.
fn digest_of(hex: &str) -> Option<Digest> {
    let digits: Vec<u8> = hex
        .chars()
        .map(|digit| {
            digit
                .to_digit(16)
                .and_then(|value| u8::try_from(value).ok())
        })
        .collect::<Option<_>>()?;
    if digits.len() != 64 {
        return None;
    }
    let mut digest = [0_u8; 32];
    for (byte, [high, low]) in digest.iter_mut().zip(digits.as_chunks::<2>().0) {
        *byte = (high << 4) | low;
    }
    Some(Digest(digest))
}
