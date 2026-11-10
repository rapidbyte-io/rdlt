//! The names a connector gives what it offers, refused where one contains a secret it was sent:
//! a name is shown as it is in reports, events, metrics, spans and table names, and scrubbing
//! it would rename what it names.

#[cfg(test)]
mod tests;

use rdlt_connector::{
    Catalog, ColumnPath, ConnectorError, Field, HostRefusal, LogicalType, Partition, PartitionId,
    PartitionPlan, Result,
};

use super::Redactions;

/// The host's code for a catalog or partition plan it refused, a name in it containing a secret
/// its connector was sent; an error's code, as a [`HostRefusal`] gives it, never its connector's.
pub const NAMED_BY_SECRET: &str = "named_by_secret";

/// `catalog`, unless a stream's name or namespace, a field's name at any depth, or a column its
/// key or change time names contains what `redactions` holds.
pub(crate) fn catalog(catalog: Catalog, redactions: &Redactions) -> Result<Catalog> {
    for stream in catalog.iter() {
        let mut names: Vec<&str> = vec![stream.name().name()];
        names.extend(stream.name().namespace());
        for field in stream
            .schema()
            .into_iter()
            .flat_map(|schema| schema.fields().iter())
        {
            fields(field, &mut names);
        }
        let keyed = stream.primary_key().into_iter().flatten();
        let columns = keyed.chain(stream.change_time());
        names.extend(columns.flat_map(ColumnPath::segments));
        if names.into_iter().any(|name| redactions.holds(name)) {
            return Err(refused(
                "a stream's, a namespace's or a column's name in the catalog contains a secret the \
                 connector was sent",
            ));
        }
    }
    Ok(catalog)
}

/// `plan`, unless a partition's id contains what `redactions` holds.
pub(crate) fn plan(plan: PartitionPlan, redactions: &Redactions) -> Result<PartitionPlan> {
    let planned = plan.partitions.iter().map(Partition::id);
    let mut ids = planned.chain(plan.starts.keys()).map(PartitionId::as_str);
    if ids.any(|id| redactions.holds(id)) {
        return Err(refused(
            "a partition's id in the plan contains a secret the connector was sent",
        ));
    }
    Ok(plan)
}

/// `field`'s name and those of the fields within it, added to `names`.
fn fields<'a>(field: &'a Field, names: &mut Vec<&'a str>) {
    names.push(field.name());
    match field.logical_type() {
        LogicalType::Struct(within) => {
            for field in within.iter() {
                fields(field, names);
            }
        }
        LogicalType::List(item) => fields(item, names),
        _ => {}
    }
}

fn refused(message: &str) -> ConnectorError {
    ConnectorError::config(message).with_source(HostRefusal::new(NAMED_BY_SECRET))
}
