//! CDC-composition advisories: the pipeline still runs in any shape, but the
//! exactly-once-outcome composition has three legs, and a missing leg WARNS
//! (never blocks) so a change-data-capture pipeline that will silently append
//! or soft-delete says so up front.

use rdlt::connector::postgres::source::Config as PostgresConfig;
use rdlt::pipeline_spec::{DestSpec, Spec, WriteModeSpec};

/// The exactly-once-outcome CDC composition is `write_mode = merge{key}` +
/// destination `merge_strategy = upsert` + `hard_delete = <flag column>`. Its
/// absence WARNS, never blocks — other shapes still run, but as at-least-once
/// delivery and/or soft-delete (deletions kept as flagged rows).
pub fn cdc_composition_warnings(spec: &Spec, config: &PostgresConfig) -> Vec<String> {
    let Some(cdc) = &config.cdc else {
        return Vec::new();
    };
    let mut warnings = Vec::new();
    if !matches!(spec.write_mode, Some(WriteModeSpec::Merge { .. })) {
        warnings.push(
            "cdc: write_mode is not merge — changed rows will append instead of \
             converging; set write_mode: {merge: {key: [...]}}"
                .to_string(),
        );
    }
    match &spec.destination {
        // Every destination on the shared SQL merge core takes the same
        // options vocabulary; ONE checker, so the advisories cannot
        // drift apart per destination. (An earlier version of this file
        // claimed snowflake and duckdb "cannot remove a flagged row" —
        // false since both ride sqlcore's hard-delete arms — which is
        // exactly the drift one implementation prevents.)
        DestSpec::Postgres(dest) => sql_destination_warnings(
            &mut warnings,
            config,
            &cdc.flag_column,
            matches!(
                dest.merge_strategy,
                Some(rdlt::connector::postgres::destination::MergeStrategy::Upsert)
            ),
            &|table| dest.tables.get(table).and_then(|t| t.hard_delete.clone()),
        ),
        DestSpec::Duckdb(dest) => sql_destination_warnings(
            &mut warnings,
            config,
            &cdc.flag_column,
            matches!(
                dest.merge_strategy,
                Some(rdlt::connector::postgres::destination::MergeStrategy::Upsert)
            ),
            // `tables` stays `Option` on duckdb's config (postgres's is
            // a plain map) — an absent map means no per-table options,
            // so no hard_delete wiring.
            &|table| {
                dest.tables
                    .as_ref()
                    .and_then(|t| t.get(table))
                    .and_then(|t| t.hard_delete.clone())
            },
        ),
        DestSpec::Snowflake(dest) => sql_destination_warnings(
            &mut warnings,
            config,
            &cdc.flag_column,
            matches!(
                dest.options.merge_strategy,
                Some(rdlt::connector::postgres::destination::MergeStrategy::Upsert)
            ),
            &|table| {
                dest.options
                    .tables
                    .get(table)
                    .and_then(|t| t.hard_delete.clone())
            },
        ),
        // These genuinely have no delete path: files are append/replace
        // media, and iceberg ships append-only this release.
        DestSpec::Parquet { .. } | DestSpec::File(_) | DestSpec::Iceberg(_) => {
            warnings.push(format!(
                "cdc: this destination has no hard-delete support — the \
                 deletion flag `{}` lands as data (soft delete); deletes are \
                 kept as flagged rows, not removed",
                cdc.flag_column
            ));
        }
        // An out-of-process connector's config is OPAQUE here by design
        // (only the connector knows its vocabulary), so the composition
        // cannot be inspected — say so rather than staying silent or
        // guessing.
        DestSpec::Connector(reference) => {
            warnings.push(format!(
                "cdc: destination `{}` is an out-of-process connector — its \
                 options are opaque here, so the CDC composition cannot be \
                 checked; ensure it merges with the deletion flag `{}` wired \
                 as a hard delete, or deletes land as flagged rows",
                reference.id, cdc.flag_column
            ));
        }
    }
    warnings
}

/// The merge-core composition check, shared by every SQL destination:
/// upsert strategy recommended, and each CDC table needs the flag
/// column wired as its hard_delete.
fn sql_destination_warnings(
    warnings: &mut Vec<String>,
    config: &PostgresConfig,
    flag_column: &str,
    strategy_is_upsert: bool,
    hard_delete_of: &dyn Fn(&str) -> Option<String>,
) {
    if !strategy_is_upsert {
        warnings.push(
            "cdc: destination merge_strategy is not upsert — the \
             recommended composition is merge_strategy = \"upsert\""
                .to_string(),
        );
    }
    match &config.tables {
        // Schema-wide discovery: the table set is unknown here, so the
        // per-table check below can't run — emit one generic notice
        // rather than staying silent about the missing hard_delete.
        None => warnings.push(format!(
            "cdc: schema-wide discovery (no `tables:` list) — give every \
             CDC table hard_delete = \"{}\" in the destination options, \
             or deletes land as flagged rows (soft delete) instead of \
             removals",
            flag_column
        )),
        Some(listed) => {
            for table in listed {
                let has_flag = hard_delete_of(&table.name).as_deref() == Some(flag_column);
                if !has_flag {
                    warnings.push(format!(
                        "cdc: table `{}` has no hard_delete = \"{}\" — \
                         deletes will land as flagged rows (soft delete) \
                         instead of removals",
                        table.name, flag_column
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdlt::sdk::config::Document;

    fn spec(yaml: &str) -> Spec {
        serde_yaml::from_str(yaml).expect("spec parses")
    }

    fn cdc_config() -> PostgresConfig {
        PostgresConfig::from_yaml(
            "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n\
             tables:\n  - name: orders\n",
        )
        .expect("config")
    }

    /// CDC-composition warning matrix: the recommended composition is silent;
    /// every missing leg warns with the fix; non-merge destinations warn soft
    /// delete.
    #[test]
    fn cdc_composition_warning_matrix() {
        let recommended = spec(
            r#"
pipeline: p
write_mode: {merge: {key: [id]}}
source:
  postgres: {config: src.yaml}
destination:
  postgres:
    conn: host=x
    dataset: d
    merge_strategy: upsert
    tables:
      orders: {hard_delete: _rdlt_deleted}
"#,
        );
        assert!(cdc_composition_warnings(&recommended, &cdc_config()).is_empty());

        let append = spec(
            r#"
pipeline: p
source:
  postgres: {config: src.yaml}
destination:
  postgres: {conn: host=x, dataset: d}
"#,
        );
        let warnings = cdc_composition_warnings(&append, &cdc_config());
        assert_eq!(warnings.len(), 3, "{warnings:?}");
        assert!(warnings[0].contains("write_mode"), "{warnings:?}");
        assert!(warnings[1].contains("upsert"), "{warnings:?}");
        assert!(
            warnings[2].contains("`orders`") && warnings[2].contains("hard_delete"),
            "{warnings:?}"
        );

        // duckdb and snowflake ride the SAME merge core as postgres: a
        // correctly-composed pipeline warns NOTHING (an earlier version
        // of the advisory falsely told them they could not hard-delete),
        // and a bare one gets the same two composition warnings.
        let duckdb_recommended = spec(
            r#"
pipeline: p
write_mode: {merge: {key: [id]}}
source:
  postgres: {config: src.yaml}
destination:
  duckdb:
    path: out.db
    merge_strategy: upsert
    tables:
      orders: {hard_delete: _rdlt_deleted}
"#,
        );
        assert!(
            cdc_composition_warnings(&duckdb_recommended, &cdc_config()).is_empty(),
            "a correct duckdb composition must not be told it soft-deletes"
        );
        let snowflake_recommended = spec(
            r#"
pipeline: p
write_mode: {merge: {key: [id]}}
source:
  postgres: {config: src.yaml}
destination:
  snowflake:
    account: MYORG-MYACCT
    user: u
    auth: {password: {password: x}}
    database: DB
    schema: S
    merge_strategy: upsert
    tables:
      orders: {hard_delete: _rdlt_deleted}
"#,
        );
        assert!(
            cdc_composition_warnings(&snowflake_recommended, &cdc_config()).is_empty(),
            "a correct snowflake composition must not be told it soft-deletes"
        );
        let duckdb_bare = spec(
            r#"
pipeline: p
write_mode: {merge: {key: [id]}}
source:
  postgres: {config: src.yaml}
destination:
  duckdb: {path: out.db}
"#,
        );
        let warnings = cdc_composition_warnings(&duckdb_bare, &cdc_config());
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        assert!(warnings[0].contains("upsert"), "{warnings:?}");
        assert!(warnings[1].contains("hard_delete"), "{warnings:?}");

        // A destination with genuinely no delete path keeps the honest
        // soft-delete warning.
        let file_dest = spec(
            r#"
pipeline: p
write_mode: {merge: {key: [id]}}
source:
  postgres: {config: src.yaml}
destination:
  parquet: {path: out}
"#,
        );
        let warnings = cdc_composition_warnings(&file_dest, &cdc_config());
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("soft delete"), "{warnings:?}");

        // Schema-wide discovery (no tables list): the hard_delete leg still
        // warns — once, generically.
        let schema_wide =
            PostgresConfig::from_yaml("conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n")
                .expect("config");
        let recommended_no_tables = spec(
            r#"
pipeline: p
write_mode: {merge: {key: [id]}}
source:
  postgres: {config: src.yaml}
destination:
  postgres: {conn: host=x, dataset: d, merge_strategy: upsert}
"#,
        );
        let warnings = cdc_composition_warnings(&recommended_no_tables, &schema_wide);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].contains("schema-wide") && warnings[0].contains("hard_delete"),
            "{warnings:?}"
        );

        // No cdc block: silent regardless of shape.
        let plain = PostgresConfig::from_yaml("conn: host=localhost\n").expect("config");
        assert!(cdc_composition_warnings(&append, &plain).is_empty());
    }
}
