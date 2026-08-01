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
        DestSpec::Postgres {
            merge_strategy,
            tables,
            ..
        } => {
            if !matches!(
                merge_strategy,
                Some(rdlt::connector::postgres::destination::MergeStrategy::Upsert)
            ) {
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
                    cdc.flag_column
                )),
                Some(listed) => {
                    for table in listed {
                        let has_flag = tables
                            .as_ref()
                            .and_then(|t| t.get(&table.name))
                            .and_then(|t| t.hard_delete.as_deref())
                            == Some(cdc.flag_column.as_str());
                        if !has_flag {
                            warnings.push(format!(
                                "cdc: table `{}` has no hard_delete = \"{}\" — \
                                 deletes will land as flagged rows (soft delete) \
                                 instead of removals",
                                table.name, cdc.flag_column
                            ));
                        }
                    }
                }
            }
        }
        // Snowflake sits here only while its merge dialect is unbuilt: it
        // accepts the shared options vocabulary, so once merges execute it
        // belongs with the arm above, checking merge_strategy and hard_delete.
        // Until then it genuinely cannot remove a flagged row, and saying so
        // is the honest warning.
        DestSpec::Duckdb { .. }
        | DestSpec::Parquet { .. }
        | DestSpec::File(_)
        | DestSpec::Iceberg(_)
        | DestSpec::Snowflake(_) => {
            warnings.push(format!(
                "cdc: this destination has no hard-delete support — the \
                 deletion flag `{}` lands as data (soft delete); deletes are \
                 kept as flagged rows, not removed",
                cdc.flag_column
            ));
        }
    }
    warnings
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

        let duckdb = spec(
            r#"
pipeline: p
write_mode: {merge: {key: [id]}}
source:
  postgres: {config: src.yaml}
destination:
  duckdb: {path: out.db}
"#,
        );
        let warnings = cdc_composition_warnings(&duckdb, &cdc_config());
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
