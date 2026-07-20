//! Catalog reflection (research R3): one pg_catalog round trip per run →
//! `ReflectedTable` per selected relation. The reflected structure is the
//! authority for the published stream schema, column projection, and
//! cursor-column validation (contract rules 2–3).

use std::collections::BTreeMap;

use rdlt_connector::SourceError;
use tokio_postgres::Client;

use crate::config::{PostgresConfig, TableConfig};
use crate::errors::{self, Phase};
use crate::types::{MappedType, PgTypeInfo, map_type};

#[derive(Debug, Clone)]
pub struct ReflectedColumn {
    pub(crate) name: String,
    /// Postgres type name, diagnostics only.
    pub(crate) type_name: String,
    pub(crate) mapped: MappedType,
    pub(crate) not_null: bool,
    pub(crate) is_pk: bool,
}

impl ReflectedColumn {
    /// The Arrow type this column will carry on the structured path.
    pub fn arrow_type(&self) -> &arrow_schema::DataType {
        &self.mapped.arrow
    }

    pub fn is_not_null(&self) -> bool {
        self.not_null
    }
}

#[derive(Debug, Clone)]
pub struct ReflectedTable {
    pub(crate) name: String,
    pub(crate) columns: Vec<ReflectedColumn>,
}

impl ReflectedTable {
    pub fn primary_key(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|c| c.is_pk)
            .map(|c| c.name.as_str())
            .collect()
    }

    pub fn column(&self, name: &str) -> Option<&ReflectedColumn> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Columns after applying the table's include/exclude selection
    /// (contract rule 4: unknown names and empty results are typed errors).
    pub fn selected_columns(
        &self,
        config: Option<&TableConfig>,
    ) -> Result<Vec<&ReflectedColumn>, SourceError> {
        let (include, exclude) = match config {
            Some(t) => (t.included_columns.as_deref(), t.excluded_columns.as_deref()),
            None => (None, None),
        };
        for requested in include.iter().chain(exclude.iter()).flat_map(|v| v.iter()) {
            if self.column(requested).is_none() {
                return Err(errors::fatal(
                    Phase::Reflect,
                    Some(&self.name),
                    format!("selected column `{requested}` does not exist"),
                ));
            }
        }
        let selected: Vec<&ReflectedColumn> = self
            .columns
            .iter()
            .filter(|c| match (include, exclude) {
                (Some(inc), _) => inc.iter().any(|n| n == &c.name),
                (_, Some(exc)) => !exc.iter().any(|n| n == &c.name),
                _ => true,
            })
            .collect();
        if selected.is_empty() {
            return Err(errors::fatal(
                Phase::Reflect,
                Some(&self.name),
                "column selection leaves zero data columns",
            ));
        }
        Ok(selected)
    }
}

/// One round trip: every column of every relation in `schema` matching the
/// relkind filter, with type shape + PK membership, in attnum order. Domains
/// resolve one level to their base (nested domains fall to the textual
/// fallback — documented); the domain's own typmod wins when present.
const REFLECT_SQL: &str = r#"
SELECT c.relname::text                       AS table_name,
       a.attname::text                       AS column_name,
       a.atttypmod                           AS typmod,
       a.attnotnull                          AS not_null,
       t.oid::int8                           AS type_oid,
       t.typtype::text                       AS typtype,
       t.typcategory::text                   AS typcategory,
       t.typname::text                       AS type_name,
       t.typtypmod                           AS domain_typmod,
       bt.oid::int8                          AS base_oid,
       bt.typtype::text                      AS base_typtype,
       bt.typcategory::text                  AS base_typcategory,
       (pk.conkey IS NOT NULL AND a.attnum = ANY(pk.conkey)) AS is_pk
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
JOIN pg_type t ON t.oid = a.atttypid
LEFT JOIN pg_type bt ON t.typtype = 'd' AND bt.oid = t.typbasetype
LEFT JOIN (SELECT conrelid, conkey FROM pg_constraint WHERE contype = 'p') pk
       ON pk.conrelid = c.oid
WHERE n.nspname = $1
  AND c.relkind::text = ANY($2)
ORDER BY c.relname, a.attnum
"#;

pub(crate) async fn reflect_schema(
    client: &Client,
    config: &PostgresConfig,
) -> Result<BTreeMap<String, ReflectedTable>, SourceError> {
    let relkinds: Vec<&str> = if config.include_views {
        vec!["r", "p", "v", "m"]
    } else {
        vec!["r", "p"]
    };
    let rows = client
        .query(REFLECT_SQL, &[&config.schema, &relkinds])
        .await
        .map_err(|e| errors::classify(Phase::Reflect, None, &e))?;

    let mut tables: BTreeMap<String, ReflectedTable> = BTreeMap::new();
    for row in rows {
        let table_name: String = row.get("table_name");
        let column_name: String = row.get("column_name");
        let typmod: i32 = row.get("typmod");
        let not_null: bool = row.get("not_null");
        let type_oid: i64 = row.get("type_oid");
        let typtype: String = row.get("typtype");
        let typcategory: String = row.get("typcategory");
        let type_name: String = row.get("type_name");
        let is_pk: bool = row.get("is_pk");

        // Domains resolve one level; a domain's own typmod (typtypmod) wins
        // over the attribute's (-1 for domain attributes).
        let info = if typtype == "d" {
            let base_oid: Option<i64> = row.get("base_oid");
            let base_typtype: Option<String> = row.get("base_typtype");
            let base_typcategory: Option<String> = row.get("base_typcategory");
            let domain_typmod: i32 = row.get("domain_typmod");
            match (base_oid, base_typtype, base_typcategory) {
                // A nested domain (base is itself a domain) → textual fallback
                // via an OID no lossless arm matches.
                (Some(_), Some(bt), _) if bt == "d" => PgTypeInfo {
                    oid: 0,
                    typtype: 'b',
                    typcategory: 'X',
                    typmod: -1,
                },
                (Some(oid), Some(bt), Some(bc)) => PgTypeInfo {
                    oid: oid as u32,
                    typtype: bt.chars().next().unwrap_or('b'),
                    typcategory: bc.chars().next().unwrap_or('X'),
                    typmod: if domain_typmod != -1 {
                        domain_typmod
                    } else {
                        typmod
                    },
                },
                _ => PgTypeInfo {
                    oid: 0,
                    typtype: 'b',
                    typcategory: 'X',
                    typmod: -1,
                },
            }
        } else {
            PgTypeInfo {
                oid: type_oid as u32,
                typtype: typtype.chars().next().unwrap_or('b'),
                typcategory: typcategory.chars().next().unwrap_or('X'),
                typmod,
            }
        };

        tables
            .entry(table_name.clone())
            .or_insert_with(|| ReflectedTable {
                name: table_name,
                columns: Vec::new(),
            })
            .columns
            .push(ReflectedColumn {
                name: column_name,
                type_name,
                mapped: map_type(&info),
                not_null,
                is_pk,
            });
    }

    // Contract rule 2: every listed table must exist in the reflected set.
    if let Some(listed) = &config.tables {
        for t in listed {
            if !tables.contains_key(&t.name) {
                return Err(errors::fatal(
                    Phase::Reflect,
                    Some(&t.name),
                    format!(
                        "table not found in schema `{}` (include_views={})",
                        config.schema, config.include_views
                    ),
                ));
            }
        }
    }
    Ok(tables)
}

/// Contract rule 3: a configured cursor column must exist and be
/// cursor-capable. Returns the reflected column for downstream use.
pub(crate) fn validate_cursor_column<'t>(
    table: &'t ReflectedTable,
    cursor_column: &str,
) -> Result<&'t ReflectedColumn, SourceError> {
    let column = table.column(cursor_column).ok_or_else(|| {
        errors::fatal(
            Phase::Reflect,
            Some(&table.name),
            format!("cursor column `{cursor_column}` does not exist"),
        )
    })?;
    if !column.mapped.cursor_capable {
        return Err(errors::fatal(
            Phase::Reflect,
            Some(&table.name),
            format!(
                "cursor column `{cursor_column}` has type `{}` which is not cursor-capable \
                 (contract: type-mapping.md \"Cursor-capable types\")",
                column.type_name
            ),
        ));
    }
    Ok(column)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Decode, oid};

    fn table(cols: &[(&str, u32, bool)]) -> ReflectedTable {
        ReflectedTable {
            name: "t".into(),
            columns: cols
                .iter()
                .map(|(name, o, pk)| ReflectedColumn {
                    name: (*name).into(),
                    type_name: "x".into(),
                    mapped: map_type(&PgTypeInfo {
                        oid: *o,
                        typtype: 'b',
                        typcategory: 'X',
                        typmod: -1,
                    }),
                    not_null: false,
                    is_pk: *pk,
                })
                .collect(),
        }
    }

    #[test]
    fn selection_include_exclude_and_errors() {
        let t = table(&[
            ("a", oid::INT8, true),
            ("b", oid::TEXT, false),
            ("c", oid::BOOL, false),
        ]);
        let all = t.selected_columns(None).expect("all");
        assert_eq!(all.len(), 3);

        let cfg_inc = TableConfig {
            name: "t".into(),
            cursor: None,
            primary_key: None,
            included_columns: Some(vec!["a".into(), "c".into()]),
            excluded_columns: None,
        };
        let picked = t.selected_columns(Some(&cfg_inc)).expect("included");
        assert_eq!(
            picked.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            ["a", "c"]
        );

        let cfg_missing = TableConfig {
            included_columns: Some(vec!["nope".into()]),
            ..cfg_inc.clone()
        };
        assert!(t.selected_columns(Some(&cfg_missing)).is_err());

        let cfg_empty = TableConfig {
            included_columns: None,
            excluded_columns: Some(vec!["a".into(), "b".into(), "c".into()]),
            ..cfg_inc
        };
        assert!(t.selected_columns(Some(&cfg_empty)).is_err());
    }

    #[test]
    fn cursor_validation() {
        let t = table(&[("id", oid::INT8, true), ("blob", oid::BYTEA, false)]);
        assert_eq!(
            validate_cursor_column(&t, "id")
                .expect("capable")
                .mapped
                .decode,
            Decode::Int8
        );
        assert!(validate_cursor_column(&t, "blob").is_err());
        assert!(validate_cursor_column(&t, "ghost").is_err());
    }

    #[test]
    fn primary_key_extraction() {
        let t = table(&[
            ("id", oid::INT8, true),
            ("ts", oid::TIMESTAMPTZ, true),
            ("v", oid::TEXT, false),
        ]);
        assert_eq!(t.primary_key(), ["id", "ts"]);
    }
}
