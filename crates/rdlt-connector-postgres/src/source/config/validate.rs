//! The entry points and the gate: every constructor parses, then runs the
//! same local validation — shape rules checkable without a database; rules
//! that need the live catalog run at open, against reflection.

use std::collections::BTreeSet;

use super::*;

impl PostgresConfig {
    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigError> {
        let config: PostgresConfig = serde_yaml::from_str(yaml)?;
        config.validate()?;
        Ok(config)
    }

    /// JSON text form — same document shape and validation as YAML.
    pub fn from_json(json: &str) -> Result<Self, ConfigError> {
        let config: PostgresConfig = serde_json::from_str(json)?;
        config.validate()?;
        Ok(config)
    }

    /// The embedder entry point: a platform holding connector configs as
    /// JSON documents (validated against the connector's declared config
    /// schema, `ConnectorSpec`) passes the `serde_json::Value` directly —
    /// no string round-trip, same validation as every other entry point.
    pub fn from_value(value: serde_json::Value) -> Result<Self, ConfigError> {
        let config: PostgresConfig = serde_json::from_value(value)?;
        config.validate()?;
        Ok(config)
    }

    /// Local validation (shape rules checkable without the database); rules
    /// that need the live catalog run at open, against reflection. Split by
    /// concern — connection, cursors, CDC, and stream selection — each owning a
    /// coherent slice of the same typed errors.
    fn validate(&self) -> Result<(), ConfigError> {
        self.validate_conn()?;
        self.validate_cursors()?;
        self.validate_cdc()?;
        self.validate_tables()?;
        Ok(())
    }

    /// Top-level connection + batching scalars.
    fn validate_conn(&self) -> Result<(), ConfigError> {
        let invalid = |msg: String| Err(ConfigError::Invalid(msg));
        if self.conn.trim().is_empty() {
            return invalid("`conn` must not be empty".into());
        }
        // Parse failure = FATAL config error, up front — a malformed conn
        // string must never reach the Transient/retry path. The shared gate
        // also translates libpq's TLS parameter trio and names every rejected
        // parameter — no bare parse errors.
        if let Err(e) = crate::tls::parse_conn(&self.conn, self.tls.as_ref()) {
            return invalid(e.to_string());
        }
        if self.schema.trim().is_empty() {
            return invalid("`schema` must not be empty".into());
        }
        if self.batch_target_bytes == 0 || self.batch_max_rows == 0 {
            return invalid("batch knobs must be positive".into());
        }
        Ok(())
    }

    /// Cursor-shape rules checkable without the catalog.
    fn validate_cursors(&self) -> Result<(), ConfigError> {
        let invalid = |msg: String| Err(ConfigError::Invalid(msg));
        for cursor in self
            .tables
            .iter()
            .flatten()
            .filter_map(|t| t.cursor.as_ref())
            .chain(self.queries.iter().filter_map(|q| q.cursor.as_ref()))
        {
            if cursor.lag.is_some() && cursor.boundary == Bound::Exclusive {
                return invalid(format!(
                    "cursor `{}`: lag requires an INCLUSIVE boundary (an exclusive boundary \
                     skips the dedup that makes the lag window safe): ",
                    cursor.column
                ));
            }
        }
        Ok(())
    }

    /// The CDC block: required non-empty names + the cursor/cdc exclusivity.
    fn validate_cdc(&self) -> Result<(), ConfigError> {
        let invalid = |msg: String| Err(ConfigError::Invalid(msg));
        if let Some(cdc) = &self.cdc {
            for (field, value) in [
                ("cdc.slot", &cdc.slot),
                ("cdc.publication", &cdc.publication),
                ("cdc.flag_column", &cdc.flag_column),
            ] {
                if value.trim().is_empty() {
                    return invalid(format!("`{field}` must not be empty"));
                }
            }
            // CDC captures the CONFIGURED tables; selecting none captures
            // nothing, so the slot would never be preflighted or advanced and
            // the block would behave as if it were absent.
            if self.tables.as_ref().is_some_and(Vec::is_empty) {
                return invalid(
                    "`cdc` is configured but `tables` is empty — change data capture reads \
                     the configured tables, so selecting none captures nothing; list the \
                     tables to capture or remove the `cdc` block"
                        .into(),
                );
            }
            // CDC covers every configured table; a cursor block on any
            // of them is a contradiction, not an override.
            if let Some(table) = self.tables.iter().flatten().find(|t| t.cursor.is_some()) {
                return invalid(format!(
                    "table `{}`: `cursor` and `cdc` are mutually exclusive — with a \
                     `cdc:` block every configured table is captured through the \
                     replication slot",
                    table.name
                ));
            }
        }
        Ok(())
    }

    /// Query + table stream selection: unique names, no qualified table names,
    /// exclusive column selections, non-empty overrides.
    fn validate_tables(&self) -> Result<(), ConfigError> {
        let invalid = |msg: String| Err(ConfigError::Invalid(msg));
        {
            let mut names = BTreeSet::new();
            if let Some(tables) = &self.tables {
                for t in tables {
                    names.insert(t.name.as_str());
                }
            }
            for q in &self.queries {
                if q.name.trim().is_empty() {
                    return invalid("query with empty name".into());
                }
                if q.sql.trim().is_empty() {
                    return invalid(format!("query `{}`: empty sql", q.name));
                }
                if !names.insert(q.name.as_str()) {
                    return invalid(format!(
                        "stream name `{}` used by more than one table/query",
                        q.name
                    ));
                }
                if let Some(pk) = &q.primary_key
                    && pk.is_empty()
                {
                    return invalid(format!("query `{}`: primary_key present but empty", q.name));
                }
            }
        }
        if let Some(tables) = &self.tables {
            // An empty list selects no tables — legitimate alongside queries,
            // and the only way to say "deliver the declared queries and nothing
            // else". With no queries either, the run would select nothing at
            // all and silently move zero rows, so that combination is refused
            // here rather than at read time.
            if tables.is_empty() && self.queries.is_empty() {
                return invalid(
                    "no streams selected: `tables` is empty and no `queries` are declared — \
                     list the tables to read, declare a query, or omit `tables` to discover \
                     every table in the schema"
                        .into(),
                );
            }
            let mut seen = BTreeSet::new();
            for table in tables {
                if table.name.contains('.') {
                    return invalid(format!(
                        "table `{}`: schema-qualified names are rejected; `schema` owns qualification",
                        table.name
                    ));
                }
                if table.name.trim().is_empty() {
                    return invalid("table with empty name".into());
                }
                if !seen.insert(table.name.as_str()) {
                    return invalid(format!("table `{}` listed twice", table.name));
                }
                if table.included_columns.is_some() && table.excluded_columns.is_some() {
                    return invalid(format!(
                        "table `{}`: included_columns and excluded_columns are mutually exclusive",
                        table.name
                    ));
                }
                if let Some(cols) = table
                    .included_columns
                    .as_deref()
                    .or(table.excluded_columns.as_deref())
                    && cols.is_empty()
                {
                    return invalid(format!(
                        "table `{}`: column selection present but empty",
                        table.name
                    ));
                }
                if let Some(pk) = &table.primary_key
                    && pk.is_empty()
                {
                    return invalid(format!(
                        "table `{}`: primary_key present but empty",
                        table.name
                    ));
                }
            }
        }
        Ok(())
    }

    /// The per-table config for a stream name, when the user listed tables.
    pub(crate) fn table_config(&self, name: &str) -> Option<&TableConfig> {
        self.tables.as_ref()?.iter().find(|t| t.name == name)
    }

    pub(crate) fn query_config(&self, name: &str) -> Option<&QueryConfig> {
        self.queries.iter().find(|q| q.name == name)
    }

    /// A query stream's config viewed through the table-config shape, so the
    /// hint/selection/cursor machinery applies unchanged.
    pub(crate) fn synthesized_table_config(&self, name: &str) -> Option<TableConfig> {
        let q = self.query_config(name)?;
        Some(TableConfig {
            name: q.name.clone(),
            cursor: q.cursor.clone(),
            primary_key: q.primary_key.clone(),
            included_columns: None,
            excluded_columns: None,
            type_hints: q.type_hints.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_document_round_trips() {
        let c = PostgresConfig::from_yaml(
            r#"
conn: "postgresql://u:p@localhost/db"
schema: sales
include_views: true
batch_target_bytes: 1048576
batch_max_rows: 1000
tables:
  - name: orders
    cursor:
      column: updated_at
      initial_value: "2026-01-01T00:00:00Z"
      boundary: exclusive
      direction: min
      end_value: "2027-01-01T00:00:00Z"
      nulls: include
    primary_key: [id]
    excluded_columns: [internal_notes]
  - name: customers
"#,
        )
        .expect("full config");
        let orders = c.table_config("orders").expect("orders");
        let cursor = orders.cursor.as_ref().expect("cursor");
        assert_eq!(cursor.boundary, Bound::Exclusive);
        assert_eq!(cursor.direction, Direction::Min);
        assert_eq!(cursor.nulls, NullPolicy::Include);
        assert!(
            c.table_config("customers")
                .expect("customers")
                .cursor
                .is_none()
        );
    }

    // ---- lag vocabulary + validation ----

    #[test]
    fn lag_vocabulary_round_trips_and_rejects() {
        use crate::source::type_map::Decode;
        // Duration forms.
        assert_eq!("90s".parse::<Lag>().unwrap(), Lag::Duration { seconds: 90 });
        assert_eq!("5m".parse::<Lag>().unwrap(), Lag::Duration { seconds: 300 });
        assert_eq!(
            "2h".parse::<Lag>().unwrap(),
            Lag::Duration { seconds: 7200 }
        );
        assert_eq!(
            "1d".parse::<Lag>().unwrap(),
            Lag::Duration { seconds: 86_400 }
        );
        // Magnitudes.
        assert_eq!(
            "1000".parse::<Lag>().unwrap(),
            Lag::Magnitude("1000".into())
        );
        assert_eq!("0.5".parse::<Lag>().unwrap(), Lag::Magnitude("0.5".into()));
        // Rejections: zero, negative, garbage, empty.
        for bad in ["0s", "-5m", "soon", "", "5 m"] {
            assert!(bad.parse::<Lag>().is_err(), "{bad}");
        }
        // Display round-trips through FromStr semantically.
        let lag: Lag = "5m".parse().unwrap();
        assert_eq!(lag.to_string().parse::<Lag>().unwrap(), lag);

        // sql_delta family matrix.
        let five_m = Lag::Duration { seconds: 300 };
        assert_eq!(
            five_m.sql_delta(Decode::Timestamp { tz: true }).unwrap(),
            "INTERVAL '300 seconds'"
        );
        let two_d = Lag::Duration { seconds: 172_800 };
        assert_eq!(two_d.sql_delta(Decode::Date).unwrap(), "2::int4");
        assert!(five_m.sql_delta(Decode::Date).is_err(), "sub-day on date");
        let thousand = Lag::Magnitude("1000".into());
        assert_eq!(thousand.sql_delta(Decode::Int8).unwrap(), "1000::int8");
        let half = Lag::Magnitude("0.5".into());
        assert!(half.sql_delta(Decode::Int8).is_err(), "fractional on int");
        assert_eq!(
            half.sql_delta(Decode::Decimal {
                precision: 10,
                scale: 2
            })
            .unwrap(),
            "'0.5'::numeric"
        );
        // Undefined families and unit mismatches.
        assert!(five_m.sql_delta(Decode::Utf8).is_err(), "text cursor");
        assert!(thousand.sql_delta(Decode::Timestamp { tz: false }).is_err());
        assert!(five_m.sql_delta(Decode::Int8).is_err());
    }
}
