//! THE oracle-rs boundary: connect with the pinned session, the
//! poison-on-error rule, ORA-code classification, and row→JSON
//! conversion. Library types stop at this module's edge.
//!
//! POISON-ON-ERROR is the boundary's one law (T001, probed): this
//! driver leaves a connection in an undefined protocol state after
//! ANY failed statement — a later call may hang, error strangely, or
//! be reset by the server. So a connection that has seen an error is
//! NEVER reused: the client consumes itself on failure and the caller
//! reconnects. Classification comes from the STRUCTURED ORA code —
//! never message text.

use rdlt_connector_sdk::spi::SourceError;

use super::config::Config;

/// One live connection with the pinned session settings.
pub(crate) struct Client {
    conn: oracle_rs::connection::Connection,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

impl Client {
    /// Connect and pin the session: the time zone is UTC so
    /// `TIMESTAMP WITH LOCAL TIME ZONE` values do not depend on the
    /// client environment.
    pub(crate) async fn connect(config: &Config) -> Result<Self, SourceError> {
        let connect_string = format!("{}:{}/{}", config.host, config.port, config.service);
        let conn = oracle_rs::connection::Connection::connect(
            &connect_string,
            &config.user,
            config.password.reveal(),
        )
        .await
        .map_err(|e| classify(&format!("connecting to `{connect_string}`"), e))?;
        let client = Self { conn };
        client.pin_session().await?;
        Ok(client)
    }

    /// Pin the session to UTC and VERIFY it by reading the setting
    /// back.
    ///
    /// The verification is not belt-and-braces: the container's own
    /// session zone is whatever the host offers (+02:00 on the probe
    /// machine), so an unpinned session renders
    /// `TIMESTAMP WITH LOCAL TIME ZONE` differently per machine. And
    /// the driver mis-parses the ALTER's RESPONSE intermittently
    /// (`InvalidLengthIndicator`, probed) while the statement itself
    /// always lands — so trusting the response would fail healthy
    /// connections, and ignoring it blindly would hide a real miss.
    /// Reading the effect back settles both.
    async fn pin_session(&self) -> Result<(), SourceError> {
        let _ = self
            .conn
            .execute("ALTER SESSION SET TIME_ZONE='UTC'", &[])
            .await;
        let observed = self
            .conn
            .query("SELECT SESSIONTIMEZONE FROM DUAL", &[])
            .await
            .map_err(|e| classify("reading back the session time zone", e))?;
        let zone = observed
            .rows
            .first()
            .and_then(|row| row.values().first())
            .and_then(|value| match value {
                oracle_rs::row::Value::String(s) => Some(s.trim().to_owned()),
                _ => None,
            })
            .unwrap_or_default();
        if zone != "UTC" {
            return Err(SourceError::fatal(format!(
                "the session time zone is `{zone}` after pinning it to UTC —                  TIMESTAMP WITH LOCAL TIME ZONE values would depend on the client's                  environment; refusing to read"
            )));
        }
        Ok(())
    }

    /// Run one bounded query, consuming the client on failure (the
    /// poison rule). Success hands the client back for the next page.
    pub(crate) async fn query(
        self,
        context: &str,
        sql: &str,
        params: &[oracle_rs::row::Value],
    ) -> Result<(Self, oracle_rs::connection::QueryResult), SourceError> {
        match self.conn.query(sql, params).await {
            Ok(result) => Ok((self, result)),
            // `self` drops here: the connection is poisoned by its own
            // failure and must never answer again.
            Err(e) => Err(classify(context, e)),
        }
    }

    /// Fetch one LOB's content through the driver's locator read —
    /// the ONLY way LOB data crosses the wire (probed: SELECT returns
    /// a locator at every size). Chunked above a megabyte because the
    /// driver rescans its whole accumulation buffer per packet, which
    /// is quadratic on a large single read.
    pub(crate) async fn read_lob(
        &self,
        value: &oracle_rs::row::Value,
    ) -> Result<serde_json::Value, SourceError> {
        use oracle_rs::row::Value;
        use oracle_rs::{LobData, LobValue};

        const CHUNK: u64 = 1 << 20;
        let Value::Lob(lob) = value else {
            // Not a locator after all (an empty LOB reads as NULL):
            // fall back to the ordinary rendering.
            return value_to_json(value).map_err(SourceError::fatal);
        };
        let locator = match lob {
            LobValue::Locator(locator) => locator,
            // Already inline: render it directly.
            other => {
                return match other.as_string() {
                    Ok(Some(text)) => Ok(serde_json::Value::String(text)),
                    Ok(None) => Ok(serde_json::Value::Null),
                    Err(e) => Err(SourceError::fatal(format!("reading an inline LOB: {e}"))),
                };
            }
        };
        if locator.is_clob() {
            let mut text = String::new();
            self.conn
                .read_lob_chunked(locator, CHUNK, |chunk| {
                    if let LobData::String(piece) = &chunk {
                        text.push_str(piece);
                    }
                    async { Ok(()) }
                })
                .await
                .map_err(|e| classify("reading a CLOB", e))?;
            Ok(serde_json::Value::String(text))
        } else {
            let mut bytes = Vec::new();
            self.conn
                .read_lob_chunked(locator, CHUNK, |chunk| {
                    if let LobData::Bytes(piece) = &chunk {
                        bytes.extend_from_slice(piece);
                    }
                    async { Ok(()) }
                })
                .await
                .map_err(|e| classify("reading a BLOB", e))?;
            Ok(serde_json::Value::String(
                bytes.iter().map(|b| format!("{b:02x}")).collect(),
            ))
        }
    }
}

/// The classification rulebook: keyed on the STRUCTURED ORA code.
/// Everything the environment can heal is transient — and because
/// the caller reconnects after ANY error, transient additionally
/// covers the whole connection-death family (the reconnect IS the
/// recovery). Unknown codes default fatal: retrying an undiagnosed
/// failure hides it.
pub(crate) fn classify(context: &str, e: oracle_rs::error::Error) -> SourceError {
    use oracle_rs::error::Error;
    match &e {
        Error::OracleError { code, .. } => {
            if TRANSIENT_ORA.contains(code) {
                SourceError::transient(format!("{context}: {e}"))
            } else {
                SourceError::fatal(format!("{context}: {e}"))
            }
        }
        // Socket-level failures: the server or network went away
        // mid-conversation; a fresh connection may well succeed.
        Error::Io(_) => SourceError::transient(format!("{context}: {e}")),
        // Driver-internal protocol/state errors (the T001 poisoning
        // class): the CONNECTION is undefined, the condition is not —
        // transient, and the reconnect starts clean.
        _ => SourceError::transient(format!("{context}: {e}")),
    }
}

/// The transient ORA family (research R3.4): timeouts, listener and
/// connection loss, instance starting, session limits, and snapshot
/// staleness for fresh statements.
const TRANSIENT_ORA: &[u32] = &[
    18,    // maximum number of sessions exceeded
    1033,  // initialization or shutdown in progress
    1555,  // snapshot too old (fresh statements retry clean)
    3113,  // end-of-file on communication channel
    3114,  // not connected to ORACLE
    12170, // connect timeout
    12514, // listener does not know of service (PDB still registering)
    12541, // no listener
];

/// Render one driver value as JSON, GUIDED BY THE DECLARED TYPE.
///
/// The type matters because the driver hands most numerics back as
/// text (probed: `NUMBER` arrives as `Value::String`, whatever its
/// precision). Rendering by the value's Rust variant alone would emit
/// every integer as a JSON string; rendering by the DECLARED type
/// emits a JSON number where the type is exactly representable and
/// keeps the exact digits as text where it is not — which is the
/// whole point of the D4 rulebook's NUMBER rows.
pub(crate) fn value_to_json_typed(
    value: &oracle_rs::row::Value,
    declared: rdlt_connector_sdk::spi::core::LogicalType,
) -> Result<serde_json::Value, String> {
    use rdlt_connector_sdk::spi::core::LogicalType;

    let rendered = value_to_json(value)?;
    // Only text needs re-typing; every other variant already renders
    // as itself.
    let serde_json::Value::String(text) = &rendered else {
        return Ok(rendered);
    };
    Ok(match declared {
        LogicalType::Int64 => match text.parse::<i64>() {
            Ok(i) => serde_json::Value::Number(i.into()),
            // A value outside i64 under an Int64 declaration means the
            // catalog and the data disagree — keep the exact digits
            // rather than truncating silently.
            Err(_) => rendered,
        },
        LogicalType::Float64 => match text.parse::<f64>() {
            Ok(f) => serde_json::Number::from_f64(f)
                .map(serde_json::Value::Number)
                .unwrap_or(rendered),
            Err(_) => rendered,
        },
        LogicalType::Bool => match text.as_str() {
            "1" | "TRUE" | "true" => serde_json::Value::Bool(true),
            "0" | "FALSE" | "false" => serde_json::Value::Bool(false),
            _ => rendered,
        },
        // Decimal keeps its exact digits as text (JSON numbers cannot
        // carry 38 significant digits), and every string-shaped type
        // is already what it should be.
        _ => rendered,
    })
}

/// Render one driver value by its own variant — the shape-only half
/// of the rulebook, used where no declared type applies (LOBs, the
/// rowid bookkeeping column).
pub(crate) fn value_to_json(value: &oracle_rs::row::Value) -> Result<serde_json::Value, String> {
    use oracle_rs::row::Value;
    Ok(match value {
        Value::Null => serde_json::Value::Null,
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Integer(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .ok_or("non-finite BINARY_FLOAT/DOUBLE value has no JSON representation")?,
        Value::Number(n) => {
            // Full-precision NUMBER: emit as a JSON number when the
            // text IS one (serde_json preserves arbitrary precision
            // via its Number repr? no — so keep integers under i64,
            // everything else as the exact STRING; the type_hints
            // channel types it downstream).
            let text = n.as_str();
            if let Ok(i) = text.parse::<i64>() {
                serde_json::Value::Number(i.into())
            } else {
                serde_json::Value::String(text.to_owned())
            }
        }
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Date(d) => serde_json::Value::String(format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            d.year, d.month, d.day, d.hour, d.minute, d.second
        )),
        Value::Timestamp(t) => {
            let base = format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}",
                t.year, t.month, t.day, t.hour, t.minute, t.second, t.microsecond
            );
            let rendered = if t.tz_hour_offset == 0 && t.tz_minute_offset == 0 {
                format!("{base}Z")
            } else {
                format!(
                    "{base}{:+03}:{:02}",
                    t.tz_hour_offset,
                    t.tz_minute_offset.unsigned_abs()
                )
            };
            serde_json::Value::String(rendered)
        }
        Value::RowId(r) => serde_json::Value::String(r.to_string().unwrap_or_default()),
        Value::Bytes(b) => {
            // Binary travels as lowercase hex — the workspace's
            // grep-friendly convention for opaque bytes.
            serde_json::Value::String(b.iter().map(|x| format!("{x:02x}")).collect())
        }
        Value::Json(j) => j.clone(),
        other => {
            return Err(format!(
                "unmapped Oracle value kind {other:?} — declare a supported column type"
            ));
        }
    })
}

/// The one injection-safety rule: Oracle folds bare identifiers
/// UPPERCASE, so the connector uppercases and always emits quoted
/// (the 022 rule), with embedded quotes doubled.
pub(crate) fn quote_upper(ident: &str) -> String {
    format!("\"{}\"", ident.to_uppercase().replace('"', "\"\""))
}
