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
    /// How long ONE statement may take. A statement that outlives it
    /// has left the protocol mid-conversation, so the timeout drops
    /// the connection exactly as any other failure does.
    read_timeout: std::time::Duration,
    /// Bytes per LOB read round trip.
    lob_chunk: u64,
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
        // The instance is named by SERVICE or by the legacy SID; the
        // driver's config carries both spellings, and the document
        // gate already refused having neither or both.
        let mut driver = oracle_rs::config::Config::new(
            config.host.clone(),
            config.port,
            config.service.clone().unwrap_or_default(),
            config.user.clone(),
            config.password.reveal().to_owned(),
        );
        if let Some(sid) = &config.sid {
            driver.service = oracle_rs::config::ServiceMethod::Sid(sid.clone());
        }
        driver.connect_timeout = std::time::Duration::from_millis(config.tuning.connect_timeout_ms);
        driver.sdu = config.tuning.sdu_bytes;
        let named = config
            .service
            .clone()
            .map(|s| format!("service `{s}`"))
            .or_else(|| config.sid.clone().map(|s| format!("SID `{s}`")))
            .unwrap_or_default();
        let connect_string = format!("{}:{} {named}", config.host, config.port);
        // The driver's own `connect_timeout` never reaches this path
        // (it belongs to a transport the config constructor does not
        // use), so the budget is enforced here — a black-holed host
        // must not hang a pipeline forever.
        let budget = std::time::Duration::from_millis(config.tuning.connect_timeout_ms);
        let conn = match tokio::time::timeout(
            budget,
            oracle_rs::connection::Connection::connect_with_config(driver),
        )
        .await
        {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => return Err(classify(&format!("connecting to `{connect_string}`"), e)),
            Err(_) => {
                return Err(SourceError::transient(format!(
                    "connecting to `{connect_string}`: no answer within {budget:?}"
                )));
            }
        };
        let client = Self {
            conn,
            read_timeout: std::time::Duration::from_millis(config.tuning.read_timeout_ms),
            lob_chunk: config.tuning.lob_chunk_bytes,
        };
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
        let _ = tokio::time::timeout(
            self.read_timeout,
            self.conn.execute("ALTER SESSION SET TIME_ZONE='UTC'", &[]),
        )
        .await;
        let observed = tokio::time::timeout(
            self.read_timeout,
            self.conn.query("SELECT SESSIONTIMEZONE FROM DUAL", &[]),
        )
        .await
        .map_err(|_| SourceError::transient("reading back the session time zone: no answer"))?
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
                "the session time zone is `{zone}` after pinning it to UTC — \
                 TIMESTAMP WITH LOCAL TIME ZONE values would depend on the \
                 client's environment; refusing to read"
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
        match tokio::time::timeout(self.read_timeout, self.conn.query(sql, params)).await {
            Ok(Ok(result)) => Ok((self, result)),
            // `self` drops here: the connection is poisoned by its own
            // failure and must never answer again.
            Ok(Err(e)) => Err(classify(context, e)),
            Err(_) => Err(SourceError::transient(format!(
                "{context}: no answer within {:?} — abandoning the connection",
                self.read_timeout
            ))),
        }
    }

    /// Raise the driver's per-round-trip prefetch to cover a page.
    ///
    /// The read path treats a SHORT page as the last page. That rule
    /// is only sound when the server may return as many rows as the
    /// page asked for — the driver caps every reply at its prefetch
    /// (100 by default), so a derived page above that would end the
    /// stream early AND call it success. This is the knob the
    /// vendored patch exists to expose.
    pub(crate) fn with_page_size(mut self, rows: u32) -> Self {
        self.conn.set_prefetch_rows(rows);
        self
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
        // The statement budget covers LOB reads too: they are many
        // round trips, and a stalled one hangs the load exactly as a
        // stalled query would.
        if locator.is_clob() {
            let mut text = String::new();
            tokio::time::timeout(self.read_timeout, self.conn
                .read_lob_chunked(locator, self.lob_chunk, |chunk| {
                    if let LobData::String(piece) = &chunk {
                        text.push_str(piece);
                    }
                    async { Ok(()) }
                }))
                .await
                .map_err(|_| SourceError::transient("reading a CLOB: no answer within the budget"))?
                .map_err(|e| classify("reading a CLOB", e))?;
            Ok(serde_json::Value::String(text))
        } else {
            let mut bytes = Vec::new();
            tokio::time::timeout(self.read_timeout, self.conn
                .read_lob_chunked(locator, self.lob_chunk, |chunk| {
                    if let LobData::Bytes(piece) = &chunk {
                        bytes.extend_from_slice(piece);
                    }
                    async { Ok(()) }
                }))
                .await
                .map_err(|_| SourceError::transient("reading a BLOB: no answer within the budget"))?
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
        // A zoned timestamp carries its offset; the driver hands the
        // offset separately, so it is appended here where the
        // declaration says it belongs.
        LogicalType::TimestampTz => match value {
            oracle_rs::row::Value::Timestamp(t) => serde_json::Value::String(format!(
                "{text}{:+03}:{:02}",
                t.tz_hour_offset,
                t.tz_minute_offset.unsigned_abs()
            )),
            _ => rendered,
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
        // NO zone designator here: whether this value carries one is
        // the DECLARED type's business (`value_to_json_typed` appends
        // it for TimestampTz). Stamping every timestamp `Z` would
        // claim UTC for naive wall-clock values.
        Value::Timestamp(t) => serde_json::Value::String(format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}",
            t.year, t.month, t.day, t.hour, t.minute, t.second, t.microsecond
        )),
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
