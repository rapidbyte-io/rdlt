//! The conn-string parse gate for both connectors: extract libpq's TLS
//! parameter trio from either libpq spelling, hand the remainder to the
//! driver, translate the extractions into the [`TlsPolicy`] with the same
//! agree-or-error rule `sslmode` has, and make sure no rejection is ever a
//! bare parse error.

use super::policy::{
    PemSource, TlsConfigError, TlsMode, TlsPolicy, resolve_policy, validate_credentials,
};

/// A fully parsed connection: the driver config (TLS trio stripped,
/// `application_name` defaulted) + the effective TLS policy.
#[derive(Debug)]
pub struct ParsedConn {
    pub pg: tokio_postgres::Config,
    pub policy: TlsPolicy,
}

/// THE parse gate for both connectors: extract libpq's TLS parameter trio,
/// hand the
/// remainder to the driver, translate extractions into the policy with the
/// same agree-or-error rule sslmode has, and make sure no rejection is ever
/// a bare parse error.
pub fn parse_conn(conn: &str, block: Option<&TlsPolicy>) -> Result<ParsedConn, TlsConfigError> {
    let extracted = extract_tls_params(conn);
    if let Some((param, value)) = &extracted.bad_escape {
        return Err(TlsConfigError::ConnSyntax(format!(
            "malformed percent-escape in `{param}` value `{value}`"
        )));
    }
    let mut pg: tokio_postgres::Config = extracted.remainder.parse().map_err(|e| {
        // A driver rejection must name the parameter when one is the
        // cause — scan OUR key list for anything outside the driver's set.
        for key in &extracted.seen_keys {
            if !DRIVER_PARAMS.contains(&key.as_str()) {
                return TlsConfigError::UnsupportedParam {
                    param: key.clone(),
                    hint: param_hint(key),
                };
            }
        }
        TlsConfigError::ConnSyntax(format!("{e}"))
    })?;
    let mut policy = resolve_policy(&pg, block)?;
    // A conn-string `sslmode=verify-*` (libpq spelling the driver rejects)
    // sets the mode; a block may keep or STRENGTHEN it, never weaken it —
    // the same never-silently-reversed rule as the other sslmode values.
    if let Some(conn_mode) = extracted.sslmode_verify {
        let strength = |m: TlsMode| match m {
            TlsMode::Disable => 0,
            TlsMode::Prefer => 1,
            TlsMode::Require => 2,
            TlsMode::VerifyCa => 3,
            TlsMode::VerifyFull => 4,
        };
        match block {
            None => policy.mode = conn_mode,
            Some(b) if strength(b.mode) >= strength(conn_mode) => {}
            Some(b) => {
                return Err(TlsConfigError::Contradiction {
                    conn: if conn_mode == TlsMode::VerifyCa {
                        "verify-ca"
                    } else {
                        "verify-full"
                    },
                    block: b.mode,
                });
            }
        }
    }

    // Merge the trio: conn value + absent block field fills in; agreeing
    // duplicates pass; disagreement is a typed conflict.
    let merge = |param: &'static str,
                 param_field: &'static str,
                 conn_value: Option<String>,
                 field: &mut Option<PemSource>|
     -> Result<(), TlsConfigError> {
        let Some(conn_value) = conn_value else {
            return Ok(());
        };
        // `sslrootcert=system` (libpq 16+) = the platform store — our
        // native-roots default, i.e. an EMPTY root_cert.
        if param == "sslrootcert" && conn_value == "system" {
            if let Some(PemSource(existing)) = field {
                return Err(TlsConfigError::ConnParamConflict {
                    param,
                    param_field,
                    conn_value,
                    block_value: existing.clone(),
                });
            }
            return Ok(());
        }
        match field {
            Some(PemSource(existing)) if *existing != conn_value => {
                Err(TlsConfigError::ConnParamConflict {
                    param,
                    param_field,
                    conn_value,
                    block_value: existing.clone(),
                })
            }
            Some(_) => Ok(()),
            None => {
                *field = Some(PemSource(conn_value));
                Ok(())
            }
        }
    };
    merge(
        "sslrootcert",
        "root_cert",
        extracted.sslrootcert,
        &mut policy.root_cert,
    )?;
    merge(
        "sslcert",
        "client_cert",
        extracted.sslcert,
        &mut policy.client_cert,
    )?;
    merge(
        "sslkey",
        "client_key",
        extracted.sslkey,
        &mut policy.client_key,
    )?;
    // The both-or-neither rule holds ACROSS sources of the values.
    validate_credentials(&policy)?;

    // Identify ourselves unless the user chose a name.
    if pg.get_application_name().is_none() {
        pg.application_name("rdlt");
    }
    Ok(ParsedConn { pg, policy })
}

/// The driver's accepted parameter set (tokio-postgres 0.7) — used ONLY to
/// name the offending key when the driver rejects a string.
const DRIVER_PARAMS: &[&str] = &[
    "user",
    "password",
    "dbname",
    "options",
    "application_name",
    "sslmode",
    "host",
    "hostaddr",
    "port",
    "connect_timeout",
    "tcp_user_timeout",
    "keepalives",
    "keepalives_idle",
    "keepalives_interval",
    "keepalives_retries",
    "target_session_attrs",
    "channel_binding",
    "load_balance_hosts",
];

fn param_hint(param: &str) -> String {
    match param {
        "sslpassword" => "encrypted client keys are unsupported — provide an \
                          unencrypted PKCS#8/RSA/SEC1 key via tls.client_key"
            .into(),
        "gssencmode" | "krbsrvname" | "gsslib" => "GSS/Kerberos transport is not supported".into(),
        "requiressl" => "use sslmode= (requiressl is the pre-7.2 libpq spelling)".into(),
        "sslcrl" | "sslcrldir" => "certificate revocation lists are not supported".into(),
        "service" => "libpq service files are not read — inline the parameters".into(),
        other => format!("`{other}` has no rdlt equivalent; see the supported parameter list"),
    }
}

/// The extraction half: strip the TLS trio from both libpq forms, remember
/// every key seen (for named rejections).
struct ExtractedConn {
    remainder: String,
    /// First malformed percent-escape seen in an EXTRACTED value:
    /// (param, raw value) — surfaced as a typed error.
    bad_escape: Option<(String, String)>,
    /// Keys seen and KEPT for the driver (extracted ones are excluded —
    /// they cannot be the cause of a driver rejection).
    seen_keys: Vec<String>,
    sslrootcert: Option<String>,
    sslcert: Option<String>,
    sslkey: Option<String>,
    /// `sslmode=verify-ca|verify-full` — libpq spellings the driver does
    /// not accept; translated into the policy mode.
    sslmode_verify: Option<TlsMode>,
}

fn extract_tls_params(conn: &str) -> ExtractedConn {
    let mut out = ExtractedConn {
        remainder: String::new(),
        bad_escape: None,
        seen_keys: Vec::new(),
        sslrootcert: None,
        sslcert: None,
        sslkey: None,
        sslmode_verify: None,
    };
    let capture = |key: &str, value: String, out: &mut ExtractedConn| -> bool {
        match key {
            "sslrootcert" => out.sslrootcert = Some(value),
            "sslcert" => out.sslcert = Some(value),
            "sslkey" => out.sslkey = Some(value),
            "sslmode" if value == "verify-ca" => out.sslmode_verify = Some(TlsMode::VerifyCa),
            "sslmode" if value == "verify-full" => out.sslmode_verify = Some(TlsMode::VerifyFull),
            _ => return false,
        }
        true
    };
    if conn.contains("://") {
        // URL form: rewrite the query string, percent-decoding captured values.
        let (base, query) = match conn.split_once('?') {
            Some((b, q)) => (b, q),
            None => (conn, ""),
        };
        let mut kept: Vec<&str> = Vec::new();
        for pair in query.split('&').filter(|p| !p.is_empty()) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let decoded = match percent_decode(value) {
                Ok(v) => v,
                Err(()) => {
                    if out.bad_escape.is_none() {
                        out.bad_escape = Some((key.to_string(), value.to_string()));
                    }
                    value.to_string()
                }
            };
            if !capture(key, decoded, &mut out) {
                out.seen_keys.push(key.to_string());
                kept.push(pair);
            }
        }
        out.remainder = if kept.is_empty() {
            base.to_string()
        } else {
            format!("{base}?{}", kept.join("&"))
        };
    } else {
        // key=value form: quote-aware scan (libpq allows `key = 'a value'`).
        let bytes = conn.as_bytes();
        let mut kept_spans: Vec<(usize, usize)> = Vec::new();
        let mut i = 0usize;
        while i < bytes.len() {
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i >= bytes.len() {
                break;
            }
            let start = i;
            while i < bytes.len() && bytes[i] != b'=' && !bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            let key = conn[start..i].to_string();
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i < bytes.len() && bytes[i] == b'=' {
                i += 1;
                while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                    i += 1;
                }
                let value = if i < bytes.len() && bytes[i] == b'\'' {
                    i += 1;
                    let vstart = i;
                    while i < bytes.len() && bytes[i] != b'\'' {
                        i += if bytes[i] == b'\\' { 2 } else { 1 };
                    }
                    let v = conn[vstart..i.min(bytes.len())].replace("\\'", "'");
                    i = (i + 1).min(bytes.len());
                    v
                } else {
                    let vstart = i;
                    while i < bytes.len() && !bytes[i].is_ascii_whitespace() {
                        i += 1;
                    }
                    conn[vstart..i].to_string()
                };
                if !capture(&key, value, &mut out) {
                    out.seen_keys.push(key);
                    kept_spans.push((start, i));
                }
            } else {
                // Not a k=v token (malformed) — keep it for the driver's
                // syntax error to describe.
                kept_spans.push((start, i));
            }
        }
        out.remainder = kept_spans
            .iter()
            .map(|&(s, e)| &conn[s..e])
            .collect::<Vec<_>>()
            .join(" ");
    }
    out
}

/// Strict: a `%` not followed by two hex digits is an error, never a silent
/// literal passthrough.
fn percent_decode(value: &str) -> Result<String, ()> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let (Some(h), Some(l)) = (
                bytes.get(i + 1).and_then(|b| (*b as char).to_digit(16)),
                bytes.get(i + 2).and_then(|b| (*b as char).to_digit(16)),
            ) else {
                return Err(());
            };
            out.push((h * 16 + l) as u8);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}
