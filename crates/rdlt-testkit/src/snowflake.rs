//! Credential gating for the Snowflake live legs.
//!
//! The container posture with credential presence in place of runtime
//! presence: a contributor without an account runs the full gate and every
//! Snowflake leg skips VISIBLY, while a contributor with credentials in the
//! documented place runs the same gate and the legs execute against the real
//! service.
//!
//! Nothing here reads a credential's VALUE into anything it returns beyond the
//! config it hands the connector — the values live in the environment or in
//! files under the user's config directory, and never in the repository.

use std::path::PathBuf;

/// Env override forcing the skip posture ON even where credentials ARE
/// present, so the skip-not-fail behaviour is verifiable without removing
/// them — the same escape hatch the container probe carries.
const FORCE_NO_SNOWFLAKE: &str = "RDLT_TESTKIT_FORCE_NO_SNOWFLAKE";

/// Where the file form of the convention lives.
fn config_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config/rdlt/snowflake"))
}

/// How one convention entry is looked up.
///
/// A trait object rather than direct environment access so the resolution
/// RULES are testable without mutating the process environment — which this
/// workspace could not do anyway, since setting an environment variable is
/// `unsafe` and unsafe is denied.
pub trait Lookup {
    /// The value of an environment variable, if set and non-empty.
    fn env(&self, var: &str) -> Option<String>;
    /// The contents of a file under the convention's config directory.
    fn file(&self, name: &str) -> Option<String>;
}

/// The real convention: the environment, then `~/.config/rdlt/snowflake/`.
#[derive(Debug, Clone, Copy, Default)]
pub struct RealLookup;

impl Lookup for RealLookup {
    fn env(&self, var: &str) -> Option<String> {
        let value = std::env::var_os(var)?.to_string_lossy().trim().to_owned();
        (!value.is_empty()).then_some(value)
    }

    fn file(&self, name: &str) -> Option<String> {
        let value = std::fs::read_to_string(config_dir()?.join(name)).ok()?;
        let value = value.trim().to_owned();
        (!value.is_empty()).then_some(value)
    }
}

/// One convention entry: the environment variable first, then the file.
///
/// Environment first so a CI or a one-off run can override without editing
/// the user's config, and trimmed because an editor's trailing newline in a
/// credential file is not part of the credential.
fn entry(lookup: &dyn Lookup, var: &str, file: &str) -> Option<String> {
    lookup.env(var).or_else(|| lookup.file(file))
}

/// Everything a live leg needs to reach the qual account.
///
/// Deliberately NOT a connector config: the testkit does not depend on the
/// Snowflake connector, so each leg builds its own document from these parts.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SnowflakeCreds {
    /// Account identifier, as it appears in the host name.
    pub account: String,
    /// Login name.
    pub user: String,
    /// Path to the private key file.
    pub private_key_path: String,
    /// Passphrase, when the key is encrypted.
    pub passphrase: Option<String>,
    /// Database the legs may create and drop schemas in.
    pub database: String,
    /// Warehouse to run on, when one is configured.
    pub warehouse: Option<String>,
    /// Role to assume, when one is configured.
    pub role: Option<String>,
}

/// Resolve the credentials, or `None` when the convention is not satisfied.
///
/// A leg calling this returns early on `None`. That is a PASS — which is the
/// hazard worth naming: a leg that can only skip proves nothing, so the
/// container-free cells carry the burden of demonstrating behaviour and these
/// legs confirm it against the real service.
pub fn credentials() -> Option<SnowflakeCreds> {
    credentials_with(&RealLookup)
}

/// Say once, out loud, that the live legs are not running.
///
/// Without this a skipped suite and an executed one look identical: both
/// report every test passing. The notice is what lets a contributor tell
/// "green because it ran" from "green because it did not" — and it is printed
/// ONCE per process rather than per call, so it stays a notice rather than
/// noise.
fn announce_skip(reason: &str) {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        eprintln!("SKIP: snowflake live legs — {reason}");
    });
}

/// [`credentials`] against a supplied lookup — the testable form.
pub fn credentials_with(lookup: &dyn Lookup) -> Option<SnowflakeCreds> {
    let resolved = resolve_credentials(lookup);
    if resolved.is_none() {
        announce_skip("no account credentials in the environment or ~/.config/rdlt/snowflake/");
    }
    resolved
}

fn resolve_credentials(lookup: &dyn Lookup) -> Option<SnowflakeCreds> {
    if lookup.env(FORCE_NO_SNOWFLAKE).is_some() {
        return None;
    }
    // The key entry is a PATH, not a value — so unlike every other entry it
    // must not be read from the convention file's CONTENTS. The file IS the
    // key; its path is what the connector wants.
    let key_path = lookup
        .env("RDLT_SNOWFLAKE_PRIVATE_KEY_PATH")
        .or_else(|| {
            Some(
                config_dir()?
                    .join("rdlt_qual_key.p8")
                    .to_string_lossy()
                    .into_owned(),
            )
        })
        .filter(|path| std::path::Path::new(path).exists())?;
    Some(SnowflakeCreds {
        account: entry(lookup, "RDLT_SNOWFLAKE_ACCOUNT", "account")?,
        user: entry(lookup, "RDLT_SNOWFLAKE_USER", "user")?,
        private_key_path: key_path,
        passphrase: entry(lookup, "RDLT_SNOWFLAKE_KEY_PASSPHRASE", "passphrase"),
        database: entry(lookup, "RDLT_SNOWFLAKE_DATABASE", "database")?,
        warehouse: entry(lookup, "RDLT_SNOWFLAKE_WAREHOUSE", "warehouse"),
        role: entry(lookup, "RDLT_SNOWFLAKE_ROLE", "role"),
    })
}

/// Everything a bulk-path leg needs: a bucket this process can write and the
/// account can read.
///
/// Separate from [`SnowflakeCreds`] and gated separately, because staging is
/// infrastructure a contributor may not have even with a working account —
/// missing it must skip the bulk legs alone, not the whole suite.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct StageCreds {
    /// The bucket.
    pub bucket: String,
    /// Its region.
    pub region: Option<String>,
    /// An endpoint override, for S3-compatible storage.
    pub endpoint: Option<String>,
    /// A key prefix to stay within.
    pub prefix: Option<String>,
    /// Access key.
    pub access_key: String,
    /// Secret key.
    pub secret_key: String,
}

/// Resolve the stage credentials, or `None` when no bucket is configured.
pub fn stage_credentials() -> Option<StageCreds> {
    stage_credentials_with(&RealLookup)
}

/// [`stage_credentials`] against a supplied lookup — the testable form.
///
/// The bucket's settings arrive together in one `stage.env` rather than as a
/// file each: they are a single coherent credential, and splitting them across
/// files invites a half-configured bucket that fails mid-load instead of
/// skipping.
pub fn stage_credentials_with(lookup: &dyn Lookup) -> Option<StageCreds> {
    let resolved = resolve_stage(lookup);
    if resolved.is_none() {
        announce_skip("no staging bucket configured (stage.env or RDLT_SNOWFLAKE_STAGE_*)");
    }
    resolved
}

fn resolve_stage(lookup: &dyn Lookup) -> Option<StageCreds> {
    if lookup.env(FORCE_NO_SNOWFLAKE).is_some() {
        return None;
    }
    let file = lookup.file("stage.env").map(|text| dotenv(&text));
    let entry = |var: &str| {
        lookup.env(var).or_else(|| {
            file.as_ref()
                .and_then(|map| map.get(var).cloned())
                .filter(|value| !value.is_empty())
        })
    };
    Some(StageCreds {
        bucket: entry("RDLT_SNOWFLAKE_STAGE_BUCKET")?,
        region: entry("RDLT_SNOWFLAKE_STAGE_REGION"),
        endpoint: entry("RDLT_SNOWFLAKE_STAGE_ENDPOINT"),
        prefix: entry("RDLT_SNOWFLAKE_STAGE_PREFIX"),
        access_key: entry("RDLT_SNOWFLAKE_STAGE_ACCESS_KEY")?,
        secret_key: entry("RDLT_SNOWFLAKE_STAGE_SECRET_KEY")?,
    })
}

/// Parse `KEY=VALUE` lines, skipping blanks and comments.
fn dotenv(text: &str) -> std::collections::BTreeMap<String, String> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| line.split_once('='))
        .map(|(key, value)| {
            // A quoted value is a shell habit, not part of the credential.
            let value = value.trim().trim_matches(['"', '\'']).to_owned();
            (key.trim().to_owned(), value)
        })
        .collect()
}

/// An additional token credential, for the auth-matrix legs.
///
/// Each auth method gates on its OWN entry, so a missing PAT skips only the
/// PAT leg rather than the whole suite.
pub fn token(kind: TokenKind) -> Option<String> {
    token_with(&RealLookup, kind)
}

/// [`token`] against a supplied lookup — the testable form.
pub fn token_with(lookup: &dyn Lookup, kind: TokenKind) -> Option<String> {
    if lookup.env(FORCE_NO_SNOWFLAKE).is_some() {
        return None;
    }
    match kind {
        TokenKind::Pat => entry(lookup, "RDLT_SNOWFLAKE_PAT", "pat"),
        TokenKind::OauthToken => entry(lookup, "RDLT_SNOWFLAKE_OAUTH_TOKEN", "oauth-token"),
        TokenKind::Password => entry(lookup, "RDLT_SNOWFLAKE_PASSWORD", "password"),
    }
}

/// Which additional credential a leg wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TokenKind {
    /// A programmatic access token.
    Pat,
    /// A caller-supplied OAuth access token.
    OauthToken,
    /// A password, for the password-capable test user.
    Password,
}

/// A scratch schema name unique to one test run.
///
/// Live legs create and drop their own schema so parallel runs — and a run
/// that died before cleanup — cannot collide. Uppercase because that is the
/// image the catalog holds.
pub fn scratch_schema(label: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("RDLT_T_{}_{nanos:X}", label.to_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A lookup with nothing but what the test puts in it.
    struct Fake(std::collections::BTreeMap<String, String>);

    impl Fake {
        fn with(pairs: &[(&str, &str)]) -> Self {
            Self(
                pairs
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                    .collect(),
            )
        }
    }

    impl Lookup for Fake {
        fn env(&self, var: &str) -> Option<String> {
            self.0.get(var).cloned()
        }
        fn file(&self, name: &str) -> Option<String> {
            self.0.get(name).cloned()
        }
    }

    #[test]
    fn the_force_flag_makes_the_gate_report_absent() {
        // The skip path has to be reachable on a machine that HAS credentials,
        // or it is never exercised where it matters.
        let forced = Fake::with(&[
            (FORCE_NO_SNOWFLAKE, "1"),
            ("RDLT_SNOWFLAKE_ACCOUNT", "A"),
            ("RDLT_SNOWFLAKE_PAT", "tok"),
        ]);
        assert!(credentials_with(&forced).is_none());
        assert!(token_with(&forced, TokenKind::Pat).is_none());
    }

    #[test]
    fn a_missing_entry_skips_rather_than_guessing() {
        // No account: there is nothing to connect to, and inventing a default
        // would turn a skip into a confusing live failure.
        let partial = Fake::with(&[("RDLT_SNOWFLAKE_USER", "U")]);
        assert!(credentials_with(&partial).is_none());
    }

    #[test]
    fn the_environment_wins_over_the_file() {
        // Same key in both positions: a one-off run must be able to override
        // the user's config without editing it.
        let both = Fake::with(&[("RDLT_SNOWFLAKE_PAT", "from-env"), ("pat", "from-file")]);
        assert_eq!(
            token_with(&both, TokenKind::Pat).as_deref(),
            Some("from-env")
        );
        let file_only = Fake::with(&[("pat", "from-file")]);
        assert_eq!(
            token_with(&file_only, TokenKind::Pat).as_deref(),
            Some("from-file")
        );
    }

    #[test]
    fn the_key_entry_is_a_path_and_never_the_key_itself() {
        // The trap this cell exists for: every other entry resolves a VALUE
        // from its file, but reading the key file's contents as a path makes
        // the gate silently report absent — and a leg that can only skip
        // looks exactly like a leg that passed.
        let dir = std::env::temp_dir().join("rdlt-sf-gate-test");
        std::fs::create_dir_all(&dir).expect("temp dir");
        let key = dir.join("k.p8");
        std::fs::write(&key, b"-----BEGIN ENCRYPTED PRIVATE KEY-----\n").expect("write");
        let creds = credentials_with(&Fake::with(&[
            ("RDLT_SNOWFLAKE_PRIVATE_KEY_PATH", &key.to_string_lossy()),
            ("RDLT_SNOWFLAKE_ACCOUNT", "ACCT"),
            ("RDLT_SNOWFLAKE_USER", "USER"),
            ("RDLT_SNOWFLAKE_DATABASE", "DB"),
        ]))
        .expect("a complete convention resolves");
        assert_eq!(creds.private_key_path, key.to_string_lossy());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_key_path_that_does_not_exist_reports_absent() {
        assert!(
            credentials_with(&Fake::with(&[
                ("RDLT_SNOWFLAKE_PRIVATE_KEY_PATH", "/no/such/key.p8"),
                ("RDLT_SNOWFLAKE_ACCOUNT", "ACCT"),
                ("RDLT_SNOWFLAKE_USER", "USER"),
                ("RDLT_SNOWFLAKE_DATABASE", "DB"),
            ]))
            .is_none()
        );
    }

    #[test]
    fn each_token_gates_on_its_own_entry() {
        // A missing PAT must skip only the PAT leg.
        let pat_only = Fake::with(&[("RDLT_SNOWFLAKE_PAT", "tok")]);
        assert!(token_with(&pat_only, TokenKind::Pat).is_some());
        assert!(token_with(&pat_only, TokenKind::OauthToken).is_none());
        assert!(token_with(&pat_only, TokenKind::Password).is_none());
    }

    #[test]
    fn the_stage_gate_reads_one_env_file_and_the_environment_overrides_it() {
        let file = "# a bucket\n\
                    RDLT_SNOWFLAKE_STAGE_BUCKET=from-file\n\
                    RDLT_SNOWFLAKE_STAGE_REGION=\"eu-west-2\"\n\
                    \n\
                    RDLT_SNOWFLAKE_STAGE_ACCESS_KEY=AK\n\
                    RDLT_SNOWFLAKE_STAGE_SECRET_KEY=SK\n";
        let creds = stage_credentials_with(&Fake::with(&[("stage.env", file)]))
            .expect("a complete stage.env resolves");
        assert_eq!(creds.bucket, "from-file");
        assert_eq!(
            creds.region.as_deref(),
            Some("eu-west-2"),
            "a quoted value is a shell habit, not part of the credential"
        );
        assert!(creds.endpoint.is_none(), "absent means AWS");

        let overridden = stage_credentials_with(&Fake::with(&[
            ("stage.env", file),
            ("RDLT_SNOWFLAKE_STAGE_BUCKET", "from-env"),
        ]))
        .expect("resolves");
        assert_eq!(overridden.bucket, "from-env");
    }

    #[test]
    fn a_half_configured_bucket_skips_rather_than_failing_mid_load() {
        // A bucket with no secret key would connect and fail on the first
        // part, long after the leg reported itself running.
        let partial = Fake::with(&[(
            "stage.env",
            "RDLT_SNOWFLAKE_STAGE_BUCKET=b\nRDLT_SNOWFLAKE_STAGE_ACCESS_KEY=AK\n",
        )]);
        assert!(stage_credentials_with(&partial).is_none());
        // And the stage gate is independent of the account gate: one missing
        // must not take the other's legs with it.
        assert!(stage_credentials_with(&Fake::with(&[("RDLT_SNOWFLAKE_ACCOUNT", "A")])).is_none());
    }

    #[test]
    fn the_force_flag_covers_the_stage_gate_too() {
        let forced = Fake::with(&[
            (FORCE_NO_SNOWFLAKE, "1"),
            ("RDLT_SNOWFLAKE_STAGE_BUCKET", "b"),
            ("RDLT_SNOWFLAKE_STAGE_ACCESS_KEY", "AK"),
            ("RDLT_SNOWFLAKE_STAGE_SECRET_KEY", "SK"),
        ]);
        assert!(stage_credentials_with(&forced).is_none());
    }

    #[test]
    fn scratch_schemas_do_not_collide_between_runs() {
        let a = scratch_schema("live");
        let b = scratch_schema("live");
        assert_ne!(a, b);
        assert!(a.starts_with("RDLT_T_LIVE_"), "{a}");
    }
}
