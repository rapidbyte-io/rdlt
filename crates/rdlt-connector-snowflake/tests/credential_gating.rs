//! What the credential gate decides, pinned.
//!
//! Every live leg in this crate calls `common::credentials()` to choose
//! between running against the real service and skipping visibly. That makes
//! the gate's behaviour a contract, not an implementation detail: a gate that
//! wrongly reports credentials absent disarms every live leg while the suite
//! still reports green. These cases moved here with the gate itself when it
//! left rdlt-testkit; the container half of the same contract stays pinned in
//! rdlt-testkit's `gating_pin`.
//!
//! Two postures exist and both are deliberate. **Skip-not-fail is the
//! default**, because a contributor without an account must still be able to
//! run the gate. **Demand-and-fail is opt-in** (`RDLT_TESTKIT_REQUIRE_SNOWFLAKE`),
//! for the opposite audience: a maintainer with the account, who needs to know
//! a leg actually ran rather than trusting green.
//!
//! Nothing here mutates the process environment (this workspace denies
//! `unsafe_code`, and `std::env::set_var` is unsafe); the gate separates the
//! DECISION from where its answers come from, and these tests drive the
//! decision through a supplied lookup.

mod common;

use std::collections::BTreeMap;

use common::{Lookup, TokenKind, credentials_with, scratch_schema, token_with};

/// A lookup with nothing but what the test puts in it.
struct Fake(BTreeMap<String, String>);

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
        ("RDLT_TESTKIT_FORCE_NO_SNOWFLAKE", "1"),
        ("RDLT_SNOWFLAKE_ACCOUNT", "A"),
        ("RDLT_SNOWFLAKE_PAT", "tok"),
    ]);
    assert!(credentials_with(&forced).is_none());
    assert!(token_with(&forced, TokenKind::Pat).is_none());
}

#[test]
#[should_panic(expected = "RDLT_TESTKIT_REQUIRE_SNOWFLAKE is set but no account credentials")]
fn demanding_credentials_that_are_absent_fails_naming_them() {
    // The opt-in posture. The failure must name the missing resource, because a
    // maintainer who set this is asking a question and deserves the answer
    // rather than an obscure error further down.
    let env = Fake::with(&[("RDLT_TESTKIT_REQUIRE_SNOWFLAKE", "1")]);
    let _ = credentials_with(&env);
}

#[test]
#[should_panic(expected = "are both set")]
fn demanding_and_forcing_absence_together_is_an_error() {
    // Not a precedence puzzle. A run that both demands credentials and pretends
    // there are none is a mistake in how it was invoked; honouring either one
    // silently would hide the mistake and produce a result nobody can read.
    let env = Fake::with(&[
        ("RDLT_TESTKIT_REQUIRE_SNOWFLAKE", "1"),
        ("RDLT_TESTKIT_FORCE_NO_SNOWFLAKE", "1"),
    ]);
    let _ = credentials_with(&env);
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
fn scratch_schemas_do_not_collide_between_runs() {
    let a = scratch_schema("live");
    let b = scratch_schema("live");
    assert_ne!(a, b);
    assert!(a.starts_with("RDLT_T_LIVE_"), "{a}");
}
