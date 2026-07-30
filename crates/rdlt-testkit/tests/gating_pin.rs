//! What the environment probes decide, pinned.
//!
//! Eight crates depend on these two probes to choose between running a suite and
//! skipping it. That makes their behaviour a contract, not an implementation
//! detail: a probe that wrongly reports a resource absent disarms every dependent
//! leg across the workspace while every suite still reports green.
//!
//! Two postures exist and both are deliberate. **Skip-not-fail is the default**,
//! because a contributor without a container runtime must still be able to run the
//! gate — the constitution requires it. **Demand-and-fail is opt-in**, for the
//! opposite audience: a maintainer on a machine where the resource IS present, who
//! needs to know a leg actually ran rather than trusting green.
//!
//! Nothing here mutates the process environment. It could not: this workspace
//! denies `unsafe_code` and `std::env::set_var` is unsafe. Both probes therefore
//! separate the DECISION from where its answers come from, and these tests drive
//! the decision directly. That is a better arrangement than the env-mutating one
//! it replaced — the demand-and-fail path can be exercised on a machine that HAS
//! the resource, which is the machine a maintainer would actually be on.

use rdlt_testkit::snowflake::{Lookup, credentials_with};

/// A lookup answering from a fixed table, so credential resolution can be driven
/// without touching the real environment or `~/.config`.
struct FakeEnv(Vec<(&'static str, &'static str)>);

impl Lookup for FakeEnv {
    fn env(&self, key: &str) -> Option<String> {
        self.0
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| (*v).to_owned())
    }
    fn file(&self, _name: &str) -> Option<String> {
        None
    }
}

#[test]
fn forcing_absence_makes_credentials_resolve_to_none() {
    // The default posture, verifiable on a machine that HAS credentials: the
    // probe reports absent and the caller skips.
    let env = FakeEnv(vec![("RDLT_TESTKIT_FORCE_NO_SNOWFLAKE", "1")]);
    assert!(
        credentials_with(&env).is_none(),
        "forcing absence must make the probe report absent"
    );
}

#[test]
#[should_panic(expected = "RDLT_TESTKIT_REQUIRE_SNOWFLAKE is set but no account credentials")]
fn demanding_credentials_that_are_absent_fails_naming_them() {
    // The opt-in posture. The failure must name the missing resource, because a
    // maintainer who set this is asking a question and deserves the answer
    // rather than an obscure error further down.
    let env = FakeEnv(vec![("RDLT_TESTKIT_REQUIRE_SNOWFLAKE", "1")]);
    let _ = credentials_with(&env);
}

#[test]
#[should_panic(expected = "are both set")]
fn demanding_and_forcing_absence_together_is_an_error() {
    // Not a precedence puzzle. A run that both demands credentials and pretends
    // there are none is a mistake in how it was invoked; honouring either one
    // silently would hide the mistake and produce a result nobody can read.
    let env = FakeEnv(vec![
        ("RDLT_TESTKIT_REQUIRE_SNOWFLAKE", "1"),
        ("RDLT_TESTKIT_FORCE_NO_SNOWFLAKE", "1"),
    ]);
    let _ = credentials_with(&env);
}

#[test]
fn forcing_absence_makes_the_container_probe_report_absent() {
    // The probe is not even consulted: forced absence short-circuits it. Proven
    // by passing a probe that would report TRUE and asserting the answer is false.
    let reported = rdlt_testkit::decide_availability(false, true, || true);
    assert!(
        !reported,
        "forcing absence must report absent even where a runtime is present — this \
         is how the skip-not-fail path stays verifiable on a machine that has one"
    );
}

#[test]
fn a_present_runtime_is_reported_present() {
    assert!(rdlt_testkit::decide_availability(false, false, || true));
    assert!(!rdlt_testkit::decide_availability(false, false, || false));
}

#[test]
#[should_panic(expected = "RDLT_TESTKIT_REQUIRE_CONTAINERS is set but no container runtime")]
fn demanding_a_runtime_that_is_absent_fails_naming_it() {
    // The whole point of the opt-in posture: a leg that would have skipped now
    // fails, and the message names what was missing rather than leaving a
    // maintainer to infer it from a suite that quietly did nothing.
    rdlt_testkit::decide_availability(true, false, || false);
}

#[test]
fn demanding_a_runtime_that_is_present_is_satisfied() {
    assert!(rdlt_testkit::decide_availability(true, false, || true));
}

#[test]
#[should_panic(expected = "are both set")]
fn demanding_and_forcing_the_runtime_absent_together_is_an_error() {
    rdlt_testkit::decide_availability(true, true, || true);
}
