//! What the environment probes decide, pinned.
//!
//! Every container-gated suite in the workspace depends on this probe to choose
//! between running and skipping. That makes its behaviour a contract, not an
//! implementation detail: a probe that wrongly reports the runtime absent
//! disarms every dependent leg while every suite still reports green. (The
//! credential gate carries the same contract, pinned beside the gate itself in
//! the connector crate that owns it.)
//!
//! Two postures exist and both are deliberate. **Skip-not-fail is the default**,
//! because a contributor without a container runtime must still be able to run the
//! gate — the constitution requires it. **Demand-and-fail is opt-in**, for the
//! opposite audience: a maintainer on a machine where the resource IS present, who
//! needs to know a leg actually ran rather than trusting green.
//!
//! Nothing here mutates the process environment. It could not: this workspace
//! denies `unsafe_code` and `std::env::set_var` is unsafe. The probe therefore
//! separates the DECISION from where its answers come from, and these tests
//! drive the decision directly. That is a better arrangement than the
//! env-mutating one it replaced — the demand-and-fail path can be exercised on
//! a machine that HAS the resource, which is the machine a maintainer would
//! actually be on.

#[test]
fn forcing_absence_makes_the_container_probe_report_absent() {
    // The probe is not even consulted: forced absence short-circuits it. Proven
    // by passing a probe that would report TRUE and asserting the answer is false.
    let reported = rdlt_testkit::gate::decide_availability(false, true, || true);
    assert!(
        !reported,
        "forcing absence must report absent even where a runtime is present — this \
         is how the skip-not-fail path stays verifiable on a machine that has one"
    );
}

#[test]
fn a_present_runtime_is_reported_present() {
    assert!(rdlt_testkit::gate::decide_availability(
        false,
        false,
        || true
    ));
    assert!(!rdlt_testkit::gate::decide_availability(
        false,
        false,
        || false
    ));
}

#[test]
#[should_panic(expected = "RDLT_TESTKIT_REQUIRE_CONTAINERS is set but no container runtime")]
fn demanding_a_runtime_that_is_absent_fails_naming_it() {
    // The whole point of the opt-in posture: a leg that would have skipped now
    // fails, and the message names what was missing rather than leaving a
    // maintainer to infer it from a suite that quietly did nothing.
    rdlt_testkit::gate::decide_availability(true, false, || false);
}

#[test]
fn demanding_a_runtime_that_is_present_is_satisfied() {
    assert!(rdlt_testkit::gate::decide_availability(true, false, || {
        true
    }));
}

#[test]
#[should_panic(expected = "are both set")]
fn demanding_and_forcing_the_runtime_absent_together_is_an_error() {
    rdlt_testkit::gate::decide_availability(true, true, || true);
}
