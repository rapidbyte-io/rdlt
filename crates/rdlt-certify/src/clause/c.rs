//! The honest-check clause, both roles: D7 (destination) and S5
//! (source). Driven on a SECOND spawn of the connector, configured with
//! the operator-supplied MISCONFIGURED document (`--hostile-config`) —
//! only the connector's own vocabulary can spell a target its
//! operations must refuse (the canonical shapes: a regular file behind
//! a trailing slash where a directory is expected; a directory where a
//! file is expected). The contract: the connector refuses FATAL — at
//! `check()`, or even earlier at its own config gate (a refused
//! handshake passes: refusing earlier than check is honest too).
//! `Ok` is a lying probe and FAILS; a transient classification is
//! retry bait and FAILS.

use rdlt_connector::destination::Destination as _;
use rdlt_connector::error::{DestinationError, SourceError};
use rdlt_connector::source::Source as _;
use rdlt_connector_client::error::Error as ClientError;
use rdlt_connector_client::handshake::Role;
use rdlt_runtime::local::Local;
use rdlt_runtime::provider::{self, Provider as _};

use crate::report::Report;
use crate::target::{self, Target};

/// The two ids this module can emit — the census's derivation source,
/// like every sibling family's constant.
pub const CLAUSES: [&str; 2] = ["S5", "D7"];

/// The skip reason when no misconfigured document was supplied — the
/// clause needs one, and only the operator knows the connector's
/// vocabulary.
pub const NO_HOSTILE_CONFIG_SKIP: &str = "no misconfigured document supplied — the honest-check clause needs one; pass \
     --hostile-config '<file>' with a config the connector's gate accepts but its \
     operations must refuse (a file behind a trailing slash, a directory where a file \
     is expected)";

/// The absent-flag disposition, spelled once for both roles: an honest
/// Skip carrying [`NO_HOSTILE_CONFIG_SKIP`].
pub fn skip_source(report: &mut Report) {
    report.skip("S5", NO_HOSTILE_CONFIG_SKIP.to_string());
}

/// See [`skip_source`].
pub fn skip_destination(report: &mut Report) {
    report.skip("D7", NO_HOSTILE_CONFIG_SKIP.to_string());
}

/// S5 against a spawned source configured with the misconfigured
/// document. The requirement is resolved through the connector's own
/// spec first, exactly as the main suite resolves it — a path-only
/// target's identity is learned, never guessed — so an identity
/// mismatch can never masquerade as an honest refusal.
pub async fn source(report: &mut Report, hostile: &Target) {
    let provider = Local::new();
    let spec = target::fetch_spec(&provider, &hostile.requirement, Role::Source).await;
    let requirement = match target::resolved_requirement(&hostile.requirement, &spec) {
        Ok(requirement) => requirement,
        Err(why) => {
            report.fail("S5", why);
            return;
        }
    };
    match provider.source(&requirement, &hostile.config).await {
        // The connector's own typed handshake refusal of the document —
        // honest even earlier than check.
        Err(provider::Error::Client(ClientError::Handshake { .. })) => report.pass("S5"),
        // Anything else that kept the clause from running is the
        // clause's failure to report, never a pass.
        Err(other) => report.fail(
            "S5",
            format!("the misconfigured spawn failed before check could be judged: {other}"),
        ),
        Ok(managed) => match managed.check().await {
            Err(SourceError::Fatal(_)) => report.pass("S5"),
            Err(other) => report.fail(
                "S5",
                format!(
                    "check() on a misconfigured target must refuse with a fatal \
                     classification — no retry fixes a misconfiguration — but it \
                     classified: {other}"
                ),
            ),
            Ok(()) => report.fail(
                "S5",
                "check() answered Ok on a target its own read must refuse — a probe that \
                 passes what a read then fails is retry bait for every caller that \
                 trusts it"
                    .to_string(),
            ),
        },
    }
}

/// D7 against a spawned destination configured with the misconfigured
/// document — the same resolved-identity discipline as [`source`].
pub async fn destination(report: &mut Report, hostile: &Target) {
    let provider = Local::new();
    let spec = target::fetch_spec(&provider, &hostile.requirement, Role::Destination).await;
    let requirement = match target::resolved_requirement(&hostile.requirement, &spec) {
        Ok(requirement) => requirement,
        Err(why) => {
            report.fail("D7", why);
            return;
        }
    };
    match provider.destination(&requirement, &hostile.config).await {
        Err(provider::Error::Client(ClientError::Handshake { .. })) => report.pass("D7"),
        Err(other) => report.fail(
            "D7",
            format!("the misconfigured spawn failed before check could be judged: {other}"),
        ),
        Ok(managed) => match managed.check().await {
            Err(DestinationError::Fatal(_)) => report.pass("D7"),
            Err(other) => report.fail(
                "D7",
                format!(
                    "check() on a misconfigured target must refuse with a fatal \
                     classification — no retry fixes a misconfiguration — but it \
                     classified: {other}"
                ),
            ),
            Ok(()) => report.fail(
                "D7",
                "check() answered Ok on a target its own operations must refuse — a probe \
                 that passes what a run then fails is retry bait for every caller that \
                 trusts it"
                    .to_string(),
            ),
        },
    }
}
