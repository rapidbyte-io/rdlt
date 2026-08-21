//! The honest-check clauses, both roles: D7 (destination) and S5
//! (source), plus S6 — the source's READ twin. Driven on a SECOND
//! spawn of the connector, configured with the operator-supplied
//! MISCONFIGURED document (`--hostile-config`) — only the connector's
//! own vocabulary can spell a target its operations must refuse (the
//! canonical shapes: a regular file behind a trailing slash where a
//! directory is expected; a directory where a file is expected).
//!
//! THE LAW LIVES ONCE: the judgments themselves are the testkit's
//! conformance suites (`verify_check_refusal`, `verify_read_refusal`),
//! run here through the managed wire adapter — the wire is certified by
//! the SAME law an in-process connector answers to, at the SAME
//! strength, and the two sides cannot drift. What is genuinely
//! wire-specific and lives here: spawning the connector with the
//! hostile document, and the handshake arm — a connector whose own
//! config gate refuses the document before any RPC is honest even
//! earlier than check.

use rdlt_connector_client::error::Error as ClientError;
use rdlt_connector_client::handshake::Role;
use rdlt_runtime::local::Local;
use rdlt_runtime::provider::{self, Provider as _};
use rdlt_testkit::conformance::{destination as d_suite, source as s_suite};

use crate::report::{self, Report};
use crate::target::{self, Target};

/// The three ids this module can emit — the census's derivation source,
/// like every sibling family's constant.
pub const CLAUSES: [&str; 3] = ["S5", "S6", "D7"];

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
    report.skip("S6", NO_HOSTILE_CONFIG_SKIP.to_string());
}

/// See [`skip_source`].
pub fn skip_destination(report: &mut Report) {
    report.skip("D7", NO_HOSTILE_CONFIG_SKIP.to_string());
}

/// Fold one suite verdict into the report: every failure renders under
/// its clause id, and the pass for an exercised-and-silent clause is
/// minted by the same absorb the sibling families use — never by hand.
/// The skips are taken explicitly (the honest-check suites emit none;
/// taking them by name keeps the consumption honest by construction).
fn absorb(report: &mut Report, clause: &'static str, verdict: rdlt_testkit::conformance::Verdict) {
    let (failures, skips, concluded) = verdict.tolerating_skips();
    report.absorb(failures, skips, report::Concluded(&concluded), &[clause]);
}

/// S5 against a spawned source configured with the misconfigured
/// document. The requirement is resolved through the connector's own
/// spec first, exactly as the main suite resolves it — a path-only
/// target's identity is learned, never guessed — so an identity
/// mismatch can never masquerade as an honest refusal.
///
/// WHAT THE HANDSHAKE ARM TRUSTS, stated because it cannot be checked:
/// the refusal says the document was rejected, not WHICH part of it
/// was. A connector that refuses this document for a reason unrelated
/// to the misconfiguration — a typo the operator left in it, a field it
/// never supported — passes the clause without ever judging the hostile
/// shape. The protocol is black-box here by design (a refusal names no
/// seat), so the clause rests on the operator supplying a document that
/// is otherwise VALID and hostile in exactly one dimension. The same
/// trust the suite already places in the operator's main config, and
/// the reason the misconfigured document is theirs to write rather than
/// the certifier's to synthesize.
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
        // honest even earlier than check. The read's own arm records S6
        // when it runs, so this reports only what it judged.
        Err(provider::Error::Client(ClientError::Handshake { .. })) => report.pass("S5"),
        // Anything else that kept the clause from running is the
        // clause's failure to report, never a pass.
        Err(other) => report.fail(
            "S5",
            format!("the misconfigured spawn failed before check could be judged: {other}"),
        ),
        Ok(managed) => absorb(report, "S5", s_suite::verify_check_refusal(&managed).await),
    }
}

/// S6 against a second misconfigured spawn: the READ must refuse the
/// target too. Driven on its own spawn so a source left in whatever
/// state a refused check leaves it cannot decide the verdict. The law
/// — typed refusal, the deadline, and the flood ceiling a refusing read
/// must stay under — is the testkit's `verify_read_refusal`, unmodified.
pub async fn source_read(report: &mut Report, hostile: &Target) {
    let provider = Local::new();
    let spec = target::fetch_spec(&provider, &hostile.requirement, Role::Source).await;
    let requirement = match target::resolved_requirement(&hostile.requirement, &spec) {
        Ok(requirement) => requirement,
        Err(why) => {
            report.fail("S6", why);
            return;
        }
    };
    match provider.source(&requirement, &hostile.config).await {
        // Refused at the document: honest, and recorded HERE too —
        // this function is public and a library caller may drive it
        // alone, which must still leave an S6 verdict.
        Err(provider::Error::Client(ClientError::Handshake { .. })) => {
            report.pass("S6");
        }
        Err(other) => report.fail(
            "S6",
            format!("the misconfigured spawn failed before read could be judged: {other}"),
        ),
        Ok(managed) => absorb(report, "S6", s_suite::verify_read_refusal(&managed).await),
    }
}

/// D7 against a spawned destination configured with the misconfigured
/// document — the same resolved-identity discipline as [`source`], and
/// the same one-home law through the destination suite.
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
        Ok(managed) => absorb(report, "D7", d_suite::verify_check_refusal(&managed).await),
    }
}
