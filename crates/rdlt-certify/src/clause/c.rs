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
//!
//! S6 is the source's READ twin, driven on the same spawn: the seat
//! that opens the target must refuse it too. A probe is half the
//! promise — the read is where a bad path becomes an unbounded buffer
//! or a parked thread — and a suite that drives `check()` alone
//! certifies clean while the read stays unguarded.

use rdlt_connector::destination::Destination as _;
use rdlt_connector::error::{DestinationError, SourceError};
use rdlt_connector::source::Source as _;
use rdlt_connector_client::error::Error as ClientError;
use rdlt_connector_client::handshake::Role;
use rdlt_runtime::local::Local;
use rdlt_runtime::provider::{self, Provider as _};

use crate::report::Report;
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
        //
        // WHAT THIS ARM TRUSTS, stated because it cannot be checked:
        // the refusal says the document was rejected, not WHICH part of
        // it was. A connector that refuses this document for a reason
        // unrelated to the misconfiguration — a typo the operator left
        // in it, a field it never supported — passes the clause without
        // ever judging the hostile shape. The protocol is black-box
        // here by design (a refusal names no seat), so the clause rests
        // on the operator supplying a document that is otherwise VALID
        // and hostile in exactly one dimension. The same trust the
        // suite already places in the operator's main config, and the
        // reason the misconfigured document is theirs to write rather
        // than the certifier's to synthesize.
        // Refused at the document, before either seat could touch the
        // target: honest for the probe. The read's own arm records S6
        // when it runs, so this reports only what it judged.
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

/// S6 against the same misconfigured spawn: the READ must refuse the
/// target too. Driven on its own spawn so a source left in whatever
/// state a refused check leaves it cannot decide the verdict.
///
/// The read needs a stream to ask for. A declaration that refuses is
/// the target refused at the earliest seat there is, and passes; a
/// declaration that succeeds hands its first stream to the read, and
/// only a completed read fails the clause. The read is bounded by the
/// harness's deadline — a source that PARKS on a hostile path (the
/// FIFO shape) fails by name rather than hanging the certification.
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
    let managed = match provider.source(&requirement, &hostile.config).await {
        // Refused at the document: honest, and recorded HERE too —
        // this function is public and a library caller may drive it
        // alone, which must still leave an S6 verdict.
        Err(provider::Error::Client(ClientError::Handshake { .. })) => {
            report.pass("S6");
            return;
        }
        Err(other) => {
            report.fail(
                "S6",
                format!("the misconfigured spawn failed before read could be judged: {other}"),
            );
            return;
        }
        Ok(managed) => managed,
    };
    let stream = match managed.streams().await {
        Err(_) => {
            report.pass("S6");
            return;
        }
        Ok(streams) => match streams.into_iter().next() {
            None => {
                report.pass("S6");
                return;
            }
            Some(stream) => stream,
        },
    };
    let (out, mut rows) = rdlt_connector::channel::records(READ_PROBE_BUDGET);
    let request = rdlt_connector::source::ReadRequest::new(stream, None, out);
    let drain = tokio::spawn(async move { while rows.recv().await.is_some() {} });
    let read = tokio::time::timeout(READ_PROBE_DEADLINE, managed.read(request)).await;
    drain.abort();
    match read {
        Err(_) => report.fail(
            "S6",
            format!(
                "read() against a misconfigured target neither finished nor refused within \
                 {}s — a read that parks on a bad path is the shape a probe cannot catch",
                READ_PROBE_DEADLINE.as_secs()
            ),
        ),
        Ok(Err(_)) => report.pass("S6"),
        Ok(Ok(())) => report.fail(
            "S6",
            "read() completed against a target check() must refuse — the probe and the \
             read disagree, and it is the read that touches the target"
                .to_string(),
        ),
    }
}

/// The read probe's channel budget: the drain keeps nothing, so this
/// bounds only what one hostile push may hold in flight.
const READ_PROBE_BUDGET: usize = 4 << 20;

/// How long a misconfigured read may take before parking is the
/// verdict. Generous for any refusal a connector can spell, short
/// enough that a certification never becomes a hang.
const READ_PROBE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

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
