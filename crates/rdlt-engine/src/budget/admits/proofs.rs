//! Proofs that what a budget admits a connector to send fits the shares it draws on, for
//! every budget and count of reads within each proof's bound.

use rdlt_connector::cost::json_charge;
use rdlt_wire::Limits;
use rdlt_wire::limits::{Class, MESSAGE_ROOM};

use super::{HALVED, admitted, kept, least, row_overhead};
use crate::budget::Shares;
use crate::limits::{COLUMN_RECORD, RECORDED, SCHEMA_KEPT};

/// Any count of reads at once, from one to `HALVED`, the most whose least budget is found by
/// halving.
fn any_readers() -> usize {
    kani::any_where(|readers: &usize| (1..=HALVED).contains(readers))
}

/// Each limit a budget admits, all at once, fits the share it draws on: a frame beside what a
/// read keeps and its message's room within what pushes take, and one row of it within half a
/// request; JSON text, its message's room and the batches it becomes within what pushes take,
/// at the longest text that fits but where the wire's own limit binds; where the budget
/// admits a peer, a frame decoded within what pushes take; any other answer decoded within the
/// answers' share; every read's cursor and answer within the cursors' share, and a seal's
/// within the log's; a schema, its decoding and the dictionaries within what a read keeps; and a
/// table's records within the tables' share.
///
/// Bound: every capacity, read by 1 to `HALVED` reads at once.
#[kani::proof]
#[kani::unwind(9)]
fn what_each_admitted_limit_admits_fits_the_share_it_draws_on() {
    let (capacity, readers) = (kani::any::<u64>(), any_readers());
    let shares = Shares::of(capacity);
    let limits = admitted(shares, readers);
    let kept = kept(shares, readers);
    let defaults = Limits::default();
    assert_eq!(limits.lesser(&defaults), limits);
    let message = limits.frame_bytes.saturating_add(MESSAGE_ROOM);
    assert!(limits.frame_bytes == 0 || message.saturating_add(kept) <= shares.intake.max(kept));
    let row = limits
        .frame_bytes
        .saturating_add(row_overhead(limits.schema_columns));
    assert!(limits.frame_bytes == 0 || row <= shares.request / 2);
    let json = limits.json_push_bytes;
    assert!(json_charge(json) <= shares.intake.max(MESSAGE_ROOM));
    assert!(json == defaults.json_push_bytes || json_charge(json + 1) > shares.intake);
    let decoded = |class| u64::try_from(limits.decoded(class)).unwrap_or(u64::MAX);
    assert!(limits.admit_peer().is_err() || decoded(Class::Data) <= shares.intake);
    for class in [Class::Catalog, Class::State, Class::Control] {
        assert!(decoded(class) <= shares.control);
    }
    let reads = u64::try_from(readers).unwrap_or(u64::MAX);
    assert!(limits.cursor_bytes.saturating_mul(2 * (reads + 1)) <= shares.cursors);
    assert!(limits.cursor_bytes.saturating_mul(4) <= shares.log);
    assert!(shares.cursors.saturating_mul(3) <= shares.log);
    let schema = limits.schema_bytes.saturating_mul(SCHEMA_KEPT);
    assert!(schema.saturating_add(limits.dictionary_bytes) <= kept);
    let records = limits
        .schema_columns
        .saturating_mul(RECORDED * COLUMN_RECORD);
    assert!(records <= shares.tables);
    assert_eq!(
        (limits.batch_rows, limits.batch_values, limits.nesting_depth),
        (
            defaults.batch_rows,
            defaults.batch_values,
            defaults.nesting_depth
        )
    );
}

/// Every budget from the least for a count of reads up admits a peer, read by that many reads,
/// and the budget a byte below does not: a memory the configuration accepts is one whose limits
/// the protocol accepts.
///
/// Bound: every capacity, read by 1 to `HALVED` reads at once.
#[kani::proof]
#[kani::unwind(42)]
fn every_budget_from_the_least_up_admits_a_peer() {
    let (capacity, readers) = (kani::any::<u64>(), any_readers());
    let admits = |capacity| admitted(Shares::of(capacity), readers).admit_peer();
    let least = least(readers).expect("a budget admits a peer");
    kani::assume(capacity >= least);
    assert_eq!(admits(capacity), Ok(()));
    assert!(admits(least - 1).is_err());
}
