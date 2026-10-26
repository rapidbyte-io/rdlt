use proptest::prelude::*;
use rdlt_connector::cost::{Allocations, batch_charge, json_charge};
use rdlt_wire::Limits;
use rdlt_wire::limits::{Class, MESSAGE_ROOM, MIN_DICTIONARY_BYTES, MIN_FRAME_BYTES};

use super::{HALVED, admitted, kept, least, pushed_arrays, row_overhead};
use crate::budget::Shares;
use crate::fixtures::lowering::wide::{dictionaries, integers};
use crate::limits::COLUMN_RECORD;

const MIB: u64 = 1 << 20;

#[test]
fn the_default_budget_admits_these_limits() {
    let limits = admitted(Shares::of(256 * MIB), 16);
    assert_eq!(
        limits,
        Limits {
            // Half the 64 MiB a request takes, less a row's nulls, flags (937 bytes and their
            // hex), metadata and its piece's arrays (320 bytes each, two a column and sixteen),
            // in a table of every column: that binds, not the 96 MiB pushes may take.
            frame_bytes: 32 * MIB - 7_489 * 33 - 3 * 937 - 10 - 1_024 - (2 * 7_489 + 16) * 320,
            // A third of the 100 MiB pushes may take, less a message's room.
            json_push_bytes: (100 * MIB - MESSAGE_ROOM) / 3,
            // A cursor waiting and an answer for each of sixteen reads, and half the share.
            cursor_bytes: 4 * MIB / 34,
            // A read keeps 4 MiB: half for its dictionaries, half for its schema, whose
            // message is a fifth of that.
            dictionary_bytes: 2 * MIB,
            schema_bytes: 2 * MIB / 5,
            // What the 0.4 MiB of a schema's message carries of short names, fewer than the
            // 16,384 that 8 MiB of tables' records holds at half a kilobyte a column.
            schema_columns: 7_489,
            // What the 16 MiB of answers being decoded holds of one decoded: a catalog at
            // sixteen times its bytes; state, bounded decoded, the protocol's, as is any other
            // control message.
            catalog_bytes: MIB,
            ..Limits::default()
        }
    );
    // Read alone, a read keeps the whole share, whose schema's message carries more columns
    // than the protocol allows, and 8 MiB of tables' records holds as many.
    assert_eq!(admitted(Shares::of(256 * MIB), 1).schema_columns, 10_000);
}

#[test]
fn a_push_s_arrays_are_at_most_two_a_column_of_its_schema() {
    // Columns of dictionaries hold the most arrays a column a push may: their keys' and their
    // values'. Integers hold one a column.
    for columns in [1, 100, 7_489] {
        let keyed = dictionaries(1, columns);
        let arrays = batch_charge(&keyed) - Allocations::of(&keyed).bytes();
        let columns = u64::try_from(columns).unwrap();
        assert_eq!(arrays, pushed_arrays(columns));
        let plain = integers(1, columns.try_into().unwrap(), |row| row);
        let arrays = batch_charge(&plain) - Allocations::of(&plain).bytes();
        assert_eq!(arrays * 2, pushed_arrays(columns));
    }
}

#[test]
fn the_least_memory_admits_these_limits() {
    assert_eq!(least(16), Some(39_545_528));
    // One read keeps all of the reads' share, so a table may have more columns, whose nulls,
    // flags and arrays a row of a frame takes beside it.
    assert_eq!(least(1), Some(50_060_952));
    let limits = admitted(Shares::of(least(16).unwrap()), 16);
    assert_eq!(
        (
            limits.frame_bytes,
            limits.json_push_bytes,
            limits.cursor_bytes,
            limits.dictionary_bytes,
            limits.schema_bytes,
            limits.schema_columns
        ),
        (MIN_FRAME_BYTES, 5_149_072, 18_173, 308_949, 61_789, 1_103)
    );
    // What a sixteenth of the budget holds of one answer decoded.
    assert_eq!(
        (
            limits.catalog_bytes,
            limits.state_bytes,
            limits.control_message_bytes
        ),
        (154_474, 2_471_595, 154_474)
    );
}

/// Budgets of every size from a few kilobytes up, each read by as few and as many reads.
fn budgets() -> impl Iterator<Item = (u64, usize)> {
    let capacities = [
        64 * 1024,
        MIB,
        16 * MIB,
        least(16).unwrap(),
        least(16).unwrap() + 1,
        64 * MIB,
        256 * MIB,
        4_096 * MIB,
        u64::MAX / 2,
    ];
    capacities
        .into_iter()
        .flat_map(|capacity| [1, 2, 4, 16, 64].map(|readers| (capacity, readers)))
}

#[test]
fn what_each_limit_admits_all_at_once_fits_the_share_it_draws_on() {
    for (capacity, readers) in budgets() {
        let shares = Shares::of(capacity);
        let limits = admitted(shares, readers);
        let kept = kept(shares, readers);
        let what = format!("{capacity} bytes read by {readers}");
        let defaults = Limits::default();
        assert_eq!(limits.lesser(&defaults), limits, "{what}");
        // A frame's message, at most its frame and MESSAGE_ROOM, beside what a read keeps and a
        // batch's arrays, fits what pushes may take; the frame, with a row's null in each column
        // and the metadata, fits a request over what a data message holds decoded for each byte.
        let message = limits.frame_bytes.saturating_add(MESSAGE_ROOM);
        let pushed = message + kept + pushed_arrays(limits.schema_columns);
        assert!(
            limits.frame_bytes == 0 || pushed <= shares.intake.max(kept),
            "{what}"
        );
        let row = limits
            .frame_bytes
            .saturating_add(row_overhead(limits.schema_columns));
        assert!(
            limits.frame_bytes == 0 || row <= shares.request / 2,
            "{what}"
        );
        // A JSON push at its limit is charged within what pushes may take, and one a byte
        // longer beyond it, but where the wire's own limit binds.
        let json = limits.json_push_bytes;
        assert!(
            json_charge(json) <= shares.intake.max(MESSAGE_ROOM),
            "{what}"
        );
        let longest = json == defaults.json_push_bytes || json_charge(json + 1) > shares.intake;
        assert!(longest, "{what}");
        // A frame decoded fits what pushes may take, where the budget admits a peer at all, and
        // any other answer decoded the share of answers being decoded: each is charged there
        // before it is decoded.
        let decoded = |class| u64::try_from(limits.decoded(class)).expect("bytes");
        let peer = limits.admit_peer().is_ok();
        assert!(!peer || decoded(Class::Data) <= shares.intake, "{what}");
        for class in [Class::Catalog, Class::State, Class::Control] {
            assert!(decoded(class) <= shares.control, "{what}: {class:?}");
        }
        // Every read's waiting cursor and answer, and half the share, fit the cursors' share.
        let readers = u64::try_from(readers).expect("a count");
        let cursors = limits.cursor_bytes.saturating_mul(2 * (readers + 1));
        assert!(cursors <= shares.cursors, "{what}");
        // A schema's message, the schema decoded from it and the dictionaries fit what a read
        // keeps.
        let schema = limits.schema_bytes.saturating_mul(5);
        assert!(
            schema.saturating_add(limits.dictionary_bytes) <= kept,
            "{what}"
        );
        // A table of every column a schema may have records within the tables' share.
        let records = limits.schema_columns.saturating_mul(COLUMN_RECORD);
        assert!(records <= shares.tables, "{what}");
        // What the budget does not bound is the protocol's.
        assert_eq!(
            (limits.batch_rows, limits.batch_values, limits.nesting_depth),
            (
                defaults.batch_rows,
                defaults.batch_values,
                defaults.nesting_depth
            ),
            "{what}"
        );
    }
}

#[test]
fn every_budget_from_the_least_up_admits_a_peer() {
    // Each byte of a band above the least for thousands of counts of reads, and a stride a
    // million bytes up for a few.
    let band = (1..=4_000).step_by(7).map(|readers| (readers, 3_000, 1));
    let stride = [1, 2, 4, 16, 64, 1_000].map(|readers| (readers, 1_000_000, 997));
    for (readers, above, step) in band.chain(stride) {
        let least = least(readers).unwrap();
        for capacity in (least..least + above).step_by(step) {
            let limits = admitted(Shares::of(capacity), readers);
            assert_eq!(
                limits.admit_peer(),
                Ok(()),
                "{capacity} bytes, {readers} reads"
            );
        }
    }
}

/// Counts of reads at once from one to the most a budget admits a peer for, those whose least is
/// found by halving drawn most often.
fn any_readers() -> impl Strategy<Value = usize> {
    prop_oneof![1..=HALVED + 1, 1..=1_usize << 20, 1..1_usize << 43]
}

proptest! {
    // Kani proves this only up to `HALVED` reads, and a case takes microseconds: many are drawn.
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(4_096)))]

    /// For any count of reads a budget admits a peer for, every budget from the least up does,
    /// near it or far above, and the budget a byte below does not.
    #[test]
    fn every_budget_from_the_least_up_admits_a_peer_for_any_count_of_reads(
        readers in any_readers(),
        above in prop_oneof![0..1_u64 << 12, any::<u64>()],
    ) {
        let least = least(readers).unwrap();
        let admits = |capacity: u64| admitted(Shares::of(capacity), readers).admit_peer();
        prop_assert_eq!(admits(least.saturating_add(above)), Ok(()));
        prop_assert!(admits(least - 1).is_err());
    }
}

#[test]
fn the_least_budget_admits_the_protocol_s_least_limits_and_a_byte_less_does_not() {
    let many = [524_289, 600_000, 1 << 30, (1 << 43) - 1];
    for readers in [1, 2, 4, 16, 64, 1_000].into_iter().chain(many) {
        let least = least(readers).unwrap();
        let admits = |capacity: u64| admitted(Shares::of(capacity), readers);
        assert!(
            admits(least).frame_bytes >= MIN_FRAME_BYTES,
            "{readers} readers"
        );
        assert!(
            admits(least).dictionary_bytes >= MIN_DICTIONARY_BYTES,
            "{readers} readers"
        );
        assert_eq!(admits(least).admit_peer(), Ok(()), "{readers} readers");
        assert!(admits(least - 1).admit_peer().is_err(), "{readers} readers");
    }
    // Many reads each keep little: their dictionaries' least then sets the budget's, two least
    // dictionaries for each read in a quarter of it.
    assert_eq!(least(19), Some(19 * 8 * MIN_DICTIONARY_BYTES));
    assert_eq!(least(600_000), Some(600_000 * 8 * MIN_DICTIONARY_BYTES));
    // From 2^43 reads not even the largest budget does.
    for readers in [1 << 43, usize::MAX] {
        assert_eq!(least(readers), None, "{readers} readers");
        let limits = admitted(Shares::of(u64::MAX), readers);
        assert!(limits.admit_peer().is_err(), "{readers} readers");
    }
}
