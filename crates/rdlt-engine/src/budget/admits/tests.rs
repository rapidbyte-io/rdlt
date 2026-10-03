use rdlt_wire::Limits;
use rdlt_wire::limits::{Class, MIN_DICTIONARY_BYTES, MIN_FRAME_BYTES};

use super::{admitted, kept, least, row_overhead};
use crate::budget::Shares;
use crate::limits::{COLUMN_RECORD, RECORDED};

const MIB: u64 = 1 << 20;

#[test]
fn the_default_budget_admits_these_limits() {
    let limits = admitted(Shares::of(256 * MIB), 16);
    assert_eq!(
        limits,
        Limits {
            // Half the 64 MiB a request for lowering takes, less a row's nulls and metadata in
            // a table of every column it may have.
            frame_bytes: 32 * MIB - 7_489 * 33 - 1_024,
            // A third of the 84 MiB pushes may take.
            json_push_bytes: 84 * MIB / 3,
            // A cursor waiting and an answer for each of sixteen reads, and half the share.
            cursor_bytes: 4 * MIB / 34,
            // A read keeps 4 MiB: half for its dictionaries, half for its schema, whose
            // message is a fifth of that.
            dictionary_bytes: 2 * MIB,
            schema_bytes: 2 * MIB / 5,
            // What the 0.4 MiB of a schema's message carries of short names, fewer than the
            // 8,192 that 8 MiB of tables' records holds at a kilobyte a column.
            schema_columns: 7_489,
            // What the 16 MiB of answers being decoded holds of one decoded: a catalog at
            // sixteen times its bytes; state, bounded decoded, the protocol's, as is any other
            // control message.
            catalog_bytes: MIB,
            ..Limits::default()
        }
    );
}

#[test]
fn the_least_memory_admits_these_limits() {
    assert_eq!(least(16), 33_811_576);
    // One read keeps all of the reads' share, and a frame beside it fits what pushes may take.
    assert_eq!(least(1), 53_687_073);
    let limits = admitted(Shares::of(least(16)), 16);
    assert_eq!(
        (
            limits.frame_bytes,
            limits.json_push_bytes,
            limits.cursor_bytes,
            limits.dictionary_bytes,
            limits.schema_bytes,
            limits.schema_columns
        ),
        (MIN_FRAME_BYTES, 3_698_142, 15_538, 264_152, 52_830, 943)
    );
    // What a sixteenth of the budget holds of one answer decoded.
    assert_eq!(
        (
            limits.catalog_bytes,
            limits.state_bytes,
            limits.control_message_bytes
        ),
        (132_076, 2_113_223, 132_076)
    );
}

/// Budgets of every size from a few kilobytes up, each read by as few and as many reads.
fn budgets() -> impl Iterator<Item = (u64, usize)> {
    let capacities = [
        64 * 1024,
        MIB,
        16 * MIB,
        least(16),
        least(16) + 1,
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
        // A frame's batch, beside what a read keeps, fits what pushes may take; and one row of
        // it, with a null in every column its table may have and its metadata, fits what a row
        // may take where the load keeps a log.
        assert!(
            limits.frame_bytes.saturating_add(kept) <= shares.intake.max(kept),
            "{what}"
        );
        let row = limits
            .frame_bytes
            .saturating_add(row_overhead(limits.schema_columns));
        assert!(
            limits.frame_bytes == 0 || row <= shares.request / 2,
            "{what}"
        );
        assert!(
            limits.json_push_bytes.saturating_mul(3) <= shares.intake,
            "{what}"
        );
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
        // A seal's frame records two cursors twice over; a commit's every cursor waiting, its
        // value in base64 twice over.
        assert!(
            limits.cursor_bytes.saturating_mul(4) <= shares.log,
            "{what}"
        );
        assert!(shares.cursors.saturating_mul(3) <= shares.log, "{what}");
        // A schema's message, the schema decoded from it and the dictionaries fit what a read
        // keeps.
        let schema = limits.schema_bytes.saturating_mul(5);
        assert!(
            schema.saturating_add(limits.dictionary_bytes) <= kept,
            "{what}"
        );
        // A table of every column a schema may have records within the tables' share.
        let records = limits
            .schema_columns
            .saturating_mul(RECORDED * COLUMN_RECORD);
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
fn the_least_budget_admits_the_protocol_s_least_limits_and_a_byte_less_does_not() {
    for readers in [1, 2, 4, 16, 64, 1_000] {
        let least = least(readers);
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
        // Every budget above the least admits a peer too.
        for capacity in (least..least + 1_000_000).step_by(997) {
            assert_eq!(admits(capacity).admit_peer(), Ok(()), "{capacity}");
        }
    }
    // Many reads each keep little: their dictionaries' least then sets the budget's.
    assert!(least(1_000) > least(16));
}
