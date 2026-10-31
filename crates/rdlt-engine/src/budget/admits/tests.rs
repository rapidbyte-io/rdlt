use std::sync::Arc;

use arrow_array::builder::StringViewBuilder;
use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, DictionaryArray, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Fields, Schema, UnionFields, UnionMode};
use proptest::prelude::*;
use rdlt_connector::cost::{Allocations, batch_charge, json_charge};
use rdlt_wire::limits::{Class, MESSAGE_ROOM, MIN_DICTIONARY_BYTES, MIN_FRAME_BYTES};
use rdlt_wire::{Decoder, Encoder, Limits};

use super::{HALVED, admitted, kept, least, parts, pushed_arrays, row_overhead};
use crate::budget::Shares;
use crate::fixtures::lowering::wide::{dictionaries, integers};
use crate::limits::{COLUMN_RECORD, SCHEMA_KEPT};

const MIB: u64 = 1 << 20;

#[test]
fn the_default_budget_admits_these_limits() {
    let limits = admitted(Shares::of(256 * MIB), 16);
    assert_eq!(
        limits,
        Limits {
            // Half the 64 MiB a request takes, less a row's nulls, flags (781 bytes and their
            // hex), metadata and its piece's arrays (320 bytes each, two a column and sixteen),
            // in a table of every column: that binds, not the 96 MiB pushes may take.
            frame_bytes: 32 * MIB - 6_241 * 33 - 3 * 781 - 10 - 1_024 - (2 * 6_241 + 16) * 320,
            // A third of the 100 MiB pushes may take, less a message's room.
            json_push_bytes: (100 * MIB - MESSAGE_ROOM) / 3,
            // A cursor waiting and an answer for each of sixteen reads, and half the share.
            cursor_bytes: 4 * MIB / 34,
            // A read keeps 4 MiB: half for its dictionaries, half for its schema, whose
            // message is a sixth of that.
            dictionary_bytes: 2 * MIB,
            schema_bytes: 2 * MIB / 6,
            // What the third of a mebibyte of a schema's message carries of short names, fewer
            // than the 16,384 that 8 MiB of tables' records holds at half a kilobyte a column.
            schema_columns: 6_241,
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

/// `columns` columns of a row of text behind 32-bit keys, `c0` on, each column's dictionary a
/// view naming sixteen bytes of its one data buffer, as a sender narrows a frame's views.
fn keyed_views(columns: usize) -> RecordBatch {
    let named = (0..columns).map(|column| {
        let mut views = StringViewBuilder::new();
        views.append_value(format!("{column:>16}"));
        let values = Arc::new(views.finish());
        let keyed = DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0]), values);
        (format!("c{column}"), Arc::new(keyed.unwrap()) as ArrayRef)
    });
    RecordBatch::try_from_iter(named).unwrap()
}

#[test]
fn a_push_s_arrays_and_views_take_at_most_what_each_of_its_columns_leaves_room_for() {
    // Columns of dictionaries of views take the most a column of a frame may beside its buffers:
    // the keys' array, the values' and the values' one data buffer. Dictionaries of text take
    // the two arrays, and integers one.
    for columns in [1, 100, 6_241] {
        let keyed = keyed_views(columns);
        let (keyed, words) = (&keyed, &dictionaries(1, columns));
        let plain = &integers(1, columns, |row| row);
        let beside = |batch: &RecordBatch| batch_charge(batch) - Allocations::of(batch).bytes();
        let columns = u64::try_from(columns).unwrap();
        assert_eq!(beside(keyed), pushed_arrays(columns));
        assert_eq!(beside(words), 2 * beside(plain));
        assert_eq!(beside(plain), columns * 320);
        assert!(beside(words) < beside(keyed));
    }
}

#[test]
fn the_least_memory_admits_these_limits() {
    assert_eq!(least(16), 38_414_264);
    // One read keeps all of the reads' share, so a table may have more columns, whose nulls,
    // flags and arrays a row of a frame takes beside it.
    assert_eq!(least(1), 50_060_952);
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
        (MIN_FRAME_BYTES, 5_001_772, 17_653, 300_111, 50_018, 893)
    );
    // What a sixteenth of the budget holds of one answer decoded.
    assert_eq!(
        (
            limits.catalog_bytes,
            limits.state_bytes,
            limits.control_message_bytes
        ),
        (150_055, 2_400_891, 150_055)
    );
}

#[test]
fn what_reads_keep_is_a_part_a_read_or_twice_the_least_dictionary_where_it_holds_fewer() {
    let least = 2 * MIN_DICTIONARY_BYTES;
    // The default budget: sixteen parts of 4 MiB, a part a read up to 128 reads, each then twice
    // the least dictionary, and no smaller for more.
    let shares = Shares::of(256 * MIB);
    assert_eq!(kept(shares, 16), 4 * MIB);
    assert_eq!(kept(shares, 128), least);
    let kept_limits = |limits: Limits| {
        (
            limits.frame_bytes,
            limits.dictionary_bytes,
            limits.schema_bytes,
            limits.schema_columns,
        )
    };
    let most = kept_limits(admitted(shares, 128));
    assert_eq!(most, (most.0, MIN_DICTIONARY_BYTES, 43_690, 780));
    for readers in [129, 257, 16_384, usize::MAX] {
        assert_eq!(kept(shares, readers), least, "{readers} reads");
        assert_eq!(kept_limits(admitted(shares, readers)), most, "{readers}");
    }
    // A share of fewer bytes than one such part is one part.
    let small = Shares::of(MIB);
    assert_eq!(kept(small, 16), small.reads);
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
        // A frame's message beside what a read keeps and a batch's arrays and buffers fits what
        // pushes may take; the frame, with a row's null in each column and the metadata, fits a
        // request over what a data message holds decoded for each byte.
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
        let reads = u64::try_from(readers).expect("a count");
        let cursors = limits.cursor_bytes.saturating_mul(2 * (reads + 1));
        assert!(cursors <= shares.cursors, "{what}");
        // A schema's message, what its decoder keeps for the schema decoded from it and the
        // dictionaries fit what a read keeps.
        let schema = limits.schema_bytes.saturating_mul(SCHEMA_KEPT);
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
fn every_part_of_what_reads_keep_fits_their_share_and_holds_twice_the_least_dictionary() {
    let twice = 2 * MIN_DICTIONARY_BYTES;
    for (capacity, readers) in budgets().chain([(256 * MIB, 257), (256 * MIB, usize::MAX)]) {
        let shares = Shares::of(capacity);
        let (parts, kept) = (parts(shares, readers).get(), kept(shares, readers));
        let what = format!("{capacity} bytes read by {readers}");
        assert!(parts <= readers, "{what}");
        let all = u64::try_from(parts).unwrap().saturating_mul(kept);
        assert!(all <= shares.reads, "{what}");
        assert!(shares.reads < twice || kept >= twice, "{what}");
    }
}

#[test]
fn every_budget_from_the_least_up_admits_a_peer() {
    // Each byte of a band above the least for thousands of counts of reads, and a stride a
    // million bytes up for a few.
    let band = (1..=4_000).step_by(7).map(|readers| (readers, 3_000, 1));
    let stride = [1, 2, 4, 16, 64, 1_000].map(|readers| (readers, 1_000_000, 997));
    for (readers, above, step) in band.chain(stride) {
        let least = least(readers);
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
        let least = least(readers);
        let admits = |capacity: u64| admitted(Shares::of(capacity), readers).admit_peer();
        prop_assert_eq!(admits(least.saturating_add(above)), Ok(()));
        prop_assert!(admits(least - 1).is_err());
    }
}

#[test]
fn the_least_budget_admits_the_protocol_s_least_limits_and_a_byte_less_does_not() {
    let many = [524_289, 600_000, 1 << 30, (1 << 43) - 1];
    for readers in [1, 2, 4, 16, 64, 1_000].into_iter().chain(many) {
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
    }
    // Many reads each keep a part of twice the least dictionary at least: a frame sets their
    // least, the same from eighteen reads up, whose share holds eighteen parts there.
    assert_eq!(least(18), 37_810_920);
    for readers in [19, HALVED, HALVED + 1, 128, 600_000, 1 << 43, usize::MAX] {
        assert_eq!(least(readers), least(18), "{readers} readers");
        let limits = admitted(Shares::of(least(18) - 1), readers);
        assert!(limits.frame_bytes < MIN_FRAME_BYTES, "{readers} readers");
        assert!(
            limits.dictionary_bytes >= MIN_DICTIONARY_BYTES,
            "{readers} readers"
        );
    }
}

/// Schemas of a thousand fields as Arrow's writer encodes densest beside what a decoder keeps of
/// them: each of a type of no parameters or an empty struct, behind keys or not, named in up to
/// three bytes, with no metadata or an entry or two of up to three bytes each; and a struct nested
/// sixty-three deep, and unions of a hundred and twenty-seven members, each with an entry.
fn densest_schemas() -> Vec<(String, Schema)> {
    let empty = || DataType::Struct(Fields::empty());
    let keyed = |values| DataType::Dictionary(Box::new(DataType::Int8), Box::new(values));
    let types = [
        DataType::Null,
        DataType::Boolean,
        DataType::Utf8,
        empty(),
        keyed(DataType::Null),
        keyed(empty()),
    ];
    let entries: [&[(&str, &str)]; 3] = [&[], &[("abc", "abc")], &[("a", ""), ("b", "")]];
    let field = |name: &str, data_type: DataType, entries: &[(&str, &str)]| {
        let metadata = entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()));
        Field::new(name, data_type, true).with_metadata(metadata.collect())
    };
    let mut schemas = Vec::new();
    for data_type in &types {
        for name in ["", "abc"] {
            for described in entries {
                let fields = (0..1_000).map(|_| field(name, data_type.clone(), described));
                let what = format!("{data_type} named {name:?} with {described:?}");
                schemas.push((what, Schema::new(fields.collect::<Vec<_>>())));
            }
        }
    }
    let one = [("abc", "abc")];
    let mut deep = DataType::Null;
    for _ in 1..63 {
        deep = DataType::Struct(Fields::from(vec![field("abc", deep, &one)]));
    }
    schemas.push((
        "nested 63 deep".into(),
        Schema::new(vec![field("abc", deep, &one)]),
    ));
    let members = (0..127).map(|_| field("abc", DataType::Null, &one));
    let members = UnionFields::try_new(0..127, members).unwrap();
    let union = DataType::Union(members, UnionMode::Dense);
    let unions = (0..8).map(|_| field("abc", union.clone(), &one));
    schemas.push(("unions".into(), Schema::new(unions.collect::<Vec<_>>())));
    schemas
}

#[test]
fn what_a_decoder_keeps_for_a_schema_and_its_message_fit_what_a_read_keeps_for_them() {
    let mut densest = (0, 1, String::new());
    for (what, schema) in densest_schemas() {
        let message = Encoder::default().schema(&schema).unwrap();
        let mut decoder = Decoder::new(Limits::default());
        decoder.schema(&message).unwrap();
        let (kept, message) = (decoder.kept_bytes(), u64::try_from(message.len()).unwrap());
        assert!(
            kept <= SCHEMA_KEPT * message,
            "{what}: {kept} kept for a message of {message}"
        );
        if kept * densest.1 > densest.0 * message {
            densest = (kept, message, what);
        }
    }
    // A read keeping its schema's message one time fewer would be refused the densest.
    let (kept, message, what) = densest;
    assert!(
        kept > (SCHEMA_KEPT - 1) * message,
        "{what}: {kept} kept for a message of {message}"
    );
}
