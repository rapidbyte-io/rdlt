use proptest::prelude::*;

use super::TraceParent;

#[test]
fn only_a_well_formed_version_00_traceparent_reads() {
    let good = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let read = TraceParent::parse(good.as_bytes()).expect("well formed");
    assert_eq!(read.to_string(), good);
    assert_eq!(read.trace_id()[0], 0x0a);
    assert_eq!(read.parent_id()[7], 0x31);
    assert_eq!(read.flags(), 1);
    let flagged = TraceParent::parse(b"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-a7");
    assert_eq!(flagged.map(|parent| parent.flags()), Some(0xa7));
    for bad in [
        "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        "00-0AF7651916CD43DD8448EB211C80319C-b7ad6b7169203331-01",
        "00-00000000000000000000000000000000-b7ad6b7169203331-01",
        "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01",
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01-extra",
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b716920333-01",
        "00_0af7651916cd43dd8448eb211c80319c_b7ad6b7169203331_01",
        "00-0af7651916cd43dd8448eb211c80319c_b7ad6b7169203331-01",
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331_01",
        "00-0af7651916cd43dd8448eb211c80319g-b7ad6b7169203331-01",
        "",
    ] {
        assert_eq!(TraceParent::parse(bad.as_bytes()), None, "{bad}");
    }
}

proptest! {
    #[test]
    fn a_traceparent_reads_back_as_it_was_written(trace in any::<[u8; 16]>(), parent in any::<[u8; 8]>(), flags in any::<u8>()) {
        if let Some(written) = TraceParent::new(trace, parent, flags) {
            prop_assert_eq!(written.to_string().len(), crate::limits::TRACEPARENT_BYTES);
            prop_assert_eq!(TraceParent::parse(written.to_string().as_bytes()), Some(written));
        }
    }

    #[test]
    fn a_valid_header_with_one_byte_changed_reads_only_where_it_is_still_valid(
        at in 0usize..55,
        byte in any::<u8>(),
    ) {
        let good = b"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        let mut changed = good.to_vec();
        changed[at] = byte;
        let read = TraceParent::parse(&changed);
        let digit = byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte);
        let valid = match at {
            0 | 1 => false,
            2 | 35 | 52 => byte == b'-' || byte == good[at],
            _ => digit,
        };
        // A byte that keeps the header as it was reads as it did.
        prop_assert_eq!(read.is_some(), valid || byte == good[at]);
    }

    #[test]
    fn any_bytes_read_without_a_panic(bytes in proptest::collection::vec(any::<u8>(), 0..80)) {
        let _ = TraceParent::parse(&bytes);
    }
}
