//! A head's reference to the body of a chunk uploaded in parts, read back only whole and intact,
//! and the kinds of heads a store remembers.

use super::{chunk, pipeline};
use crate::limits::OBJECT_HEADS;
use crate::wal::object::head::{Heads, Kind, REFERENCE, Reference};

fn reference() -> Reference {
    Reference {
        token: 0x0123_4567_89ab_cdef_fedc_ba98_7654_3210,
        len: 17 << 30,
    }
}

#[test]
fn a_reference_reads_back_as_written() {
    let encoded = reference().encode();
    assert_eq!(encoded.len(), REFERENCE);
    assert_eq!(Reference::decode(&encoded), Some(reference()));
}

#[test]
fn a_reference_with_any_byte_changed_reads_as_none() {
    let encoded = reference().encode();
    for at in 0..REFERENCE {
        for flip in [0x01, 0x80, 0xff] {
            let mut damaged = encoded.to_vec();
            damaged[at] ^= flip;
            assert_eq!(Reference::decode(&damaged), None, "byte {at} ^ {flip:#x}");
        }
    }
}

#[test]
fn a_reference_of_another_format_its_checksum_good_reads_as_none() {
    let mut other = reference().encode().to_vec();
    other[8] = 2;
    let crc = crc32c::crc32c(&other[..REFERENCE - 4]);
    other[REFERENCE - 4..].copy_from_slice(&crc.to_le_bytes());
    assert_eq!(Reference::decode(&other), None);
}

#[test]
fn bytes_shorter_or_longer_than_a_reference_read_as_none() {
    let encoded = reference().encode();
    for len in 0..REFERENCE {
        assert_eq!(Reference::decode(&encoded[..len]), None, "{len} bytes");
    }
    let mut longer = encoded.to_vec();
    longer.push(0);
    assert_eq!(Reference::decode(&longer), None);
}

#[test]
fn a_store_remembers_as_many_heads_as_it_may_forgetting_the_first_beyond() {
    let heads = Heads::default();
    let orders = pipeline("orders");
    let at = |number: usize| chunk(1, u64::try_from(number).expect("a number"));
    for number in 0..OBJECT_HEADS {
        heads.note(&orders, at(number), Kind::Whole);
    }
    for number in 0..OBJECT_HEADS {
        assert_eq!(
            heads.kind(&orders, at(number)),
            Some(Kind::Whole),
            "{number}"
        );
    }
    let parts = Kind::Parts(reference());
    heads.note(&orders, at(OBJECT_HEADS), parts);
    assert_eq!(heads.kind(&orders, at(0)), None, "the first is forgotten");
    assert_eq!(heads.kind(&orders, at(1)), Some(Kind::Whole));
    assert_eq!(heads.kind(&orders, at(OBJECT_HEADS)), Some(parts));
}

#[test]
fn a_store_forgets_a_head_deleted_and_every_head_of_a_log_removed() {
    let heads = Heads::default();
    let (orders, users) = (pipeline("orders"), pipeline("users"));
    for (owner, load) in [(&orders, 1), (&orders, 2), (&users, 1)] {
        for number in 0..3 {
            heads.note(owner, chunk(load, number), Kind::Whole);
        }
    }
    heads.forget(&orders, chunk(1, 1));
    assert_eq!(heads.kind(&orders, chunk(1, 1)), None);
    assert_eq!(heads.kind(&orders, chunk(1, 0)), Some(Kind::Whole));
    heads.forget_log(&orders, chunk(1, 0).load);
    for number in 0..3 {
        assert_eq!(heads.kind(&orders, chunk(1, number)), None, "{number}");
        assert_eq!(heads.kind(&orders, chunk(2, number)), Some(Kind::Whole));
        assert_eq!(heads.kind(&users, chunk(1, number)), Some(Kind::Whole));
    }
}
