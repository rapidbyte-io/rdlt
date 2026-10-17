use super::OpenKeys;
use crate::shred::meter::{Meter, Over};

/// Notes each of `keys` in `text` and closes the object: the key repeated first.
fn repeated<'a>(open: &mut OpenKeys, text: &'a str, keys: &[&str]) -> Option<&'a str> {
    let meter = Meter::reserved();
    open.open(&meter).unwrap();
    for key in keys {
        let start = text.find(key).unwrap();
        open.key(start..start + key.len(), &meter).unwrap();
    }
    open.close(text).map(|key| &text[key])
}

#[test]
fn an_object_s_keys_refuse_only_a_repeat_the_first_one_named_again() {
    let meter = Meter::reserved();
    let mut open = OpenKeys::default();
    assert_eq!(repeated(&mut open, "abc", &["a", "b", "c"]), None);
    let text = "a b c b a";
    open.open(&meter).unwrap();
    for start in [0, 2, 4, 6, 8] {
        open.key(start..start + 1, &meter).unwrap();
    }
    // `b` is named again before `a` is.
    assert_eq!(open.close(text).map(|key| &text[key]), Some("b"));
    assert_eq!(repeated(&mut open, "", &[]), None);
}

#[test]
fn keys_belong_to_their_own_object_and_close_with_it() {
    let meter = Meter::reserved();
    let text = "ab";
    let mut open = OpenKeys::default();
    open.open(&meter).unwrap();
    open.key(0..1, &meter).unwrap();
    open.open(&meter).unwrap();
    // The inner object names `a` and `b` once each, though its outer one named `a`.
    open.key(0..1, &meter).unwrap();
    open.key(1..2, &meter).unwrap();
    assert_eq!(open.close(text), None);
    open.key(1..2, &meter).unwrap();
    assert_eq!(open.close(text), None);
    // An object left open, as by a value that stopped, holds keys no later object reads.
    open.open(&meter).unwrap();
    open.key(0..1, &meter).unwrap();
    open.open(&meter).unwrap();
    open.key(0..1, &meter).unwrap();
    assert_eq!(open.close(text), None);
}

#[test]
fn keys_and_objects_charge_what_they_grow_by_before_they_grow() {
    let meter = Meter::new(1_000);
    let mut open = OpenKeys::default();
    open.open(&meter).unwrap();
    assert_eq!(meter.spent(), super::OBJECT_BYTES);
    // Room for one key, then two, then four: each growth charged as it is made.
    for start in 0..3 {
        open.key(start..start + 1, &meter).unwrap();
    }
    assert_eq!(meter.spent(), super::OBJECT_BYTES + 4 * super::KEY_BYTES);
    assert_eq!(open.close("abc"), None);
    // An object the meter has no room for is not opened.
    let tight = Meter::new(0);
    let mut refused = OpenKeys::default();
    assert_eq!(refused.open(&tight), Err(Over));
    assert_eq!(refused.close(""), None);
}

#[test]
fn a_key_and_an_object_are_reckoned_as_large_as_their_entries() {
    let entry = |bytes: usize| u64::try_from(bytes).unwrap();
    assert_eq!(super::KEY_BYTES, entry(size_of::<std::ops::Range<usize>>()));
    assert_eq!(super::OBJECT_BYTES, entry(size_of::<usize>()));
}
