use super::OpenKeys;

/// Notes each of `keys` in `text` and closes the object: the key repeated first.
fn repeated<'a>(open: &mut OpenKeys, text: &'a str, keys: &[&str]) -> Option<&'a str> {
    open.open();
    for key in keys {
        let start = text.find(key).unwrap();
        open.key(start..start + key.len());
    }
    open.close(text).map(|key| &text[key])
}

#[test]
fn an_object_s_keys_refuse_only_a_repeat_the_first_one_named_again() {
    let mut open = OpenKeys::default();
    assert_eq!(repeated(&mut open, "abc", &["a", "b", "c"]), None);
    let text = "a b c b a";
    open.open();
    for start in [0, 2, 4, 6, 8] {
        open.key(start..start + 1);
    }
    // `b` is named again before `a` is.
    assert_eq!(open.close(text).map(|key| &text[key]), Some("b"));
    assert_eq!(repeated(&mut open, "", &[]), None);
}

#[test]
fn keys_belong_to_their_own_object_and_close_with_it() {
    let text = "ab";
    let mut open = OpenKeys::default();
    open.open();
    open.key(0..1);
    open.open();
    // The inner object names `a` and `b` once each, though its outer one named `a`.
    open.key(0..1);
    open.key(1..2);
    assert_eq!(open.close(text), None);
    open.key(1..2);
    assert_eq!(open.close(text), None);
    // An object left open, as by a value that stopped, holds keys no later object reads.
    open.open();
    open.key(0..1);
    open.open();
    open.key(0..1);
    assert_eq!(open.close(text), None);
}
