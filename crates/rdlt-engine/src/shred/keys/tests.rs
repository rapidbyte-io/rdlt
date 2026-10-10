use std::borrow::Cow;

use super::ObjectKeys;
use crate::shred::ShredError;

#[test]
fn an_object_s_keys_refuse_only_a_repeat_whether_borrowed_or_owned() {
    let mut keys = ObjectKeys::default();
    assert!(keys.note(Cow::Borrowed("a")).is_ok());
    assert!(keys.note(Cow::Owned("b".to_owned())).is_ok());
    assert!(matches!(
        keys.note(Cow::Owned("a".to_owned())),
        Err(ShredError::DuplicateKey(key)) if key == "a"
    ));
    assert!(matches!(
        keys.note(Cow::Borrowed("b")),
        Err(ShredError::DuplicateKey(_))
    ));
    assert!(keys.note(Cow::Borrowed("c")).is_ok());
}
