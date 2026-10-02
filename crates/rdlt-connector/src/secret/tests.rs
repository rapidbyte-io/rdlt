use super::Secret;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Config {
    user: String,
    password: Secret<String>,
}

#[test]
fn secrets_deserialize_transparently_and_expose_their_value() {
    let config: Config = serde_json::from_str(r#"{"user":"ann","password":"hunter2"}"#).unwrap();
    assert_eq!(config.password.expose(), "hunter2");
}

#[test]
fn secrets_never_print_or_serialize_their_value() {
    let config = Config {
        user: "ann".to_owned(),
        password: Secret::new("hunter2".to_owned()),
    };
    let rendered = [
        format!("{config:?}"),
        format!("{}", config.password),
        serde_json::to_string(&config).unwrap(),
    ];
    for text in rendered {
        assert!(!text.contains("hunter2"), "{text}");
        assert!(text.contains("***"), "{text}");
    }
}

/// A value that records, where its owner can still read it, that it was wiped.
struct Watched(std::rc::Rc<std::cell::Cell<bool>>);

impl zeroize::Zeroize for Watched {
    fn zeroize(&mut self) {
        self.0.set(true);
    }
}

#[test]
fn a_secret_is_wiped_when_it_is_dropped() {
    let wiped = std::rc::Rc::new(std::cell::Cell::new(false));
    let secret = Secret::new(Watched(std::rc::Rc::clone(&wiped)));
    assert!(!wiped.get());
    drop(secret);
    assert!(wiped.get());
}

#[test]
fn a_secret_is_copied_only_on_purpose_and_each_copy_is_its_own() {
    // No `Clone`: a struct holding a secret derives none, and a copy is asked for by name.
    secret_is_not_clone();
    let secret = Secret::new("hunter2".to_owned());
    let copy = secret.duplicate();
    assert_ne!(secret.expose().as_ptr(), copy.expose().as_ptr());
    drop(secret);
    assert_eq!(copy.expose(), "hunter2");
}

/// Fails to compile if `Secret<String>` is `Clone`: the two impls below would overlap.
fn secret_is_not_clone() {
    trait NotClone<Marker> {
        fn check() {}
    }
    impl<T> NotClone<()> for T {}
    impl<T: Clone> NotClone<u8> for T {}
    <Secret<String> as NotClone<_>>::check();
}

#[test]
fn secrets_of_text_and_bytes_are_equal_exactly_when_their_values_are() {
    let text = |value: &str| Secret::new(value.to_owned());
    assert_eq!(text("hunter2"), text("hunter2"));
    for other in ["hunter3", "Hunter2", "hunter", "hunter22", ""] {
        assert_ne!(text("hunter2"), text(other), "{other}");
    }
    assert_eq!(text(""), text(""));
    assert_eq!(Secret::new(vec![1_u8, 2]), Secret::new(vec![1_u8, 2]));
    assert_ne!(Secret::new(vec![1_u8, 2]), Secret::new(vec![1_u8, 3]));
}

#[test]
fn a_redaction_written_is_not_read_back_as_the_secret() {
    let config = Config {
        user: "ann".to_owned(),
        password: Secret::new("hunter2".to_owned()),
    };
    let text = serde_json::to_string(&config).unwrap();
    assert_eq!(text, r#"{"user":"ann","password":"***"}"#);
    assert_eq!(
        serde_json::to_string(&Secret::new(7_u64)).unwrap(),
        r#""***""#
    );
}
