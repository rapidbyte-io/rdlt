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
