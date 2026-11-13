//! The configuration's published JSON Schema says what each field takes by default.

use serde_json::{Value, json};

#[test]
fn the_schema_publishes_each_field_s_default_and_the_page_size_s_range() {
    let schema = serde_json::to_value(schemars::schema_for!(pokeapi::Config)).unwrap();
    let field = |name: &str| &schema["properties"][name];
    let defaults = ["base_url", "page_size", "limit"].map(|name| field(name)["default"].clone());
    let expected = [json!("https://pokeapi.co/api/v2"), json!(50), json!(151)];
    assert_eq!(defaults, expected);
    let range = (
        &field("page_size")["minimum"],
        &field("page_size")["maximum"],
    );
    assert_eq!(range, (&Value::from(1), &Value::from(100)));
}
