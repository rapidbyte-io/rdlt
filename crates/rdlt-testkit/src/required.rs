//! Checks that a persisted form refuses a record that lacks any member it writes.

use serde::de::DeserializeOwned;
use serde_json::Value;

/// One step into a JSON value: an object's member, or an array's item.
#[derive(Clone, Debug)]
enum Step {
    Member(String),
    Item(usize),
}

/// The paths of every member of every object in `value`, below `at`.
fn members(value: &Value, at: &[Step], found: &mut Vec<Vec<Step>>) {
    let below = |step: Step| [at, &[step]].concat();
    match value {
        Value::Object(fields) => {
            for (name, field) in fields {
                let path = below(Step::Member(name.clone()));
                members(field, &path, found);
                found.push(path);
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                members(item, &below(Step::Item(index)), found);
            }
        }
        _ => {}
    }
}

/// `value` without the member at `path`.
fn without(value: &Value, path: &[Step]) -> Value {
    let mut value = value.clone();
    let (last, steps) = path.split_last().expect("a member's path has a step");
    let mut at = &mut value;
    for step in steps {
        at = match step {
            Step::Member(name) => &mut at[name.as_str()],
            Step::Item(index) => &mut at[*index],
        };
    }
    if let (Step::Member(name), Value::Object(fields)) = (last, at) {
        fields.remove(name);
    }
    value
}

/// Asserts that `T` reads `value`, and refuses it without any one of the members its objects
/// hold, those holding `null` among them.
///
/// # Panics
///
/// Panics where `T` does not read `value`, or reads it without one of its members.
pub fn every_member_required<T: DeserializeOwned>(value: &Value) {
    assert!(
        serde_json::from_value::<T>(value.clone()).is_ok(),
        "{value}"
    );
    let mut found = Vec::new();
    members(value, &[], &mut found);
    for path in found {
        let lacking = without(value, &path);
        assert!(
            serde_json::from_value::<T>(lacking.clone()).is_err(),
            "{lacking} lacks {path:?} and is read"
        );
    }
}
