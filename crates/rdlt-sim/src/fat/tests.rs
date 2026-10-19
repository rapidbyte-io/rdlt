use super::{carried, condense, condensed, expected, least_columns, least_push, value, widest};
use crate::workload::Row;

fn row(id: i64, offset: i64) -> Row {
    Row {
        id,
        partition: 0,
        offset,
        value: 0,
        key: None,
        tag: None,
        extras: Vec::new(),
        delivered: 0,
    }
}

#[test]
fn a_fat_value_condenses_to_the_short_form_the_model_expects() {
    let row = row(13, 8);
    assert!(carried(&row));
    assert_eq!(condense(&value(&row, 1_000)), Some(expected(&row, 1_000)));
    assert_eq!(expected(&row, 1_000), condensed(3, 1_000));
}

#[test]
fn text_that_differs_from_a_fat_value_anywhere_is_kept_as_it_is() {
    let fat = value(&row(13, 8), 20);
    let changed = [
        fat.replacen("\"f7\":\"", "\"f7\":\"x", 1),
        fat.replacen("\",\"f7\"", "0\",\"f7\"", 1),
        fat.replacen("\"f7\"", "\"g7\"", 1),
        format!("{fat} "),
        fat.replacen('{', "{ ", 1),
        "{\"a\":1}".to_owned(),
        "1".to_owned(),
    ];
    for text in changed {
        assert_eq!(condense(&text), None, "{text:.40}");
    }
}

#[test]
fn the_widest_fat_value_takes_no_more_than_a_sixteenth_of_the_least_push() {
    let text = value(&row(i64::MAX, 0), widest());
    let bytes = u64::try_from(text.len()).unwrap();
    assert!(bytes <= least_push() / 16, "{bytes} of {}", least_push());
}

#[test]
fn the_widest_fat_value_takes_more_than_a_thirty_second_of_the_least_push() {
    let text = value(&row(i64::MAX, 0), widest());
    let bytes = u64::try_from(text.len()).unwrap();
    assert!(bytes > least_push() / 32, "{bytes} of {}", least_push());
}

#[test]
fn the_widest_fat_value_takes_no_more_than_a_thirty_second_of_the_least_columns() {
    assert!(
        widest() <= least_columns() / 32,
        "{} of {}",
        widest(),
        least_columns()
    );
}

#[test]
fn every_eighth_row_carries_a_fat_value_so_no_push_holds_two() {
    let carrying: Vec<i64> = (0..24).filter(|offset| carried(&row(0, *offset))).collect();
    assert_eq!(carrying, [0, 8, 16]);
}
