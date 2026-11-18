use super::staged;

#[test]
fn no_two_segments_stage_a_row_of_one_id() {
    let mut ids: Vec<i64> = (1..=9).flat_map(staged).map(|(id, _)| id).collect();
    let staged = ids.len();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), staged, "{ids:?}");
}

#[test]
fn a_segment_stages_three_rows_of_its_hundreds() {
    let named = |id, name: Option<&str>| (id, name.map(str::to_owned));
    let expected = [
        named(101, Some("ann")),
        named(102, None),
        named(103, Some("ola")),
    ];
    assert_eq!(staged(1), expected);
}
