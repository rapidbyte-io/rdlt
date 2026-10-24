use super::Ids;

#[test]
fn two_rows_of_a_table_sharing_an_id_are_a_finding_but_a_merges_roots() {
    let mut ids = Ids::new(false);
    assert_eq!(ids.record("a".to_owned(), "1:x"), None);
    assert_eq!(
        ids.record("b".to_owned(), "1:x"),
        None,
        "rows alike, ids apart"
    );
    assert_eq!(
        ids.record("a".to_owned(), "2:y"),
        Some("rows 1:x and 2:y share an id".to_owned())
    );
    let mut roots = Ids::new(true);
    assert_eq!(roots.record("k".to_owned(), "1:x"), None);
    assert_eq!(roots.record("k".to_owned(), "1:y"), None);
    let rows: Vec<(String, String)> = roots.into_rows().collect();
    assert_eq!(rows, [("k".to_owned(), "1:y".to_owned())]);
}
