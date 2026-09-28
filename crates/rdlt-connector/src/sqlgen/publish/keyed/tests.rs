use super::super::super::Column;
use super::unused;

fn columns(names: &[&str]) -> Vec<Column> {
    names
        .iter()
        .map(|name| Column {
            name: (*name).to_owned(),
            declared: "INTEGER".to_owned(),
        })
        .collect()
}

#[test]
fn a_name_is_its_own_unless_a_column_has_it_in_any_case() {
    assert_eq!(unused("_rdlt_rank", &columns(&["id", "seq"])), "_rdlt_rank");
    assert_eq!(
        unused("_rdlt_rank", &columns(&["_RDLT_Rank", "_rdlt_rank_"])),
        "_rdlt_rank__"
    );
}
