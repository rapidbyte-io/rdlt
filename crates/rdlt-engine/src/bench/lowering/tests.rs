use super::Lowering;

#[test]
fn each_kind_of_batch_prepares_the_rows_it_keeps_on_every_run() {
    let all = Lowering::all(64);
    let names: Vec<&str> = all.iter().map(Lowering::name).collect();
    assert_eq!(names.len(), 9);
    assert_eq!(names[0], "append_native");
    for lowering in &all {
        assert_eq!(lowering.rows(), 64, "{}", lowering.name());
        let kept = if lowering.name() == "merge_duplicates" {
            32
        } else {
            64
        };
        for _ in 0..2 {
            assert_eq!(lowering.prepare(), kept, "{}", lowering.name());
        }
    }
}
