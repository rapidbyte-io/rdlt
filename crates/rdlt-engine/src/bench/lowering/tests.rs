use super::Lowering;

#[test]
fn each_kind_of_batch_prepares_the_rows_it_keeps_on_every_run() {
    let all = Lowering::all(64);
    let names: Vec<&str> = all.iter().map(Lowering::name).collect();
    assert_eq!(names.len(), 10);
    assert_eq!(names[0], "append_native");
    for lowering in &all {
        assert_eq!(lowering.rows(), 64, "{}", lowering.name());
        let kept = match lowering.name() {
            "merge_duplicates" => 32,
            "one_dropped_row" => 63,
            _ => 64,
        };
        for _ in 0..2 {
            assert_eq!(lowering.prepare(), kept, "{}", lowering.name());
        }
    }
}
