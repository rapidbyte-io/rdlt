use super::rate;

const SEED: &str = r#"{"outcome":"passed","seed":0,"wall_ms":210.5}"#;

#[test]
fn the_rate_is_the_summary_s() {
    let timings = format!(
        "{SEED}\n{}\n",
        r#"{"elapsed_ms":80.0,"failed":0,"seeds":1,"seeds_per_s":12.5,"threads":4}"#
    );
    assert!((rate(&timings).unwrap() - 12.5).abs() < f64::EPSILON);
}

#[test]
fn a_sweep_without_a_summary_with_failures_or_without_seeds_has_no_rate() {
    let cases = [
        String::new(),
        format!("{SEED}\n"),
        format!("{SEED}\n{}", r#"{"failed":1,"seeds":1,"seeds_per_s":12.5}"#),
        r#"{"failed":0,"seeds":0,"seeds_per_s":0.0}"#.to_owned(),
        "not json".to_owned(),
    ];
    for timings in cases {
        assert!(rate(&timings).is_err(), "{timings:?}");
    }
}
