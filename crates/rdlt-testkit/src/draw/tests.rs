use proptest::prelude::*;

use super::{draw, mix};

#[test]
fn a_draw_is_a_pure_function_of_its_seed() {
    let strategy = any::<(u64, String)>();
    assert_eq!(draw(&strategy, 7), draw(&strategy, 7));
    let draws: std::collections::BTreeSet<(u64, String)> =
        (0..16).map(|seed| draw(&strategy, seed)).collect();
    assert_eq!(draws.len(), 16, "different seeds draw different values");
}

#[test]
fn mixing_spreads_neighboring_states() {
    assert_ne!(mix(1), mix(2));
    assert_ne!(mix(0), 0);
    assert_eq!(mix(1), mix(1));
}

/// The environment variable under which [`print_a_draw`] prints.
const PRINT: &str = "RDLT_TESTKIT_PRINT_DRAW";

#[test]
#[ignore = "run by a_draw_does_not_depend_on_proptests_environment, which reads what it prints"]
fn print_a_draw() {
    if std::env::var_os(PRINT).is_some() {
        // Half of what this strategy generates is rejected, as the kit's finite floats are.
        let odd = any::<u64>().prop_filter("odd", |value| value % 2 == 1);
        println!("DRAW {:?}", draw(&odd, 7));
    }
}

#[test]
fn a_draw_does_not_depend_on_proptests_environment() {
    let drawn = |rejects: Option<&str>| {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "draw::tests::print_a_draw",
                "--ignored",
                "--nocapture",
            ])
            .env(PRINT, "1")
            .env_remove("PROPTEST_MAX_LOCAL_REJECTS");
        if let Some(rejects) = rejects {
            command.env("PROPTEST_MAX_LOCAL_REJECTS", rejects);
        }
        let output = String::from_utf8(command.output().unwrap().stdout).unwrap();
        output
            .lines()
            .find(|line| line.starts_with("DRAW "))
            .map(ToOwned::to_owned)
    };
    let drawn_alone = drawn(None);
    assert!(drawn_alone.is_some());
    assert_eq!(drawn(Some("0")), drawn_alone);
}
