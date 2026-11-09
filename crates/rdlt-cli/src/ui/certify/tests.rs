use std::time::Duration;

use rdlt_certify::{Clause, ClauseResult, Outcome, Report};

use super::*;

fn passed() -> ClauseResult {
    ClauseResult {
        clause: Clause {
            id: "P-ORDER",
            statement: "a statement",
            unless: "",
        },
        outcome: Outcome::Passed,
        note: None,
    }
}

#[test]
fn a_clause_shows_its_time_only_from_a_second_and_in_whole_seconds() {
    let ui = Ui::for_tests();
    let line = |took| kept_line(&passed(), took, &ui);
    assert_eq!(line(None), "  ✓ P-ORDER");
    assert_eq!(
        line(Some(Duration::from_millis(999))),
        "  ✓ P-ORDER",
        "under a second is the sampling's"
    );
    assert_eq!(line(Some(Duration::from_millis(2_500))), "  ✓ P-ORDER  2s");
}

#[test]
fn a_verdict_says_which_role_it_is_for() {
    let ui = Ui::for_tests();
    let report = Report {
        connector: "io.rapidbyte.generator".to_owned(),
        results: vec![passed()],
    };
    let source = verdict_line(&report, rdlt_connector::Role::Source, &ui);
    assert!(source.starts_with("✓ Passed  source  "), "{source}");
    let destination = verdict_line(&report, rdlt_connector::Role::Destination, &ui);
    assert!(
        destination.starts_with("✓ Passed  destination  "),
        "{destination}"
    );
}
