use std::time::Duration;

use rdlt_certify::{Clause, ClauseResult, Held, Outcome, Report};

use super::{kept_line, verdict_line};
use crate::ui::Ui;

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
    assert_eq!(
        line(Some(Duration::from_secs(1))),
        "  ✓ P-ORDER  1s",
        "from a second on"
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
    let held = Held::default();
    let source = verdict_line(&report, rdlt_connector::Role::Source, &held, &ui);
    assert!(source.starts_with("✓ Passed  source  "), "{source}");
    let destination = verdict_line(&report, rdlt_connector::Role::Destination, &held, &ui);
    assert!(
        destination.starts_with("✓ Passed  destination  "),
        "{destination}"
    );
}

#[test]
fn a_verdict_names_the_version_and_binary_its_certification_held_the_connector_to() {
    let ui = Ui::for_tests();
    let report = Report {
        connector: "io.rapidbyte.generator".to_owned(),
        results: vec![passed()],
    };
    let held = Held {
        version: Some("1.0.0".to_owned()),
        digest: Some(rdlt_host::Digest([0xab; 32])),
    };
    let line = verdict_line(&report, rdlt_connector::Role::Source, &held, &ui);
    let named = format!("io.rapidbyte.generator 1.0.0 sha256:{}  ", "ab".repeat(6));
    assert!(line.contains(&named), "{line}");
    let unheld = verdict_line(&report, rdlt_connector::Role::Source, &Held::default(), &ui);
    assert!(unheld.contains("io.rapidbyte.generator  "), "{unheld}");
}
