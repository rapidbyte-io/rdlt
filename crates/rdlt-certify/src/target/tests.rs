use std::time::Duration;

use rdlt_connector::serve::Served;
use rdlt_connector::wire::v1;
use rdlt_host::{Options, ReadWindow, Redactions};
use rdlt_wire::Limits;
use rdlt_wire::prost::Message as _;
use rdlt_wire::tonic::{Code, Status};

use super::{BARRIER_FLOOR, KILL_FLOOR, Target};

#[test]
fn s_barrier_reads_a_frame_at_a_time_and_every_other_source_clause_as_the_engine_reads() {
    let served = || Target::served(Served::new());
    let given = Options {
        heartbeat: Duration::from_secs(1),
        ..Options::default()
    };
    // Options of a certifier's own set every clause's credit but `S-BARRIER`'s and the kill
    // clauses', which stays as the certification grants it.
    for (target, options) in [
        (served(), Options::default()),
        (served().options(given), given),
    ] {
        assert_eq!(target.connections(), options);
        assert_eq!(target.connections().read_window, ReadWindow::Growing);
        let barriers = target.barriers_read();
        assert_eq!(barriers.heartbeat, options.heartbeat);
        let barriers = (barriers.read_floor, barriers.read_window);
        assert_eq!(barriers, (BARRIER_FLOOR, ReadWindow::Fixed));
        let killing = target.killing(&Limits::default());
        let killed = (killing.read_floor, killing.read_window);
        assert_eq!(killed, (KILL_FLOOR, ReadWindow::Growing));
    }
}

#[test]
fn an_error_a_connector_sends_is_scrubbed_of_what_it_was_sent_before_it_is_shown() {
    // Shown first, the run of spaces would be one, and the secret said escaped not found.
    let secret = "pass  \"w0rd\"";
    let redactions = Redactions::new();
    redactions.add(secret);
    let target = Target::served(Served::new()).redactions(redactions);
    let sent = v1::Error {
        kind: v1::ErrorKind::Auth as i32,
        message: format!("the password {secret:?} was refused\u{1b}[2J"),
        code: Some("auth".to_owned()),
        retry_after: None,
        limit: None,
        causes: Vec::new(),
    };
    let carried = Status::with_details(Code::Unauthenticated, "", sent.encode_to_vec().into());
    let shown = r#"the password "***" was refused\u{1b}[2J"#;
    assert_eq!(target.error(&carried).to_string(), shown);
    assert_eq!(target.decoded(sent).expect("it decodes").to_string(), shown);
    // A status with no error in its details shows the peer's message, scrubbed too.
    let said = Status::new(Code::Unavailable, format!("gone with {secret:?}"));
    let transport = target.error(&said).to_string();
    assert!(!transport.contains("w0rd"), "{transport}");
}
