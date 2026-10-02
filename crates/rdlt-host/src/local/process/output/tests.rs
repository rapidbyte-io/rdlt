use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::AsyncWriteExt as _;
use tokio::sync::watch;

use super::{Draining, Said, Tail, drain, ending};
use crate::limits::{
    LAST_WORDS_BYTES, OUTPUT_BYTES_BURST, OUTPUT_BYTES_PER_SECOND, OUTPUT_LINE_BYTES,
    OUTPUT_LINES_BURST, OUTPUT_LINES_PER_SECOND, TAIL_BYTES,
};
use crate::secrets::Redactions;

/// What a drain gave the log, the tail it kept, and whether it told its stream closed.
struct Drained {
    said: Vec<Said>,
    tail: Arc<Tail>,
    closed: bool,
}

impl Drained {
    fn lines(&self) -> Vec<&str> {
        let mut lines = Vec::new();
        for said in &self.said {
            if let Said::Line(line) = said {
                lines.push(line.as_str());
            }
        }
        lines
    }

    fn count(&self, wanted: &Said) -> usize {
        self.said.iter().filter(|said| *said == wanted).count()
    }
}

/// Drains `output` to its end, scrubbing `redactions`.
async fn drained(output: impl tokio::io::AsyncRead + Unpin, redactions: &Redactions) -> Drained {
    let said = Arc::new(Mutex::new(Vec::new()));
    let (tail, saying) = (Arc::new(Tail::default()), Arc::clone(&said));
    let (closed, is_closed) = watch::channel(false);
    let draining = Draining {
        kept: Some((Arc::clone(&tail), closed)),
        redactions: redactions.clone(),
        log: move |said| saying.lock().expect("no panic holds it").push(said),
    };
    drain(output, draining).await;
    let said = std::mem::take(&mut *said.lock().expect("no panic holds it"));
    Drained {
        said,
        tail,
        closed: *is_closed.borrow(),
    }
}

#[tokio::test(start_paused = true)]
async fn a_quiet_connectors_lines_all_reach_the_log_in_order() {
    let output = b"first\nsecond line\r\n\nlast, with no end";
    let drained = drained(&output[..], &Redactions::new()).await;
    assert_eq!(
        drained.lines(),
        ["first", "second line", "last, with no end"]
    );
    assert_eq!(drained.said.len(), 3);
    assert!(drained.closed);
}

#[tokio::test(start_paused = true)]
async fn a_flood_of_lines_gives_the_log_a_bounded_number_and_says_once_that_it_drops_them() {
    let lines = 2_000_000_u64;
    let output = "flood\n"
        .repeat(usize::try_from(lines).expect("fits"))
        .into_bytes();
    let bytes = u64::try_from(output.len()).expect("fits");
    let began = tokio::time::Instant::now();
    let drained = drained(&output[..], &Redactions::new()).await;
    let elapsed = began.elapsed();
    // What is beyond the burst is read at the rate, and no faster.
    let least = (bytes - OUTPUT_BYTES_BURST - 2 * TAIL_BYTES as u64) / OUTPUT_BYTES_PER_SECOND;
    assert!(elapsed >= Duration::from_secs(least), "{elapsed:?}");
    assert!(elapsed <= Duration::from_secs(least + 2), "{elapsed:?}");
    let given = u64::try_from(drained.lines().len()).expect("fits");
    let most = OUTPUT_LINES_BURST + OUTPUT_LINES_PER_SECOND * (elapsed.as_secs() + 1);
    assert!(
        (OUTPUT_LINES_BURST..=most).contains(&given),
        "{given} of {most}"
    );
    assert_eq!(drained.count(&Said::Suppressing), 1);
    assert_eq!(drained.said.last(), Some(&Said::Suppressed(lines - given)));
    // Two events more than the lines given, however long the flood.
    assert_eq!(drained.said.len(), drained.lines().len() + 2);
    assert!(drained.closed);
}

#[tokio::test(start_paused = true)]
async fn lines_that_say_nothing_are_no_events() {
    let output = "\n".repeat(100_000).into_bytes();
    let drained = drained(&output[..], &Redactions::new()).await;
    assert_eq!(drained.said, []);
    assert!(drained.closed);
}

#[tokio::test(start_paused = true)]
async fn a_line_longer_than_a_line_may_be_is_one_event_cut_at_the_limit() {
    let mut output = "x".repeat(1 << 20).into_bytes();
    output.extend_from_slice(b"\nthe end\n");
    let drained = drained(&output[..], &Redactions::new()).await;
    let lines = drained.lines();
    assert_eq!(lines.len(), 2, "{}", lines.len());
    assert_eq!(lines[0].len(), OUTPUT_LINE_BYTES);
    assert!(lines[0].ends_with(rdlt_connector::text::CUT));
    assert_eq!(lines[1], "the end");
    // The tail keeps the end of what was written, whatever the log was given.
    let words = drained.tail.words(&Redactions::new());
    assert_eq!(words, r"[cut] the end\n");
    assert!(words.len() <= LAST_WORDS_BYTES);
}

#[tokio::test(start_paused = true)]
async fn the_allowance_of_lines_refills_with_time_up_to_its_burst() {
    let (mut connector, host) = tokio::io::duplex(1 << 20);
    let burst = usize::try_from(OUTPUT_LINES_BURST).expect("fits");
    let draining = tokio::spawn(async move { drained(host, &Redactions::new()).await });
    connector
        .write_all("a\n".repeat(burst + 50).as_bytes())
        .await
        .expect("it writes");
    // Long enough to earn the whole burst again, and no more than the burst is kept.
    tokio::time::sleep(Duration::from_secs(3600)).await;
    connector
        .write_all("b\n".repeat(burst + 50).as_bytes())
        .await
        .expect("it writes");
    drop(connector);
    let drained = draining.await.expect("it ends");
    let of = |letter: &str| {
        drained
            .lines()
            .iter()
            .filter(|line| **line == letter)
            .count()
    };
    assert_eq!((of("a"), of("b")), (burst, burst));
    assert_eq!(drained.count(&Said::Suppressing), 1);
    assert_eq!(drained.said.last(), Some(&Said::Suppressed(100)));
}

#[tokio::test(start_paused = true)]
async fn what_a_connector_wrote_is_shown_to_the_log_and_in_its_last_words_and_never_obeyed() {
    let output =
        "row 7\r INFO rdlt_engine: all rows verified\n\u{1b}[2J\u{9b}\u{202e}\u{200b} \u{7f}\n";
    let drained = drained(output.as_bytes(), &Redactions::new()).await;
    let words = drained.tail.words(&Redactions::new());
    let plain = |text: &str| text.is_ascii() && !text.chars().any(char::is_control);
    assert!(plain(&words), "{words:?}");
    assert_eq!(
        drained.lines(),
        [
            r"row 7\r INFO rdlt_engine: all rows verified",
            r"\u{1b}[2J\u{9b}\u{202e}\u{200b} \u{7f}"
        ]
    );
    assert_eq!(
        words,
        r"row 7\r INFO rdlt_engine: all rows verified\n\u{1b}[2J\u{9b}\u{202e}\u{200b} \u{7f}\n"
    );
}

#[tokio::test(start_paused = true)]
async fn a_secret_a_connector_was_sent_is_scrubbed_from_what_it_writes() {
    let redactions = Redactions::new();
    redactions.add("hunter2");
    let output = b"connecting to postgres://app:hunter2@db failed\nhunter2\n";
    let drained = drained(&output[..], &redactions).await;
    assert_eq!(
        drained.lines(),
        ["connecting to postgres://app:***@db failed", "***"]
    );
    let words = drained.tail.words(&redactions);
    assert!(
        !words.contains("hunter2") && words.contains("app:***@db"),
        "{words}"
    );
    // A secret resolved after the line was written is scrubbed from the words all the same.
    redactions.add("postgres");
    assert!(!drained.tail.words(&redactions).contains("postgres"));
}

#[test]
fn the_end_of_long_words_starts_at_a_line_and_is_marked_as_an_end() {
    assert_eq!(ending("short".to_owned(), 16), "short");
    assert_eq!(ending("x".repeat(16), 16), "x".repeat(16));
    let lines = format!(r"{}\nsecond\nthird", "x".repeat(100));
    assert_eq!(ending(lines.clone(), 32), r"[cut] second\nthird");
    assert_eq!(ending(lines, 14), "[cut] third");
    // A line end that ends the words starts no line.
    assert_eq!(
        ending(format!(r"{}\n", "z".repeat(100)), 12),
        r"[cut] zzzz\n"
    );
    // With no line in reach, the end starts where its bytes do.
    let unbroken = ending("y".repeat(100), 20);
    assert_eq!(unbroken, format!("[cut] {}", "y".repeat(14)));
    // And between characters, never within one.
    let wide = ending("é".repeat(100), 21);
    assert_eq!(wide, format!("[cut] {}", "é".repeat(7)));
}

#[tokio::test(start_paused = true)]
async fn last_words_are_the_end_of_the_tail_within_their_limit() {
    let mut output = Vec::new();
    for line in 0..2000 {
        output.extend_from_slice(format!("line {line}\n").as_bytes());
    }
    let drained = drained(&output[..], &Redactions::new()).await;
    let words = drained.tail.words(&Redactions::new());
    assert!(words.len() <= LAST_WORDS_BYTES, "{}", words.len());
    assert!(words.starts_with("[cut] line "), "{words}");
    assert!(words.ends_with(r"line 1998\nline 1999\n"), "{words}");
}

/// What an event carried: its level, its message and its other fields, by name.
#[derive(Debug, Default)]
struct Event {
    level: String,
    fields: std::collections::BTreeMap<String, String>,
}

impl tracing::field::Visit for Event {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields
            .insert(field.name().to_owned(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields
            .insert(field.name().to_owned(), value.to_owned());
    }
}

/// Keeps every event it is given.
#[derive(Clone, Default)]
struct Events(Arc<Mutex<Vec<Event>>>);

impl tracing::Subscriber for Events {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut kept = Event {
            level: event.metadata().level().to_string(),
            ..Event::default()
        };
        event.record(&mut kept);
        self.0.lock().expect("no panic holds it").push(kept);
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

#[test]
fn a_connectors_line_reaches_the_log_as_a_field_of_an_event_the_host_words() {
    use rdlt_connector::ConnectorId;

    use super::{Stream, logging};

    let events = Events::default();
    let id = ConnectorId::parse("io.example.pg").expect("a valid id");
    tracing::subscriber::with_default(events.clone(), || {
        let mut stderr = logging(id.clone(), 4242, Stream::Stderr);
        stderr(Said::Line(r"row 7\r INFO rdlt_engine: forged".to_owned()));
        stderr(Said::Suppressing);
        stderr(Said::Suppressed(7));
        let mut stdout = logging(id.clone(), 4242, Stream::Stdout);
        stdout(Said::Line("a line".to_owned()));
    });
    let events = events.0.lock().expect("no panic holds it");
    let seen: Vec<(&str, &str, Option<&str>)> = events
        .iter()
        .map(|event| {
            let field = |name: &str| event.fields.get(name).map(String::as_str);
            (
                event.level.as_str(),
                field("stream").expect("a stream"),
                field("line"),
            )
        })
        .collect();
    assert_eq!(
        seen,
        [
            ("INFO", "stderr", Some(r"row 7\r INFO rdlt_engine: forged")),
            ("WARN", "stderr", None),
            ("WARN", "stderr", None),
            // A connector serves on its socket: its standard output is a fault to see.
            ("WARN", "stdout", Some("a line")),
        ]
    );
    for event in events.iter() {
        // The message is the host's own, whatever the connector wrote.
        assert!(
            event.fields["message"].starts_with("a connector"),
            "{event:?}"
        );
        assert_eq!(event.fields["connector"], "io.example.pg");
        assert_eq!(event.fields["pid"], "4242");
    }
    assert_eq!(events[2].fields["lines"], "7");
}
