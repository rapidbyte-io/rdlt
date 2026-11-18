//! The turns' tests: `Turns::call` and `Turns::whole` cut at every wait, and calls into Python.
//!
//! The turns alone are cut at every wait on the paused clock. A call into Python runs on a daemon
//! thread in real time, which the paused clock would outrun, so those calls are polled by hand
//! and each wait is bounded.

use std::num::NonZeroU16;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use pyo3::types::{PyAnyMethods as _, PyDict};
use pyo3::{IntoPyObject as _, Py, PyAny, PyErr};
use rdlt_connector::{
    Capabilities, DestinationConnector as _, Field, LogicalType, OpenContext, PipelineId,
    SegmentId, Session as _, TableChange, TableWriter as _,
};
use rdlt_testkit::cancel::{Ended, at_every_point};

use super::Turns;
use crate::destination::tests::{commit_meta, polled, table, within};
use crate::destination::{PythonDestination, PythonSession, method};
use crate::embedded;

#[test]
fn calls_that_come_one_at_a_time_take_their_turns_in_the_order_made() {
    let turns = Turns::new();
    let first = polled(pin!(turns.call())).expect("an idle session gives its turn at once");
    let mut second = pin!(turns.call());
    let mut third = pin!(turns.call());
    assert!(
        polled(second.as_mut()).is_none(),
        "an earlier call holds its turn"
    );
    assert!(
        polled(third.as_mut()).is_none(),
        "an earlier call holds its turn"
    );
    drop(first);
    assert!(
        polled(third.as_mut()).is_none(),
        "the second call was made first"
    );
    let second = polled(second.as_mut()).expect("the second call's turn came");
    drop(second);
    assert!(
        polled(third.as_mut()).is_some(),
        "the third call's turn came"
    );
}

#[test]
fn a_table_writer_comes_one_at_a_time_with_the_session_unless_writers_run_at_once() {
    let session = Turns::new();
    let held = polled(pin!(session.call())).expect("an idle session gives its turn at once");
    let one = session.writer(false);
    assert!(
        polled(pin!(one.call())).is_none(),
        "one writer's calls come one at a time with the session's"
    );
    let (first, second) = (session.writer(true), session.writer(true));
    let writing = polled(pin!(first.call())).expect("a writer at once comes beside the session");
    assert!(
        polled(pin!(second.call())).is_some(),
        "writers at once come beside each other"
    );
    assert!(
        polled(pin!(first.call())).is_none(),
        "a writer's own calls take turns"
    );
    drop((held, writing));
}

#[test]
fn a_whole_turn_waits_for_every_call_made_before_it_and_holds_back_every_call_after() {
    let session = Turns::new();
    let writer = session.writer(true);
    let (writing, applying) = (
        polled(pin!(writer.call())).expect("an idle session gives its turn at once"),
        polled(pin!(session.call())).expect("the session's calls come beside a writer's at once"),
    );
    let mut closing = pin!(session.whole());
    assert!(polled(closing.as_mut()).is_none(), "calls are in flight");
    let mut later = pin!(writer.call());
    assert!(
        polled(later.as_mut()).is_none(),
        "a call made after close waits for it"
    );
    drop(writing);
    assert!(
        polled(closing.as_mut()).is_none(),
        "a session call is in flight"
    );
    drop(applying);
    let closed = polled(closing.as_mut()).expect("every call made before close returned");
    assert!(polled(later.as_mut()).is_none(), "close runs");
    drop(closed);
    assert!(polled(later.as_mut()).is_some(), "close returned");
}

#[test]
fn a_call_waiting_for_an_earlier_one_keeps_its_place_ahead_of_a_later_commit() {
    let session = Turns::new();
    let held = polled(pin!(session.call())).expect("an idle session gives its turn at once");
    let mut queued = pin!(session.call());
    assert!(
        polled(queued.as_mut()).is_none(),
        "an earlier call holds its turn"
    );
    let mut committing = pin!(session.whole());
    assert!(polled(committing.as_mut()).is_none(), "calls are in flight");
    drop(held);
    assert!(
        polled(committing.as_mut()).is_none(),
        "the queued call was made first"
    );
    let queued = polled(queued.as_mut()).expect("the queued call's turn came");
    drop(queued);
    assert!(
        polled(committing.as_mut()).is_some(),
        "every call made before it returned"
    );
}

#[test]
fn a_call_let_go_before_its_turn_came_holds_back_nothing() {
    let session = Turns::new();
    let held = polled(pin!(session.call())).expect("an idle session gives its turn at once");
    {
        let mut abandoned = pin!(session.call());
        assert!(
            polled(abandoned.as_mut()).is_none(),
            "an earlier call holds its turn"
        );
    }
    drop(held);
    assert!(
        polled(pin!(session.whole())).is_some(),
        "nothing holds the session"
    );
}

/// Sessions as the native side holds them, kept in `told` as their schema changes return:
/// `Blocking`'s once `release` is set, within ten seconds, `Quick`'s at once and `Raising`'s
/// never, as it raises; `Opening`, a connection whose sessions open table writers; and `Held`, a
/// session whose every call, and its table writers', waits for `release` while `hold` is set.
const SESSIONS: &str = "\
import threading
told = []
hold, release = threading.Event(), threading.Event()
class Blocking:
    def apply_schema(self, change):
        release.wait(10)
        told.append('apply_schema')
class Quick:
    def apply_schema(self, change):
        told.append('apply_schema')
class Raising:
    def apply_schema(self, change):
        raise RuntimeError('the change is refused')
class Writing:
    def writer(self, table):
        return object()
class Opening:
    def open(self, pipeline):
        return Writing(), '{\"epoch\": 1, \"landed\": null, \"state\": []}'
def held():
    if hold.is_set():
        release.wait(10)
class HeldWriter:
    def write(self, segment, batch):
        held()
    def flush(self):
        held()
        return (0, 0)
class Held:
    def apply_schema(self, change):
        held()
    def writer(self, table):
        held()
        return HeldWriter()
    def discard_staged(self):
        held()
    def commit(self, meta):
        held()
        return '{}'
    def close(self):
        held()
";

/// An instance of `class`, one of [`SESSIONS`], with the globals it was made in.
fn made(class: &str) -> (Py<PyAny>, Py<PyDict>) {
    embedded::with(|py| {
        let globals = PyDict::new(py);
        let code = std::ffi::CString::new(SESSIONS).unwrap();
        py.run(&code, Some(&globals), None).unwrap();
        let made = globals.get_item(class).unwrap().call0().unwrap();
        (made.unbind(), globals.unbind())
    })
}

/// A session over an instance of `class`, one of [`SESSIONS`], its writers at once where
/// `at_once`, with its globals.
fn session_of(class: &str, at_once: bool) -> (PythonSession, Py<PyDict>) {
    let (made, globals) = made(class);
    let session = PythonSession {
        session: Arc::new(made),
        turns: Turns::new(),
        at_once,
    };
    (session, globals)
}

/// A session of one writer over an instance of `class`, one of [`SESSIONS`], with its globals.
fn session(class: &str) -> (PythonSession, Py<PyDict>) {
    session_of(class, false)
}

/// Sets the event `name` of [`SESSIONS`].
fn set(globals: &Py<PyDict>, name: &str) {
    embedded::with(|py| {
        let event = globals.bind(py).get_item(name).unwrap();
        event.call_method0("set").unwrap();
    });
}

/// The global `name` of [`SESSIONS`] as JSON.
fn global(globals: &Py<PyDict>, name: &str) -> String {
    embedded::with(|py| {
        let value = globals.bind(py).get_item(name).unwrap();
        let text = py.import("json").unwrap().call_method1("dumps", (value,));
        text.unwrap().extract().unwrap()
    })
}

/// A schema change the sessions take.
fn added() -> TableChange {
    TableChange::AddColumn {
        table: table(),
        field: Field::new("n", LogicalType::Int64, true),
    }
}

#[test]
fn a_call_holds_its_turn_until_python_returns_though_the_engine_stopped_waiting() {
    let (mut session, globals) = session("Blocking");
    assert!(
        polled(pin!(session.apply_schema(&added()))).is_none(),
        "the call was made"
    );
    assert!(
        polled(pin!(session.turns.whole())).is_none(),
        "the call holds its turn"
    );
    set(&globals, "release");
    drop(within(session.turns.whole()));
    assert_eq!(global(&globals, "told"), r#"["apply_schema"]"#);
}

#[test]
fn a_call_whose_python_raises_lets_its_turn_go() {
    let (mut session, _globals) = session("Raising");
    assert!(within(session.apply_schema(&added())).is_err());
    assert!(
        polled(pin!(session.turns.whole())).is_some(),
        "the turn was let go"
    );
}

#[test]
fn a_call_the_engine_stopped_waiting_for_before_its_turn_came_is_never_made() {
    let (mut session, globals) = session("Quick");
    let held = polled(pin!(session.turns.call())).expect("an idle session gives its turn at once");
    assert!(
        polled(pin!(session.apply_schema(&added()))).is_none(),
        "an earlier call holds its turn"
    );
    drop(held);
    within(session.apply_schema(&added())).unwrap();
    // A call let go but made once its turn came would have run before the second took its own.
    assert_eq!(global(&globals, "told"), r#"["apply_schema"]"#);
}

#[test]
fn a_call_whose_answer_panics_as_it_is_read_lets_its_turn_go() {
    let (session, _globals) = session("Quick");
    let turn = within(session.turns.call());
    let lost = within(method(
        &session.session,
        "apply_schema",
        turn,
        |py| ("{}",).into_pyobject(py),
        |_| -> Result<(), PyErr> { panic!("a bug in the reader of an answer") },
    ));
    assert!(lost.is_err(), "the call went unanswered");
    drop(within(session.turns.whole()));
}

#[test]
fn an_open_gives_each_table_writer_turns_of_its_own_only_where_writers_run_at_once() {
    let (opening, _globals) = made("Opening");
    let connected = Arc::new(opening);
    let context = OpenContext {
        pipeline: PipelineId::parse("p").unwrap(),
    };
    for (writers, at_once) in [(1, false), (2, true), (3, true)] {
        let mut capabilities = Capabilities::minimal();
        capabilities.max_parallel_writers = NonZeroU16::new(writers).unwrap();
        let destination = PythonDestination {
            connected: Arc::clone(&connected),
            capabilities,
        };
        let mut session = within(destination.open(&context)).unwrap().session;
        let first = within(session.writer(&table())).unwrap();
        let second = within(session.writer(&table())).unwrap();
        let held = polled(pin!(first.turns.call())).expect("an idle writer gives its turn at once");
        let beside = polled(pin!(second.turns.call()));
        assert_eq!(beside.is_some(), at_once, "{writers} writers");
        drop((held, beside));
    }
}

/// A call of a session or of its first table writer.
#[derive(Clone, Copy, Debug)]
enum Call {
    ApplySchema,
    Writer,
    DiscardStaged,
    Commit,
    Close,
    Write,
    Flush,
}

/// Which turns each call leaves free while Python runs it: the session's one-at-a-time turn, the
/// first table writer's, the second's and the whole; with one writer, then with writers at once.
const FREE: [(Call, [bool; 4], [bool; 4]); 7] = [
    (Call::ApplySchema, [false; 4], [false, true, true, false]),
    (Call::Writer, [false; 4], [false, true, true, false]),
    (Call::DiscardStaged, [false; 4], [false, true, true, false]),
    (Call::Commit, [false; 4], [false; 4]),
    (Call::Close, [false; 4], [false; 4]),
    (Call::Write, [false; 4], [true, false, true, false]),
    (Call::Flush, [false; 4], [true, false, true, false]),
];

/// Whether `turn` is given at once; what it gives, or the part of it taken, is let go before
/// the next is asked for.
fn free(turn: impl Future) -> bool {
    polled(pin!(turn)).is_some()
}

#[test]
fn each_call_holds_the_turn_its_kind_takes_while_python_runs_it() {
    let ids = Arc::new(Int64Array::from(vec![1]));
    let batch = RecordBatch::try_from_iter([("n", ids as _)]).unwrap();
    for (call, one, at_once) in FREE {
        for (writers_at_once, expected) in [(false, one), (true, at_once)] {
            let (session, globals) = session_of("Held", writers_at_once);
            let mut session = Some(session);
            let opened = session.as_mut().expect("the session is open");
            let mut first = within(opened.writer(&table())).unwrap();
            let second = within(opened.writer(&table())).unwrap();
            let turns = opened.turns.clone();
            set(&globals, "hold");
            let waits = match call {
                Call::ApplySchema => polled(pin!(opened.apply_schema(&added()))).is_none(),
                Call::Writer => polled(pin!(opened.writer(&table()))).is_none(),
                Call::DiscardStaged => polled(pin!(opened.discard_staged())).is_none(),
                Call::Commit => polled(pin!(opened.commit(&commit_meta()))).is_none(),
                Call::Close => {
                    let closing = session.take().expect("the session is open").close();
                    polled(pin!(closing)).is_none()
                }
                Call::Write => polled(pin!(first.write(SegmentId(1), batch.clone()))).is_none(),
                Call::Flush => polled(pin!(first.flush())).is_none(),
            };
            assert!(waits, "{call:?} was made and waits for Python");
            let probed = [
                free(turns.call()),
                free(first.turns.call()),
                free(second.turns.call()),
                free(turns.whole()),
            ];
            assert_eq!(
                probed, expected,
                "{call:?}, writers at once: {writers_at_once}"
            );
            set(&globals, "release");
            drop(within(turns.whole()));
        }
    }
}

/// Turns, one of whose turns a task holds for a second of the paused clock.
async fn contended(whole: bool) -> Turns {
    let turns = Turns::new();
    if whole {
        let held = turns.whole().await;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            drop(held);
        });
    } else {
        let held = turns.call().await;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            drop(held);
        });
    }
    turns
}

/// After a turn taken or let go, the turns give every kind of turn again.
async fn given_again(turns: &mut Turns) {
    drop(turns.call().await);
    drop(turns.whole().await);
}

#[test]
fn a_call_s_turn_cut_at_any_wait_leaves_the_turns_whole() {
    for whole in [true, false] {
        let points = at_every_point(
            async || contended(whole).await,
            async |turns: &mut Turns| drop(turns.call().await),
            async |turns: &mut Turns, _ended: Ended<()>| given_again(turns).await,
        );
        assert_eq!(points, 1, "behind a held whole turn: {whole}");
    }
}

#[test]
fn a_whole_turn_cut_at_any_wait_leaves_the_turns_whole() {
    for whole in [true, false] {
        let points = at_every_point(
            async || contended(whole).await,
            async |turns: &mut Turns| drop(turns.whole().await),
            async |turns: &mut Turns, _ended: Ended<()>| given_again(turns).await,
        );
        assert_eq!(points, 1, "behind a held whole turn: {whole}");
    }
}
