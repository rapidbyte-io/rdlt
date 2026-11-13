//! A local server that answers as the API does, with pages it generates at the size a test asks.

use std::io::{BufRead as _, BufReader, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use serde_json::{Value, json};

/// The types every server lists, as many as the API lists.
const TYPES: u64 = 21;

/// What the connector names itself, which the server answers alone.
const USER_AGENT: &str = "rdlt-example-pokeapi";

/// How long the server holds each request before it answers, so requests a client sent at once
/// would overlap.
const HELD: Duration = Duration::from_millis(1);

/// An answer the server gives a path once, before the path answers as the API does.
pub(crate) struct Answer {
    /// The path, such as `/api/v2/pokemon/3`.
    path: &'static str,
    status: u16,
    retry_after: Option<u64>,
    body: String,
    /// Whether the answer is cut short: it claims more than its body and closes.
    cut: bool,
}

impl Answer {
    /// `status`, with a body that says the test asked for it.
    pub(crate) fn failed(path: &'static str, status: u16) -> Self {
        let body = r#"{ "detail": "failed as the test asked" }"#.to_owned();
        Self {
            path,
            status,
            retry_after: None,
            body,
            cut: false,
        }
    }

    /// A 429 that asks for a wait of `seconds`.
    pub(crate) fn waiting(path: &'static str, seconds: u64) -> Self {
        Self {
            retry_after: Some(seconds),
            ..Self::failed(path, 429)
        }
    }

    /// A 200 whose body is `body`.
    pub(crate) fn body(path: &'static str, body: impl Into<String>) -> Self {
        Self {
            path,
            status: 200,
            retry_after: None,
            body: body.into(),
            cut: false,
        }
    }

    /// A 200 whose connection closes partway through its body.
    pub(crate) fn cut(path: &'static str) -> Self {
        let body = r#"{ "next": null, "results": [{ "name": "#;
        Self {
            cut: true,
            ..Self::body(path, body)
        }
    }
}

/// A server of `count` pokemon, each of a type, which answers until it is dropped.
pub(crate) struct Api {
    /// Its root, as a connector's `base_url`.
    pub(crate) url: String,
    address: SocketAddr,
    served: Arc<Served>,
    accepting: Option<JoinHandle<()>>,
}

/// What the server knows of what it served.
struct Served {
    count: u64,
    root: String,
    answers: Vec<(Answer, AtomicBool)>,
    in_flight: AtomicUsize,
    most: AtomicUsize,
    stopped: AtomicBool,
}

impl Api {
    pub(crate) fn serve(count: u64) -> Self {
        Self::answering(count, Vec::new())
    }

    /// A server that gives each of `answers` once, in turn.
    pub(crate) fn answering(count: u64, answers: Vec<Answer>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a port is free on the loopback");
        let address = listener
            .local_addr()
            .expect("a bound listener has an address");
        let url = format!("http://{address}/api/v2");
        let served = Arc::new(Served {
            count,
            root: url.clone(),
            answers: answers
                .into_iter()
                .map(|answer| (answer, AtomicBool::new(false)))
                .collect(),
            in_flight: AtomicUsize::new(0),
            most: AtomicUsize::new(0),
            stopped: AtomicBool::new(false),
        });
        let accepted = Arc::clone(&served);
        let accepting = std::thread::Builder::new()
            .name("api-accepting".to_owned())
            .spawn(move || accept(&listener, &accepted))
            .expect("a thread starts");
        Self {
            url,
            address,
            served,
            accepting: Some(accepting),
        }
    }

    /// The paths of the answers the server has not given.
    pub(crate) fn unserved(&self) -> Vec<&'static str> {
        let answers = self.served.answers.iter();
        let unserved = answers.filter(|(_, given)| !given.load(Ordering::SeqCst));
        unserved.map(|(answer, _)| answer.path).collect()
    }

    /// The most requests the server held at once.
    pub(crate) fn most_at_once(&self) -> usize {
        self.served.most.load(Ordering::SeqCst)
    }
}

impl Drop for Api {
    fn drop(&mut self) {
        self.served.stopped.store(true, Ordering::SeqCst);
        // A connection wakes the accepting thread, which then sees it is stopped.
        TcpStream::connect(self.address).ok();
        if let Some(accepting) = self.accepting.take() {
            accepting.join().ok();
        }
    }
}

/// Answers each connection `listener` accepts on a thread of its own, until the server stops,
/// then waits for those threads.
fn accept(listener: &TcpListener, served: &Arc<Served>) {
    let mut answering = Vec::new();
    for stream in listener.incoming() {
        if served.stopped.load(Ordering::SeqCst) {
            break;
        }
        let Ok(stream) = stream else { continue };
        let served = Arc::clone(served);
        let thread = std::thread::Builder::new()
            .name("api-answering".to_owned())
            .spawn(move || answer(&stream, &served));
        answering.extend(thread.ok());
    }
    for thread in answering {
        thread.join().ok();
    }
}

/// Answers the request `stream` carries.
fn answer(stream: &TcpStream, served: &Served) {
    // A connection that sends nothing ends, so the thread answering it can be joined.
    stream.set_read_timeout(Some(Duration::from_secs(10))).ok();
    let (target, agent) = request(stream);
    let held = served.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    served.most.fetch_max(held, Ordering::SeqCst);
    std::thread::sleep(HELD);
    let (path, query) = target.split_once('?').unwrap_or((&target, ""));
    let mut unanswered = served
        .answers
        .iter()
        .filter(|(answer, _)| answer.path == path);
    let given = unanswered.find(|(_, given)| !given.swap(true, Ordering::SeqCst));
    let (status, retry_after, body, cut) = match (agent.as_deref(), given) {
        (Some(USER_AGENT), Some((answer, _))) => {
            let body = answer.body.clone();
            (answer.status, answer.retry_after, body, answer.cut)
        }
        (Some(USER_AGENT), None) => match page(&served.root, served.count, path, query) {
            Some(page) => (200, None, page.to_string(), false),
            None => (
                404,
                None,
                json!({ "detail": "Not found." }).to_string(),
                false,
            ),
        },
        _ => {
            let body = json!({ "detail": "no User-Agent names the example" });
            (400, None, body.to_string(), false)
        }
    };
    let claimed = body.len() + if cut { 64 } else { 0 };
    let retry = retry_after.map(|seconds| format!("Retry-After: {seconds}\r\n"));
    // A status line may leave its reason out, which clients do not read.
    let response = format!(
        "HTTP/1.1 {status} \r\nContent-Type: application/json\r\nContent-Length: {}\r\n{}\
         Connection: close\r\n\r\n{body}",
        claimed,
        retry.unwrap_or_default(),
    );
    // The request is no longer held once its answer is on its way, which the client reads before
    // it sends another.
    served.in_flight.fetch_sub(1, Ordering::SeqCst);
    let mut stream = stream;
    stream.write_all(response.as_bytes()).ok();
}

/// The target of the request `stream` carries, and the `User-Agent` it names, its every header
/// read so the connection closes cleanly.
fn request(stream: &TcpStream) -> (String, Option<String>) {
    let mut lines = BufReader::new(stream).lines().map_while(Result::ok);
    let line = lines.next().unwrap_or_default();
    let target = line.split(' ').nth(1).unwrap_or_default().to_owned();
    let headers: Vec<String> = lines.take_while(|line| !line.is_empty()).collect();
    let agent = headers.iter().find_map(|header| {
        let (name, value) = header.split_once(':')?;
        name.eq_ignore_ascii_case("user-agent")
            .then(|| value.trim().to_owned())
    });
    (target, agent)
}

/// What the API answers at `path` with `query`, or none where it has nothing there.
fn page(root: &str, count: u64, path: &str, query: &str) -> Option<Value> {
    let path = path.strip_prefix("/api/v2/")?.trim_end_matches('/');
    match path.split_once('/') {
        None if path == "pokemon" => Some(list(root, "pokemon", count, query)),
        None if path == "type" => Some(list(root, "type", TYPES, query)),
        Some(("pokemon", id)) => {
            let id: u64 = id.parse().ok()?;
            (1..=count).contains(&id).then(|| pokemon(root, id))
        }
        _ => None,
    }
}

/// A page of `resource`'s list of `count` entries, from the `offset` and `limit` of `query`.
fn list(root: &str, resource: &str, count: u64, query: &str) -> Value {
    let asked = |name: &str, default: u64| {
        let pairs = query.split('&').filter_map(|pair| pair.split_once('='));
        pairs
            .filter(|(key, _)| *key == name)
            .find_map(|(_, value)| value.parse().ok())
            .unwrap_or(default)
    };
    let (offset, limit) = (asked("offset", 0), asked("limit", 20));
    let end = offset.saturating_add(limit).min(count);
    let link = |offset: u64| format!("{root}/{resource}?offset={offset}&limit={limit}");
    let results: Vec<Value> = (offset + 1..=end)
        .map(|id| named(root, resource, id))
        .collect();
    json!({
        "count": count,
        "next": (end < count).then(|| link(end)),
        "previous": (offset > 0).then(|| link(offset.saturating_sub(limit))),
        "results": results,
    })
}

/// Pokemon `id` in the detail's shape: its own fields, nested lists of named links, and fields the
/// connector leaves out.
fn pokemon(root: &str, id: u64) -> Value {
    let named = |resource: &str, number: u64| named(root, resource, number);
    let stat =
        |n: u64| json!({ "base_stat": 30 + (id + n) % 100, "effort": 0, "stat": named("stat", n) });
    json!({
        "id": id,
        "name": format!("pokemon-{id}"),
        // The API knows no base experience of some pokemon.
        "base_experience": (!id.is_multiple_of(7)).then_some(40 + id % 200),
        "height": 1 + id % 20,
        "weight": 10 + id % 900,
        "order": id,
        "is_default": true,
        "types": [{ "slot": 1, "type": named("type", 1 + id % TYPES) }],
        "abilities": [
            { "ability": named("ability", 1 + id % 30), "is_hidden": false, "slot": 1 },
            { "ability": named("ability", 31 + id % 30), "is_hidden": true, "slot": 3 },
        ],
        "stats": [stat(1), stat(2), stat(3)],
        "moves": [{ "move": named("move", 1 + id % 50), "version_group_details": [] }],
        "sprites": { "front_default": null },
    })
}

/// The link an entry of `resource` numbered `number` is named by, as the API's lists hold it.
fn named(root: &str, resource: &str, number: u64) -> Value {
    let url = format!("{root}/{resource}/{number}/");
    json!({ "name": format!("{resource}-{number}"), "url": url })
}
