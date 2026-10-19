//! The log on object storage driven by its specification's traces: each request the store makes
//! is what the trace's step makes, met as the step says, and what each operation answers and
//! what the store then holds are the trace's.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, BoxStream};
use futures_util::{StreamExt as _, TryStreamExt as _};
use object_store::memory::InMemory;
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{
    Attribute, CopyOptions, GetOptions, GetResult, ListResult, MultipartId, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt as _, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result,
};
use parking_lot::Mutex;
use rdlt_connector::{LoadId, PipelineId};
use rdlt_engine::{Chunk, Clock, ObjectStoreOptions, ObjectStoreWal, Sleep, StagedChunk, WalStore};
use rdlt_testkit::objects::{Call, Fault, Faulty, Op, Plan};
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::rng::SplitMix64;

/// The prefix the driven store keeps its logs beneath.
const PREFIX: &str = "traced";

/// How long a wait for the code may take before the trace fails: the clock is paused, so only a
/// request or an answer that never comes reaches it.
const BOUND: Duration = Duration::from_secs(60);

/// How far apart the steps a late request may land at are, on the paused clock.
const STEP: Duration = Duration::from_secs(3_600);

/// Bytes a part holds: a chunk longer than this is uploaded in parts.
const PART: usize = 64;

/// Bytes the writer's chunks uploaded in parts hold, past a part and short of two.
const LONG: usize = 100;

/// The most entries a listing the driver collects may hold: the driver takes one more, and fails
/// the trace if it gets it.
const LISTING: usize = 10_000;

/// `entries`, taken from a listing at most `LISTING + 1` long, failing `what` if there are more
/// than `LISTING`.
fn bounded<T>(entries: Vec<T>, what: &str) -> Vec<T> {
    assert!(
        entries.len() <= LISTING,
        "{what}: more than {LISTING} entries"
    );
    entries
}

// ---- The trace's states, as the specification writes them

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(tag = "tag", content = "value")]
enum Who {
    W,
    R,
    X,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(tag = "tag", content = "value")]
enum Name {
    Mark,
    ChunkHead(u64),
    Body((u64, Who)),
    Stray,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(tag = "tag", content = "value")]
enum Kind {
    Whole,
    InParts(Who),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
struct Obj {
    name: Name,
    by: Who,
    tok: u64,
    live: BTreeSet<u64>,
    kind: Kind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "tag", content = "value")]
enum Dir {
    #[serde(rename = "LogDir")]
    Log,
    #[serde(rename = "MarksDir")]
    Marks,
    #[serde(rename = "LogsDir")]
    Logs,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "tag", content = "value")]
enum SpecOp {
    Creates,
    Gets,
    Lists,
    Deletes,
    Begins,
    Parts,
    Completes,
    Aborts,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "tag", content = "value")]
enum At {
    Object(Name),
    Under(Dir),
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
struct Req {
    op: SpecOp,
    at: At,
    obj: Obj,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "tag", content = "value")]
enum Outcome {
    Ok,
    Failed,
    Unknown,
    Late,
    Taken,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "tag", content = "value")]
enum Landing {
    Lands(Obj),
    Unlands(Name),
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
struct Pending {
    who: Who,
    attempt: u64,
    wave: u64,
    landing: Landing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "tag", content = "value")]
enum Refusal {
    Unavailable,
    NameTaken,
    Missing,
    Strays,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "tag", content = "value")]
enum Answer {
    Nil,
    Done,
    Numbers(BTreeSet<u64>),
    Content(Obj),
    Listed(bool),
    Refused(Refusal),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "tag", content = "value")]
enum Doing {
    Idle,
    Opening,
    Staging(u64),
    Appending(u64),
    Publishing(u64),
    Removing(u64),
    RemovingLog,
    Listing,
    Reading(u64),
    Loads,
    Leftovers,
}

#[derive(Clone, Debug, Deserialize)]
struct Proc {
    doing: Doing,
    req: Req,
    attempt: u64,
    wave: u64,
    staged: Obj,
    answer: Answer,
    ended: bool,
}

/// A value `quint run --mbt` notes for each `nondet` of the module: picked in the step, or not.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "tag", content = "value")]
enum Pick<T> {
    None,
    Some(T),
}

impl<T: Clone> Pick<T> {
    fn get(&self) -> Option<T> {
        match self {
            Self::None => None,
            Self::Some(value) => Some(value.clone()),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct Picks {
    who: Pick<Who>,
    outcome: Pick<Outcome>,
    landing: Pick<Pending>,
    whom: Pick<Who>,
}

#[derive(Clone, Debug, Deserialize)]
struct State {
    store: BTreeSet<Obj>,
    procs: BTreeMap<Who, Proc>,
    #[serde(rename = "mbt::actionTaken")]
    action: String,
    #[serde(rename = "mbt::nondetPicks")]
    picks: Picks,
}

// ---- The store each load asks through: a gate the driver opens request by request

/// A request a load made, waiting at its gate for the driver to let it go.
struct Asked {
    op: Op,
    key: String,
    /// The token a create marks its object with.
    token: Option<String>,
    release: oneshot::Sender<()>,
}

/// The shared store as one load reaches it: every request waits at the gate, once armed, until
/// the driver lets it go, and is then met as the store's plan says.
#[derive(Clone)]
struct Gated {
    inner: Faulty<InMemory>,
    asks: mpsc::UnboundedSender<Asked>,
    /// Whether a request of the load waits at the gate or for its answer.
    held: Arc<watch::Sender<bool>>,
    armed: Arc<AtomicBool>,
}

impl fmt::Debug for Gated {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Gated")
    }
}

impl fmt::Display for Gated {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Gated")
    }
}

impl Gated {
    /// Waits for the driver to let the request `op` at `key` go, once armed.
    async fn turn(&self, op: Op, key: &Path, token: Option<String>) {
        if !self.armed.load(Ordering::SeqCst) {
            return;
        }
        self.held.send_replace(true);
        let (release, released) = oneshot::channel();
        let key = key.to_string();
        if self
            .asks
            .send(Asked {
                op,
                key,
                token,
                release,
            })
            .is_ok()
        {
            drop(released.await);
        }
    }

    /// Notes that the load's request was answered.
    fn answered<T>(&self, answer: T) -> T {
        self.held.send_replace(false);
        answer
    }
}

#[async_trait]
impl ObjectStore for Gated {
    async fn put_opts(
        &self,
        key: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let token = opts
            .attributes
            .get(&token_name())
            .map(|value| value.as_ref().to_owned());
        let create = matches!(opts.mode, PutMode::Create);
        self.turn(Op::Put { create }, key, token).await;
        let put = self.inner.put_opts(key, payload, opts).await;
        self.answered(put)
    }

    async fn put_multipart_opts(
        &self,
        key: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(key, opts).await
    }

    async fn get_opts(&self, key: &Path, options: GetOptions) -> Result<GetResult> {
        self.turn(Op::Get, key, None).await;
        let got = self.inner.get_opts(key, options).await;
        self.answered(got)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let gate = self.clone();
        let gated = locations
            .then(move |location| {
                let gate = gate.clone();
                async move {
                    let location = location?;
                    gate.turn(Op::Delete, &location, None).await;
                    Ok(location)
                }
            })
            .boxed();
        let gate = self.clone();
        self.inner
            .delete_stream(gated)
            .map(move |deleted| gate.answered(deleted))
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let gate = self.clone();
        let dir = prefix.cloned().unwrap_or_default();
        stream::once(async move {
            gate.turn(Op::List, &dir, None).await;
            let listed: Vec<Result<ObjectMeta>> = gate
                .inner
                .list(Some(&dir))
                .take(LISTING + 1)
                .collect()
                .await;
            let listed = bounded(listed, &format!("a list of {dir}"));
            stream::iter(gate.answered(listed))
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let dir = prefix.cloned().unwrap_or_default();
        self.turn(Op::List, &dir, None).await;
        let listed = self.inner.list_with_delimiter(prefix).await;
        self.answered(listed)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[async_trait]
impl MultipartStore for Gated {
    async fn create_multipart(&self, key: &Path) -> Result<MultipartId> {
        self.turn(Op::Begin, key, None).await;
        let begun = self.inner.create_multipart(key).await;
        self.answered(begun)
    }

    async fn put_part(
        &self,
        key: &Path,
        id: &MultipartId,
        index: usize,
        payload: PutPayload,
    ) -> Result<PartId> {
        self.turn(Op::Part, key, None).await;
        let part = self.inner.put_part(key, id, index, payload).await;
        self.answered(part)
    }

    async fn complete_multipart(
        &self,
        key: &Path,
        id: &MultipartId,
        parts: Vec<PartId>,
    ) -> Result<PutResult> {
        self.turn(Op::Complete, key, None).await;
        let completed = self.inner.complete_multipart(key, id, parts).await;
        self.answered(completed)
    }

    async fn abort_multipart(&self, key: &Path, id: &MultipartId) -> Result<()> {
        self.turn(Op::Abort, key, None).await;
        let aborted = self.inner.abort_multipart(key, id).await;
        self.answered(aborted)
    }
}

/// A load's clock: a sleep ends at once, but while the load's request waits at its gate or for
/// its answer, so an attempt's deadline passes only where the driver leaves it unanswered.
#[derive(Debug)]
struct TraceClock {
    held: watch::Receiver<bool>,
    rng: Mutex<SplitMix64>,
}

impl Clock for TraceClock {
    fn sleep(&self, _duration: Duration) -> Sleep {
        let mut held = self.held.clone();
        Box::pin(async move {
            loop {
                let holding = *held.borrow_and_update();
                if !holding {
                    return;
                }
                if held.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
            }
        })
    }

    fn random(&self) -> u64 {
        self.rng.lock().next_u64()
    }
}

/// The metadata naming the token of the create that made an object.
fn token_name() -> Attribute {
    Attribute::Metadata("rdlt-token".into())
}

/// The fault the driver gives the next request it lets go, where its operation and key match.
type Slot = Arc<Mutex<Option<(Op, String, Fault)>>>;

/// The plan that meets each request as the slot says, and any other, the driver's own included,
/// without fault.
fn plan(slot: Slot) -> Plan {
    Box::new(move |call: &Call| {
        let mut slot = slot.lock();
        let matches = slot
            .as_ref()
            .is_some_and(|(op, key, _)| *op == call.op && *key == call.key);
        if matches {
            slot.take().map_or(Fault::None, |(_, _, fault)| fault)
        } else {
            Fault::None
        }
    })
}

// ---- A load: its log store, and the task that runs its operations

/// What the driver asks a load to do: one of the store's operations.
enum Command {
    Open,
    Stage(u64),
    Append(Bytes),
    /// The staged chunk published, with its bytes appended first where it was not uploaded.
    Publish(Option<Bytes>),
    Remove(u64),
    RemoveLog,
    Chunks,
    Read(u64),
    Loads,
    Leftovers,
}

/// What one of the store's operations answered.
#[derive(Debug)]
enum Answered {
    Done,
    Numbers(BTreeSet<u64>),
    Bytes(Bytes),
    Listed(bool),
    Refused(io::ErrorKind),
}

/// What a load did next: a request, at its gate, or its operation's answer.
enum Event {
    Asked(Asked),
    Answered(Answered),
}

/// A load the driver runs.
struct Load {
    /// The request the load made before its step came.
    ahead: Option<Asked>,
    asks: mpsc::UnboundedReceiver<Asked>,
    commands: mpsc::UnboundedSender<Command>,
    answers: mpsc::UnboundedReceiver<Answered>,
    held: Arc<watch::Sender<bool>>,
    task: JoinHandle<()>,
}

fn pipeline() -> PipelineId {
    PipelineId::parse("traced").expect("a valid pipeline")
}

fn load_id() -> LoadId {
    LoadId::from_parts(UNIX_EPOCH, 1)
}

impl Load {
    /// A load of its own log store over `objects`, its draws from `seed`, opened, which probes the
    /// store with its gate open.
    async fn open(objects: &Faulty<InMemory>, seed: u64) -> Self {
        let (asking, asks) = mpsc::unbounded_channel();
        let (held, watching) = watch::channel(false);
        let held = Arc::new(held);
        let armed = Arc::new(AtomicBool::new(false));
        let gated = Gated {
            inner: objects.clone(),
            asks: asking,
            held: Arc::clone(&held),
            armed: Arc::clone(&armed),
        };
        let clock = TraceClock {
            held: watching,
            rng: Mutex::new(SplitMix64::new(seed)),
        };
        let options = ObjectStoreOptions::default()
            .with_part_bytes(NonZeroUsize::new(PART).expect("parts are not empty"))
            .with_attempts(NonZeroU32::new(2).expect("attempts are not none"))
            .with_backoff(Duration::from_millis(10), Duration::from_millis(200))
            .with_deadline(Duration::from_secs(1), Duration::ZERO);
        let opening = ObjectStoreWal::open(Arc::new(gated) as _, PREFIX, Arc::new(clock), options);
        let wal = tokio::time::timeout(BOUND, opening)
            .await
            .expect("the probe's requests pass an open gate, so opening answers")
            .expect("a store in memory does what a log needs");
        armed.store(true, Ordering::SeqCst);
        let (commanding, commands) = mpsc::unbounded_channel();
        let (answering, answers) = mpsc::unbounded_channel();
        let task = tokio::spawn(serve(wal, commands, answering));
        Self {
            ahead: None,
            asks,
            commands: commanding,
            answers,
            held,
            task,
        }
    }

    /// The load's next request or answer.
    async fn next(&mut self, what: &str) -> Event {
        if let Some(asked) = self.ahead.take() {
            return Event::Asked(asked);
        }
        let event = tokio::time::timeout(BOUND, async {
            tokio::select! {
                biased;
                // A request comes first: an operation answers only once its last request did.
                asked = self.asks.recv() => asked.map(Event::Asked),
                answered = self.answers.recv() => answered.map(Event::Answered),
            }
        })
        .await;
        match event {
            Ok(Some(event)) => event,
            _ => panic!("{what}: the code made no request and gave no answer"),
        }
    }
}

/// Runs `wal`'s operations as `commands` ask, answering each on `answers`.
async fn serve(
    wal: ObjectStoreWal,
    mut commands: mpsc::UnboundedReceiver<Command>,
    answers: mpsc::UnboundedSender<Answered>,
) {
    let (pipeline, load) = (pipeline(), load_id());
    let mut staged: Option<Box<dyn StagedChunk>> = None;
    while let Some(command) = commands.recv().await {
        let answered = match command {
            Command::Open => done(wal.open_log(&pipeline, load).await),
            Command::Stage(number) => match wal.stage(&pipeline, Chunk { load, number }).await {
                Ok(chunk) => {
                    staged = Some(chunk);
                    Answered::Done
                }
                Err(error) => Answered::Refused(error.kind()),
            },
            Command::Append(bytes) => {
                let chunk = staged
                    .as_mut()
                    .expect("the trace appends to a staged chunk");
                done(chunk.append(bytes).await)
            }
            Command::Publish(bytes) => {
                let mut chunk = staged.take().expect("the trace publishes a staged chunk");
                if let Some(bytes) = bytes {
                    chunk
                        .append(bytes)
                        .await
                        .expect("a chunk within a part appends");
                }
                done(chunk.publish().await)
            }
            Command::Remove(number) => done(wal.remove(&pipeline, Chunk { load, number }).await),
            Command::RemoveLog => done(wal.remove_log(&pipeline, load).await),
            Command::Chunks => match wal.chunks(&pipeline, load).await {
                Ok(chunks) => {
                    Answered::Numbers(chunks.into_iter().map(|(number, _)| number).collect())
                }
                Err(error) => Answered::Refused(error.kind()),
            },
            Command::Read(number) => {
                match wal.read(&pipeline, Chunk { load, number }, 0, 4_096).await {
                    Ok(bytes) => Answered::Bytes(bytes),
                    Err(error) => Answered::Refused(error.kind()),
                }
            }
            Command::Loads => listed(wal.loads(&pipeline).await),
            Command::Leftovers => listed(wal.leftovers(&pipeline).await),
        };
        if answers.send(answered).is_err() {
            return;
        }
    }
}

fn done(result: io::Result<()>) -> Answered {
    match result {
        Ok(()) => Answered::Done,
        Err(error) => Answered::Refused(error.kind()),
    }
}

fn listed(result: io::Result<Vec<LoadId>>) -> Answered {
    match result {
        Ok(loads) => Answered::Listed(loads.contains(&load_id())),
        Err(error) => Answered::Refused(error.kind()),
    }
}

// ---- What the trace names, as the store names it

/// The text a chunk's bytes are: its maker, its number and the chunks it needs; a chunk in parts
/// is padded past a part.
fn content(obj: &Obj) -> Bytes {
    let number = match obj.name {
        Name::ChunkHead(number) | Name::Body((number, _)) => number,
        Name::Mark | Name::Stray => 0,
    };
    let live: Vec<String> = obj.live.iter().map(u64::to_string).collect();
    let mut text = format!("{:?}{number} live {}", obj.by, live.join(",")).into_bytes();
    if obj.kind != Kind::Whole {
        text.resize(LONG, b'.');
    }
    Bytes::from(text)
}

/// The maker and the chunks it needs that `bytes`, a chunk's, name.
fn parsed(bytes: &[u8]) -> (String, BTreeSet<u64>) {
    let text = std::str::from_utf8(bytes).expect("a chunk's bytes are text");
    let text = text.trim_end_matches('.');
    let (maker, live) = text
        .split_once(" live ")
        .expect("a chunk names what it needs");
    let live = live
        .split(',')
        .filter(|number| !number.is_empty())
        .map(|number| number.parse().expect("a chunk number"))
        .collect();
    (maker.chars().take(1).collect(), live)
}

fn log_dir() -> String {
    format!("{PREFIX}/p.{}/logs/{}", pipeline().as_str(), load_id())
}

/// Tokens of creates, each with the load that made it and the start of that load.
type Tokens = BTreeMap<String, (Who, u64)>;

/// Bodies' tokens, each with the load that uploaded it.
type Bodies = BTreeMap<String, Who>;

/// The key the request `at` asks of: a body by the token its uploader drew.
fn key_of(at: At, bodies: &Bodies) -> String {
    let base = format!("{PREFIX}/p.{}", pipeline().as_str());
    match at {
        At::Object(Name::Mark) => format!("{base}/open/{}", load_id()),
        At::Object(Name::ChunkHead(number)) => format!("{}/{number:08}.wal", log_dir()),
        At::Object(Name::Body((number, who))) => {
            let token = bodies
                .iter()
                .find_map(|(token, by)| (*by == who).then_some(token.as_str()))
                .unwrap_or("unbegun");
            format!("{}/{number:08}.{token}.body", log_dir())
        }
        At::Object(Name::Stray) => format!("{}/stray", log_dir()),
        At::Under(Dir::Log) => log_dir(),
        At::Under(Dir::Marks) => format!("{base}/open"),
        At::Under(Dir::Logs) => format!("{base}/logs"),
    }
}

fn op_of(op: SpecOp) -> Op {
    match op {
        SpecOp::Creates => Op::Put { create: true },
        SpecOp::Gets => Op::Get,
        SpecOp::Lists => Op::List,
        SpecOp::Deletes => Op::Delete,
        SpecOp::Begins => Op::Begin,
        SpecOp::Parts => Op::Part,
        SpecOp::Completes => Op::Complete,
        SpecOp::Aborts => Op::Abort,
    }
}

/// What the store's fault plan makes of `outcome` met by a request of `op`, a late put or
/// deletion landing at `lands` on the clock.
fn fault_of(op: SpecOp, outcome: Outcome, lands: Instant) -> Fault {
    match outcome {
        Outcome::Ok => Fault::None,
        Outcome::Failed => Fault::Fail,
        Outcome::Unknown => Fault::Answerless,
        Outcome::Taken => Fault::Raced,
        Outcome::Late if matches!(op, SpecOp::Creates | SpecOp::Deletes) => {
            Fault::Late(lands.saturating_duration_since(Instant::now()))
        }
        Outcome::Late => Fault::Hang,
    }
}

/// The operation the load in `proc` begins.
fn command_of(proc: &Proc) -> Command {
    match proc.doing {
        Doing::Opening => Command::Open,
        Doing::Staging(number) => Command::Stage(number),
        Doing::Appending(_) => Command::Append(content(&proc.staged)),
        Doing::Publishing(_) => {
            Command::Publish((proc.staged.kind == Kind::Whole).then(|| content(&proc.staged)))
        }
        Doing::Removing(number) => Command::Remove(number),
        Doing::RemovingLog => Command::RemoveLog,
        Doing::Listing => Command::Chunks,
        Doing::Reading(number) => Command::Read(number),
        Doing::Loads => Command::Loads,
        Doing::Leftovers => Command::Leftovers,
        Doing::Idle => panic!("an idle load begins nothing"),
    }
}

/// Whether the code's `answered` is the trace's `answer`.
fn agrees(answered: &Answered, answer: &Answer) -> bool {
    match (answered, answer) {
        (Answered::Done, Answer::Done) => true,
        (Answered::Numbers(numbers), Answer::Numbers(expected)) => numbers == expected,
        (Answered::Listed(listed), Answer::Listed(expected)) => listed == expected,
        (Answered::Bytes(bytes), Answer::Content(obj)) => {
            parsed(bytes) == (format!("{:?}", obj.by), obj.live.clone())
        }
        (Answered::Refused(kind), Answer::Refused(refusal)) => {
            *kind
                == match refusal {
                    Refusal::Unavailable => io::ErrorKind::TimedOut,
                    Refusal::NameTaken => io::ErrorKind::AlreadyExists,
                    Refusal::Missing => io::ErrorKind::NotFound,
                    Refusal::Strays => io::ErrorKind::InvalidData,
                }
        }
        _ => false,
    }
}

// ---- Replaying a trace

/// The step of `states` each late put or deletion lands at: the next step that lands what it
/// would do, as the trace's `pending` names it; none where the trace never lands it.
fn landings(states: &[State]) -> BTreeMap<usize, Option<usize>> {
    let mut landings = BTreeMap::new();
    for (step, state) in states.iter().enumerate().skip(1) {
        let (Some(who), Some(Outcome::Late)) = (state.picks.who.get(), state.picks.outcome.get())
        else {
            continue;
        };
        if state.action != "request" {
            continue;
        }
        let proc = &states[step - 1].procs[&who];
        let landing = match (proc.req.op, proc.req.at) {
            (SpecOp::Creates, _) => Landing::Lands(proc.req.obj.clone()),
            (SpecOp::Deletes, At::Object(name)) => Landing::Unlands(name),
            _ => continue,
        };
        let pending = Pending {
            who,
            attempt: proc.attempt,
            wave: proc.wave,
            landing,
        };
        let lands = (step + 1..states.len()).find(|later| {
            states[*later].action == "land"
                && states[*later].picks.landing.get().as_ref() == Some(&pending)
        });
        landings.insert(step, lands);
    }
    landings
}

/// What the store holds beneath the driver's prefix, as the trace names it.
async fn held(
    at: &str,
    objects: &Faulty<InMemory>,
    tokens: &Tokens,
    bodies: &Bodies,
) -> BTreeSet<Obj> {
    let base = Path::from(format!("{PREFIX}/p.{}", pipeline().as_str()));
    let listed: Vec<ObjectMeta> = objects
        .inner()
        .list(Some(&base))
        .take(LISTING + 1)
        .try_collect()
        .await
        .expect("a store in memory lists");
    let listed = bounded(listed, &format!("{at}: what the store holds"));
    let mut held = BTreeSet::new();
    for meta in listed {
        let key = meta.location.as_ref();
        let name = key.rsplit('/').next().unwrap_or_default();
        let got = objects
            .inner()
            .get_opts(&meta.location, GetOptions::default())
            .await
            .expect("a listed object is there");
        let token = got
            .attributes
            .get(&token_name())
            .map(|value| value.as_ref().to_owned());
        let bytes = got.bytes().await.expect("an object in memory reads");
        held.insert(named(key, name, token.as_deref(), &bytes, tokens, bodies));
    }
    held
}

/// The object the store holds at `key`, `name` its last part, with `bytes` and the `token` its
/// create was given, as the trace names it.
fn named(
    key: &str,
    name: &str,
    token: Option<&str>,
    bytes: &[u8],
    tokens: &Tokens,
    bodies: &Bodies,
) -> Obj {
    let (maker, start) = token
        .and_then(|token| tokens.get(token))
        .map_or((Who::X, 0), |(who, start)| (*who, *start));
    let whole = |name: Name, by: Who, tok: u64, live: BTreeSet<u64>| Obj {
        name,
        by,
        tok,
        live,
        kind: Kind::Whole,
    };
    if key.contains("/open/") {
        whole(Name::Mark, maker, 0, BTreeSet::new())
    } else if name == "stray" {
        whole(Name::Stray, Who::X, 0, BTreeSet::new())
    } else if let Some(number) = name.strip_suffix(".wal") {
        let number = number.parse().expect("a head's number");
        if bytes.starts_with(b"rdltref\0") {
            Obj {
                kind: Kind::InParts(maker),
                ..whole(Name::ChunkHead(number), maker, start, BTreeSet::new())
            }
        } else {
            whole(Name::ChunkHead(number), maker, start, parsed(bytes).1)
        }
    } else {
        let (number, rest) = name.split_once('.').expect("a body's number");
        let token = rest.strip_suffix(".body").expect("a body");
        let maker = bodies.get(token).copied().unwrap_or(Who::X);
        let number = number.parse().expect("a body's number");
        whole(Name::Body((number, maker)), maker, 0, parsed(bytes).1)
    }
}

/// The trace's store as the driver compares it: a head in parts names its body, whose bytes the
/// store holds, so the head's own needs are not compared.
fn comparable(store: &BTreeSet<Obj>) -> BTreeSet<Obj> {
    store
        .iter()
        .map(|obj| match obj.kind {
            Kind::InParts(_) => Obj {
                live: BTreeSet::new(),
                ..obj.clone()
            },
            Kind::Whole => obj.clone(),
        })
        .collect()
}

/// A trace being replayed: the shared store, its loads, and what the driver learnt of the tokens
/// the code drew.
struct Driver<'a> {
    name: &'a str,
    states: &'a [State],
    slot: Slot,
    objects: Faulty<InMemory>,
    loads: BTreeMap<Who, Load>,
    /// The seed of the next replaying load's draws, once one crashed.
    seed: u64,
    start: Instant,
    landings: BTreeMap<usize, Option<usize>>,
    tokens: Tokens,
    /// The replaying load's starts so far: each start, after a crash or not, makes its creates'
    /// tokens its own.
    epoch: u64,
    bodies: Bodies,
}

impl<'a> Driver<'a> {
    async fn new(name: &'a str, states: &'a [State]) -> Self {
        let slot: Slot = Arc::default();
        let objects = Faulty::new(InMemory::new(), plan(Arc::clone(&slot)));
        let mut loads = BTreeMap::new();
        loads.insert(Who::W, Load::open(&objects, 1).await);
        loads.insert(Who::R, Load::open(&objects, 2).await);
        Self {
            name,
            states,
            slot,
            objects,
            loads,
            seed: 3,
            start: Instant::now(),
            landings: landings(states),
            tokens: Tokens::new(),
            epoch: 0,
            bodies: Bodies::new(),
        }
    }

    /// The instant on the paused clock step `step` lands what is due at.
    fn instant(&self, step: usize) -> Instant {
        self.start + STEP * u32::try_from(step).expect("a short trace")
    }

    /// Replays every step, failing at the first where the code does other than the trace.
    async fn replay(mut self) {
        let states = self.states;
        let first = command_of(&states[0].procs[&Who::W]);
        self.loads[&Who::W]
            .commands
            .send(first)
            .expect("the writer runs");
        for (step, state) in states.iter().enumerate().skip(1) {
            let at = format!("{}, step {step} ({})", self.name, state.action);
            match state.action.as_str() {
                "request" => self.request(step, &at).await,
                "land" => self.land(step).await,
                "crash" => self.crash(step).await,
                "start" => {
                    self.epoch += 1;
                    let proc = &state.procs[&Who::R];
                    self.loads[&Who::R]
                        .commands
                        .send(command_of(proc))
                        .expect("the replay runs");
                }
                "intrude" => {
                    let stray = Path::from(format!("{}/stray", log_dir()));
                    let put = self
                        .objects
                        .inner()
                        .put(&stray, PutPayload::from_static(b"stray"));
                    put.await.expect("a store in memory takes a put");
                }
                other => panic!("{at}: an action the driver does not know: {other}"),
            }
            let observed = held(&at, &self.objects, &self.tokens, &self.bodies).await;
            let expected = comparable(&state.store);
            assert_eq!(
                comparable(&observed),
                expected,
                "{at}: the store holds other than the trace"
            );
        }
        for load in self.loads.into_values() {
            load.task.abort();
        }
    }

    /// The request step `step` makes, met as the trace says; then the load runs to its next
    /// request or its operation's answer, which must be the trace's.
    async fn request(&mut self, step: usize, at: &str) {
        let states = self.states;
        let (before, after) = (&states[step - 1], &states[step]);
        let who = after.picks.who.get().expect("a request's load");
        let outcome = after.picks.outcome.get().expect("a request's outcome");
        let req = &before.procs[&who].req;
        let lands = self.landings.get(&step).copied().flatten();
        let lands = lands.map_or(self.instant(10_000), |lands| self.instant(lands));
        let load = self.loads.get_mut(&who).expect("a running load");
        let Event::Asked(asked) = load.next(at).await else {
            panic!("{at}: the trace's load {who:?} makes a request, and the code answered instead");
        };
        if asked.op == Op::Begin {
            let file = asked.key.rsplit('/').next().unwrap_or_default();
            let token = file.split('.').nth(1).expect("a body's token");
            self.bodies.insert(token.to_owned(), who);
        }
        let expected = key_of(req.at, &self.bodies);
        assert_eq!(
            (asked.op, asked.key.as_str()),
            (op_of(req.op), expected.as_str()),
            "{at}"
        );
        if let Some(token) = &asked.token {
            let start = if who == Who::R { self.epoch } else { 0 };
            self.tokens.insert(token.clone(), (who, start));
        }
        let fault = fault_of(req.op, outcome, lands);
        *self.slot.lock() = Some((asked.op, asked.key, fault));
        asked.release.send(()).ok();
        if matches!(fault, Fault::Late(_) | Fault::Hang) {
            load.held.send_replace(false);
        }
        let proc = &after.procs[&who];
        match (load.next(at).await, proc.ended) {
            (Event::Answered(answered), true) => {
                let answer = &proc.answer;
                assert!(
                    agrees(&answered, answer),
                    "{at}: the code answered {answered:?}, the trace {answer:?}"
                );
                if proc.doing != Doing::Idle {
                    load.commands.send(command_of(proc)).expect("the load runs");
                }
            }
            (Event::Asked(next), false) => load.ahead = Some(next),
            (Event::Asked(_), true) => {
                panic!(
                    "{at}: the trace ends {who:?}'s operation, and the code made another request"
                );
            }
            (Event::Answered(answered), false) => {
                panic!(
                    "{at}: the trace has {who:?} request on, and the code answered {answered:?}"
                );
            }
        }
    }

    /// What step `step` lands, landed: the clock moves on to the instant it was due at, and a
    /// request of the driver's own, of no object of a log, lands it.
    async fn land(&mut self, step: usize) {
        let states = self.states;
        assert!(
            states[step].picks.landing.get().is_some(),
            "a landing names what lands"
        );
        let due = self.instant(step);
        tokio::time::advance(due.saturating_duration_since(Instant::now())).await;
        drop(self.objects.head(&Path::from("nudge")).await);
    }

    /// The load step `step` crashes, gone with what it held; a replaying load starts again in a
    /// process of its own.
    async fn crash(&mut self, step: usize) {
        let states = self.states;
        let whom = states[step].picks.whom.get().expect("who crashes");
        let crashed = self.loads.remove(&whom).expect("a running load");
        crashed.task.abort();
        drop(tokio::time::timeout(BOUND, crashed.task).await);
        if whom == Who::R {
            self.loads
                .insert(Who::R, Load::open(&self.objects, self.seed).await);
            self.seed += 1;
        }
    }
}

/// The traces `just quint` writes of the log on object storage's specification.
fn traces() -> Vec<PathBuf> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../specs/traces/wal_objects");
    let traces: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|error| panic!("{}: {error}", dir.display()))
        .map(|entry| entry.expect("a directory entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .take(LISTING + 1)
        .collect();
    let mut traces = bounded(traces, &format!("the traces in {}", dir.display()));
    traces.sort();
    traces
}

#[tokio::test(start_paused = true)]
async fn the_store_does_what_each_trace_of_its_specification_does() {
    let traces = traces();
    assert!(!traces.is_empty(), "no trace to replay");
    for path in traces {
        let text = std::fs::read_to_string(&path).expect("a trace reads");
        let trace: itf::Trace<State> = itf::trace_from_str(&text)
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        let states: Vec<State> = trace.states.into_iter().map(|state| state.value).collect();
        let name = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or_default()
            .to_owned();
        Driver::new(&name, &states).await.replay().await;
    }
}

#[test]
#[should_panic(expected = "a listing: more than 10000 entries")]
fn a_listing_past_its_bound_fails() {
    drop(bounded(vec![(); LISTING + 1], "a listing"));
}
