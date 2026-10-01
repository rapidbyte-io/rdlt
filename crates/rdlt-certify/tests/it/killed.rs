//! Connectors that lose or repeat rows once killed as an engine loads through them: the kill
//! clauses fail them.

use rdlt_certify::{Outcome, Probe, Target, certify_destination, certify_source};
use rdlt_connector::serve::Served;
use rdlt_connector::{
    BoxFuture, Capabilities, Catalog, CommitMeta, ConnectContext, ConnectorSpec, Cursor,
    Destination, DestinationFactory, DestinationSession, DestinationWriter, OpenContext,
    OpenedSession, PartitionId, PartitionPlan, PartitionSink, ReadRequest, Receipt, SegmentSet,
    Source, SourceFactory, StreamName, StreamState, TableChange, TableRef, WriteModes,
    destination_factory, source_factory,
};
use rdlt_connector_reference::{ChangesSource, GeneratorSource, MemoryDestination, published};
use serde_json::json;

/// A seed whose kills land while a source of thousands of rows still reads.
///
/// They land at the first write after the third commit, before the third, and after the second,
/// losing its answer: a source read within a small window of credit is still reading then.
pub(crate) const SETTLED_LATE: u64 = 1;

/// A source that reads nothing of a partition it resumes: whatever a checkpoint left unread is
/// lost.
struct Forgetful(Box<dyn SourceFactory>);

struct ForgetfulSource(Box<dyn Source>);

impl SourceFactory for Forgetful {
    fn spec(&self) -> &ConnectorSpec {
        self.0.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn Source>>> {
        Box::pin(async move {
            let source = self.0.connect(config, context).await?;
            Ok(Box::new(ForgetfulSource(source)) as Box<dyn Source>)
        })
    }
}

impl Source for ForgetfulSource {
    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        self.0.check()
    }

    fn discover(&self) -> BoxFuture<'_, rdlt_connector::Result<Catalog>> {
        self.0.discover()
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, rdlt_connector::Result<PartitionPlan>> {
        self.0.plan(stream, state)
    }

    fn read(
        &self,
        request: ReadRequest,
        sink: PartitionSink,
    ) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        if request.cursor.is_some() {
            return Box::pin(async { Ok(()) });
        }
        self.0.read(request, sink)
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        self.0.committed(stream, cursors)
    }
}

/// A destination that records each commit's state at once but publishes its rows only with the
/// next commit, or as the session closes: a kill between them loses rows its state says it has.
struct Deferring(Box<dyn DestinationFactory>);

struct DeferringDestination(Box<dyn Destination>);

struct DeferringSession {
    session: Box<dyn DestinationSession>,
    held: SegmentSet,
    last: Option<CommitMeta>,
}

impl DestinationFactory for Deferring {
    fn spec(&self) -> &ConnectorSpec {
        self.0.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn Destination>>> {
        Box::pin(async move {
            let destination = self.0.connect(config, context).await?;
            Ok(Box::new(DeferringDestination(destination)) as Box<dyn Destination>)
        })
    }
}

impl Destination for DeferringDestination {
    fn capabilities(&self) -> &Capabilities {
        self.0.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        self.0.check()
    }

    fn open<'a>(
        &'a self,
        context: &'a OpenContext,
    ) -> BoxFuture<'a, rdlt_connector::Result<OpenedSession>> {
        Box::pin(async move {
            let opened = self.0.open(context).await?;
            let session = DeferringSession {
                session: opened.session,
                held: SegmentSet::new(),
                last: None,
            };
            Ok(OpenedSession {
                session: Box::new(session),
                ..opened
            })
        })
    }
}

impl DestinationSession for DeferringSession {
    fn apply_schema<'a>(
        &'a mut self,
        change: &'a TableChange,
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        self.session.apply_schema(change)
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, rdlt_connector::Result<Box<dyn DestinationWriter>>> {
        self.session.writer(table)
    }

    fn commit<'a>(
        &'a mut self,
        meta: &'a CommitMeta,
    ) -> BoxFuture<'a, rdlt_connector::Result<Receipt>> {
        Box::pin(async move {
            let mut deferred = meta.clone();
            deferred.segments = std::mem::replace(&mut self.held, meta.segments.clone());
            self.last = Some(meta.clone());
            self.session.commit(&deferred).await
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, rdlt_connector::Result<()>> {
        let Self {
            mut session,
            held,
            last,
        } = *self;
        Box::pin(async move {
            if let Some(last) = last.filter(|_| !held.is_empty()) {
                let rest = CommitMeta {
                    commit_seq: last.commit_seq.next(),
                    segments: held,
                    state_delta: Vec::new(),
                    finish_generations: Vec::new(),
                    ..last
                };
                session.commit(&rest).await?;
            }
            session.close().await
        })
    }
}

/// A memory destination that declares only the write modes it is given.
struct Declaring(Box<dyn DestinationFactory>, WriteModes);

struct DeclaringDestination(Box<dyn Destination>, Capabilities);

impl DestinationFactory for Declaring {
    fn spec(&self) -> &ConnectorSpec {
        self.0.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn Destination>>> {
        Box::pin(async move {
            let destination = self.0.connect(config, context).await?;
            let mut capabilities = destination.capabilities().clone();
            capabilities.write_modes = self.1;
            Ok(Box::new(DeclaringDestination(destination, capabilities)) as Box<dyn Destination>)
        })
    }
}

impl Destination for DeclaringDestination {
    fn capabilities(&self) -> &Capabilities {
        &self.1
    }

    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        self.0.check()
    }

    fn open<'a>(
        &'a self,
        context: &'a OpenContext,
    ) -> BoxFuture<'a, rdlt_connector::Result<OpenedSession>> {
        self.0.open(context)
    }
}

struct MemoryProbe(String);

impl Probe for MemoryProbe {
    fn published<'a>(
        &'a self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, rdlt_connector::Result<Vec<arrow_array::RecordBatch>>> {
        let batches = published(&self.0, &table.name);
        Box::pin(async move { Ok(batches) })
    }
}

fn failed(outcome: Option<&Outcome>) -> bool {
    matches!(outcome, Some(Outcome::Failed(_)))
}

/// A seed for every schedule of kill points a certification can draw: the source's six, or,
/// with `answers`, the destination's eighteen.
fn every_schedule(answers: bool) -> Vec<u64> {
    let answered = if answers { 0..3 } else { 0..1 };
    let mut seeds = Vec::new();
    for settled in 0..2 {
        for commit in 0..3 {
            for answer in answered.clone() {
                seeds.push(settled | commit << 8 | answer << 16);
            }
        }
    }
    seeds
}

#[tokio::test(flavor = "multi_thread")]
async fn a_source_that_loses_what_it_resumes_fails_k_source_at_every_schedule() {
    let mut certifying = tokio::task::JoinSet::new();
    for seed in every_schedule(false) {
        certifying.spawn(async move {
            let forgetful = Forgetful(source_factory::<GeneratorSource>());
            let target =
                Target::served(Served::new().with_source(Box::new(forgetful))).kill_seed(seed);
            let config = json!({
                "seed": 3,
                "streams": [{ "name": "events", "rows": 20000, "partitions": 2, "batch_rows": 50 }],
            });
            (seed, certify_source(&target, config).await)
        });
    }
    while let Some(certified) = certifying.join_next().await {
        let (seed, report) = certified.expect("the certification ends");
        assert!(failed(report.outcome("K-SOURCE")), "seed {seed}: {report}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_destination_that_records_state_before_rows_fails_k_destination_at_every_schedule() {
    let mut certifying = tokio::task::JoinSet::new();
    for seed in every_schedule(true) {
        certifying.spawn(async move {
            let deferring = Deferring(destination_factory::<MemoryDestination>());
            let target =
                Target::served(Served::new().with_destination(Box::new(deferring))).kill_seed(seed);
            let store = format!("certify_deferring_{seed}");
            let config = json!({ "store": store });
            (
                seed,
                certify_destination(&target, config, &MemoryProbe(store)).await,
            )
        });
    }
    while let Some(certified) = certifying.join_next().await {
        let (seed, report) = certified.expect("the certification ends");
        assert!(
            failed(report.outcome("K-DESTINATION")),
            "seed {seed}: {report}"
        );
    }
}

#[tokio::test]
async fn a_chosen_kill_seed_is_the_one_a_failure_reports() {
    let deferring = Deferring(destination_factory::<MemoryDestination>());
    let target =
        Target::served(Served::new().with_destination(Box::new(deferring))).kill_seed(4242);
    let report = certify_destination(
        &target,
        json!({ "store": "certify_deferring_seeded" }),
        &MemoryProbe("certify_deferring_seeded".to_owned()),
    )
    .await;
    let Some(Outcome::Failed(reason)) = report.outcome("K-DESTINATION") else {
        panic!("K-DESTINATION did not fail: {report}");
    };
    assert!(reason.contains("kill seed 4242"), "{reason}");
}

#[tokio::test]
async fn a_destination_is_killed_in_a_write_mode_it_declares_whichever_that_is() {
    let none = WriteModes {
        append: false,
        replace: false,
        merge: false,
        history: false,
    };
    let declared = [
        (
            "merge",
            WriteModes {
                merge: true,
                ..none
            },
        ),
        (
            "replace",
            WriteModes {
                replace: true,
                ..none
            },
        ),
        (
            "both",
            WriteModes {
                replace: true,
                merge: true,
                ..none
            },
        ),
    ];
    for (name, modes) in declared {
        let declaring = Declaring(destination_factory::<MemoryDestination>(), modes);
        let target = Target::served(Served::new().with_destination(Box::new(declaring)));
        let store = format!("certify_declaring_{name}");
        let config = json!({ "store": store });
        let report = certify_destination(&target, config, &MemoryProbe(store)).await;
        // The engine loads it in that mode, so the clause does: it is not declared away.
        let killed = report.outcome("K-DESTINATION");
        assert_eq!(killed, Some(&Outcome::Passed), "{name}: {report}");
    }
}

#[tokio::test]
async fn a_change_source_is_killed_as_it_reads_its_changes() {
    let target = Target::served(Served::new().with_source(source_factory::<ChangesSource>()))
        .kill_seed(SETTLED_LATE);
    let config = json!({
        "seed": 5,
        "streams": [{ "name": "accounts", "keys": 400, "changes": 4000, "batch_rows": 20 }],
    });
    let report = certify_source(&target, config).await;
    // A stream read only as changes is loaded as the engine loads it, and killed as it is.
    let killed = report.outcome("K-SOURCE");
    assert_eq!(killed, Some(&Outcome::Passed), "{report}");
}
