//! Checking a plan against its connectors: what a run's start refuses is refused, no session is
//! opened, and a connector's failure or panic is reported as the run would report it.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use rdlt_connector::{
    BoxFuture, Catalog, ColumnPath, ConnectorError, ConnectorErrorKind, Cursor, PartitionId,
    PartitionPlan, PartitionSink, ReadMode, ReadRequest, Result, Source, StreamName, StreamState,
};
use rdlt_engine::{
    Code, EngineConfig, ErrorKind, Mask, Nested, Op, SchemaSettings, StreamPlan, WriteMode, codes,
};

use crate::carried::{calls, carrying};
use crate::support::destinations::{Step, counting, failing, limited};
use crate::support::served::{Form, Served, people, people_schema, source};
use crate::support::{engine, memory, pipeline, stream};

/// What fails besides the plan: a source that carries few calls or whose check fails, a
/// destination whose identifier rules are refused or which fails as a step says.
#[derive(Clone, Copy)]
enum Fault {
    None,
    Calls(usize),
    SourceCheck,
    Rules,
    Destination(Step),
}

/// What a check refuses: the plan, what the source serves, the fault, and what the refusal is.
struct Case {
    plan: StreamPlan,
    served: Served,
    fault: Fault,
    expected: (ErrorKind, Option<Code>),
}

/// A source whose own check fails.
struct Unchecked(Arc<dyn Source>);

impl Source for Unchecked {
    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            Err(ConnectorError::new(
                ConnectorErrorKind::Transient,
                "injected",
            ))
        })
    }

    fn discover(&self) -> BoxFuture<'_, Result<Catalog>> {
        self.0.discover()
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, Result<PartitionPlan>> {
        self.0.plan(stream, state)
    }

    fn read(&self, request: ReadRequest, sink: PartitionSink) -> BoxFuture<'_, Result<()>> {
        self.0.read(request, sink)
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>> {
        self.0.committed(stream, cursors)
    }
}

fn served() -> Served {
    Served::new(Form::Arrow, vec![people(0, 1)])
}

fn declared() -> Served {
    Served {
        declared: Some(people_schema()),
        ..served()
    }
}

fn keyed() -> Served {
    Served {
        key: Some(ColumnPath::from("id")),
        ..served()
    }
}

#[tokio::test(start_paused = true)]
async fn a_check_answers_the_catalog_and_opens_no_session() {
    let name = "check_catalog";
    let (destination, sessions) = counting(memory(name).await);
    let plan = pipeline(name, [stream("people")]);
    let catalog = engine(EngineConfig::builder())
        .check(&plan, source(name, served()).await, destination)
        .await
        .expect("checked");
    let names: Vec<String> = catalog.iter().map(|spec| spec.name().to_string()).collect();
    assert_eq!(names, ["people"]);
    assert_eq!(sessions.opened.load(Ordering::SeqCst), 0);
}

impl Case {
    fn new(plan: StreamPlan, served: Served, code: Option<Code>) -> Self {
        Self {
            plan,
            served,
            fault: Fault::None,
            expected: (ErrorKind::Config, code),
        }
    }

    fn faulty(mut self, fault: Fault, kind: ErrorKind, code: Option<Code>) -> Self {
        (self.fault, self.expected) = (fault, (kind, code));
        self
    }
}

/// Plans a run's start refuses.
fn refused_plans() -> Vec<Case> {
    let normalize = SchemaSettings::new().nested(Nested::Normalize { max_depth: 2 });
    let people = || stream("people");
    let masked = |column: &str, mode: Mask| people().transform(Op::mask([column], mode));
    let merged = people().write(WriteMode::Merge);
    vec![
        Case::new(stream("orders"), served(), Some(codes::STREAM_NOT_FOUND)),
        Case::new(merged.clone(), served(), Some(codes::MERGE_KEY_MISSING)),
        Case::new(
            masked("ssn", Mask::redact("***")),
            declared(),
            Some(codes::TRANSFORM_SCHEMA),
        ),
        Case::new(
            merged.transform(Op::mask(["id"], Mask::Null)),
            keyed(),
            Some(codes::TRANSFORM_INVALID),
        ),
        Case::new(
            people().read(ReadMode::Cdc).schema(normalize),
            served(),
            Some(codes::NORMALIZE_CHANGES_UNSUPPORTED),
        ),
    ]
}

/// Connectors a check finds fault with, beside a plan it accepts.
fn faulty_connectors() -> Vec<Case> {
    let healthy = || Case::new(stream("people"), served(), None);
    vec![
        healthy().faulty(
            Fault::Calls(1),
            ErrorKind::Config,
            Some(codes::CALLS_TOO_FEW),
        ),
        healthy().faulty(Fault::SourceCheck, ErrorKind::Source, None),
        healthy().faulty(
            Fault::Rules,
            ErrorKind::Destination,
            Some(codes::CAPABILITIES_INVALID),
        ),
        healthy().faulty(
            Fault::Destination(Step::Check),
            ErrorKind::Destination,
            None,
        ),
        healthy().faulty(
            Fault::Destination(Step::PanicOnCheck),
            ErrorKind::Internal,
            None,
        ),
    ]
}

#[tokio::test(start_paused = true)]
async fn a_check_refuses_what_a_run_s_start_refuses_and_opens_no_session() {
    let cases = refused_plans().into_iter().chain(faulty_connectors());
    for (place, case) in cases.enumerate() {
        let name = format!("check_refused_{place}");
        let mut source = source(&name, case.served).await;
        let mut destination = memory(&name).await;
        match case.fault {
            Fault::None => {}
            Fault::Calls(count) => source = carrying(source, calls(count), Arc::default()),
            Fault::SourceCheck => source = Arc::new(Unchecked(source)),
            Fault::Rules => {
                destination = limited(destination, |capabilities| {
                    capabilities.identifiers.reserved_table_prefixes = [String::new()].into();
                });
            }
            Fault::Destination(step) => destination = failing(destination, step),
        }
        let (destination, sessions) = counting(destination);
        let refused = engine(EngineConfig::builder())
            .check(&pipeline(&name, [case.plan]), source, destination)
            .await
            .expect_err("refused");
        assert_eq!(
            (refused.kind(), refused.code()),
            case.expected,
            "case {place}: {refused}"
        );
        assert_eq!(
            sessions.opened.load(Ordering::SeqCst),
            0,
            "case {place} opened a session"
        );
    }
}
