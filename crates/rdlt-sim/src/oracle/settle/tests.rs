use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use rdlt_connector::{
    Field, LoadId, LogicalType, OpenContext, PipelineId, SchemaVersion, SegmentId, TableChange,
    TablePath, TableRef, TableSchema,
};
use rdlt_wire::Limits;
use tokio::runtime::Handle;

use super::{LOOK, OUTLIVED, alive, released, served_outlived, settle};
use crate::canary::Canary;
use crate::network::{Networked, Serving, options, run_networked};
use crate::owners::Live;
use crate::rng::SplitMix64;
use crate::seed::{Seed, run};
use crate::world::{FaultPoint, World};

/// How long `settle` took on the simulated clock, with a task that ends after `lives`.
fn settling_with_a_task_that_lives(seed: u64, lives: Duration) -> Duration {
    run(Seed::new(seed), move |_| async move {
        tokio::spawn(tokio::time::sleep(lives));
        let started = tokio::time::Instant::now();
        settle(Seed::new(seed)).await;
        started.elapsed()
    })
}

#[test]
fn settling_ends_once_the_last_task_has() {
    let waited = settling_with_a_task_that_lives(1, Duration::from_millis(100));
    assert!(
        waited >= Duration::from_millis(100) && waited <= Duration::from_millis(101),
        "{waited:?}"
    );
}

#[test]
fn settling_with_no_task_left_takes_no_time() {
    let waited = run(Seed::new(2), |_| async {
        let started = tokio::time::Instant::now();
        settle(Seed::new(2)).await;
        started.elapsed()
    });
    assert_eq!(waited, Duration::ZERO);
}

#[test]
fn a_task_ending_just_before_the_bound_has_not_outlived_its_run() {
    // Two steps short, so whichever of its timer and settling's fires first on a step, it has
    // ended before the bound.
    let waited =
        settling_with_a_task_that_lives(3, OUTLIVED.saturating_sub(Duration::from_millis(2)));
    assert!(waited < OUTLIVED, "{waited:?}");
}

#[test]
fn a_task_alive_at_the_bound_outlived_its_run_and_fails_naming_the_seed() {
    // It ends a millisecond past the bound, so a settle that never gave up would still return.
    let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
        settling_with_a_task_that_lives(4, OUTLIVED + Duration::from_millis(1))
    }));
    let payload = outcome.expect_err("the task outlived its run");
    let message = payload
        .downcast_ref::<String>()
        .expect("a formatted panic message");
    assert!(
        message.contains("seed 4: 1 tasks outlived their runs"),
        "{message}"
    );
}

#[test]
fn connectors_that_hold_nothing_are_released_at_once() {
    let registered = World::register("released", &mut SplitMix64::new(5));
    let world = Arc::clone(registered.world());
    let waited = run(Seed::new(5), move |_| async move {
        let started = tokio::time::Instant::now();
        released(Seed::new(5), &world, &[]).await;
        started.elapsed()
    });
    drop(registered);
    assert_eq!(waited, Duration::ZERO);
}

#[test]
fn a_writer_let_go_within_the_bound_is_released_when_it_is() {
    let registered = World::register("released-late", &mut SplitMix64::new(6));
    let world = Arc::clone(registered.world());
    let waited = run(Seed::new(6), move |_| async move {
        let held = Live::of(&[&world.owners.writers]);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            drop(held);
        });
        let started = tokio::time::Instant::now();
        released(Seed::new(6), &world, &[]).await;
        started.elapsed()
    });
    drop(registered);
    assert!(
        waited >= Duration::from_secs(10) && waited <= Duration::from_millis(10_002),
        "{waited:?}"
    );
}

#[test]
fn a_session_held_past_the_bound_fails_naming_the_seed() {
    let registered = World::register("released-never", &mut SplitMix64::new(7));
    let world = Arc::clone(registered.world());
    let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
        run(Seed::new(7), move |_| async move {
            let _held = Live::of(&[&world.owners.sessions]);
            released(Seed::new(7), &world, &[]).await;
        });
    }));
    drop(registered);
    let payload = outcome.expect_err("the session outlived its run");
    let message = payload
        .downcast_ref::<String>()
        .expect("a formatted panic message");
    assert!(
        message.contains(
            "seed 7: 0 tasks on the connectors' hosts, and 1 sessions, 0 writers and 0 reads, \
             outlived their runs"
        ),
        "{message}"
    );
}

/// Runs `released` over a connector host that spawns a task living `task`, and returns the
/// panic message, if it panicked.
fn released_over_a_host_that_runs(seed: u64, task: Duration) -> Option<String> {
    let registered = World::register("released-host", &mut SplitMix64::new(seed));
    let world = Arc::clone(registered.world());
    let handle = Arc::new(std::sync::Mutex::new(None::<Handle>));
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(150))
        .build();
    let host = Arc::clone(&handle);
    sim.host("connector", move || {
        let host = Arc::clone(&host);
        async move {
            *host.lock().unwrap() = Some(Handle::current());
            tokio::spawn(tokio::time::sleep(task));
            tokio::time::sleep(Duration::from_secs(130)).await;
            Ok(())
        }
    });
    sim.client("engine", async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let hosts = [handle.lock().unwrap().clone().expect("the host began")];
        released(Seed::new(seed), &world, &hosts).await;
        Ok(())
    });
    let outcome = sim.run();
    drop(registered);
    outcome.err().map(|error| error.to_string())
}

#[test]
fn a_task_a_connector_host_still_runs_past_the_bound_fails_naming_the_seed() {
    let message = released_over_a_host_that_runs(8, Duration::from_secs(120))
        .expect("the task outlived its run");
    assert!(
        message.contains(
            "seed 8: 1 tasks on the connectors' hosts, and 0 sessions, 0 writers and 0 reads, \
             outlived their runs"
        ),
        "{message}"
    );
}

#[test]
fn a_task_a_connector_host_ends_within_the_bound_is_released_when_it_ends() {
    assert_eq!(
        released_over_a_host_that_runs(9, Duration::from_secs(5)),
        None
    );
}

/// Bounds every wait of a test that should end at once.
const WAIT: Duration = Duration::from_secs(60);

/// The table the served writer writes.
fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["t"]).expect("a valid path"),
        name: "t".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

/// One row of it.
fn rows() -> RecordBatch {
    RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
        .expect("a valid batch")
}

#[test]
fn a_served_writer_whose_write_hangs_ends_once_its_host_lets_it_go() {
    let (ended, left) = run_networked(Seed::new(1), Serving::Rows, false, |_env, net| async move {
        let mut rng = SplitMix64::new(7);
        let name = "released-writer";
        let registered = World::register(name, &mut rng);
        let world = Arc::clone(registered.world());
        world.hang_at(FaultPoint::Write);
        let canary = Canary::of(name);
        let config = canary.config();
        let placing = Networked::new(net, &options(&mut rng, Limits::default()), &canary);
        let destination = placing.destination(&config, false).await;
        let context = OpenContext {
            pipeline: PipelineId::parse("released").expect("a valid pipeline id"),
            load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
        };
        let mut opened = tokio::time::timeout(WAIT, destination.open(&context))
            .await
            .expect("the open ends")
            .expect("the session opens");
        let table = table();
        let create = TableChange::Create {
            table: table.clone(),
            schema: TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)])
                .expect("a valid schema"),
        };
        tokio::time::timeout(WAIT, opened.session.apply_schema(&create))
            .await
            .expect("the schema change ends")
            .expect("the table is created");
        let mut writer = tokio::time::timeout(WAIT, opened.session.writer(&table))
            .await
            .expect("the writer's opening ends")
            .expect("the writer opens");
        let rows = rows();
        tokio::time::timeout(WAIT, writer.write(SegmentId(1), rows))
            .await
            .expect("the write ends")
            .expect("the batch is sent");
        // The destination's write never returns, so neither does the flush behind it: the host
        // gives up on it, as the engine does at its wait.
        let flushed = tokio::time::timeout(Duration::from_secs(5), writer.flush()).await;
        assert!(flushed.is_err(), "a flush behind a write that hangs waits");
        drop((writer, opened, destination));
        let hosts = placing.hosts();
        let releasing = async {
            while alive(&hosts) > 0 || !world.owners.held().is_empty() {
                tokio::time::sleep(LOOK).await;
            }
        };
        let ended = tokio::time::timeout(served_outlived(), releasing)
            .await
            .is_ok();
        let left = (alive(&hosts), world.owners.held());
        drop(registered);
        (ended, left)
    });
    assert!(
        ended,
        "a served writer whose write hangs outlived its host: {} tasks, {}",
        left.0, left.1
    );
}
