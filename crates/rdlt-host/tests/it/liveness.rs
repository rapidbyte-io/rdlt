//! The heartbeat's verdict against connectors that answer it wrongly, and hosts that stall.

use std::time::Duration;

use rdlt_connector::serve::Served;
use rdlt_connector::{Role, Source as _, source_factory};
use rdlt_connector_reference::MemorySource;
use rdlt_host::{CONNECTOR_LOST, Connection, Options, RemoteSource};

use crate::support::connectors::{CHECKS, Counted};
use crate::support::{Fake, Fault, serve_fake, served};

/// Options that notice a lost connector within about a tenth of a second.
fn quick() -> Options {
    Options {
        heartbeat: Duration::from_millis(20),
        missed: 3,
        ..Options::default()
    }
}

#[tokio::test]
async fn a_connector_that_echoes_heartbeats_never_sent_is_lost() {
    let connection = Connection::connect(
        serve_fake(Fake(Fault::EchoAhead)),
        Role::Source,
        &serde_json::json!({}),
        quick(),
    )
    .await
    .expect("the fake handshakes");
    let source = RemoteSource::new(connection);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let error = source.check().await.unwrap_err();
    assert_eq!(error.code(), Some(CONNECTOR_LOST));
}

#[tokio::test]
async fn a_host_that_stalls_does_not_lose_a_live_connector() {
    let io = served(Served::new().with_source(source_factory::<MemorySource>()));
    let config = serde_json::json!({ "streams": {} });
    let connection = Connection::connect(io, Role::Source, &config, quick())
        .await
        .expect("the source handshakes");
    let source = RemoteSource::new(connection);
    // The heartbeat runs; then the whole runtime stalls, as a suspended process does, for many
    // heartbeats' worth.
    tokio::time::sleep(Duration::from_millis(100)).await;
    std::thread::sleep(Duration::from_millis(300));
    tokio::time::sleep(Duration::from_millis(200)).await;
    source.check().await.expect("the source is still connected");
}

/// A network partition between the host and a connector, which can come and go.
#[derive(Clone, Default)]
struct Partition(std::sync::Arc<std::sync::Mutex<(bool, Vec<std::task::Waker>)>>);

impl Partition {
    fn cut(&self) {
        self.0.lock().expect("not poisoned").0 = true;
    }

    fn heal(&self) {
        let mut state = self.0.lock().expect("not poisoned");
        state.0 = false;
        for waker in state.1.drain(..) {
            waker.wake();
        }
    }

    /// Whether the partition holds, keeping `context`'s task to wake once it heals.
    fn holds(&self, context: &std::task::Context<'_>) -> bool {
        let mut state = self.0.lock().expect("not poisoned");
        if state.0 {
            state.1.push(context.waker().clone());
        }
        state.0
    }
}

/// The host's end of a pipe, across `partition`: while it holds, the host's writes wait, as they
/// do once a socket's buffer is full of what the peer never acknowledges, and nothing reaches the
/// host, though neither end has closed.
struct Across {
    pipe: tokio::io::DuplexStream,
    partition: Partition,
}

impl tokio::io::AsyncRead for Across {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.partition.holds(context) {
            return std::task::Poll::Pending;
        }
        std::pin::Pin::new(&mut self.pipe).poll_read(context, buffer)
    }
}

impl tokio::io::AsyncWrite for Across {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if self.partition.holds(context) {
            return std::task::Poll::Pending;
        }
        std::pin::Pin::new(&mut self.pipe).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.partition.holds(context) {
            return std::task::Poll::Pending;
        }
        std::pin::Pin::new(&mut self.pipe).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.partition.holds(context) {
            return std::task::Poll::Pending;
        }
        std::pin::Pin::new(&mut self.pipe).poll_shutdown(context)
    }
}

/// A connection to `served`, served on `runtime` so this one's tasks are the host's alone, across
/// a partition; the connection, and the partition.
async fn across(
    runtime: &tokio::runtime::Runtime,
    served: Served,
    config: &serde_json::Value,
) -> (std::sync::Arc<Connection>, Partition) {
    let (host, connector) = tokio::io::duplex(64 * 1024);
    runtime.spawn(rdlt_connector::serve::serve_connection(
        std::sync::Arc::new(served),
        connector,
        rdlt_wire::Limits::default(),
    ));
    let partition = Partition::default();
    let io = Across {
        pipe: host,
        partition: partition.clone(),
    };
    // A patience far longer than any wait here: only a drop may end the connection's tasks.
    let options = Options {
        heartbeat: Duration::from_secs(1),
        missed: 60,
        ..Options::default()
    };
    let connection = Connection::connect(io, Role::Source, config, options)
        .await
        .expect("the source handshakes");
    (connection, partition)
}

/// Starts a check on `source` across a partition, and abandons it.
async fn abandoned_check(source: &RemoteSource) {
    tokio::select! {
        biased;
        _ = source.check() => panic!("a check across a partition never ends"),
        () = tokio::time::sleep(Duration::from_millis(100)) => {}
    }
}

#[tokio::test]
async fn a_dropped_connection_whose_network_is_gone_leaves_no_task_behind() {
    let runtime = tokio::runtime::Runtime::new().expect("a runtime starts");
    let baseline = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    let served = Served::new().with_source(source_factory::<MemorySource>());
    let config = serde_json::json!({ "streams": { "rows": [{ "id": 1 }] } });
    let (connection, partition) = across(&runtime, served, &config).await;
    partition.cut();
    let source = RemoteSource::new(connection);
    abandoned_check(&source).await;
    drop(source);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let alive = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    assert_eq!(alive, baseline, "tasks outlived the dropped connection");
    runtime.shutdown_background();
}

#[tokio::test]
async fn a_call_abandoned_across_a_partition_never_reaches_the_connector_once_it_heals() {
    let runtime = tokio::runtime::Runtime::new().expect("a runtime starts");
    let served = Served::new().with_source(source_factory::<Counted>());
    let (connection, partition) = across(&runtime, served, &serde_json::json!({})).await;
    partition.cut();
    let source = RemoteSource::new(connection);
    abandoned_check(&source).await;
    drop(source);
    partition.heal();
    tokio::time::sleep(Duration::from_millis(500)).await;
    // A run that ended has no effect afterwards: the check it abandoned is never made.
    assert_eq!(CHECKS.load(std::sync::atomic::Ordering::SeqCst), 0);
    runtime.shutdown_background();
}
