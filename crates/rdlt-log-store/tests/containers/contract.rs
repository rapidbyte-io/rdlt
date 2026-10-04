//! The store's contract, and the probe, on each server.

use crate::servers::{Server, opened};

async fn keeps_the_contract(server: Server) {
    let running = server.start().await;
    let store = opened(&running.endpoint, "contract")
        .await
        .unwrap_or_else(|error| panic!("{server:?}: the probe refused it: {}", error.code()));
    rdlt_engine::conformance::conforms(store.as_ref()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_log_on_rustfs_keeps_the_store_s_contract() {
    keeps_the_contract(Server::RustFs).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_log_on_minio_keeps_the_store_s_contract() {
    keeps_the_contract(Server::Minio).await;
}
