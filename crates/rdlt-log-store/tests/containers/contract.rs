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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_s_port_is_published_on_the_loopback_address_alone() {
    for server in [Server::RustFs, Server::Minio] {
        let running = server.start().await;
        let published = running.published_on().await;
        assert!(!published.is_empty(), "{server:?}");
        for address in published {
            assert_eq!(address, "127.0.0.1", "{server:?}");
        }
    }
}
