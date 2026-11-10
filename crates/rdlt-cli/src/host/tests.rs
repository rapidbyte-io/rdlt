use std::path::PathBuf;

use super::{document, policy};
use crate::args::HostArgs;

fn paths(given: &[&str]) -> Vec<PathBuf> {
    given.iter().map(PathBuf::from).collect()
}

#[tokio::test]
async fn a_flag_given_stands_in_place_of_the_file_s_setting_and_the_rest_of_the_file_holds() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("host.yaml");
    std::fs::write(
        &file,
        "connectors: [/file/connectors]\nwritable: [/file/w]\nreadable: [/file/r]\n\
         allow_env: [FILE]\nallow_files: [/file/s]\n",
    )
    .unwrap();
    let args = HostArgs {
        file: Some(file),
        ..HostArgs::default()
    };
    let read = policy(&args).await.unwrap();
    assert_eq!(read.connectors, paths(&["/file/connectors"]));
    assert_eq!(read.writable, paths(&["/file/w"]));
    assert_eq!(read.readable, paths(&["/file/r"]));
    assert_eq!(read.allow_env, ["FILE"]);
    assert_eq!(read.allow_files, paths(&["/file/s"]));
    assert!(!read.trust_binaries && !read.allow_network && read.remote.is_none());
    let flags = HostArgs {
        file: args.file.clone(),
        connectors: paths(&["/flag/connectors"]),
        writable: paths(&["/flag/w"]),
        readable: paths(&["/flag/r"]),
        allow_env: vec!["FLAG".to_owned()],
        allow_files: paths(&["/flag/s"]),
        trust_binaries: true,
        allow_network: true,
        tls_cert: Some(PathBuf::from("/flag/cert")),
        tls_key: Some(PathBuf::from("/flag/key")),
        tls_ca: Some(PathBuf::from("/flag/ca")),
    };
    let over = policy(&flags).await.unwrap();
    assert_eq!(over.connectors, paths(&["/flag/connectors"]));
    assert_eq!(over.writable, paths(&["/flag/w"]));
    assert_eq!(over.readable, paths(&["/flag/r"]));
    assert_eq!(over.allow_env, ["FLAG"]);
    assert_eq!(over.allow_files, paths(&["/flag/s"]));
    assert!(over.trust_binaries && over.allow_network);
    let remote = over.remote.expect("the TLS flags name the remote");
    assert_eq!(
        (remote.cert, remote.key, remote.ca),
        (
            PathBuf::from("/flag/cert"),
            PathBuf::from("/flag/key"),
            PathBuf::from("/flag/ca")
        )
    );
}

#[tokio::test]
async fn a_host_file_cannot_be_trusting_unless_it_says_so() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("host.yaml");
    std::fs::write(&file, "trust_binaries: true\nallow_network: true\n").unwrap();
    let args = HostArgs {
        file: Some(file),
        ..HostArgs::default()
    };
    let read = policy(&args).await.unwrap();
    assert!(
        read.trust_binaries && read.allow_network,
        "a file's word stands where no flag speaks"
    );
    let plain = HostArgs {
        file: Some(directory.path().join("absent.yaml")),
        ..HostArgs::default()
    };
    assert_eq!(policy(&plain).await.unwrap_err().exit, crate::Exit::Io);
}

/// Only the pipeline's document is what `rdlt init` writes: a host file that is not there, read
/// with the same code, is not said to be missing in the same words.
#[tokio::test]
async fn only_a_document_that_is_not_there_is_said_to_be_missing() {
    let directory = tempfile::tempdir().unwrap();
    let absent = directory.path().join("absent.yaml");
    let missing = document(&absent).await.unwrap_err();
    assert!(missing.file_not_found, "{missing:?}");
    let not_a_file = document(directory.path()).await.unwrap_err();
    assert!(!not_a_file.file_not_found, "{not_a_file:?}");
    let args = HostArgs {
        file: Some(absent),
        ..HostArgs::default()
    };
    let host_file = policy(&args).await.unwrap_err();
    let code = host_file.report.as_ref().and_then(|report| report.code);
    assert_eq!(code.map(rdlt::Code::as_str), Some("document_unreadable"));
    assert!(!host_file.file_not_found, "{host_file:?}");
}
