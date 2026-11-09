//! What the CLI's tests share: the built binary, run in a scratch directory with connectors copied
//! from the test build's examples, and documents written there.

use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::Duration;

use rdlt_testkit::tls::Pki;
use tokio::process::Child;

/// The example binary `name`, which the test build builds.
pub(crate) fn example(name: &str) -> PathBuf {
    let tests = std::env::current_exe().expect("the test binary has a path");
    tests
        .ancestors()
        .map(|dir| dir.join("examples").join(name))
        .find(|example| example.is_file())
        .expect("the test build builds the examples")
}

/// The reference connectors' binary, listening over mutual TLS with a CA made for it; its
/// process, and its endpoint.
pub(crate) async fn listening(pki: &Pki) -> (Child, String) {
    rdlt_testkit::tls::listening(pki, &example("serve_reference")).await
}

/// Has `child`, spawned to lead a process group, killed with its group when this test ends.
pub(crate) fn guarded(child: &Child) {
    rdlt_testkit::process::guard(child.id().expect("the child runs"))
        .expect("the child is guarded");
}

/// A scratch directory: its `connectors/` holds the connectors copied in, its XDG directories
/// are its own, and documents are written beside them.
pub(crate) struct Scratch {
    pub(crate) dir: tempfile::TempDir,
}

impl Scratch {
    pub(crate) fn new() -> Self {
        let dir = tempfile::tempdir().expect("a scratch directory");
        std::fs::create_dir(dir.path().join("connectors")).expect("the connectors directory");
        Self { dir }
    }

    pub(crate) fn path(&self) -> &Path {
        self.dir.path()
    }

    /// The scratch, with example `example_name` copied in as `rdlt-connector-<name>`: a copy has
    /// one name, as cargo's build outputs do not.
    pub(crate) fn connector(self, example_name: &str, name: &str) -> Self {
        let copy = self
            .path()
            .join("connectors")
            .join(format!("rdlt-connector-{name}"));
        std::fs::copy(example(example_name), copy).expect("the connector copies");
        self
    }

    /// The scratch's document `file`, holding `text`.
    pub(crate) fn document(&self, file: &str, text: &str) -> PathBuf {
        let path = self.path().join(file);
        std::fs::write(&path, text).expect("the document writes");
        path
    }

    /// `rdlt args`, its host the scratch's connectors run trusted, its XDG directories the
    /// scratch's, run in the scratch, leading a process group of its own.
    pub(crate) fn command(&self, args: &[&str]) -> tokio::process::Command {
        let mut command = self.bare_command(args);
        command
            .arg("--connectors")
            .arg(self.path().join("connectors"))
            .arg("--trust-binaries");
        command
    }

    /// As [`command`](Self::command), its host what the scratch's XDG directories say: no flag
    /// of the host's is passed.
    pub(crate) fn bare_command(&self, args: &[&str]) -> tokio::process::Command {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_rdlt"));
        command
            .args(args)
            .current_dir(self.path())
            .env("XDG_CONFIG_HOME", self.path().join("config"))
            .env("XDG_DATA_HOME", self.path().join("data"))
            .env(
                "LLVM_PROFILE_FILE",
                std::env::var_os("LLVM_PROFILE_FILE").unwrap_or_default(),
            )
            .env_remove("RUST_LOG")
            .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .process_group(0)
            .kill_on_drop(true);
        command
    }

    /// What `rdlt args` printed and exited with, within two minutes.
    pub(crate) async fn rdlt(&self, args: &[&str]) -> Output {
        let child = self.command(args).spawn().expect("rdlt starts");
        guarded(&child);
        tokio::time::timeout(Duration::from_secs(120), child.wait_with_output())
            .await
            .expect("rdlt ends within its bound")
            .expect("rdlt's output reads")
    }
}
