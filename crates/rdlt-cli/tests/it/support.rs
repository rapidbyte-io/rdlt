//! What the CLI's tests share: the built binary, run in a scratch directory with connectors copied
//! from the test build's examples, and documents written there.

use std::path::Path;
use std::process::Output;
use std::time::Duration;

/// Has `child`, spawned to lead a process group, killed with its group when this test ends.
pub(crate) fn guarded(child: &tokio::process::Child) {
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

    /// `rdlt args`, its host the scratch's connectors run trusted, its XDG directories the
    /// scratch's, run in the scratch, leading a process group of its own.
    pub(crate) fn command(&self, args: &[&str]) -> tokio::process::Command {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_rdlt"));
        command
            .args(args)
            .arg("--connectors")
            .arg(self.path().join("connectors"))
            .arg("--trust-binaries")
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
