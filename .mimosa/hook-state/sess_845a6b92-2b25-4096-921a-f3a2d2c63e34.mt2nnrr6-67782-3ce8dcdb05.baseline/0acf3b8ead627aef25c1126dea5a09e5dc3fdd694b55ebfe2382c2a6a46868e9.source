//! Hermetic proto codegen: no system `protoc` is assumed to exist, so this
//! vendors one (`protoc-bin-vendored`) and points `prost-build` at it via
//! `Config::protoc_executable` — a plain safe-Rust setter, so no
//! process-wide env mutation (and none of what that would have required)
//! is needed here.

fn main() {
    let mut config = tonic_prost_build::Config::new();
    config.protoc_executable(
        protoc_bin_vendored::protoc_bin_path().expect("vendored protoc binary for this platform"),
    );

    tonic_prost_build::configure()
        .compile_with_config(config, &["proto/rdlt_connector_v1.proto"], &["proto"])
        .expect("compile rdlt_connector_v1.proto");
}
