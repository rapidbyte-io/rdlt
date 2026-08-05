//! Hermetic proto codegen: no system `protoc` is assumed to exist, so this
//! vendors one (`protoc-bin-vendored`) and points `tonic-prost-build` at it
//! via the `PROTOC` env var it reads internally.

fn main() {
    // `std::env::set_var` is `unsafe` because a concurrent reader on another
    // thread could observe a torn value; a build script is single-threaded
    // and short-lived (this is its only env mutation), so the hazard the
    // marker guards against does not apply here. This is the documented
    // protoc-bin-vendored + prost-build integration pattern.
    #[allow(unsafe_code)]
    unsafe {
        std::env::set_var(
            "PROTOC",
            protoc_bin_vendored::protoc_bin_path()
                .expect("vendored protoc binary for this platform"),
        );
    }

    tonic_prost_build::compile_protos("proto/rdlt_connector_v0.proto")
        .expect("compile rdlt_connector_v0.proto");
}
