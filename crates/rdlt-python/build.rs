//! Builds `rdlt._native`: test binaries find the embedded interpreter's library, and its tests
//! know the development environment's interpreter.

#![forbid(unsafe_code)]

fn main() {
    // A test binary links libpython; an extension module, as maturin builds it, does not.
    pyo3_build_config::add_libpython_rpath_link_args();
    if let Some(executable) = pyo3_build_config::get().executable() {
        println!("cargo:rustc-env=RDLT_PYTHON_EXECUTABLE={executable}");
    }
}
