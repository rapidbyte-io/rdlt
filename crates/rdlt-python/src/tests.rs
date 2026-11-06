use pyo3::types::PyAnyMethods as _;

use crate::embedded;

#[test]
fn the_package_reports_the_crate_version() {
    embedded::with(|py| {
        let rdlt = py.import("rdlt").expect("the package imports");
        let version: String = rdlt.getattr("__version__").unwrap().extract().unwrap();
        assert_eq!(version, env!("CARGO_PKG_VERSION"));
    });
}

#[test]
fn the_extension_s_entry_registers_the_version_under_its_name() {
    embedded::with(|py| {
        let module = pyo3::wrap_pymodule!(crate::native)(py).into_bound(py);
        let name: String = module.getattr("__name__").unwrap().extract().unwrap();
        assert_eq!(name, "_native");
        let version: String = module.getattr("__version__").unwrap().extract().unwrap();
        assert_eq!(version, env!("CARGO_PKG_VERSION"));
    });
}
