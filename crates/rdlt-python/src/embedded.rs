//! The interpreter a test binary embeds: the development environment's packages importable, the
//! repository's `python/` first on `sys.path`, and this crate's module installed as
//! `rdlt._native` before anything imports the package.

use std::path::Path;
use std::sync::Once;
use std::time::Duration;

use pyo3::types::{PyAnyMethods as _, PyDict, PyDictMethods as _, PyModule};
use pyo3::{PyErr, Python};

use crate::run::NativeRun;
use crate::runtime;

/// The interpreter the crate was built against: `PYO3_PYTHON`, the development environment's.
const EXECUTABLE: Option<&str> = option_env!("RDLT_PYTHON_EXECUTABLE");

/// Runs `f` attached to the embedded interpreter, started and prepared once per process.
pub(crate) fn with<R>(f: impl for<'py> FnOnce(Python<'py>) -> R) -> R {
    static PREPARED: Once = Once::new();
    PREPARED.call_once(|| {
        Python::initialize();
        Python::attach(|py| prepare(py).expect("the embedded interpreter is prepared"));
    });
    Python::attach(f)
}

fn prepare(py: Python<'_>) -> Result<(), PyErr> {
    let executable = EXECUTABLE
        .expect("rdlt-python is built against .venv's interpreter: run `just python-env`");
    let environment = Path::new(executable)
        .parent()
        .and_then(Path::parent)
        .expect("the interpreter is in an environment's bin directory");
    let vars = PyDict::new(py);
    vars.set_item("base", environment)?;
    vars.set_item("platbase", environment)?;
    let options = PyDict::new(py);
    options.set_item("vars", vars)?;
    let site_packages =
        py.import("sysconfig")?
            .call_method("get_path", ("purelib",), Some(&options))?;
    py.import("site")?
        .call_method1("addsitedir", (site_packages,))?;
    // `sys.path` holds text: the import system ignores a path object in it.
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../python");
    let sys = py.import("sys")?;
    sys.getattr("path")?
        .call_method1("insert", (0, source.to_string_lossy()))?;
    let module = PyModule::new(py, "rdlt._native")?;
    crate::register(&module)?;
    sys.getattr("modules")?.set_item("rdlt._native", module)?;
    Ok(())
}

/// The run's outcome, once it ends, within a minute.
pub(crate) fn ended_within(py: Python<'_>, run: &NativeRun) -> (String, Option<String>) {
    let mut ended = run.ended();
    let runtime = runtime::get(py).expect("the runtime starts");
    py.detach(|| {
        runtime.block_on(async move {
            tokio::time::timeout(Duration::from_secs(60), ended.wait_for(Option::is_some))
                .await
                .expect("within 60 s")
                .expect("the run's driver answers");
        });
    });
    run.wait(py).expect("the run's outcome")
}
