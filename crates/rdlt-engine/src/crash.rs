//! Crash points at the engine's durability steps (spec §20.6): with the `failpoints` feature, a
//! test names a point in `FAILPOINTS` (`engine.commit.before=return`, or `=2*off->return` for its
//! third hit) and the process aborts there, as a crash would; without it they are nothing.

/// The point `$name`: aborts the process where a test configured it to.
macro_rules! crash_point {
    ($name:literal) => {
        #[cfg(feature = "failpoints")]
        fail::fail_point!($name, |_| std::process::abort());
    };
}

pub(crate) use crash_point;
