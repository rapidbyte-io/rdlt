//! Crash-point injection macro — defined ONCE here because core is the one crate
//! every protocol implementation shares.
//!
//! This is test infrastructure, not vocabulary: the `failpoints` feature is
//! off by default, never enabled by release builds, and the macro expands to
//! NOTHING without it — zero release-path cost. It lives in core only because
//! the injection sites are in production code (engine, destinations), where a
//! dev-dependency cannot reach.
//!
//! Usage: `crash_point!("crate.site.name", <expression to return when armed>)`.
//! Every site's name MUST appear in its crate's fail-point registry const —
//! the crash sweep asserts the registry equals its swept list.

/// Re-export for macro hygiene: `crash_point!` expands to
/// `$crate::fail::fail_point!`, so callers never depend on `fail` directly.
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub use fail;

#[cfg(feature = "failpoints")]
#[macro_export]
macro_rules! crash_point {
    ($name:expr, $err:expr) => {
        $crate::failpoint::fail::fail_point!($name, |_| { $err });
    };
}

/// No-op without the `failpoints` feature: the arguments are parsed, never
/// evaluated, and the expansion is empty.
#[cfg(not(feature = "failpoints"))]
#[macro_export]
macro_rules! crash_point {
    ($name:expr, $err:expr) => {};
}
