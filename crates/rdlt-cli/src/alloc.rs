//! The allocator knob: the CLI's one deliberate `unsafe`, two integer
//! settings handed to glibc before any pipeline thread exists.

/// Bound glibc's allocator retention: data movement churns large
/// short-lived buffers, and glibc retains them as RSS long after free.
/// Setting `M_TRIM_THRESHOLD` (to its own default) DISABLES glibc's
/// dynamic growth of the mmap/trim thresholds — measured: dropping the
/// trim call costs +29%/+32% peak RSS on the two reference cells; the
/// `M_ARENA_MAX` cap is small and inconsistent; kept for the memory
/// reduction. CLI-only, since library embedders own their allocator
/// policy; the workspace denies unsafe and this pointer-free FFI call is
/// the exception.
#[allow(unsafe_code)]
pub(crate) fn bound_retention() {
    #[cfg(target_env = "gnu")]
    // SAFETY: mallopt takes two ints, touches no memory we own, and is
    // called before any pipeline threads exist.
    unsafe {
        libc::mallopt(libc::M_ARENA_MAX, 2);
        libc::mallopt(libc::M_TRIM_THRESHOLD, 128 * 1024);
    }
}
