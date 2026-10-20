//! Integration tests for the simulation harness.

#![forbid(unsafe_code)]

mod changes;
mod determinism;
mod exactly_once;
mod hostile;
mod stress;

/// Checks that `seed`, where `found` keeps it for a defect it found, still reaches the path the
/// defect was on, so a seed the simulation has grown around fails rather than passes for nothing.
fn reaches(found: &[(u64, rdlt_sim::Reach)], seed: rdlt_sim::Seed, reached: rdlt_sim::Reached) {
    if let Some((_, path)) = found.iter().find(|(kept, _)| *kept == seed.value()) {
        assert!(
            reached.has(*path),
            "seed {seed} no longer reaches {path:?}, where the defect it was kept for was: keep \
             a seed that does, and that fails with the defect's fix undone"
        );
    }
}

/// Counts each thread's heap, so a seed's attempts are held to what their runs reserve.
#[global_allocator]
static HEAP: alloc_tracker::Allocator<std::alloc::System> = alloc_tracker::Allocator::system();
