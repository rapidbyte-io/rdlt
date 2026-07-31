//! One compile root for the testkit's own suites, mirroring the reference
//! layout: topic files under `cases/`. No gate selects any of these by binary
//! name, so consolidation costs nothing and saves three link steps.

mod cases;
