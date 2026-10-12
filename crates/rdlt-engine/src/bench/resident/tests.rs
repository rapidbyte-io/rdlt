use super::{Resident, resident_kib};

const STATUS: &str =
    "Name:\tresident\nVmPeak:\t  900000 kB\nVmHWM:\t  123456 kB\nVmRSS:\t  65432 kB\n";

#[test]
fn a_status_field_reads_in_kibibytes() {
    assert_eq!(resident_kib(STATUS, "VmHWM"), Some(123_456));
    assert_eq!(resident_kib(STATUS, "VmRSS"), Some(65_432));
    assert_eq!(resident_kib(STATUS, "VmSwap"), None);
}

#[test]
fn the_limit_is_the_budget_s_bound_and_one_push_and_a_stack_a_thread() {
    let resident = Resident {
        memory: 256 << 20,
        push: 8 << 20,
        threads: 4,
    };
    // ADR 0039: the budget, a fifth of it and 32 MiB, one push, and 2 MiB of stack a thread.
    let fifth = (256u64 << 20) / 5;
    assert_eq!(
        resident.limit(),
        (256 << 20) + fifth + (32 << 20) + (8 << 20) + 4 * (2 << 20)
    );
}
