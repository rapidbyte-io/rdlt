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

#[cfg(target_os = "linux")]
#[test]
fn a_reset_peak_falls_to_what_the_process_holds() {
    use super::reset_peak;
    let peak = || {
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        resident_kib(&status, "VmHWM").unwrap()
    };
    let mut held = vec![0_u8; 32 << 20];
    for page in held.chunks_mut(4096) {
        page[0] = 1;
    }
    std::hint::black_box(&held);
    let high = peak();
    drop(held);
    reset_peak().unwrap();
    let reset = peak();
    assert!(
        reset < high,
        "{reset} KiB after the reset, {high} KiB before"
    );
}
