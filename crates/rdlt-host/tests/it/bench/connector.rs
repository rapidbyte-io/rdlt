//! A bench's own binary run as a served connector: the connectors it serves in a process of their
//! own, held to the connectors' cores where the bench names them.

use std::process::ExitCode;

use rdlt_connector::serve::Served;

/// The variable naming the CPUs a served connector's process runs on, a list such as `4-11`; a
/// connector started without it runs where its host does.
pub(crate) const CORES: &str = "RDLT_BENCH_CONNECTOR_CORES";

/// Whether this process was started as a connector: by a host that passed it a socket, or to
/// listen for hosts.
pub(crate) fn asked() -> bool {
    std::env::args().skip(1).any(|arg| {
        let flag = arg.split_once('=').map_or(arg.as_str(), |(flag, _)| flag);
        flag == "--rdlt-fd" || flag == "--listen"
    })
}

/// Serves the connectors `build` makes as a connector binary's `main` does, on the connectors'
/// cores.
pub(crate) fn serve(build: impl FnOnce() -> Served) -> ExitCode {
    // Before the runtime starts, so its workers are as many as the connectors' cores.
    pin();
    Served::serve(build)
}

/// Holds this process to the CPUs [`CORES`] names, where it names any.
#[cfg(target_os = "linux")]
fn pin() {
    let Ok(list) = std::env::var(CORES) else {
        return;
    };
    let mut cpus = nix::sched::CpuSet::new();
    for cpu in cpu_list(&list) {
        cpus.set(cpu)
            .expect("each of the connectors' CPUs is one Linux can name");
    }
    let this = nix::unistd::Pid::from_raw(0);
    nix::sched::sched_setaffinity(this, &cpus).expect("the connectors' CPUs are online");
}

/// Runs where its host does: only Linux holds a process to chosen CPUs.
#[cfg(not(target_os = "linux"))]
fn pin() {}

/// The CPUs of a list such as `0-3,8`.
#[cfg(target_os = "linux")]
fn cpu_list(list: &str) -> Vec<usize> {
    let number = |text: &str| -> usize {
        text.parse()
            .expect("the connectors' CPUs are a list such as 4-11")
    };
    list.split(',')
        .flat_map(|part| match part.split_once('-') {
            Some((low, high)) => number(low)..=number(high),
            None => number(part)..=number(part),
        })
        .collect()
}
