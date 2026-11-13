#![forbid(unsafe_code)]
//! The connector as its own binary, which a host finds by this name and spawns.

fn main() -> std::process::ExitCode {
    rdlt_connector::serve::<pokeapi::PokeApi>()
}
