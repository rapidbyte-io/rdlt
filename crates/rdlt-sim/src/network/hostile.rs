//! A hostile host: beside a seed's runs it attacks the connectors' serving as no host that keeps to
//! the protocol would, and checks what serving answers.

mod factories;
#[cfg(test)]
mod tests;

pub(crate) use factories::Watch;
