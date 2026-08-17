//! The clause families, one module each: [`s`] and [`d`] certify a
//! source or destination end to end (reusing the testkit's S/D suites
//! and orchestrating the protocol clauses), [`p`] owns every protocol
//! clause's probe and verdict, [`k`] the SIGKILL matrix.

pub mod d;
pub mod k;
pub mod p;
pub mod s;
