//! # rdlt-connector-reference — the connector contract's own connector
//!
//! ONE jsonl file in, jsonl parts plus commit receipts out: the
//! smallest complete connector the sdk can express, run as a PROCESS
//! (the `rdlt-connector-reference` binary serves either role over the
//! wire) and certified by the same conformance kits and wire clauses
//! every shipping connector answers to. It exists so the engine's gates
//! ride a connector the contract itself owns, and so a third party
//! writing a connector has a complete worked example to copy — every
//! discipline the sdk asks for is here, and nothing else is.
//!
//! The modules are the map, one directory per role: [`source`] reads
//! the file (`config` the document, `cursor` the persisted byte-offset
//! cursor and its resume law, `connector` the sdk `SourceConnector`);
//! [`destination`] writes the directory (`config`, `connector` + the
//! session lease, `session` the sdk `Backend` choreography, `store` the
//! on-disk formats and fsync discipline, `part` the part-file naming).
//! In-process construction goes through the sdk shell and is for
//! `serve` and this crate's own tests; every name is reached by its
//! module path.

pub mod destination;
pub mod source;
