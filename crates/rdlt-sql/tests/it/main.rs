//! rdlt-sql against its bounds, DataFusion's executor, the `T` clauses and the engine.

#![forbid(unsafe_code)]

mod arithmetic;
mod bounds;
mod certified;
mod composed;
mod differential;
mod engine;
mod queries;
mod served;
