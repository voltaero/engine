//! Transport layer for the engine (ZeroMQ over tcp).
//!
//! `tmq`/libzmq lives here, deliberately kept out of `enginelib` so that mods
//! (which link `enginelib`) don't inherit a libzmq build dependency. The wire
//! types are shared via [`enginelib::protocol`].

pub mod bench_task;
pub mod client;
pub mod server;
pub mod transport;
