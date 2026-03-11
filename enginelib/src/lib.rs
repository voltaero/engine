use std::sync::Arc;
pub mod api;
pub mod config;
pub mod event;
pub mod events;
extern crate self as enginelib;
pub mod plugin;
pub mod prelude;
pub mod task;
pub type Identifier = (String, String);
pub type RawIdentier = String;
pub const GIT_VERSION: &str = env!("CARGO_PKG_VERSION"); //get commit hash
pub const RUSTC_VERSION: &str = env!("VERGEN_RUSTC_SEMVER");
pub trait Registry<T: ?Sized>: Default + Clone {
    fn register(&mut self, registree: Arc<T>, identifier: Identifier);
    fn get(&self, identifier: &Identifier) -> Option<Box<T>>;
}
pub use chrono;
