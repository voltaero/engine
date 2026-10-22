use std::fmt::Debug;

use crate::Identifier;
use tracing::{error, instrument, warn};

/// Per-type storage/lease limits a mod may suggest via [`Task::limits`]. `None`
/// fields fall back to the engine default; `config.toml` `[tasks."ns:name"]`
/// entries override both. See [`crate::api::TaskSettings`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TaskLimits {
    /// Tasks per storage/lease chunk (1..=65536); for a derived type, task
    /// indexes per chunk (1..=u32::MAX).
    pub chunk_size: Option<u32>,
    /// Largest accepted payload, in bytes.
    pub max_payload: Option<u32>,
    /// Largest accepted result, in bytes.
    pub max_result: Option<u32>,
    /// Derived type: a chunk stores one base payload and an index range, not
    /// per-task payloads. Workers derive each task from `(base, idx)` and
    /// complete the chunk with one result. Set by the mod only (config can't
    /// override it); such a type only accepts `submit_derived`.
    pub derived: bool,
}

pub trait Verifiable {
    fn verify(&self, b: &[u8]) -> bool;
}
pub trait Task: Debug + Sync + Send + Verifiable {
    fn get_id(&self) -> Identifier;
    fn clone_box(&self) -> Box<dyn Task>;
    #[instrument]
    fn run_hip(&mut self) {
        warn!(
            "Task: HIP runtime not available for {}.{}, falling back to CPU",
            self.get_id().0,
            self.get_id().1
        );
        self.run_cpu();
    }
    #[instrument]
    fn run_cpu(&mut self) {
        error!(
            "Task: CPU implementation missing for {}.{}",
            self.get_id().0,
            self.get_id().1
        );
    }
    #[instrument]
    fn run(&mut self, run: Option<Runner>) {
        match run {
            Some(Runner::HIP) => self.run_hip(),
            Some(Runner::CPU) | None => self.run_cpu(),
        }
    }
    /// Storage/lease limits for this task type. The default defers everything
    /// to config and engine defaults, so existing mods need no change.
    fn limits(&self) -> TaskLimits {
        TaskLimits::default()
    }
    fn to_bytes(&self) -> Vec<u8>;
    #[allow(clippy::wrong_self_convention)]
    fn from_bytes(&self, bytes: &[u8]) -> Box<dyn Task>;
    #[allow(clippy::wrong_self_convention)]
    fn from_toml(&self, d: String) -> Box<dyn Task>;
    fn to_toml(&self) -> String;
}

#[derive(Debug, Clone, Copy)]
pub enum Runner {
    HIP,
    CPU,
}

impl Clone for Box<dyn Task> {
    fn clone(&self) -> Box<dyn Task> {
        self.clone_box()
    }
}
