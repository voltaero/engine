use std::fmt::Debug;

use crate::Identifier;
use tracing::{error, instrument, warn};

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
