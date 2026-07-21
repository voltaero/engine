//! Small statically linked task used only by the process benchmark.

use enginelib::Identifier;
use enginelib::task::{Task, Verifiable};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FibTask {
    pub iter: u64,
    pub result: u64,
}

impl Verifiable for FibTask {
    fn verify(&self, bytes: &[u8]) -> bool {
        enginelib::api::from_bytes::<Self>(bytes).is_ok()
    }
}

impl Task for FibTask {
    fn to_toml(&self) -> String {
        toml::to_string(self).unwrap_or_default()
    }

    fn from_toml(&self, data: String) -> Box<dyn Task> {
        Box::new(toml::from_str::<Self>(&data).unwrap_or_default())
    }

    fn get_id(&self) -> Identifier {
        ("engine_mod".to_string(), "fib".to_string())
    }

    fn clone_box(&self) -> Box<dyn Task> {
        Box::new(self.clone())
    }

    fn run_cpu(&mut self) {
        // Keep work negligible: this benchmark measures engine and transport
        // throughput rather than Fibonacci performance.
        let iterations = self.iter.min(16);
        let mut a = 0u64;
        let mut b = 1u64;
        for _ in 0..iterations {
            let previous = a;
            a = b;
            b = b.wrapping_add(previous);
        }
        self.result = a;
    }

    fn from_bytes(&self, bytes: &[u8]) -> Box<dyn Task> {
        Box::new(enginelib::api::from_bytes::<Self>(bytes).unwrap_or_default())
    }

    fn to_bytes(&self) -> Vec<u8> {
        enginelib::api::to_allocvec(self).unwrap_or_default()
    }
}
