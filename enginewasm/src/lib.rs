//! Experimental: task mods as sandboxed WebAssembly, with an optional WebGPU
//! compute kernel.
//!
//! A `.wasm` mod is untrusted code: unlike a native `.rf` mod it cannot touch
//! the `ServerAPI`, the filesystem or the network, and a hang or a crash is an
//! error for that one call. The server only uses it to verify payloads and
//! results; workers use it to run tasks on the CPU ([`WasmMod::run_batch`]) or
//! the mod's WGSL kernel on the GPU ([`gpu::Gpu::run_batch`]), which must
//! produce identical bytes. Spec: `docs/wasm.md`.

pub mod error;
#[cfg(feature = "gpu")]
pub mod gpu;
pub mod meta;
pub mod sandbox;

use std::path::Path;
use std::sync::Arc;

use enginelib::api::ServerAPI;
use enginelib::task::{Task, TaskLimits, Verifiable};
use enginelib::{Identifier, Registry};
use tracing::{error, info, warn};

pub use error::{Error, Result};
pub use sandbox::{SandboxLimits, WasmMod};

/// Adapts a [`WasmMod`] to the engine's [`Task`] registry.
#[derive(Debug, Clone)]
pub struct WasmTask(pub Arc<WasmMod>);

impl Verifiable for WasmTask {
    fn verify(&self, bytes: &[u8]) -> bool {
        self.0.verify(bytes).unwrap_or_else(|err| {
            warn!("wasm verify of {:?} failed: {err}", self.0.task_type());
            false
        })
    }
}

impl Task for WasmTask {
    fn get_id(&self) -> Identifier {
        self.0.task_type()
    }
    fn clone_box(&self) -> Box<dyn Task> {
        Box::new(self.clone())
    }
    fn limits(&self) -> TaskLimits {
        self.0.task_limits()
    }
    fn to_bytes(&self) -> Vec<u8> {
        Vec::new()
    }
    fn from_bytes(&self, _bytes: &[u8]) -> Box<dyn Task> {
        self.clone_box()
    }
    fn from_toml(&self, _d: String) -> Box<dyn Task> {
        self.clone_box()
    }
    fn to_toml(&self) -> String {
        String::new()
    }
}

/// Registers `m`'s task type. Refuses to replace an already registered type,
/// so a wasm mod can't take over a native mod's (or another wasm mod's) type.
pub fn register(api: &mut ServerAPI, m: Arc<WasmMod>) -> Result<()> {
    let task_type = m.task_type();
    if api.task_registry.tasks.contains_key(&task_type) {
        return Err(Error::Load(format!(
            "task type {task_type:?} is already registered"
        )));
    }
    api.task_registry.register(Arc::new(WasmTask(m)), task_type);
    Ok(())
}

/// Loads and registers every `*.wasm` in `dir`. Call before
/// `ServerAPI::populate`. A mod that fails to load is logged and skipped.
pub fn load_mods(api: &mut ServerAPI, dir: impl AsRef<Path>, limits: SandboxLimits) -> usize {
    let dir = dir.as_ref();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) => {
            info!("No wasm mods loaded from {}: {err}", dir.display());
            return 0;
        }
    };
    let mut paths: Vec<_> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.is_file() && p.extension().is_some_and(|ext| ext == "wasm"))
        .collect();
    paths.sort();
    let mut loaded = 0;
    for path in paths {
        match WasmMod::from_file(&path, limits).and_then(|m| {
            let gpu = m.wgsl().is_some();
            register(api, m.clone()).map(|()| (m, gpu))
        }) {
            Ok((m, gpu)) => {
                loaded += 1;
                info!(
                    "Loaded wasm mod {} -> {:?} (gpu kernel: {gpu})",
                    path.display(),
                    m.task_type()
                );
            }
            Err(err) => error!("Rejected wasm mod {}: {err}", path.display()),
        }
    }
    loaded
}
