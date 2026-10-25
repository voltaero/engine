//! What the operator lets a mod do: resource limits and capability grants.
//!
//! Everything is denied by default except computing over the bytes handed in.
//! `config.toml` widens or narrows that, globally and per task type:
//!
//! ```toml
//! [wasm]                          # defaults for every wasm mod
//! max_memory_mb = 256
//! call_timeout_ms = 2000
//!
//! [wasm.mods."demo:mix"]          # overrides for one task type
//! max_memory_mb = 8192            # > 4 GiB needs a wasm64 module
//! fs = [{ name = "data", path = "/srv/data" }, { name = "out", path = "/srv/out", write = true }]
//! net = ["api.example.com:443", "10.0.0.5:9000"]
//! ```
//!
//! A per-mod table replaces individual keys of the defaults (lists are
//! replaced, not merged).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use enginelib::Identifier;
use serde::Deserialize;

use crate::error::{Error, Result};

/// Resolved limits and grants of one mod. The mod can ask for less (its
/// `ge:meta`), never for more.
#[derive(Debug, Clone)]
pub struct SandboxLimits {
    /// Linear memory per instance, in bytes.
    pub max_memory: usize,
    /// Wall-clock budget of one `verify`/`run` call, host calls included.
    pub call_timeout: Duration,
    /// Largest payload, result, file or network read, in bytes.
    pub max_io: u32,
    /// Idle instances kept per mod for reuse.
    pub pool_size: usize,
    /// Budget of one GPU batch (submit to readback).
    pub gpu_timeout: Duration,
    /// Accept wasm64 (memory64) modules.
    pub memory64: bool,
    /// Directories the mod may reach through `ge.fs_*`.
    pub fs: Vec<FsGrant>,
    /// `host:port` endpoints the mod may connect to through `ge.net_*`.
    pub net: Vec<String>,
}

impl Default for SandboxLimits {
    fn default() -> Self {
        Self {
            max_memory: 256 << 20,
            call_timeout: Duration::from_secs(2),
            max_io: 16 << 20,
            pool_size: std::thread::available_parallelism().map_or(4, |n| n.get()),
            gpu_timeout: Duration::from_secs(10),
            memory64: true,
            fs: Vec::new(),
            net: Vec::new(),
        }
    }
}

/// One mounted directory. The guest addresses files as `<name>/<relative path>`;
/// nothing outside `path` is reachable (no `..`, absolute paths or symlink
/// escapes: access goes through a `cap-std` directory handle).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FsGrant {
    pub name: String,
    pub path: PathBuf,
    /// Allow `ge.fs_write` (create/overwrite files). Read-only otherwise.
    #[serde(default)]
    pub write: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyToml {
    max_memory_mb: Option<u64>,
    call_timeout_ms: Option<u64>,
    max_io_mb: Option<u32>,
    pool_size: Option<usize>,
    gpu_timeout_ms: Option<u64>,
    memory64: Option<bool>,
    fs: Option<Vec<FsGrant>>,
    net: Option<Vec<String>>,
}

impl PolicyToml {
    fn apply(&self, limits: &mut SandboxLimits) -> Result<()> {
        if let Some(mb) = self.max_memory_mb {
            let bytes = mb
                .checked_mul(1 << 20)
                .and_then(|b| usize::try_from(b).ok())
                .filter(|&b| b > 0)
                .ok_or_else(|| Error::Load(format!("max_memory_mb {mb} out of range")))?;
            limits.max_memory = bytes;
        }
        if let Some(ms) = self.call_timeout_ms {
            limits.call_timeout = Duration::from_millis(ms.max(1));
        }
        if let Some(mb) = self.max_io_mb {
            limits.max_io = mb
                .checked_mul(1 << 20)
                .filter(|&b| b > 0)
                .ok_or_else(|| Error::Load(format!("max_io_mb {mb} must be 1..=4095")))?;
        }
        if let Some(n) = self.pool_size {
            limits.pool_size = n;
        }
        if let Some(ms) = self.gpu_timeout_ms {
            limits.gpu_timeout = Duration::from_millis(ms.max(1));
        }
        if let Some(on) = self.memory64 {
            limits.memory64 = on;
        }
        if let Some(fs) = &self.fs {
            for grant in fs {
                let ok = !grant.name.is_empty()
                    && grant
                        .name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
                if !ok {
                    return Err(Error::Load(format!(
                        "fs mount name {:?} must be [A-Za-z0-9_-]+",
                        grant.name
                    )));
                }
            }
            limits.fs = fs.clone();
        }
        if let Some(net) = &self.net {
            for entry in net {
                if entry
                    .rsplit_once(':')
                    .is_none_or(|(h, p)| h.is_empty() || p.parse::<u16>().is_err())
                {
                    return Err(Error::Load(format!(
                        "net entry {entry:?} must be host:port"
                    )));
                }
            }
            limits.net = net.iter().map(|e| e.to_ascii_lowercase()).collect();
        }
        Ok(())
    }
}

/// The `[wasm]` table of `config.toml`.
#[derive(Debug, Clone, Default)]
pub struct WasmConfig {
    defaults: PolicyToml,
    mods: BTreeMap<String, PolicyToml>,
}

impl WasmConfig {
    /// Reads `[wasm]` from a config file. A missing file means defaults; a file
    /// that exists but doesn't parse is an error (fail closed, like the
    /// engine's own config).
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        match std::fs::read_to_string(path) {
            Ok(text) => Self::from_toml(&text),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(Error::Load(format!("read {}: {e}", path.display()))),
        }
    }

    /// Parses a whole config file and keeps its `[wasm]` table.
    pub fn from_toml(text: &str) -> Result<Self> {
        let mut root: toml::Table =
            toml::from_str(text).map_err(|e| Error::Load(format!("config: {e}")))?;
        let Some(wasm) = root.remove("wasm") else {
            return Ok(Self::default());
        };
        let toml::Value::Table(mut wasm) = wasm else {
            return Err(Error::Load("config: [wasm] must be a table".into()));
        };
        let mods = match wasm.remove("mods") {
            Some(mods) => mods
                .try_into()
                .map_err(|e| Error::Load(format!("config: [wasm.mods]: {e}")))?,
            None => BTreeMap::new(),
        };
        let defaults = toml::Value::Table(wasm)
            .try_into()
            .map_err(|e| Error::Load(format!("config: [wasm]: {e}")))?;
        let cfg = Self { defaults, mods };
        // Surface bad values at startup, not when the first mod loads.
        cfg.limits_for(&(String::new(), String::new()))?;
        for key in cfg.mods.keys() {
            let (ns, name) = key.split_once(':').ok_or_else(|| {
                Error::Load(format!("config: [wasm.mods.{key:?}] must be \"ns:name\""))
            })?;
            cfg.limits_for(&(ns.into(), name.into()))?;
        }
        Ok(cfg)
    }

    /// Engine defaults, then `[wasm]`, then `[wasm.mods."ns:name"]`.
    pub fn limits_for(&self, task_type: &Identifier) -> Result<SandboxLimits> {
        let mut limits = SandboxLimits::default();
        self.defaults.apply(&mut limits)?;
        if let Some(over) = self.mods.get(&format!("{}:{}", task_type.0, task_type.1)) {
            over.apply(&mut limits)?;
        }
        Ok(limits)
    }
}
