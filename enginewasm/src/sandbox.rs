//! The CPU path: a wasmtime sandbox around one guest module.
//!
//! What a guest can reach is exactly what this crate links: linear memory it
//! owns and the `ge.*` imports of `host.rs`, which only do something the
//! operator granted (`SandboxLimits::fs` / `net`). There is no WASI, so no
//! ambient files, clock, network or randomness. Every call runs under a
//! deadline (an epoch trap for guest code; socket timeouts for host calls),
//! every store under a memory cap, and every pointer the guest hands back is
//! bounds-checked against its own memory before the host reads it.
//!
//! Guest ABI (core wasm; `usize` = `i32` for wasm32, `i64` for wasm64):
//!
//! | export                                   | meaning                                          |
//! |------------------------------------------|--------------------------------------------------|
//! | `memory`                                 | the guest's linear memory (32- or 64-bit)        |
//! | `ge_alloc(len: usize) -> usize`          | buffer of `len` bytes; 0 = out of memory         |
//! | `ge_free(ptr: usize, len: usize)`        | optional; host calls it on output, then input    |
//! | `ge_verify(ptr, len) -> i32`             | nonzero if the bytes are a valid payload/result  |
//! | `ge_run(ptr, len, out: usize) -> i32`    | 0 and `[out_ptr, out_len]` written at `out`, or a negative error |
//!
//! `ge_verify` backs the engine's `Verifiable::verify`, which the server calls
//! on both payloads (submit) and results (complete).

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use cap_std::ambient_authority;
use cap_std::fs::Dir;
use enginelib::Identifier;
use enginelib::task::TaskLimits;
use tracing::warn;
use wasmtime::{
    Config, Engine, ExternType, InstancePre, Linker, Memory, Module, OptLevel, Store,
    StoreLimitsBuilder, Trap, TypedFunc,
};

use crate::error::{Error, Result};
use crate::host::{Grants, Host, Mount, Width, link};
use crate::meta::{ModMeta, read_sections};
pub use crate::policy::{SandboxLimits, WasmConfig};

/// Epoch tick. Deadlines are counted in ticks, so this is their resolution.
const TICK: Duration = Duration::from_millis(1);
/// Guest stack limit (wasm frames), shared by every module on the engine.
const MAX_WASM_STACK: usize = 1 << 20;

/// One process-wide engine (wasmtime's recommendation) plus the thread that
/// advances its epoch.
fn engine() -> &'static Engine {
    static ENGINE: OnceLock<Engine> = OnceLock::new();
    ENGINE.get_or_init(|| {
        let mut config = Config::new();
        config
            .epoch_interruption(true)
            .max_wasm_stack(MAX_WASM_STACK)
            .wasm_memory64(true)
            .cranelift_opt_level(OptLevel::Speed);
        let engine = Engine::new(&config).expect("wasmtime config is valid");
        let ticker = engine.clone();
        std::thread::Builder::new()
            .name("wasm-epoch".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(TICK);
                    ticker.increment_epoch();
                }
            })
            .expect("spawn wasm epoch thread");
        engine
    })
}

fn trap(err: wasmtime::Error) -> Error {
    match err.downcast_ref::<Trap>() {
        Some(Trap::Interrupt) => Error::Timeout,
        _ => Error::Trap(format!("{err:#}")),
    }
}

/// A live instance. Any error poisons it (the guest may be mid-update), so
/// callers drop it instead of returning it to the pool.
struct Session<W: Width> {
    store: Store<Host>,
    memory: Memory,
    alloc: TypedFunc<W, W>,
    free: Option<TypedFunc<(W, W), ()>>,
    verify: TypedFunc<(W, W), i32>,
    run: TypedFunc<(W, W, W), i32>,
    /// `[ptr, len]` slot `ge_run` writes its result descriptor to.
    slot: W,
    budget: Duration,
    ticks: u64,
}

impl<W: Width> Session<W> {
    fn new(m: &WasmMod) -> Result<Self> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(m.limits.max_memory)
            .memories(1)
            .tables(4)
            .table_elements(1 << 16)
            .instances(1)
            .build();
        let mut store = Store::new(
            engine(),
            Host::new(limits, m.mod_id.clone(), m.grants.clone()),
        );
        store.limiter(|host| &mut host.limits);
        store.epoch_deadline_trap();
        store.set_epoch_deadline(m.deadline_ticks);
        store.data_mut().begin_call(m.limits.call_timeout);
        let instance = m.pre.instantiate(&mut store).map_err(trap)?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| Error::Load("no exported `memory`".into()))?;
        let func = |store: &mut Store<Host>, name: &str| {
            instance
                .get_func(&mut *store, name)
                .ok_or_else(|| Error::Load(format!("missing export `{name}`")))
        };
        let typed = |e: wasmtime::Error| Error::Load(format!("bad export signature: {e:#}"));
        let alloc: TypedFunc<W, W> = func(&mut store, "ge_alloc")?.typed(&store).map_err(typed)?;
        let verify = func(&mut store, "ge_verify")?
            .typed(&store)
            .map_err(typed)?;
        let run = func(&mut store, "ge_run")?.typed(&store).map_err(typed)?;
        let free = match instance.get_func(&mut store, "ge_free") {
            Some(f) => Some(f.typed(&store).map_err(typed)?),
            None => None,
        };
        let slot_len = W::from_usize(2 * W::BYTES).expect("small");
        let slot = alloc.call(&mut store, slot_len).map_err(trap)?;
        if slot
            .to_usize()
            .is_none_or(|p| p == 0 || p + 2 * W::BYTES > memory.data_size(&store))
        {
            return Err(Error::Load("ge_alloc returned a bad result slot".into()));
        }
        store.data_mut().end_call();
        Ok(Self {
            store,
            memory,
            alloc,
            free,
            verify,
            run,
            slot,
            budget: m.limits.call_timeout,
            ticks: m.deadline_ticks,
        })
    }

    fn arm(&mut self) {
        self.store.set_epoch_deadline(self.ticks);
        self.store.data_mut().begin_call(self.budget);
    }

    fn bytes(&self, ptr: W, len: usize, what: &str) -> Result<&[u8]> {
        ptr.to_usize()
            .and_then(|start| {
                self.memory
                    .data(&self.store)
                    .get(start..start.checked_add(len)?)
            })
            .ok_or_else(|| Error::Invalid(format!("{what} out of bounds")))
    }

    fn put(&mut self, bytes: &[u8]) -> Result<(W, W)> {
        let len =
            W::from_usize(bytes.len()).ok_or_else(|| Error::Invalid("input too large".into()))?;
        let ptr = self.alloc.call(&mut self.store, len).map_err(trap)?;
        let start = ptr.to_usize().filter(|&p| p != 0);
        let dst = start
            .and_then(|start| {
                self.memory
                    .data_mut(&mut self.store)
                    .get_mut(start..start + bytes.len())
            })
            .ok_or_else(|| Error::Invalid("ge_alloc returned a bad buffer".into()))?;
        dst.copy_from_slice(bytes);
        Ok((ptr, len))
    }

    fn release(&mut self, ptr: W, len: W) -> Result<()> {
        match &self.free {
            Some(free) => free.call(&mut self.store, (ptr, len)).map_err(trap),
            None => Ok(()),
        }
    }

    fn verify(&mut self, bytes: &[u8]) -> Result<bool> {
        self.arm();
        let (ptr, len) = self.put(bytes)?;
        let ok = self
            .verify
            .call(&mut self.store, (ptr, len))
            .map_err(trap)?;
        self.release(ptr, len)?;
        self.store.data_mut().end_call();
        Ok(ok != 0)
    }

    fn run(&mut self, bytes: &[u8], max_result: u32) -> Result<Vec<u8>> {
        self.arm();
        let (ptr, len) = self.put(bytes)?;
        let status = self
            .run
            .call(&mut self.store, (ptr, len, self.slot))
            .map_err(trap)?;
        self.store.data_mut().end_call();
        if status < 0 {
            return Err(Error::Guest(status as i64));
        }
        let desc = self.bytes(self.slot, 2 * W::BYTES, "result slot")?;
        let (out_ptr, out_len) = (W::from_le(&desc[..W::BYTES]), W::from_le(&desc[W::BYTES..]));
        let n = out_len
            .to_usize()
            .filter(|&n| n <= max_result as usize)
            .ok_or_else(|| Error::Invalid(format!("result exceeds max_result {max_result}")))?;
        let out = self.bytes(out_ptr, n, "ge_run result")?.to_vec();
        // Output was allocated after input: free in reverse.
        self.release(out_ptr, out_len)?;
        self.release(ptr, len)?;
        Ok(out)
    }
}

enum AnySession {
    W32(Session<u32>),
    W64(Session<u64>),
}

impl AnySession {
    fn new(m: &WasmMod) -> Result<Self> {
        Ok(if m.wasm64 {
            AnySession::W64(Session::new(m)?)
        } else {
            AnySession::W32(Session::new(m)?)
        })
    }

    fn verify(&mut self, bytes: &[u8]) -> Result<bool> {
        match self {
            AnySession::W32(s) => s.verify(bytes),
            AnySession::W64(s) => s.verify(bytes),
        }
    }

    fn run(&mut self, bytes: &[u8], max_result: u32) -> Result<Vec<u8>> {
        match self {
            AnySession::W32(s) => s.run(bytes, max_result),
            AnySession::W64(s) => s.run(bytes, max_result),
        }
    }
}

/// A loaded, validated guest. Cheap to share: calls check an instance out of
/// a small pool (or instantiate a fresh one), so threads run in parallel.
pub struct WasmMod {
    meta: ModMeta,
    mod_id: Arc<str>,
    wgsl: Option<String>,
    limits: SandboxLimits,
    grants: Arc<Grants>,
    wasm64: bool,
    max_payload: u32,
    max_result: u32,
    deadline_ticks: u64,
    pre: InstancePre<Host>,
    pool: Mutex<Vec<AnySession>>,
}

impl std::fmt::Debug for WasmMod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmMod")
            .field("meta", &self.meta)
            .field("wasm64", &self.wasm64)
            .field("gpu", &self.wgsl.is_some())
            .finish_non_exhaustive()
    }
}

impl WasmMod {
    /// Loads a module file with the limits `config` resolves for its type.
    pub fn from_file(path: impl AsRef<std::path::Path>, config: &WasmConfig) -> Result<Arc<Self>> {
        let path = path.as_ref();
        let bytes = std::fs::read(path)
            .map_err(|e| Error::Load(format!("read {}: {e}", path.display())))?;
        Self::load(&bytes, config)
    }

    /// Like [`Self::from_bytes`], with the limits `config` resolves for the
    /// type named in the module's `ge:meta`.
    pub fn load(wasm: &[u8], config: &WasmConfig) -> Result<Arc<Self>> {
        let meta = read_sections(wasm)?.meta;
        Self::from_bytes(wasm, config.limits_for(&(meta.namespace, meta.name))?)
    }

    /// Validates metadata (and the kernel, with the `gpu` feature) without
    /// running guest code, opens the granted directories, then compiles the
    /// module and instantiates it once so a broken mod fails here rather than
    /// on its first task.
    pub fn from_bytes(wasm: &[u8], limits: SandboxLimits) -> Result<Arc<Self>> {
        let sections = read_sections(wasm)?;
        #[cfg(feature = "gpu")]
        if let (Some(gpu), Some(src)) = (&sections.meta.gpu, &sections.wgsl) {
            crate::gpu::check_kernel(src, &gpu.entry)?;
        }
        let engine = engine();
        let module =
            Module::from_binary(engine, wasm).map_err(|e| Error::Load(format!("{e:#}")))?;
        let wasm64 = match module.get_export("memory") {
            Some(ExternType::Memory(memory)) => memory.is_64(),
            _ => return Err(Error::Load("no exported `memory`".into())),
        };
        if wasm64 && !limits.memory64 {
            return Err(Error::Load(
                "wasm64 module, but memory64 is disabled for it".into(),
            ));
        }
        if !wasm64 && limits.max_memory > 4 << 30 {
            warn!("max_memory above 4 GiB has no effect on a wasm32 module");
        }
        // Fails on any import other than the `ge.*` ones `link` defines.
        let mut linker = Linker::new(engine);
        if wasm64 {
            link::<u64>(&mut linker)
        } else {
            link::<u32>(&mut linker)
        }
        .expect("define ge.* imports");
        let pre = linker
            .instantiate_pre(&module)
            .map_err(|e| Error::Load(format!("{e:#}")))?;

        let mounts = limits
            .fs
            .iter()
            .map(|grant| {
                let dir = Dir::open_ambient_dir(&grant.path, ambient_authority()).map_err(|e| {
                    Error::Load(format!(
                        "fs mount {:?} ({}): {e}",
                        grant.name,
                        grant.path.display()
                    ))
                })?;
                Ok(Mount {
                    name: grant.name.clone(),
                    dir,
                    write: grant.write,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let grants = Arc::new(Grants {
            mounts,
            net: limits.net.clone(),
            max_io: limits.max_io as usize,
        });

        let meta = sections.meta;
        let cap = |asked: Option<u32>| asked.map_or(limits.max_io, |n| n.min(limits.max_io));
        let deadline_ticks =
            (limits.call_timeout.as_nanos() / TICK.as_nanos()).clamp(1, u64::MAX as u128) as u64;
        let m = Arc::new(Self {
            mod_id: format!("{}:{}", meta.namespace, meta.name).into(),
            max_payload: cap(meta.max_payload),
            max_result: cap(meta.max_result),
            wgsl: sections.wgsl,
            meta,
            limits,
            grants,
            wasm64,
            deadline_ticks,
            pre,
            pool: Mutex::new(Vec::new()),
        });
        let first = AnySession::new(&m)?;
        m.give_back(first);
        Ok(m)
    }

    /// True for a wasm64 (memory64) module.
    pub fn is_wasm64(&self) -> bool {
        self.wasm64
    }

    pub fn meta(&self) -> &ModMeta {
        &self.meta
    }

    pub fn task_type(&self) -> Identifier {
        (self.meta.namespace.clone(), self.meta.name.clone())
    }

    pub fn wgsl(&self) -> Option<&str> {
        self.wgsl.as_deref()
    }

    pub fn limits(&self) -> &SandboxLimits {
        &self.limits
    }

    pub fn task_limits(&self) -> TaskLimits {
        TaskLimits {
            chunk_size: self.meta.chunk_size,
            max_payload: Some(self.max_payload),
            max_result: Some(self.max_result),
            derived: false,
        }
    }

    fn with_session<T>(&self, f: impl FnOnce(&mut AnySession) -> Result<T>) -> Result<T> {
        let pooled = self.pool.lock().unwrap_or_else(|p| p.into_inner()).pop();
        let mut session = match pooled {
            Some(session) => session,
            None => AnySession::new(self)?,
        };
        let out = f(&mut session)?;
        self.give_back(session);
        Ok(out)
    }

    fn give_back(&self, session: AnySession) {
        let mut pool = self.pool.lock().unwrap_or_else(|p| p.into_inner());
        if pool.len() < self.limits.pool_size {
            pool.push(session);
        }
    }

    /// The guest's verdict on a payload or result.
    pub fn verify(&self, bytes: &[u8]) -> Result<bool> {
        if bytes.len() > self.max_payload.max(self.max_result) as usize {
            return Ok(false);
        }
        self.with_session(|s| s.verify(bytes))
    }

    /// Runs one task on the CPU.
    pub fn run(&self, payload: &[u8]) -> Result<Vec<u8>> {
        if payload.len() > self.max_payload as usize {
            return Err(Error::Invalid("payload exceeds max_payload".into()));
        }
        self.with_session(|s| s.run(payload, self.max_result))
    }

    /// Runs tasks on up to `threads` CPU threads, one instance each. Results
    /// are in input order; a failing task does not fail the others.
    pub fn run_batch(&self, payloads: &[&[u8]], threads: usize) -> Vec<Result<Vec<u8>>> {
        let threads = threads.clamp(1, payloads.len().max(1));
        let per = payloads.len().div_ceil(threads).max(1);
        std::thread::scope(|scope| {
            let parts: Vec<_> = payloads
                .chunks(per)
                .map(|part| {
                    let handle =
                        scope.spawn(move || part.iter().map(|p| self.run(p)).collect::<Vec<_>>());
                    (part.len(), handle)
                })
                .collect();
            parts
                .into_iter()
                .flat_map(|(n, handle)| {
                    handle.join().unwrap_or_else(|_| {
                        warn!("wasm worker thread panicked");
                        vec![Err(Error::Trap("host thread panicked".into())); n]
                    })
                })
                .collect()
        })
    }
}
