//! The CPU path: a wasmtime sandbox around one guest module.
//!
//! What a guest can reach is exactly what this file links: linear memory it
//! owns and the `ge.log` import. There is no WASI, so no files, clock, network
//! or randomness. Every call runs under an epoch deadline (a trap, not a hang),
//! every store under a memory cap, and every pointer the guest hands back is
//! bounds-checked against its own memory before the host reads it.
//!
//! Guest ABI (core wasm, `wasm32`):
//!
//! | export                         | meaning                                         |
//! |--------------------------------|-------------------------------------------------|
//! | `memory`                       | the guest's linear memory                       |
//! | `ge_alloc(len: i32) -> i32`    | buffer of `len` bytes the host writes input to  |
//! | `ge_free(ptr: i32, len: i32)`  | optional; host calls it on input and output     |
//! | `ge_verify(ptr, len) -> i32`   | nonzero if the bytes are a valid payload/result |
//! | `ge_run(ptr, len) -> i64`      | `(out_ptr << 32) \| out_len`, or negative error |
//!
//! `ge_verify` backs the engine's `Verifiable::verify`, which the server calls
//! on both payloads (submit) and results (complete).

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use enginelib::Identifier;
use enginelib::task::TaskLimits;
use tracing::{info, warn};
use wasmtime::{
    Caller, Config, Engine, InstancePre, Linker, Memory, Module, OptLevel, Store, StoreLimits,
    StoreLimitsBuilder, Trap, TypedFunc,
};

use crate::error::{Error, Result};
use crate::meta::{ModMeta, read_sections};

/// Epoch tick. Deadlines are counted in ticks, so this is their resolution.
const TICK: Duration = Duration::from_millis(1);
/// Guest stack limit (wasm frames), shared by every module on the engine.
const MAX_WASM_STACK: usize = 1 << 20;
const MAX_LOG_BYTES: usize = 1024;
const MAX_LOGS_PER_CALL: u32 = 16;

/// Host-side caps. The mod can ask for less (its `ge:meta`), never for more.
#[derive(Debug, Clone, Copy)]
pub struct SandboxLimits {
    /// Linear memory per instance, in bytes.
    pub max_memory: usize,
    /// Wall-clock budget of one `verify`/`run` call.
    pub call_timeout: Duration,
    /// Largest payload or result, in bytes.
    pub max_io: u32,
    /// Idle instances kept per mod for reuse.
    pub pool_size: usize,
    /// Budget of one GPU batch (submit to readback).
    pub gpu_timeout: Duration,
}

impl Default for SandboxLimits {
    fn default() -> Self {
        Self {
            max_memory: 256 << 20,
            call_timeout: Duration::from_secs(2),
            max_io: 16 << 20,
            pool_size: std::thread::available_parallelism().map_or(4, |n| n.get()),
            gpu_timeout: Duration::from_secs(10),
        }
    }
}

/// One process-wide engine (wasmtime's recommendation) plus the thread that
/// advances its epoch.
fn engine() -> &'static Engine {
    static ENGINE: OnceLock<Engine> = OnceLock::new();
    ENGINE.get_or_init(|| {
        let mut config = Config::new();
        config
            .epoch_interruption(true)
            .max_wasm_stack(MAX_WASM_STACK)
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

struct Host {
    limits: StoreLimits,
    logs: u32,
    mod_id: Arc<str>,
}

fn linker(engine: &Engine) -> Linker<Host> {
    let mut linker = Linker::new(engine);
    linker
        .func_wrap(
            "ge",
            "log",
            |mut caller: Caller<'_, Host>, ptr: u32, len: u32| {
                if caller.data().logs >= MAX_LOGS_PER_CALL {
                    return;
                }
                caller.data_mut().logs += 1;
                let Some(memory) = caller.get_export("memory").and_then(|e| e.into_memory()) else {
                    return;
                };
                let start = ptr as usize;
                let end = start.saturating_add((len as usize).min(MAX_LOG_BYTES));
                if let Some(bytes) = memory.data(&caller).get(start..end) {
                    let line = String::from_utf8_lossy(bytes).into_owned();
                    info!(target: "wasm_mod", mod_id = %caller.data().mod_id, "{line}");
                }
            },
        )
        .expect("define ge.log");
    linker
}

fn trap(err: wasmtime::Error) -> Error {
    match err.downcast_ref::<Trap>() {
        Some(Trap::Interrupt) => Error::Timeout,
        _ => Error::Trap(format!("{err:#}")),
    }
}

/// A live instance. Any error poisons it (the guest may be mid-update), so
/// callers drop it instead of returning it to the pool.
struct Session {
    store: Store<Host>,
    memory: Memory,
    alloc: TypedFunc<u32, u32>,
    free: Option<TypedFunc<(u32, u32), ()>>,
    verify: TypedFunc<(u32, u32), u32>,
    run: TypedFunc<(u32, u32), i64>,
}

impl Session {
    fn new(m: &WasmMod) -> Result<Self> {
        let host = Host {
            limits: StoreLimitsBuilder::new()
                .memory_size(m.limits.max_memory)
                .memories(1)
                .tables(4)
                .table_elements(1 << 16)
                .instances(1)
                .build(),
            logs: 0,
            mod_id: m.mod_id.clone(),
        };
        let mut store = Store::new(engine(), host);
        store.limiter(|host| &mut host.limits);
        store.epoch_deadline_trap();
        store.set_epoch_deadline(m.deadline_ticks);
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
        let alloc = func(&mut store, "ge_alloc")?.typed(&store).map_err(typed)?;
        let verify = func(&mut store, "ge_verify")?
            .typed(&store)
            .map_err(typed)?;
        let run = func(&mut store, "ge_run")?.typed(&store).map_err(typed)?;
        let free = match instance.get_func(&mut store, "ge_free") {
            Some(f) => Some(f.typed(&store).map_err(typed)?),
            None => None,
        };
        Ok(Self {
            store,
            memory,
            alloc,
            free,
            verify,
            run,
        })
    }

    fn arm(&mut self, ticks: u64) {
        self.store.set_epoch_deadline(ticks);
        self.store.data_mut().logs = 0;
    }

    fn put(&mut self, bytes: &[u8]) -> Result<u32> {
        let len =
            u32::try_from(bytes.len()).map_err(|_| Error::Invalid("input too large".into()))?;
        let ptr = self.alloc.call(&mut self.store, len).map_err(trap)?;
        let start = ptr as usize;
        let dst = self
            .memory
            .data_mut(&mut self.store)
            .get_mut(start..start + bytes.len())
            .ok_or_else(|| Error::Invalid("ge_alloc returned an out-of-bounds buffer".into()))?;
        dst.copy_from_slice(bytes);
        Ok(ptr)
    }

    fn release(&mut self, ptr: u32, len: u32) -> Result<()> {
        match &self.free {
            Some(free) => free.call(&mut self.store, (ptr, len)).map_err(trap),
            None => Ok(()),
        }
    }

    fn verify(&mut self, bytes: &[u8], ticks: u64) -> Result<bool> {
        self.arm(ticks);
        let ptr = self.put(bytes)?;
        let len = bytes.len() as u32;
        let ok = self
            .verify
            .call(&mut self.store, (ptr, len))
            .map_err(trap)?;
        self.release(ptr, len)?;
        Ok(ok != 0)
    }

    fn run(&mut self, bytes: &[u8], ticks: u64, max_result: u32) -> Result<Vec<u8>> {
        self.arm(ticks);
        let ptr = self.put(bytes)?;
        let len = bytes.len() as u32;
        let packed = self.run.call(&mut self.store, (ptr, len)).map_err(trap)?;
        if packed < 0 {
            return Err(Error::Guest(packed));
        }
        let (out_ptr, out_len) = ((packed as u64 >> 32) as u32, packed as u32);
        if out_len > max_result {
            return Err(Error::Invalid(format!(
                "result of {out_len} bytes exceeds max_result {max_result}"
            )));
        }
        let start = out_ptr as usize;
        let out = self
            .memory
            .data(&self.store)
            .get(start..start + out_len as usize)
            .ok_or_else(|| Error::Invalid("ge_run returned an out-of-bounds result".into()))?
            .to_vec();
        // Output was allocated after input: free in reverse.
        self.release(out_ptr, out_len)?;
        self.release(ptr, len)?;
        Ok(out)
    }
}

/// A loaded, validated guest. Cheap to share: calls check an instance out of
/// a small pool (or instantiate a fresh one), so threads run in parallel.
pub struct WasmMod {
    meta: ModMeta,
    mod_id: Arc<str>,
    wgsl: Option<String>,
    limits: SandboxLimits,
    max_payload: u32,
    max_result: u32,
    deadline_ticks: u64,
    pre: InstancePre<Host>,
    pool: Mutex<Vec<Session>>,
}

impl std::fmt::Debug for WasmMod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmMod")
            .field("meta", &self.meta)
            .field("gpu", &self.wgsl.is_some())
            .finish_non_exhaustive()
    }
}

impl WasmMod {
    pub fn from_file(
        path: impl AsRef<std::path::Path>,
        limits: SandboxLimits,
    ) -> Result<Arc<Self>> {
        let path = path.as_ref();
        let bytes = std::fs::read(path)
            .map_err(|e| Error::Load(format!("read {}: {e}", path.display())))?;
        Self::from_bytes(&bytes, limits)
    }

    /// Validates metadata (and the kernel, with the `gpu` feature) without
    /// running guest code, then compiles the module and instantiates it once
    /// so a broken mod fails here rather than on its first task.
    pub fn from_bytes(wasm: &[u8], limits: SandboxLimits) -> Result<Arc<Self>> {
        let sections = read_sections(wasm)?;
        #[cfg(feature = "gpu")]
        if let (Some(gpu), Some(src)) = (&sections.meta.gpu, &sections.wgsl) {
            crate::gpu::check_kernel(src, &gpu.entry)?;
        }
        let engine = engine();
        let module =
            Module::from_binary(engine, wasm).map_err(|e| Error::Load(format!("{e:#}")))?;
        // Fails on any import other than the ones `linker` defines.
        let pre = linker(engine)
            .instantiate_pre(&module)
            .map_err(|e| Error::Load(format!("{e:#}")))?;
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
            deadline_ticks,
            pre,
            pool: Mutex::new(Vec::new()),
        });
        let first = Session::new(&m)?;
        m.give_back(first);
        Ok(m)
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

    fn with_session<T>(&self, f: impl FnOnce(&mut Session) -> Result<T>) -> Result<T> {
        let pooled = self.pool.lock().unwrap_or_else(|p| p.into_inner()).pop();
        let mut session = match pooled {
            Some(session) => session,
            None => Session::new(self)?,
        };
        let out = f(&mut session)?;
        self.give_back(session);
        Ok(out)
    }

    fn give_back(&self, session: Session) {
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
        self.with_session(|s| s.verify(bytes, self.deadline_ticks))
    }

    /// Runs one task on the CPU.
    pub fn run(&self, payload: &[u8]) -> Result<Vec<u8>> {
        if payload.len() > self.max_payload as usize {
            return Err(Error::Invalid("payload exceeds max_payload".into()));
        }
        self.with_session(|s| s.run(payload, self.deadline_ticks, self.max_result))
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
