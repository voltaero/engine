//! Host functions a guest may import (module `ge`), for both pointer widths.
//!
//! Every function is always linked, so a module that imports one loads
//! everywhere; what it may *do* is decided per call by the mod's grants
//! (`SandboxLimits::fs` / `net`). `ptr`/`len` are `i32` in a wasm32 module and
//! `i64` in a wasm64 one; statuses and handles are always `i32`, byte counts
//! `i64`. Negative results are the [`code`]s below.
//!
//! | import                                         | does                                                      |
//! |------------------------------------------------|-----------------------------------------------------------|
//! | `log(ptr, len)`                                | one line to `tracing` (16 lines of 1 KiB per call)        |
//! | `fs_read(path_ptr, path_len, out) -> i32`      | reads `<mount>/<path>` into a `ge_alloc` buffer; `out` gets `[ptr, len]` |
//! | `fs_write(path_ptr, path_len, ptr, len) -> i32`| creates/overwrites `<mount>/<path>` (writable mounts only) |
//! | `net_connect(addr_ptr, addr_len) -> i32`       | TCP connect to an allowlisted `host:port`; returns a handle |
//! | `net_send(h, ptr, len) -> i64`                 | bytes written                                             |
//! | `net_recv(h, ptr, len) -> i64`                 | bytes read into `ptr` (0 = peer closed)                   |
//! | `net_close(h) -> i32`                          | closes the handle                                         |
//!
//! Host calls count against the call's deadline: sockets get the remaining
//! budget as their timeouts, and all sockets are closed when the call ends.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cap_std::fs::Dir;
use tracing::info;
use wasmtime::{Caller, Extern, Linker, Memory, StoreLimits, WasmTy};

/// Negative statuses returned to the guest.
pub mod code {
    /// The capability isn't granted (or the path/address isn't allowed).
    pub const DENIED: i32 = -1;
    /// Bad pointer, length, UTF-8 or argument.
    pub const INVALID: i32 = -2;
    /// The file or connection failed.
    pub const IO: i32 = -3;
    /// Over `max_io`, or too many open sockets.
    pub const TOO_BIG: i32 = -4;
    /// The call's deadline passed.
    pub const TIMEOUT: i32 = -5;
    /// Unknown or closed handle.
    pub const BAD_HANDLE: i32 = -6;
    /// `ge_alloc` returned 0.
    pub const NO_MEMORY: i32 = -7;
}
use code::*;

const MAX_LOG_BYTES: usize = 1024;
const MAX_LOGS_PER_CALL: u32 = 16;
const MAX_SOCKETS: usize = 16;

/// A guest pointer width: `u32` (wasm32) or `u64` (wasm64).
pub trait Width: WasmTy + Copy + Send + Sync + 'static {
    const BYTES: usize;
    fn to_usize(self) -> Option<usize>;
    fn from_usize(n: usize) -> Option<Self>;
    fn to_le(self) -> Vec<u8>;
    fn from_le(bytes: &[u8]) -> Self;
}

impl Width for u32 {
    const BYTES: usize = 4;
    fn to_usize(self) -> Option<usize> {
        usize::try_from(self).ok()
    }
    fn from_usize(n: usize) -> Option<Self> {
        u32::try_from(n).ok()
    }
    fn to_le(self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
    fn from_le(bytes: &[u8]) -> Self {
        u32::from_le_bytes(bytes.try_into().expect("4 bytes"))
    }
}

impl Width for u64 {
    const BYTES: usize = 8;
    fn to_usize(self) -> Option<usize> {
        usize::try_from(self).ok()
    }
    fn from_usize(n: usize) -> Option<Self> {
        u64::try_from(n).ok()
    }
    fn to_le(self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
    fn from_le(bytes: &[u8]) -> Self {
        u64::from_le_bytes(bytes.try_into().expect("8 bytes"))
    }
}

pub(crate) struct Mount {
    pub name: String,
    pub dir: Dir,
    pub write: bool,
}

/// Capabilities opened once per mod and shared by its instances.
pub(crate) struct Grants {
    pub mounts: Vec<Mount>,
    pub net: Vec<String>,
    pub max_io: usize,
}

pub(crate) struct Host {
    pub limits: StoreLimits,
    pub mod_id: Arc<str>,
    pub grants: Arc<Grants>,
    logs: u32,
    deadline: Instant,
    sockets: Vec<Option<TcpStream>>,
}

impl Host {
    pub fn new(limits: StoreLimits, mod_id: Arc<str>, grants: Arc<Grants>) -> Self {
        Self {
            limits,
            mod_id,
            grants,
            logs: 0,
            deadline: Instant::now(),
            sockets: Vec::new(),
        }
    }

    pub fn begin_call(&mut self, budget: Duration) {
        self.logs = 0;
        self.deadline = Instant::now() + budget;
        self.sockets.clear();
    }

    pub fn end_call(&mut self) {
        self.sockets.clear();
    }

    fn remaining(&self) -> Result<Duration, i32> {
        let left = self.deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            Err(TIMEOUT)
        } else {
            Ok(left)
        }
    }

    fn socket(&mut self, handle: i32) -> Result<&mut TcpStream, i32> {
        usize::try_from(handle)
            .ok()
            .and_then(|h| self.sockets.get_mut(h))
            .and_then(Option::as_mut)
            .ok_or(BAD_HANDLE)
    }
}

fn memory(caller: &mut Caller<'_, Host>) -> Result<Memory, i32> {
    caller
        .get_export("memory")
        .and_then(Extern::into_memory)
        .ok_or(INVALID)
}

fn range<W: Width>(ptr: W, len: W) -> Result<(usize, usize), i32> {
    let start = ptr.to_usize().ok_or(INVALID)?;
    let len = len.to_usize().ok_or(INVALID)?;
    let end = start.checked_add(len).ok_or(INVALID)?;
    Ok((start, end))
}

fn read_guest<W: Width>(
    caller: &mut Caller<'_, Host>,
    ptr: W,
    len: W,
    max: usize,
) -> Result<Vec<u8>, i32> {
    let mem = memory(caller)?;
    let (start, end) = range(ptr, len)?;
    if end - start > max {
        return Err(TOO_BIG);
    }
    mem.data(&caller)
        .get(start..end)
        .map(<[u8]>::to_vec)
        .ok_or(INVALID)
}

fn write_guest(caller: &mut Caller<'_, Host>, at: usize, bytes: &[u8]) -> Result<(), i32> {
    let mem = memory(caller)?;
    let end = at.checked_add(bytes.len()).ok_or(INVALID)?;
    mem.data_mut(caller)
        .get_mut(at..end)
        .ok_or(INVALID)?
        .copy_from_slice(bytes);
    Ok(())
}

fn read_str<W: Width>(caller: &mut Caller<'_, Host>, ptr: W, len: W) -> Result<String, i32> {
    String::from_utf8(read_guest(caller, ptr, len, 4096)?).map_err(|_| INVALID)
}

/// Copies `bytes` into a fresh `ge_alloc` buffer (re-entering the guest) and
/// writes `[ptr, len]` to `slot`.
fn hand_out<W: Width>(
    caller: &mut Caller<'_, Host>,
    slot: W,
    bytes: &[u8],
) -> wasmtime::Result<Result<(), i32>> {
    let Some(len) = W::from_usize(bytes.len()) else {
        return Ok(Err(TOO_BIG));
    };
    let Some(alloc) = caller.get_export("ge_alloc").and_then(Extern::into_func) else {
        return Ok(Err(INVALID));
    };
    let ptr = alloc.typed::<W, W>(&caller)?.call(&mut *caller, len)?;
    let Some(at) = ptr.to_usize().filter(|&p| p != 0) else {
        return Ok(Err(NO_MEMORY));
    };
    let mut desc = ptr.to_le();
    desc.extend(len.to_le());
    Ok((|| {
        write_guest(caller, at, bytes)?;
        write_guest(caller, slot.to_usize().ok_or(INVALID)?, &desc)
    })())
}

/// Splits `<mount>/<relative path>` and checks it stays inside the mount.
fn resolve<'g>(grants: &'g Grants, path: &str) -> Result<(&'g Mount, String), i32> {
    let (name, rel) = path.split_once('/').ok_or(INVALID)?;
    let mount = grants
        .mounts
        .iter()
        .find(|m| m.name == name)
        .ok_or(DENIED)?;
    let rel_path = std::path::Path::new(rel);
    let inside = !rel.is_empty()
        && rel_path
            .components()
            .all(|c| matches!(c, std::path::Component::Normal(_)));
    if !inside {
        return Err(DENIED);
    }
    Ok((mount, rel.to_owned()))
}

fn fs_read<W: Width>(
    caller: &mut Caller<'_, Host>,
    path_ptr: W,
    path_len: W,
    slot: W,
) -> wasmtime::Result<i32> {
    let read = (|| {
        let path = read_str(caller, path_ptr, path_len)?;
        caller.data().remaining()?;
        let grants = caller.data().grants.clone();
        let (mount, rel) = resolve(&grants, &path)?;
        let file = mount.dir.open(&rel).map_err(|_| IO)?;
        let mut bytes = Vec::new();
        file.take(grants.max_io as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| IO)?;
        if bytes.len() > grants.max_io {
            return Err(TOO_BIG);
        }
        Ok(bytes)
    })();
    match read {
        Ok(bytes) => Ok(hand_out(caller, slot, &bytes)?.map_or_else(|e| e, |()| 0)),
        Err(code) => Ok(code),
    }
}

fn fs_write<W: Width>(
    caller: &mut Caller<'_, Host>,
    path_ptr: W,
    path_len: W,
    ptr: W,
    len: W,
) -> i32 {
    (|| {
        let path = read_str(caller, path_ptr, path_len)?;
        caller.data().remaining()?;
        let grants = caller.data().grants.clone();
        let (mount, rel) = resolve(&grants, &path)?;
        if !mount.write {
            return Err(DENIED);
        }
        let bytes = read_guest(caller, ptr, len, grants.max_io)?;
        mount.dir.write(&rel, bytes).map_err(|_| IO)?;
        Ok(0)
    })()
    .unwrap_or_else(|e: i32| e)
}

fn net_connect<W: Width>(caller: &mut Caller<'_, Host>, ptr: W, len: W) -> i32 {
    (|| {
        let addr = read_str(caller, ptr, len)?.to_ascii_lowercase();
        let host = caller.data_mut();
        if !host.grants.net.contains(&addr) {
            return Err(DENIED);
        }
        if host.sockets.iter().flatten().count() >= MAX_SOCKETS {
            return Err(TOO_BIG);
        }
        let budget = host.remaining()?;
        let stream = addr
            .to_socket_addrs()
            .map_err(|_| IO)?
            .find_map(|a| TcpStream::connect_timeout(&a, budget).ok())
            .ok_or(IO)?;
        let _ = stream.set_nodelay(true);
        let handle = match host.sockets.iter().position(Option::is_none) {
            Some(free) => {
                host.sockets[free] = Some(stream);
                free
            }
            None => {
                host.sockets.push(Some(stream));
                host.sockets.len() - 1
            }
        };
        Ok(handle as i32)
    })()
    .unwrap_or_else(|e: i32| e)
}

fn net_send<W: Width>(caller: &mut Caller<'_, Host>, handle: i32, ptr: W, len: W) -> i64 {
    (|| {
        let max = caller.data().grants.max_io;
        let bytes = read_guest(caller, ptr, len, max)?;
        let host = caller.data_mut();
        let budget = host.remaining()?;
        let socket = host.socket(handle)?;
        socket.set_write_timeout(Some(budget)).map_err(|_| IO)?;
        let n = socket.write(&bytes).map_err(|e| io_code(&e))?;
        Ok(n as i64)
    })()
    .unwrap_or_else(|e: i32| e as i64)
}

fn net_recv<W: Width>(caller: &mut Caller<'_, Host>, handle: i32, ptr: W, len: W) -> i64 {
    (|| {
        let (start, end) = range(ptr, len)?;
        let want = (end - start).min(caller.data().grants.max_io);
        // Check the destination before reading, so no received bytes are lost.
        if memory(caller)?.data(&caller).len() < start + want {
            return Err(INVALID);
        }
        let host = caller.data_mut();
        let budget = host.remaining()?;
        let socket = host.socket(handle)?;
        socket.set_read_timeout(Some(budget)).map_err(|_| IO)?;
        let mut buf = vec![0u8; want];
        let n = socket.read(&mut buf).map_err(|e| io_code(&e))?;
        write_guest(caller, start, &buf[..n])?;
        Ok(n as i64)
    })()
    .unwrap_or_else(|e: i32| e as i64)
}

fn io_code(err: &std::io::Error) -> i32 {
    match err.kind() {
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock => TIMEOUT,
        _ => IO,
    }
}

fn log<W: Width>(caller: &mut Caller<'_, Host>, ptr: W, len: W) {
    let host = caller.data_mut();
    if host.logs >= MAX_LOGS_PER_CALL {
        return;
    }
    host.logs += 1;
    let Some(len) = len.to_usize() else { return };
    let Some(len) = W::from_usize(len.min(MAX_LOG_BYTES)) else {
        return;
    };
    if let Ok(bytes) = read_guest(caller, ptr, len, MAX_LOG_BYTES) {
        let line = String::from_utf8_lossy(&bytes);
        info!(target: "wasm_mod", mod_id = %caller.data().mod_id, "{line}");
    }
}

pub(crate) fn link<W: Width>(linker: &mut Linker<Host>) -> wasmtime::Result<()> {
    linker.func_wrap("ge", "log", |mut c: Caller<'_, Host>, p: W, l: W| {
        log(&mut c, p, l)
    })?;
    linker.func_wrap(
        "ge",
        "fs_read",
        |mut c: Caller<'_, Host>, p: W, l: W, out: W| fs_read(&mut c, p, l, out),
    )?;
    linker.func_wrap(
        "ge",
        "fs_write",
        |mut c: Caller<'_, Host>, p: W, l: W, d: W, n: W| fs_write(&mut c, p, l, d, n),
    )?;
    linker.func_wrap(
        "ge",
        "net_connect",
        |mut c: Caller<'_, Host>, p: W, l: W| net_connect(&mut c, p, l),
    )?;
    linker.func_wrap(
        "ge",
        "net_send",
        |mut c: Caller<'_, Host>, h: i32, p: W, l: W| net_send(&mut c, h, p, l),
    )?;
    linker.func_wrap(
        "ge",
        "net_recv",
        |mut c: Caller<'_, Host>, h: i32, p: W, l: W| net_recv(&mut c, h, p, l),
    )?;
    linker.func_wrap("ge", "net_close", |mut c: Caller<'_, Host>, h: i32| {
        let host = c.data_mut();
        match host.socket(h) {
            Ok(_) => {
                host.sockets[h as usize] = None;
                0
            }
            Err(e) => e,
        }
    })?;
    Ok(())
}
