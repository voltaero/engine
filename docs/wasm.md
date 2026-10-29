# Wasm task mods (experimental)

Crate: `enginewasm`. Server support: `cargo run -p engine --features wasm --bin server`.
Worker: `cargo run --release -p engine --features wasm --bin wasm_worker -- --mod <file.wasm>`.

A wasm mod defines one task type with untrusted code. The server uses it only to
**verify** payloads and results (`Verifiable::verify`). Workers use it to
**run** tasks: on the CPU through the guest's `ge_run`, or on the GPU through
the WGSL kernel the mod ships. Both paths must produce identical bytes.

Native `.rf` mods are unchanged. They still get `&mut ServerAPI` and full
process access. A wasm mod gets neither. It can only reach the files and
network endpoints the operator grants it (see "Configuration").

Modules may be **wasm32 or wasm64** (memory64). The host detects the width from
the exported memory, and the same Rust source builds for both.

## Loading

`enginewasm::load_mods(api, "./mods", &config)` loads every `*.wasm` file in
`./mods`, with the limits `config` resolves for each mod's type. Call it before `ServerAPI::populate`. For each module:

1. Read the custom sections `ge:meta` (TOML) and `ge:wgsl` (optional WGSL)
   with `wasmparser`. No guest code runs at this step.
2. Validate the metadata and, if present, the kernel (naga, binding contract below).
3. Compile with wasmtime, detect wasm32/wasm64, and link. **Any import outside
   `ge.*` fails the load.** A wasm64 module also fails if `memory64` is off for it.
   Granted directories are opened at this step.
4. Instantiate once under the sandbox limits, which checks the exports and any start function.
5. Register the task type. **An already registered type is refused**, so a wasm
   mod can't take over another mod's type.

A mod that fails any step is logged and skipped.

## `ge:meta`

```toml
namespace = "demo"        # [A-Za-z0-9_-]{1,64}; ':' is refused (storage keys)
name = "mix"
version = "0.1.0"         # optional
chunk_size = 1024         # optional, as TaskLimits; config.toml still overrides
max_payload = 1048576     # optional, capped by SandboxLimits::max_io
max_result = 1048576

[gpu]                     # optional; required iff a ge:wgsl section exists
entry = "main"            # @compute entry point
record_words = 1          # u32 words per record (1..=1024)
```

Unknown keys are rejected. Derived types are not supported yet.

## CPU ABI (core wasm, no WASI)

`usize` below is `i32` in a wasm32 module and `i64` in a wasm64 one.

| export                              | meaning                                                     |
|-------------------------------------|-------------------------------------------------------------|
| `memory`                            | linear memory, 32- or 64-bit                                |
| `ge_alloc(len: usize) -> usize`     | buffer the host copies input into; 0 = out of memory        |
| `ge_free(ptr, len)`                 | optional; called on the output, then on the input           |
| `ge_verify(ptr, len) -> i32`        | nonzero = valid. Called for payloads **and** results        |
| `ge_run(ptr, len, out: usize) -> i32` | 0 and `[out_ptr, out_len]` (two `usize`) written at `out`, or a negative error code |

The host allocates the `out` slot once per instance (through `ge_alloc`).

In Rust:

```rust
#[unsafe(no_mangle)] pub extern "C" fn ge_alloc(len: usize) -> *mut u8 { .. }
#[unsafe(no_mangle)] pub unsafe extern "C" fn ge_free(ptr: *mut u8, len: usize) { .. }
#[unsafe(no_mangle)] pub extern "C" fn ge_verify(ptr: *const u8, len: usize) -> i32 { .. }
#[unsafe(no_mangle)] pub unsafe extern "C" fn ge_run(ptr: *const u8, len: usize, out: *mut [usize; 2]) -> i32 { .. }
```

### Imports (`ge.*`)

All imports are always linked, but each call checks the mod's grants. A call
that isn't granted returns `-1`. Pointers and lengths are `usize`, handles and
statuses are `i32`, and byte counts are `i64`.

| import                                    | does                                                              |
|-------------------------------------------|-------------------------------------------------------------------|
| `log(ptr, len)`                           | one line to `tracing` (target `wasm_mod`); at most 16 lines of 1 KiB per call |
| `fs_read(path_ptr, path_len, out) -> i32` | reads `<mount>/<path>` into a fresh `ge_alloc` buffer and writes `[ptr, len]` to `out` |
| `fs_write(path_ptr, path_len, ptr, len) -> i32` | creates or overwrites `<mount>/<path>`; writable mounts only |
| `net_connect(addr_ptr, addr_len) -> i32`  | TCP connect to an allowlisted `host:port`; returns a handle (at most 16 open) |
| `net_send(h, ptr, len) -> i64`            | bytes written                                                     |
| `net_recv(h, ptr, len) -> i64`            | bytes read into `ptr` (0 = peer closed)                           |
| `net_close(h) -> i32`                     | closes the handle                                                 |

Error codes (`enginewasm::host::code`):

| code | name         | meaning                                              |
|------|--------------|------------------------------------------------------|
| -1   | `DENIED`     | not granted, or the path or address isn't allowed    |
| -2   | `INVALID`    | bad pointer, length or UTF-8                         |
| -3   | `IO`         | the file or connection failed                        |
| -4   | `TOO_BIG`    | over `max_io`, or too many open sockets              |
| -5   | `TIMEOUT`    | the call's deadline passed                           |
| -6   | `BAD_HANDLE` | unknown or closed socket handle                      |
| -7   | `NO_MEMORY`  | `ge_alloc` returned 0                                |

Files:
- Paths are `<mount name>/<relative path>`.
- `..`, absolute paths, empty paths and unknown mounts are `DENIED`.
- Access goes through a `cap-std` directory handle, so a symlink that points
  out of the mount can't be followed either.

Network:
- An address must match an allowlist entry exactly. Hosts are compared
  case-insensitively.
- Sockets get the call's remaining budget as their connect, read and write
  timeouts, so a host call can't outlive the call that made it.
- Every socket is closed when the call ends.

## Configuration

The server and `wasm_worker` both read the `[wasm]` table of `config.toml`
(the worker takes `--config <path>`). A file that is missing means defaults. A
`[wasm]` table that doesn't parse stops startup, so a typo can't silently drop
a restriction.

```toml
[wasm]                              # defaults for every wasm mod
max_memory_mb = 256
call_timeout_ms = 2000
max_io_mb = 16
gpu_timeout_ms = 10000
pool_size = 16
memory64 = true

[wasm.mods."demo:mix"]              # overrides for one task type
max_memory_mb = 8192                # above 4 GiB needs a wasm64 module
fs = [
  { name = "data", path = "/srv/data" },                # read-only
  { name = "out",  path = "/srv/out", write = true },
]
net = ["api.example.com:443", "10.0.0.5:9000"]
```

Each key of a per-mod table replaces the default; lists are replaced, not
merged. Unknown keys are rejected. In code, the same settings are
`WasmConfig::limits_for(&task_type) -> SandboxLimits`, or a `SandboxLimits`
passed straight to `WasmMod::from_bytes`.

| setting (`config.toml` / `SandboxLimits`) | default | enforcement |
|---|---|---|
| `max_memory_mb` / `max_memory` | 256 MiB | wasmtime `StoreLimits`: `memory.grow` returns -1, and oversized initial memory fails to instantiate |
| `call_timeout_ms` / `call_timeout` | 2 s | epoch interruption (1 ms tick) traps guest code with `Error::Timeout`; host calls get the remaining budget |
| `max_io_mb` / `max_io` | 16 MiB | caps `max_payload`, `max_result`, file reads and writes, and socket reads and writes |
| `pool_size` | CPU count | idle instances kept per mod |
| `gpu_timeout_ms` / `gpu_timeout` | 10 s | `device.poll` timeout per GPU batch |
| `memory64` | `true` | when `false`, wasm64 modules are refused at load |
| `fs` | none | directories reachable through `ge.fs_*` |
| `net` | none | `host:port` endpoints reachable through `ge.net_*` |

wasm64 trades some speed for address space. A 64-bit memory can't use the
4 GiB guard-page trick, so every access is explicitly bounds-checked. In the
benchmark below that cost about 1 % on a compute-bound kernel.

Clocks and randomness are not exposed: results have to be deterministic for
the CPU/GPU parity check and for re-execution after a lease is lost.

Other properties:

- The guest stack is limited to 1 MiB.
- wasmtime is built without WASI, threads, GC or the component model. The
  only way out of the sandbox is a granted `ge.*` call.
- File reads and DNS lookups are blocking host calls. The epoch deadline can't
  interrupt them; only socket I/O is time-bounded.
- Every pointer and length the guest returns is bounds-checked against its own
  memory before the host reads it.
- A trap or timeout drops that instance. The next call gets a fresh one.
- A verify that errors counts as "invalid". It never panics the server.

Instances are **reused** across calls of the same mod. A guest can keep state
between the tasks it handles, but never across mods. The mod author already
sees every payload of their type, so this is no new exposure. It is still not
per-task isolation.

## GPU kernel (`ge:wgsl`)

```wgsl
@group(0) @binding(0) var<storage, read> input: array<u32>;
@group(0) @binding(1) var<storage, read_write> output: array<u32>;
@compute @workgroup_size(64)          // (1..=256, 1, 1), literal
fn main(@builtin(global_invocation_id) id: vec3<u32>) { ... }
```

- naga checks the contract at load time:
  - exactly those two storage bindings;
  - only private or workgroup globals besides them;
  - no uniforms, textures or samplers;
  - no push constants;
  - no extra capabilities.
- wgpu compiles the kernel with runtime bounds checks. It never sees host memory.
- One invocation handles one record (`record_words` u32s), and the output has
  the input's length. The host **packs many payloads into one dispatch**, up to
  the device's buffer and dispatch limits. So a kernel must bound its index by
  `arrayLength(&input)` and must not read across records.
- A payload that is empty or not a whole number of records fails alone, without
  failing the rest of the batch.
- GPU errors are captured by wgpu error scopes and returned as `Error::Gpu`.
  wgpu's default handler would panic instead.
- Software adapters are refused unless `GE_GPU_ALLOW_CPU` is set.

The GPU timeout does not preempt a running kernel. It only stops the host from
waiting. GPU drivers still apply their own watchdog.

## Worker

`wasm_worker` loads the same `.wasm` file as the server, then:

- picks the GPU when the mod has a kernel and an adapter exists, else the CPU
  (`--backend` forces one);
- leases chunks, runs them, and completes with `then_lease`;
- cancels the leases of chunks with a failed task, so they get requeued;
- hands derived chunks back.

`--bench N --words W` runs N tasks locally on every backend, checks that the
results match, and prints throughput. For `demo:mix` it adds a **native** row:
the example's `src/mix.rs`, compiled into the worker with no wasm involved.
Speedups are reported against that row.

RX 6800 (Vulkan) vs 12 CPU threads, `demo:mix` at 1024 rounds per word. Every
backend produced identical bytes. Absolute rates vary by about ±15 % between
runs (CPU boost clocks); the ratios hold:

| payload       | native        | wasm32        | wasm64        | GPU (wgpu)              |
|---------------|---------------|---------------|---------------|-------------------------|
| 20k × 1 KiB   | 20.2k tasks/s | 20.1k (0.99×) | 18.9k (0.98×) | 425k tasks/s (**21×**)  |
| 4k × 16 KiB   | 1.21k tasks/s | 1.19k (0.98×) | 1.24k (0.99×) | 30k tasks/s (**24×**)   |

On a light kernel (32 rounds) the GPU loses its edge: PCIe transfers dominate
and it runs about as fast as the CPU.

There is no `RunnerKind::Gpu` on the wire yet: the backend is chosen by the
worker, not negotiated with the server (that would need a `PROTO_VERSION` bump).

## Example

`examples/wasm-mix` is a `no_std` Rust guest (lowbias32, 1024 rounds per word)
with a matching WGSL kernel. It builds for both widths; wasm64 needs nightly:

```sh
cd examples/wasm-mix
cargo build --release --target wasm32-unknown-unknown
cargo +nightly build --release --target wasm64-unknown-unknown -Zbuild-std=core,alloc
mkdir -p ../../mods && cp target/wasm32-unknown-unknown/release/wasm_mix.wasm ../../mods/
```
