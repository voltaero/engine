# Wasm task mods (experimental)

Crate: `enginewasm`. Server support: `cargo run -p engine --features wasm --bin server`.
Worker: `cargo run --release -p engine --features wasm --bin wasm_worker -- --mod <file.wasm>`.

A wasm mod defines one task type with untrusted code. The server uses it only to
**verify** payloads and results (`Verifiable::verify`). Workers use it to
**run** tasks: on the CPU through the guest's `ge_run`, or on the GPU through
the WGSL kernel the mod ships. Both paths must produce identical bytes.

Native `.rf` mods are unchanged. They still get `&mut ServerAPI` and full
process access. A wasm mod gets neither.

## Loading

`enginewasm::load_mods(api, "./mods", limits)` loads every `*.wasm` file in
`./mods`. Call it before `ServerAPI::populate`. For each module:

1. Read the custom sections `ge:meta` (TOML) and `ge:wgsl` (optional WGSL)
   with `wasmparser`. No guest code runs at this step.
2. Validate the metadata and, if present, the kernel (naga, binding contract below).
3. Compile with wasmtime and link. **Any import other than `ge.log` fails the load.**
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

## CPU ABI (core wasm, wasm32, no WASI)

| export                        | meaning                                                  |
|-------------------------------|----------------------------------------------------------|
| `memory`                      | linear memory                                            |
| `ge_alloc(len) -> ptr`        | buffer the host copies input into                        |
| `ge_free(ptr, len)`           | optional; called on the output, then on the input        |
| `ge_verify(ptr, len) -> i32`  | nonzero = valid. Called for payloads **and** results     |
| `ge_run(ptr, len) -> i64`     | `(out_ptr << 32) \| out_len`, or a negative error code   |

The only import is `ge.log(ptr, len)`: at most 16 lines of 1 KiB per call, sent
to `tracing` under target `wasm_mod`.

## Sandbox (`SandboxLimits`)

| limit          | default        | enforcement                                                         |
|----------------|----------------|---------------------------------------------------------------------|
| `max_memory`   | 256 MiB        | wasmtime `StoreLimits`: `memory.grow` returns -1, oversized initial memory fails to instantiate |
| `call_timeout` | 2 s            | epoch interruption (1 ms tick): the call traps with `Error::Timeout` |
| `max_io`       | 16 MiB         | caps `max_payload`/`max_result`; larger results are rejected         |
| `pool_size`    | CPU count      | idle instances kept per mod                                          |
| `gpu_timeout`  | 10 s           | `device.poll` timeout per GPU batch                                  |

Other properties:

- The guest stack is limited to 1 MiB.
- wasmtime is built without WASI, threads, GC or the component model.
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

`--bench N --words W` runs N tasks locally on both backends, checks that the
results match, and prints throughput.

There is no `RunnerKind::Gpu` on the wire yet: the backend is chosen by the
worker, not negotiated with the server (that would need a `PROTO_VERSION` bump).

## Example

`examples/wasm-mix` is a `no_std` Rust guest (lowbias32, 1024 rounds per word)
with a matching WGSL kernel:

```sh
cargo build --release --target wasm32-unknown-unknown --manifest-path examples/wasm-mix/Cargo.toml
mkdir -p mods && cp examples/wasm-mix/target/wasm32-unknown-unknown/release/wasm_mix.wasm mods/
```
