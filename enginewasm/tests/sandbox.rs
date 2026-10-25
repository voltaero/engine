//! Sandbox tests over hand-written WAT guests: the ABI round trip, the limits
//! a hostile guest runs into, load-time rejection, engine registration, and
//! (when a GPU is present) CPU/GPU result parity.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use enginelib::api::ServerAPI;
use enginelib::config::Config;
use enginelib::nucleus::submit::submit_batch;
use enginewasm::{Error, FsGrant, SandboxLimits, WasmConfig, WasmMod};

const META: &str = "namespace = \"demo\"\nname = \"mix\"\nmax_payload = 1048576\n";
const GPU_META: &str = "[gpu]\nentry = \"main\"\n";

const WGSL: &str = "
@group(0) @binding(0) var<storage, read> input: array<u32>;
@group(0) @binding(1) var<storage, read_write> output: array<u32>;

fn mix(v: u32) -> u32 {
    var x = v;
    for (var i = 0u; i < 32u; i = i + 1u) {
        x = x ^ (x >> 16u);
        x = x * 0x7feb352du;
        x = x ^ (x >> 15u);
        x = x * 0x846ca68bu;
        x = x ^ (x >> 16u);
    }
    return x;
}

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) id: vec3<u32>) {
    let i = id.x;
    if (i >= arrayLength(&input)) {
        return;
    }
    output[i] = mix(input[i]);
}
";

/// Bump allocator (LIFO `ge_free`) and "non-empty, whole words" `ge_verify`.
/// Imports go before it (WAT wants imports first).
const ALLOC: &str = r#"
  (memory (export "memory") 1)
  (global $top (mut i32) (i32.const 1024))
  (func $alloc (export "ge_alloc") (param $len i32) (result i32)
    (local $p i32)
    (local.set $p (global.get $top))
    (global.set $top
      (i32.and (i32.add (i32.add (local.get $p) (local.get $len)) (i32.const 7)) (i32.const -8)))
    (block $ok
      (br_if $ok (i32.le_u (global.get $top) (i32.shl (memory.size) (i32.const 16))))
      (br_if $ok (i32.ne (i32.const -1)
        (memory.grow (i32.shr_u
          (i32.add (i32.sub (global.get $top) (i32.shl (memory.size) (i32.const 16))) (i32.const 65535))
          (i32.const 16)))))
      unreachable)
    (local.get $p))
  (func (export "ge_free") (param $p i32) (param $len i32)
    (if (i32.eq (global.get $top)
          (i32.and (i32.add (i32.add (local.get $p) (local.get $len)) (i32.const 7)) (i32.const -8)))
      (then (global.set $top (local.get $p)))))
  (func $mix (param $x i32) (result i32)
    (local $i i32)
    (loop $l
      (local.set $x (i32.xor (local.get $x) (i32.shr_u (local.get $x) (i32.const 16))))
      (local.set $x (i32.mul (local.get $x) (i32.const 0x7feb352d)))
      (local.set $x (i32.xor (local.get $x) (i32.shr_u (local.get $x) (i32.const 15))))
      (local.set $x (i32.mul (local.get $x) (i32.const 0x846ca68b)))
      (local.set $x (i32.xor (local.get $x) (i32.shr_u (local.get $x) (i32.const 16))))
      (local.set $i (i32.add (local.get $i) (i32.const 1)))
      (br_if $l (i32.lt_u (local.get $i) (i32.const 32))))
    (local.get $x))
  (func (export "ge_verify") (param $p i32) (param $len i32) (result i32)
    (i32.and (i32.ne (local.get $len) (i32.const 0))
             (i32.eqz (i32.and (local.get $len) (i32.const 3)))))
"#;

/// Word-wise `mix` in `ge_run`.
const MIX_RUN: &str = r#"
  (func (export "ge_run") (param $p i32) (param $len i32) (param $slot i32) (result i32)
    (local $out i32) (local $i i32)
    (if (i32.and (local.get $len) (i32.const 3)) (then (return (i32.const -1))))
    (local.set $out (call $alloc (local.get $len)))
    (block $done
      (loop $l
        (br_if $done (i32.ge_u (local.get $i) (local.get $len)))
        (i32.store (i32.add (local.get $out) (local.get $i))
          (call $mix (i32.load (i32.add (local.get $p) (local.get $i)))))
        (local.set $i (i32.add (local.get $i) (i32.const 4)))
        (br $l)))
    (i32.store (local.get $slot) (local.get $out))
    (i32.store offset=4 (local.get $slot) (local.get $len))
    (i32.const 0))
"#;

fn mix_funcs() -> String {
    format!(r#"(import "ge" "log" (func $log (param i32 i32))) {ALLOC} {MIX_RUN}"#)
}

/// Hex-escapes `data` into a WAT custom section.
fn custom(name: &str, data: &str) -> String {
    let escaped: String = data.bytes().map(|b| format!("\\{b:02x}")).collect();
    format!("(@custom \"{name}\" \"{escaped}\")")
}

fn module(funcs: &str, meta: &str, wgsl: Option<&str>) -> Vec<u8> {
    let mut sections = custom("ge:meta", meta);
    if let Some(wgsl) = wgsl {
        sections.push_str(&custom("ge:wgsl", wgsl));
    }
    wat::parse_str(format!("(module {funcs} {sections})")).expect("valid wat")
}

fn mix_module() -> Vec<u8> {
    module(&mix_funcs(), &format!("{META}{GPU_META}"), Some(WGSL))
}

fn limits() -> SandboxLimits {
    SandboxLimits {
        max_memory: 64 << 20,
        call_timeout: Duration::from_millis(200),
        ..Default::default()
    }
}

fn mix_ref(mut x: u32) -> u32 {
    for _ in 0..32 {
        x ^= x >> 16;
        x = x.wrapping_mul(0x7feb352d);
        x ^= x >> 15;
        x = x.wrapping_mul(0x846ca68b);
        x ^= x >> 16;
    }
    x
}

fn mix_bytes(payload: &[u8]) -> Vec<u8> {
    payload
        .as_chunks::<4>()
        .0
        .iter()
        .flat_map(|w| mix_ref(u32::from_le_bytes(*w)).to_le_bytes())
        .collect()
}

fn payloads(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            let words = 1 + i % 37;
            (0..words)
                .flat_map(|w| ((i * 7919 + w) as u32).to_le_bytes())
                .collect()
        })
        .collect()
}

#[test]
fn cpu_runs_and_verifies() {
    let m = WasmMod::from_bytes(&mix_module(), limits()).unwrap();
    assert_eq!(m.task_type(), ("demo".to_string(), "mix".to_string()));
    let payload: Vec<u8> = (1u32..=4).flat_map(u32::to_le_bytes).collect();
    assert_eq!(m.run(&payload).unwrap(), mix_bytes(&payload));
    assert!(m.verify(&payload).unwrap());
    assert!(!m.verify(&[1, 2, 3]).unwrap());
    assert!(!m.verify(&[]).unwrap());
    assert_eq!(m.run(&[1, 2, 3]), Err(Error::Guest(-1)));
}

#[test]
fn cpu_batch_matches_reference_across_threads() {
    let m = WasmMod::from_bytes(&mix_module(), limits()).unwrap();
    let inputs = payloads(500);
    let refs: Vec<&[u8]> = inputs.iter().map(Vec::as_slice).collect();
    let out = m.run_batch(&refs, 4);
    assert_eq!(out.len(), inputs.len());
    for (input, result) in inputs.iter().zip(out) {
        assert_eq!(result.unwrap(), mix_bytes(input));
    }
}

const SPIN: &str = r#"
  (memory (export "memory") 1)
  (func (export "ge_alloc") (param i32) (result i32) (i32.const 1024))
  (func (export "ge_verify") (param i32 i32) (result i32) (loop $l (br $l)) (i32.const 1))
  (func (export "ge_run") (param i32 i32 i32) (result i32) (loop $l (br $l)) (i32.const 0))
"#;

#[test]
fn infinite_loop_times_out() {
    let m = WasmMod::from_bytes(&module(SPIN, META, None), limits()).unwrap();
    let start = Instant::now();
    assert_eq!(m.run(&[0; 4]), Err(Error::Timeout));
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "{:?}",
        start.elapsed()
    );
    // The poisoned instance is dropped; the next call gets a fresh one.
    assert_eq!(m.run(&[0; 4]), Err(Error::Timeout));
    // A failing verify rejects instead of erroring through the Task trait.
    use enginelib::task::Verifiable;
    assert!(!enginewasm::WasmTask(m).verify(&[0; 4]));
}

const GROW: &str = r#"
  (memory (export "memory") 1)
  (func (export "ge_alloc") (param i32) (result i32) (i32.const 1024))
  (func (export "ge_verify") (param i32 i32) (result i32) (i32.const 1))
  (func (export "ge_run") (param i32 i32 i32) (result i32)
    ;; 128 MiB: over the 64 MiB cap, so grow fails and returns -1.
    (if (i32.eq (memory.grow (i32.const 2048)) (i32.const -1))
      (then (return (i32.const -2))))
    (i32.const 0))
"#;

#[test]
fn memory_is_capped() {
    let m = WasmMod::from_bytes(&module(GROW, META, None), limits()).unwrap();
    assert_eq!(m.run(&[0; 4]), Err(Error::Guest(-2)));
}

#[test]
fn oversized_initial_memory_is_rejected() {
    // 2048 pages = 128 MiB declared up front.
    let funcs = GROW.replace(
        "(memory (export \"memory\") 1)",
        "(memory (export \"memory\") 2048)",
    );
    assert!(WasmMod::from_bytes(&module(&funcs, META, None), limits()).is_err());
}

#[test]
fn out_of_bounds_result_is_rejected() {
    let funcs = r#"
      (memory (export "memory") 1)
      (func (export "ge_alloc") (param i32) (result i32) (i32.const 1024))
      (func (export "ge_verify") (param i32 i32) (result i32) (i32.const 1))
      (func (export "ge_run") (param i32 i32 i32) (result i32)
        (i32.store (local.get 2) (i32.const 0xfff000))
        (i32.store offset=4 (local.get 2) (i32.const 0x1000))
        (i32.const 0))
    "#;
    let m = WasmMod::from_bytes(&module(funcs, META, None), limits()).unwrap();
    assert!(matches!(m.run(&[0; 4]), Err(Error::Invalid(_))));
}

#[test]
fn out_of_bounds_alloc_is_rejected() {
    let funcs = r#"
      (memory (export "memory") 1)
      (func (export "ge_alloc") (param i32) (result i32) (i32.const 65520))
      (func (export "ge_verify") (param i32 i32) (result i32) (i32.const 1))
      (func (export "ge_run") (param i32 i32 i32) (result i32) (i32.const 0))
    "#;
    let m = WasmMod::from_bytes(&module(funcs, META, None), limits()).unwrap();
    // The 8-byte result slot fits at 65520; a 32-byte input does not.
    assert!(matches!(m.run(&[0; 32]), Err(Error::Invalid(_))));
}

fn load_err(funcs: &str, meta: &str, wgsl: Option<&str>) -> String {
    match WasmMod::from_bytes(&module(funcs, meta, wgsl), limits()) {
        Ok(_) => panic!("module should have been rejected"),
        Err(Error::Load(msg)) => msg,
        Err(other) => panic!("expected a load error, got {other}"),
    }
}

#[test]
fn foreign_imports_are_rejected() {
    let funcs = mix_funcs().replace(
        r#"(import "ge" "log" (func $log (param i32 i32)))"#,
        r#"(import "wasi_snapshot_preview1" "fd_write" (func $log (param i32 i32 i32 i32) (result i32)))"#,
    );
    let msg = load_err(&funcs, META, None);
    assert!(msg.contains("fd_write"), "{msg}");
}

#[test]
fn bad_metadata_is_rejected() {
    let no_meta = wat::parse_str(format!("(module {})", mix_funcs())).unwrap();
    assert!(matches!(
        WasmMod::from_bytes(&no_meta, limits()),
        Err(Error::Load(msg)) if msg.contains("missing")
    ));
    // ':' would let one type's key prefix reach into another's.
    load_err(&mix_funcs(), "namespace = \"a:b\"\nname = \"c\"\n", None);
    load_err(
        &mix_funcs(),
        "namespace = \"a\"\nname = \"c\"\nsurprise = 1\n",
        None,
    );
    // A kernel without [gpu], and [gpu] without a kernel.
    load_err(&mix_funcs(), META, Some(WGSL));
    load_err(&mix_funcs(), &format!("{META}{GPU_META}"), None);
    // Missing export.
    let msg = load_err(&mix_funcs().replace("\"ge_run\"", "\"other\""), META, None);
    assert!(msg.contains("ge_run"), "{msg}");
}

#[test]
fn bad_kernels_are_rejected() {
    let meta = format!("{META}{GPU_META}");
    let cases = [
        // Syntax error.
        WGSL.replace("fn main", "fn main("),
        // A third buffer the host never binds.
        WGSL.replace(
            "fn mix",
            "@group(0) @binding(2) var<storage, read_write> extra: array<u32>;\nfn mix",
        ),
        // Writable input.
        WGSL.replace("var<storage, read> input", "var<storage, read_write> input"),
        // Wrong entry point name.
        WGSL.replace("fn main", "fn start"),
        // 2D workgroup.
        WGSL.replace("@workgroup_size(64)", "@workgroup_size(8, 8)"),
    ];
    for wgsl in cases {
        let msg = load_err(&mix_funcs(), &meta, Some(&wgsl));
        assert!(
            msg.contains("wgsl")
                || msg.contains("binding")
                || msg.contains("workgroup")
                || msg.contains("entry"),
            "{msg}"
        );
    }
}

/// A unique temp dir, removed on drop.
struct TempDir(PathBuf);
impl TempDir {
    fn new(name: &str) -> Self {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "enginewasm-test-{}-{name}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&path);
        Self(path)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn registers_with_the_engine_and_verifies_submits() {
    let dir = TempDir::new("engine");
    let mut api = ServerAPI::open(dir.0.join("db"), Config::default());
    let m = WasmMod::from_bytes(&mix_module(), limits()).unwrap();
    enginewasm::register(&mut api, m.clone()).unwrap();
    assert!(
        enginewasm::register(&mut api, m.clone()).is_err(),
        "duplicate type"
    );
    let api = Arc::new(api);
    ServerAPI::populate(&api);
    assert_eq!(api.settings(&m.task_type()).max_payload, Some(1 << 20));

    let results = submit_batch(api.clone(), &[vec![0u8; 8], vec![0u8; 3]], m.task_type()).unwrap();
    assert!(results[0].is_ok());
    assert!(results[1].is_err(), "ge_verify rejects a partial word");
}

#[test]
fn load_mods_skips_bad_files() {
    let dir = TempDir::new("mods");
    std::fs::create_dir_all(dir.0.join("mods")).unwrap();
    std::fs::write(dir.0.join("mods/mix.wasm"), mix_module()).unwrap();
    std::fs::write(dir.0.join("mods/junk.wasm"), b"not wasm").unwrap();
    let mut api = ServerAPI::open(dir.0.join("db"), Config::default());
    assert_eq!(
        enginewasm::load_mods(&mut api, dir.0.join("mods"), &WasmConfig::default()),
        1
    );
    assert!(
        api.task_registry
            .tasks
            .contains_key(&("demo".into(), "mix".into()))
    );
}

#[cfg(feature = "gpu")]
#[test]
fn gpu_matches_cpu() {
    let gpu = match enginewasm::gpu::Gpu::new() {
        Ok(gpu) => gpu,
        Err(err) => {
            eprintln!("skipping gpu_matches_cpu: {err}");
            return;
        }
    };
    eprintln!("gpu: {:?}", gpu.info());
    let m = WasmMod::from_bytes(&mix_module(), limits()).unwrap();
    let kernel = gpu.kernel(&m).unwrap();

    let mut inputs = payloads(2000);
    inputs.insert(17, vec![1, 2, 3]);
    let refs: Vec<&[u8]> = inputs.iter().map(Vec::as_slice).collect();
    let on_gpu = gpu.run_batch(&kernel, &refs);
    let on_cpu = m.run_batch(&refs, 4);
    assert_eq!(on_gpu.len(), inputs.len());
    for (i, (g, c)) in on_gpu.iter().zip(&on_cpu).enumerate() {
        if i == 17 {
            assert!(g.is_err() && c.is_err());
            continue;
        }
        assert_eq!(g.as_ref().unwrap(), c.as_ref().unwrap(), "payload {i}");
        assert!(m.verify(g.as_ref().unwrap()).unwrap());
    }
}

/// Server verifies through the mod; a worker leases over loopback TCP, runs
/// the chunk on the GPU if there is one (else the CPU) and completes it.
#[test]
fn end_to_end_over_transport() {
    use enginelib::transport::client::Client;
    use enginelib::transport::server::{
        DEFAULT_MAX_INFLIGHT, DEFAULT_MAX_INFLIGHT_PER_SESSION, Server, ServerOptions,
    };
    use enginelib::wire::{Auth, Caps, LeaseRef, TaskResult};

    let dir = TempDir::new("e2e");
    let mut api = ServerAPI::open(dir.0.join("db"), Config::default());
    let m = WasmMod::from_bytes(&mix_module(), limits()).unwrap();
    enginewasm::register(&mut api, m.clone()).unwrap();
    let api = Arc::new(api);
    ServerAPI::populate(&api);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = {
        let _guard = rt.enter();
        Server::start(
            api.clone(),
            ServerOptions {
                endpoint: "tcp://127.0.0.1:*".into(),
                server_id: api.server_id(),
                advertise: None,
                proxy: None,
                proxy_token: None,
                heartbeat: Duration::from_millis(100),
                max_message_bytes: 16 << 20,
                session_idle: Duration::from_secs(60),
                max_inflight: DEFAULT_MAX_INFLIGHT,
                max_inflight_per_session: DEFAULT_MAX_INFLIGHT_PER_SESSION,
            },
        )
        .unwrap()
    };
    let auth = Auth {
        uid: "worker".into(),
        challenge: String::new(),
        admin_token: None,
    };
    let mut client = Client::connect(server.endpoint(), auth, Caps::default()).unwrap();
    let task_type = m.task_type();

    let inputs = payloads(100);
    let ids = client.insert(task_type.clone(), inputs.clone()).unwrap();
    assert!(ids.iter().all(Result::is_ok));
    let bad = client
        .insert(task_type.clone(), vec![vec![1, 2, 3]])
        .unwrap();
    assert!(bad[0].is_err(), "server-side ge_verify rejects the payload");

    let leased = client
        .lease(task_type.clone(), 1000, Duration::from_secs(2))
        .unwrap();
    assert_eq!(leased.tasks.len(), inputs.len());
    let refs: Vec<&[u8]> = leased.tasks.iter().map(|t| t.bytes.as_slice()).collect();
    #[cfg(feature = "gpu")]
    let outputs = match enginewasm::gpu::Gpu::new() {
        Ok(gpu) => gpu.run_batch(&gpu.kernel(&m).unwrap(), &refs),
        Err(_) => m.run_batch(&refs, 2),
    };
    #[cfg(not(feature = "gpu"))]
    let outputs = m.run_batch(&refs, 2);

    let lease = |i: usize| LeaseRef {
        task_type: leased.task_type.clone(),
        task_id: leased.tasks[i].task_id.clone(),
        token: leased.tasks[i].token,
    };
    // A result the mod's ge_verify rejects is refused, and the lease survives.
    let (acks, _) = client
        .complete(
            vec![TaskResult {
                lease: lease(0),
                bytes: vec![9; 3],
            }],
            None,
        )
        .unwrap();
    assert!(acks[0].is_err());

    let results = outputs
        .into_iter()
        .enumerate()
        .map(|(i, out)| {
            let bytes = out.unwrap();
            assert_eq!(bytes, mix_bytes(&leased.tasks[i].bytes));
            TaskResult {
                lease: lease(i),
                bytes,
            }
        })
        .collect();
    let (acks, _) = client.complete(results, None).unwrap();
    assert!(acks.iter().all(Result::is_ok), "{acks:?}");
    assert!(
        client
            .lease(task_type, 1, Duration::from_millis(100))
            .is_err()
    );
    drop(client);
    server.shutdown();
}

const W64_ALLOC: &str = r#"
  (memory (export "memory") i64 1)
  (global $top (mut i64) (i64.const 1024))
  (func $alloc (export "ge_alloc") (param $len i64) (result i64)
    (local $p i64)
    (local.set $p (global.get $top))
    (global.set $top
      (i64.and (i64.add (i64.add (local.get $p) (local.get $len)) (i64.const 7)) (i64.const -8)))
    (block $ok
      (br_if $ok (i64.le_u (global.get $top) (i64.shl (memory.size) (i64.const 16))))
      (br_if $ok (i64.ne (i64.const -1)
        (memory.grow (i64.shr_u
          (i64.add (i64.sub (global.get $top) (i64.shl (memory.size) (i64.const 16))) (i64.const 65535))
          (i64.const 16)))))
      unreachable)
    (local.get $p))
  (func (export "ge_verify") (param i64 i64) (result i32) (i32.const 1))
"#;

/// wasm64: flips every byte.
const W64_FLIP: &str = r#"
  (func (export "ge_run") (param $p i64) (param $len i64) (param $slot i64) (result i32)
    (local $out i64) (local $i i64)
    (local.set $out (call $alloc (local.get $len)))
    (block $done
      (loop $l
        (br_if $done (i64.ge_u (local.get $i) (local.get $len)))
        (i32.store8 (i64.add (local.get $out) (local.get $i))
          (i32.xor (i32.load8_u (i64.add (local.get $p) (local.get $i))) (i32.const 0xff)))
        (local.set $i (i64.add (local.get $i) (i64.const 1)))
        (br $l)))
    (i64.store (local.get $slot) (local.get $out))
    (i64.store offset=8 (local.get $slot) (local.get $len))
    (i32.const 0))
"#;

#[test]
fn wasm64_runs_and_can_be_disabled() {
    let wasm = module(&format!("{W64_ALLOC} {W64_FLIP}"), META, None);
    let m = WasmMod::from_bytes(&wasm, limits()).unwrap();
    assert!(m.is_wasm64());
    assert_eq!(m.run(&[0, 1, 0xff]).unwrap(), vec![0xff, 0xfe, 0]);
    let off = SandboxLimits {
        memory64: false,
        ..limits()
    };
    assert!(matches!(
        WasmMod::from_bytes(&wasm, off),
        Err(Error::Load(_))
    ));
}

#[test]
fn wasm64_memory_can_exceed_4_gib() {
    // Grows to 5 GiB (81920 pages); pages stay untouched, so this is address
    // space, not RAM.
    let grow = r#"
      (func (export "ge_run") (param i64 i64) (param $slot i64) (result i32)
        (if (i64.eq (memory.grow (i64.const 81920)) (i64.const -1))
          (then (return (i32.const -2))))
        (i64.store (local.get $slot) (i64.const 0x140000000))
        (i64.store offset=8 (local.get $slot) (i64.const 0))
        (i32.const 0))
    "#;
    let wasm = module(&format!("{W64_ALLOC} {grow}"), META, None);
    let capped = WasmMod::from_bytes(&wasm, limits()).unwrap();
    assert_eq!(capped.run(&[1]), Err(Error::Guest(-2)));
    let roomy = SandboxLimits {
        max_memory: 6 << 30,
        ..limits()
    };
    let m = WasmMod::from_bytes(&wasm, roomy).unwrap();
    assert_eq!(m.run(&[1]).unwrap(), Vec::<u8>::new());
}

fn fs_read_guest() -> String {
    format!(
        r#"(import "ge" "fs_read" (func $fs_read (param i32 i32 i32) (result i32)))
        {ALLOC}
        (func (export "ge_run") (param $p i32) (param $len i32) (param $slot i32) (result i32)
          (call $fs_read (local.get $p) (local.get $len) (local.get $slot)))"#
    )
}

fn fs_write_guest() -> String {
    // Writes the payload (a path) to that path.
    format!(
        r#"(import "ge" "fs_write" (func $fs_write (param i32 i32 i32 i32) (result i32)))
        {ALLOC}
        (func (export "ge_run") (param $p i32) (param $len i32) (param $slot i32) (result i32)
          (local $s i32)
          (local.set $s (call $fs_write (local.get $p) (local.get $len) (local.get $p) (local.get $len)))
          (if (i32.lt_s (local.get $s) (i32.const 0)) (then (return (local.get $s))))
          (i32.store (local.get $slot) (local.get $p))
          (i32.store offset=4 (local.get $slot) (i32.const 0))
          (i32.const 0))"#
    )
}

fn grant(name: &str, path: &std::path::Path, write: bool) -> FsGrant {
    FsGrant {
        name: name.into(),
        path: path.into(),
        write,
    }
}

#[test]
fn fs_read_needs_a_grant_and_stays_inside_it() {
    let dir = TempDir::new("fs-read");
    std::fs::create_dir_all(dir.0.join("data/sub")).unwrap();
    std::fs::write(dir.0.join("data/sub/hello.txt"), b"hi there").unwrap();
    std::fs::write(dir.0.join("secret.txt"), b"nope").unwrap();
    let wasm = module(&fs_read_guest(), META, None);

    let denied = WasmMod::from_bytes(&wasm, limits()).unwrap();
    assert_eq!(denied.run(b"data/sub/hello.txt"), Err(Error::Guest(-1)));

    let granted = WasmMod::from_bytes(
        &wasm,
        SandboxLimits {
            fs: vec![grant("data", &dir.0.join("data"), false)],
            ..limits()
        },
    )
    .unwrap();
    assert_eq!(granted.run(b"data/sub/hello.txt").unwrap(), b"hi there");
    for escape in ["data/../secret.txt", "data//etc/passwd", "data/", "other/x"] {
        assert_eq!(
            granted.run(escape.as_bytes()),
            Err(Error::Guest(-1)),
            "{escape}"
        );
    }
    assert_eq!(granted.run(b"data/missing.txt"), Err(Error::Guest(-3)));

    // A symlink pointing out of the mount is refused by cap-std.
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(dir.0.join("secret.txt"), dir.0.join("data/link")).unwrap();
        assert_eq!(granted.run(b"data/link"), Err(Error::Guest(-3)));
    }
}

#[test]
fn wasm64_uses_the_same_host_calls() {
    let dir = TempDir::new("fs-read64");
    std::fs::create_dir_all(&dir.0).unwrap();
    std::fs::write(dir.0.join("a.bin"), [1, 2, 3]).unwrap();
    let funcs = format!(
        r#"(import "ge" "fs_read" (func $fs_read (param i64 i64 i64) (result i32)))
        {W64_ALLOC}
        (func (export "ge_run") (param $p i64) (param $len i64) (param $slot i64) (result i32)
          (call $fs_read (local.get $p) (local.get $len) (local.get $slot)))"#
    );
    let m = WasmMod::from_bytes(
        &module(&funcs, META, None),
        SandboxLimits {
            fs: vec![grant("d", &dir.0, false)],
            ..limits()
        },
    )
    .unwrap();
    assert!(m.is_wasm64());
    assert_eq!(m.run(b"d/a.bin").unwrap(), vec![1, 2, 3]);
}

#[test]
fn fs_write_needs_a_writable_mount() {
    let dir = TempDir::new("fs-write");
    std::fs::create_dir_all(dir.0.join("ro")).unwrap();
    std::fs::create_dir_all(dir.0.join("rw")).unwrap();
    let m = WasmMod::from_bytes(
        &module(&fs_write_guest(), META, None),
        SandboxLimits {
            fs: vec![
                grant("ro", &dir.0.join("ro"), false),
                grant("rw", &dir.0.join("rw"), true),
            ],
            ..limits()
        },
    )
    .unwrap();
    assert_eq!(m.run(b"ro/out.txt"), Err(Error::Guest(-1)));
    assert!(!dir.0.join("ro/out.txt").exists());
    assert_eq!(m.run(b"rw/out.txt").unwrap(), b"");
    assert_eq!(
        std::fs::read(dir.0.join("rw/out.txt")).unwrap(),
        b"rw/out.txt"
    );
    assert_eq!(m.run(b"rw/../escape.txt"), Err(Error::Guest(-1)));
}

fn echo_server() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        use std::io::{Read, Write};
        for mut stream in listener.incoming().flatten() {
            let mut buf = [0u8; 256];
            if let Ok(n) = stream.read(&mut buf) {
                let _ = stream.write_all(&buf[..n]);
            }
        }
    });
    addr
}

fn net_guest() -> String {
    // Connects to the payload (host:port), sends it, returns what comes back.
    format!(
        r#"(import "ge" "net_connect" (func $connect (param i32 i32) (result i32)))
        (import "ge" "net_send" (func $send (param i32 i32 i32) (result i64)))
        (import "ge" "net_recv" (func $recv (param i32 i32 i32) (result i64)))
        {ALLOC}
        (func (export "ge_run") (param $p i32) (param $len i32) (param $slot i32) (result i32)
          (local $h i32) (local $n i64) (local $buf i32)
          (local.set $h (call $connect (local.get $p) (local.get $len)))
          (if (i32.lt_s (local.get $h) (i32.const 0)) (then (return (local.get $h))))
          (local.set $n (call $send (local.get $h) (local.get $p) (local.get $len)))
          (if (i64.lt_s (local.get $n) (i64.const 0)) (then (return (i32.wrap_i64 (local.get $n)))))
          (local.set $buf (call $alloc (i32.const 256)))
          (local.set $n (call $recv (local.get $h) (local.get $buf) (i32.const 256)))
          (if (i64.lt_s (local.get $n) (i64.const 0)) (then (return (i32.wrap_i64 (local.get $n)))))
          (i32.store (local.get $slot) (local.get $buf))
          (i32.store offset=4 (local.get $slot) (i32.wrap_i64 (local.get $n)))
          (i32.const 0))"#
    )
}

#[test]
fn net_needs_an_allowlisted_endpoint() {
    let addr = echo_server();
    let other = echo_server();
    let wasm = module(&net_guest(), META, None);

    let denied = WasmMod::from_bytes(&wasm, limits()).unwrap();
    assert_eq!(denied.run(addr.as_bytes()), Err(Error::Guest(-1)));

    let granted = WasmMod::from_bytes(
        &wasm,
        SandboxLimits {
            net: vec![addr.clone()],
            ..limits()
        },
    )
    .unwrap();
    assert_eq!(granted.run(addr.as_bytes()).unwrap(), addr.as_bytes());
    assert_eq!(granted.run(other.as_bytes()), Err(Error::Guest(-1)));
}

#[test]
fn config_sets_defaults_and_per_mod_overrides() {
    let cfg = WasmConfig::from_toml(
        r#"
        host = "[::1]:50051"

        [wasm]
        max_memory_mb = 512
        call_timeout_ms = 50
        memory64 = false

        [wasm.mods."demo:mix"]
        max_memory_mb = 8192
        memory64 = true
        fs = [{ name = "data", path = "/srv/data" }]
        net = ["Example.com:443"]
        "#,
    )
    .unwrap();
    let other = cfg.limits_for(&("x".into(), "y".into())).unwrap();
    assert_eq!(other.max_memory, 512 << 20);
    assert_eq!(other.call_timeout, Duration::from_millis(50));
    assert!(!other.memory64 && other.fs.is_empty() && other.net.is_empty());

    let mix = cfg.limits_for(&("demo".into(), "mix".into())).unwrap();
    assert_eq!(mix.max_memory, 8192 << 20);
    assert_eq!(mix.call_timeout, Duration::from_millis(50), "inherited");
    assert!(mix.memory64);
    assert_eq!(
        mix.fs,
        vec![grant("data", std::path::Path::new("/srv/data"), false)]
    );
    assert_eq!(mix.net, vec!["example.com:443".to_string()]);

    // No [wasm] table: defaults.
    let none = WasmConfig::from_toml("host = \"x\"").unwrap();
    assert_eq!(
        none.limits_for(&("a".into(), "b".into()))
            .unwrap()
            .max_memory,
        256 << 20
    );

    for bad in [
        "[wasm]\nmax_memroy_mb = 1",
        "[wasm]\nnet = [\"no-port\"]",
        "[wasm]\nfs = [{ name = \"a/b\", path = \"/x\" }]",
        "[wasm.mods.nocolon]\nmax_memory_mb = 1",
        "[wasm]\nmax_io_mb = 5000",
    ] {
        assert!(WasmConfig::from_toml(bad).is_err(), "{bad}");
    }
}

#[test]
fn load_applies_config_for_the_mods_type() {
    let cfg = WasmConfig::from_toml("[wasm.mods.\"demo:mix\"]\nmemory64 = false\n").unwrap();
    let wasm = module(&format!("{W64_ALLOC} {W64_FLIP}"), META, None);
    assert!(matches!(WasmMod::load(&wasm, &cfg), Err(Error::Load(_))));
    assert!(WasmMod::load(&mix_module(), &cfg).is_ok());
}
