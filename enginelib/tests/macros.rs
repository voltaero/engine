//! Compile-and-call coverage for the mod entry-point macros. Nothing in the
//! workspace is a mod, so without this the expansion goes unchecked.
use enginelib::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};

static RAN: AtomicBool = AtomicBool::new(false);

#[metadata]
fn metadata() -> LibraryMetadata {
    LibraryMetadata {
        mod_id: "macro_test".into(),
        ..Default::default()
    }
}

#[module]
fn run(api: &mut ServerAPI) {
    let _ = api;
    RAN.store(true, Ordering::SeqCst);
}

#[test]
fn module_macro_expands_and_runs() {
    let dir = std::env::temp_dir().join(format!("enginelib-test-{}-macros", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut api = ServerAPI::open(&dir, Config::default());
    run(&mut api);
    assert!(RAN.load(Ordering::SeqCst));
    assert_eq!(metadata().mod_id, "macro_test");
    drop(api);
    let _ = std::fs::remove_dir_all(&dir);
}

#[derive(serde::Serialize, serde::Deserialize, Verifiable)]
struct Payload {
    id: u32,
    name: String,
}

#[test]
fn derive_verifiable_accepts_only_decodable_payloads() {
    let payload = Payload {
        id: 7,
        name: "seven".into(),
    };
    let bytes = enginelib::api::to_allocvec(&payload).unwrap();
    assert!(payload.verify(&bytes));
    // Truncated: the string length promises more bytes than are left.
    assert!(!payload.verify(&bytes[..bytes.len() - 1]));
    assert!(!payload.verify(&[]));
}
