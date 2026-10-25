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
