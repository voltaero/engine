// build.rs

use vergen_gix::{Build, Cargo, Emitter, Gix, Rustc};

fn main() {
    // NOTE: This will output everything, and requires all features enabled.
    // NOTE: See the specific builder documentation for configuration options.
    let build = Build::all_build();
    let cargo = Cargo::all_cargo();
    let git = Gix::all_git();
    let rustc = Rustc::all_rustc();

    Emitter::default()
        .add_instructions(&build)
        .unwrap()
        .add_instructions(&cargo)
        .unwrap()
        .add_instructions(&rustc)
        .unwrap()
        .add_instructions(&git)
        .unwrap()
        .emit()
        .unwrap();
}
