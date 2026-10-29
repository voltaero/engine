//! Example wasm task mod: each payload is little-endian u32 words, each result
//! is `mix::ROUNDS` rounds of the lowbias32 integer hash applied to every word.
//! The same function ships as a WGSL kernel (`kernel.wgsl`) for the GPU path.
//! ABI: `docs/wasm.md`. Builds for wasm32 and, on nightly, wasm64:
//!
//! ```sh
//! cargo build --release --target wasm32-unknown-unknown
//! cargo +nightly build --release --target wasm64-unknown-unknown -Zbuild-std=core,alloc
//! ```

#![no_std]
// wasm64 intrinsics (`core::arch::wasm64`) are nightly-only.
#![cfg_attr(target_arch = "wasm64", feature(simd_wasm64))]

extern crate alloc;

mod mix;

use alloc::boxed::Box;
use alloc::vec;
use core::slice;

#[cfg(target_arch = "wasm32")]
use core::arch::wasm32 as arch;
#[cfg(target_arch = "wasm64")]
use core::arch::wasm64 as arch;

#[global_allocator]
static ALLOC: Bump = Bump;

#[panic_handler]
fn panic(_: &core::panic::PanicInfo) -> ! {
    arch::unreachable()
}

// Metadata and kernel are custom sections: the host reads them without
// running any of this module's code.
#[used]
#[unsafe(link_section = "ge:meta")]
static META: [u8; include_bytes!("../meta.toml").len()] = *include_bytes!("../meta.toml");

#[used]
#[unsafe(link_section = "ge:wgsl")]
static WGSL: [u8; include_bytes!("kernel.wgsl").len()] = *include_bytes!("kernel.wgsl");

#[unsafe(no_mangle)]
pub extern "C" fn ge_alloc(len: usize) -> *mut u8 {
    Box::into_raw(vec![0u8; len].into_boxed_slice()).cast()
}

/// # Safety
/// `ptr`/`len` must come from `ge_alloc` or `ge_run` and be freed once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ge_free(ptr: *mut u8, len: usize) {
    drop(unsafe { Box::from_raw(slice::from_raw_parts_mut(ptr, len)) });
}

#[unsafe(no_mangle)]
pub extern "C" fn ge_verify(_ptr: *const u8, len: usize) -> i32 {
    (len != 0 && len % 4 == 0) as i32
}

/// # Safety
/// `ptr`/`len` must be a live `ge_alloc` buffer and `out` the host's slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ge_run(ptr: *const u8, len: usize, out: *mut [usize; 2]) -> i32 {
    if len % 4 != 0 {
        return -1;
    }
    let input = unsafe { slice::from_raw_parts(ptr, len) };
    let mut result = vec![0u8; len].into_boxed_slice();
    mix::mix_words(input, &mut result);
    unsafe { *out = [Box::into_raw(result) as *mut u8 as usize, len] };
    0
}

/// Minimal allocator over `memory.grow`; frees only the most recent block,
/// which is the host's call pattern (output, then input).
struct Bump;

static mut TOP: usize = 0;

unsafe impl core::alloc::GlobalAlloc for Bump {
    unsafe fn alloc(&self, layout: core::alloc::Layout) -> *mut u8 {
        unsafe {
            if TOP == 0 {
                TOP = arch::memory_size(0) << 16;
            }
            let start = (TOP + layout.align() - 1) & !(layout.align() - 1);
            let end = start + layout.size();
            let have = arch::memory_size(0) << 16;
            if end > have {
                let pages = (end - have).div_ceil(1 << 16);
                if arch::memory_grow(0, pages) == usize::MAX {
                    return core::ptr::null_mut();
                }
            }
            TOP = end;
            start as *mut u8
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: core::alloc::Layout) {
        unsafe {
            if ptr as usize + layout.size() == TOP {
                TOP = ptr as usize;
            }
        }
    }
}
