//! Example wasm task mod: each payload is little-endian u32 words, each result
//! is `ROUNDS` rounds of the lowbias32 integer hash applied to every word. The same
//! function ships as a WGSL kernel (`kernel.wgsl`) for the GPU path.
//! ABI: `docs/wasm.md`.

#![no_std]

extern crate alloc;

use alloc::boxed::Box;
use alloc::vec;
use core::slice;

#[global_allocator]
static ALLOC: Bump = Bump;

#[panic_handler]
fn panic(_: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

// Metadata and kernel are custom sections: the host reads them without
// running any of this module's code.
#[used]
#[unsafe(link_section = "ge:meta")]
static META: [u8; include_bytes!("../meta.toml").len()] = *include_bytes!("../meta.toml");

#[used]
#[unsafe(link_section = "ge:wgsl")]
static WGSL: [u8; include_bytes!("kernel.wgsl").len()] = *include_bytes!("kernel.wgsl");

/// Hash rounds per word; keep in sync with `kernel.wgsl`. High enough that the
/// work is compute-bound rather than PCIe-bound.
const ROUNDS: u32 = 1024;

fn mix(mut x: u32) -> u32 {
    for _ in 0..ROUNDS {
        x ^= x >> 16;
        x = x.wrapping_mul(0x7feb352d);
        x ^= x >> 15;
        x = x.wrapping_mul(0x846ca68b);
        x ^= x >> 16;
    }
    x
}

#[unsafe(no_mangle)]
pub extern "C" fn ge_alloc(len: u32) -> *mut u8 {
    Box::into_raw(vec![0u8; len as usize].into_boxed_slice()).cast()
}

/// # Safety
/// `ptr`/`len` must come from `ge_alloc` or `ge_run` and be freed once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ge_free(ptr: *mut u8, len: u32) {
    drop(unsafe { Box::from_raw(slice::from_raw_parts_mut(ptr, len as usize)) });
}

#[unsafe(no_mangle)]
pub extern "C" fn ge_verify(_ptr: *const u8, len: u32) -> u32 {
    (len != 0 && len % 4 == 0) as u32
}

/// # Safety
/// `ptr`/`len` must be a live `ge_alloc` buffer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ge_run(ptr: *const u8, len: u32) -> i64 {
    if len % 4 != 0 {
        return -1;
    }
    let input = unsafe { slice::from_raw_parts(ptr, len as usize) };
    let mut out = vec![0u8; input.len()].into_boxed_slice();
    for (dst, src) in out.chunks_exact_mut(4).zip(input.chunks_exact(4)) {
        let word = u32::from_le_bytes([src[0], src[1], src[2], src[3]]);
        dst.copy_from_slice(&mix(word).to_le_bytes());
    }
    let out_ptr = Box::into_raw(out) as *mut u8 as u32;
    ((out_ptr as i64) << 32) | len as i64
}

/// Minimal allocator over `memory.grow`; frees only the most recent block,
/// which is the host's call pattern (output, then input).
struct Bump;

static mut TOP: usize = 0;

unsafe impl core::alloc::GlobalAlloc for Bump {
    unsafe fn alloc(&self, layout: core::alloc::Layout) -> *mut u8 {
        unsafe {
            if TOP == 0 {
                TOP = core::arch::wasm32::memory_size(0) << 16;
            }
            let start = (TOP + layout.align() - 1) & !(layout.align() - 1);
            let end = start + layout.size();
            let have = core::arch::wasm32::memory_size(0) << 16;
            if end > have {
                let pages = (end - have).div_ceil(1 << 16);
                if core::arch::wasm32::memory_grow(0, pages) == usize::MAX {
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
