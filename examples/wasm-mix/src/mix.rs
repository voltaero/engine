//! The task itself, free of any wasm or engine detail: `lib.rs` exports it to
//! the host, `kernel.wgsl` mirrors it for the GPU, and `wasm_worker --bench`
//! compiles this same file natively as the no-wasm baseline.

/// Hash rounds per word; keep in sync with `kernel.wgsl`. High enough that the
/// work is compute-bound rather than PCIe-bound.
pub const ROUNDS: u32 = 1024;

/// lowbias32, `ROUNDS` times.
pub fn mix(mut x: u32) -> u32 {
    for _ in 0..ROUNDS {
        x ^= x >> 16;
        x = x.wrapping_mul(0x7feb352d);
        x ^= x >> 15;
        x = x.wrapping_mul(0x846ca68b);
        x ^= x >> 16;
    }
    x
}

/// Applies [`mix`] to every little-endian u32 of `input`. `out` has its length.
pub fn mix_words(input: &[u8], out: &mut [u8]) {
    let (out, _) = out.as_chunks_mut::<4>();
    for (dst, src) in out.iter_mut().zip(input.as_chunks::<4>().0) {
        *dst = mix(u32::from_le_bytes(*src)).to_le_bytes();
    }
}
