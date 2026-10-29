// GPU twin of `mix` in lib.rs: must produce the same bytes.
@group(0) @binding(0) var<storage, read> input: array<u32>;
@group(0) @binding(1) var<storage, read_write> output: array<u32>;

fn mix(v: u32) -> u32 {
    var x = v;
    for (var i = 0u; i < 1024u; i = i + 1u) {
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
