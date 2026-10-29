//! The GPU path: a mod's `ge:wgsl` kernel run through wgpu (WebGPU on
//! Vulkan/Metal/DX12).
//!
//! The kernel never sees host memory. It gets exactly two storage buffers the
//! host fills and reads back, wgpu compiles it with runtime bounds checks, and
//! naga checks the binding contract at load time:
//!
//! ```wgsl
//! @group(0) @binding(0) var<storage, read> input: array<u32>;
//! @group(0) @binding(1) var<storage, read_write> output: array<u32>;
//! @compute @workgroup_size(64)
//! fn main(@builtin(global_invocation_id) id: vec3<u32>) { ... }
//! ```
//!
//! One invocation handles one record (`gpu.record_words` u32s); invocation `i`
//! owns `input[i*R..(i+1)*R]` and writes `output` at the same offsets. Many
//! payloads are packed into one dispatch, so a kernel must bound its index by
//! `arrayLength(&input)` and must not look across records.

use std::sync::Arc;
use std::sync::mpsc;

use naga::valid::{Capabilities, ValidationFlags, Validator};
use naga::{AddressSpace, ShaderStage, StorageAccess};
use tracing::warn;
use wgpu::util::DeviceExt;

use crate::error::{Error, Result};
use crate::sandbox::WasmMod;

const MAX_WORKGROUP_SIZE: u32 = 256;

/// Parses and validates `src` with naga and checks the binding contract.
/// Returns the entry point's workgroup size (x; y and z must be 1).
pub fn check_kernel(src: &str, entry: &str) -> Result<u32> {
    let module = naga::front::wgsl::parse_str(src)
        .map_err(|e| Error::Load(format!("wgsl: {}", e.emit_to_string(src))))?;
    Validator::new(ValidationFlags::all(), Capabilities::empty())
        .validate(&module)
        .map_err(|e| Error::Load(format!("wgsl validation: {}", e.emit_to_string(src))))?;

    let ep = module
        .entry_points
        .iter()
        .find(|ep| ep.name == entry && ep.stage == ShaderStage::Compute)
        .ok_or_else(|| Error::Load(format!("no @compute entry point `{entry}`")))?;
    if ep.workgroup_size_overrides.is_some() {
        return Err(Error::Load("workgroup size must be a literal".into()));
    }
    let [x, y, z] = ep.workgroup_size;
    if y != 1 || z != 1 || !(1..=MAX_WORKGROUP_SIZE).contains(&x) {
        return Err(Error::Load(format!(
            "workgroup size must be (1..={MAX_WORKGROUP_SIZE}, 1, 1), got ({x}, {y}, {z})"
        )));
    }

    let (mut input, mut output) = (false, false);
    for (_, var) in module.global_variables.iter() {
        let name = var.name.as_deref().unwrap_or("?");
        match (var.space, &var.binding) {
            (AddressSpace::Private | AddressSpace::WorkGroup, None) => {}
            (AddressSpace::Storage { access }, Some(b)) if b.group == 0 && b.binding == 0 => {
                if access != StorageAccess::LOAD {
                    return Err(Error::Load(format!(
                        "`{name}` (binding 0) must be read-only"
                    )));
                }
                input = true;
            }
            (AddressSpace::Storage { access }, Some(b)) if b.group == 0 && b.binding == 1 => {
                if access != StorageAccess::LOAD | StorageAccess::STORE {
                    return Err(Error::Load(format!(
                        "`{name}` (binding 1) must be read_write"
                    )));
                }
                output = true;
            }
            _ => {
                return Err(Error::Load(format!(
                    "global `{name}`: only @group(0) @binding(0) read and @binding(1) \
                     read_write storage buffers are allowed"
                )));
            }
        }
    }
    if !(input && output) {
        return Err(Error::Load(
            "kernel must declare bindings 0 (input) and 1 (output)".into(),
        ));
    }
    Ok(x)
}

/// A GPU device. Share one per process; it is `Send + Sync`.
pub struct Gpu {
    device: wgpu::Device,
    queue: wgpu::Queue,
    info: wgpu::AdapterInfo,
    limits: wgpu::Limits,
}

impl Gpu {
    /// Picks the high-performance adapter. Err if there is no usable GPU.
    pub fn new() -> Result<Self> {
        let instance =
            wgpu::Instance::new(wgpu::InstanceDescriptor::new_without_display_handle_from_env());
        let adapter = pollster::block_on(instance.request_adapter(&wgpu::RequestAdapterOptions {
            power_preference: wgpu::PowerPreference::HighPerformance,
            ..Default::default()
        }))
        .map_err(|e| Error::Gpu(format!("no adapter: {e}")))?;
        let info = adapter.get_info();
        if info.device_type == wgpu::DeviceType::Cpu
            && std::env::var_os("GE_GPU_ALLOW_CPU").is_none()
        {
            return Err(Error::Gpu(format!(
                "only a software adapter ({})",
                info.name
            )));
        }
        let limits = adapter.limits();
        let (device, queue) = pollster::block_on(adapter.request_device(&wgpu::DeviceDescriptor {
            label: Some("enginewasm"),
            required_limits: limits.clone(),
            ..Default::default()
        }))
        .map_err(|e| Error::Gpu(format!("request device: {e}")))?;
        // Errors are captured by scopes around each call; anything that still
        // escapes is logged instead of wgpu's default panic.
        device.on_uncaptured_error(Arc::new(|e| warn!("wgpu: {e}")));
        Ok(Self {
            device,
            queue,
            info,
            limits,
        })
    }

    pub fn info(&self) -> &wgpu::AdapterInfo {
        &self.info
    }

    /// Compiles a mod's kernel for this device.
    pub fn kernel(&self, m: &WasmMod) -> Result<GpuKernel> {
        let (Some(gpu), Some(src)) = (&m.meta().gpu, m.wgsl()) else {
            return Err(Error::Gpu(format!("{:?} has no GPU kernel", m.task_type())));
        };
        let workgroup_size = check_kernel(src, &gpu.entry)?;
        let storage = |binding, read_only| wgpu::BindGroupLayoutEntry {
            binding,
            visibility: wgpu::ShaderStages::COMPUTE,
            ty: wgpu::BindingType::Buffer {
                ty: wgpu::BufferBindingType::Storage { read_only },
                has_dynamic_offset: false,
                min_binding_size: None,
            },
            count: None,
        };

        let scope = self.device.push_error_scope(wgpu::ErrorFilter::Validation);
        let module = self
            .device
            .create_shader_module(wgpu::ShaderModuleDescriptor {
                label: Some("ge:wgsl"),
                source: wgpu::ShaderSource::Wgsl(src.into()),
            });
        let layout = self
            .device
            .create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
                label: Some("ge:io"),
                entries: &[storage(0, true), storage(1, false)],
            });
        let pipeline_layout = self
            .device
            .create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
                label: Some("ge:io"),
                bind_group_layouts: &[Some(&layout)],
                immediate_size: 0,
            });
        let pipeline = self
            .device
            .create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
                label: Some("ge:kernel"),
                layout: Some(&pipeline_layout),
                module: &module,
                entry_point: Some(&gpu.entry),
                compilation_options: Default::default(),
                cache: None,
            });
        if let Some(err) = pollster::block_on(scope.pop()) {
            return Err(Error::Load(format!("kernel rejected by wgpu: {err}")));
        }

        let record_bytes = gpu.record_words as usize * 4;
        let max_records =
            self.limits.max_compute_workgroups_per_dimension as u64 * workgroup_size as u64;
        let max_batch = [
            self.limits.max_storage_buffer_binding_size,
            self.limits.max_buffer_size,
            max_records * record_bytes as u64,
        ]
        .into_iter()
        .min()
        .unwrap_or(0) as usize
            / record_bytes
            * record_bytes;
        Ok(GpuKernel {
            pipeline,
            layout,
            workgroup_size,
            record_bytes,
            max_batch,
            max_io: m.task_limits().max_payload.unwrap_or(u32::MAX) as usize,
            timeout: m.limits().gpu_timeout,
        })
    }

    /// Runs one result per payload, packing payloads into as few dispatches
    /// as the device limits allow. A payload that isn't a whole, non-empty
    /// number of records fails alone.
    pub fn run_batch(&self, k: &GpuKernel, payloads: &[&[u8]]) -> Vec<Result<Vec<u8>>> {
        let mut out: Vec<Result<Vec<u8>>> = Vec::with_capacity(payloads.len());
        // (index into `out`, payload) of the batch being packed.
        let mut batch: Vec<(usize, &[u8])> = Vec::new();
        let mut bytes = 0usize;
        for payload in payloads {
            let i = out.len();
            out.push(Ok(Vec::new()));
            if payload.is_empty() || payload.len() % k.record_bytes != 0 {
                out[i] = Err(Error::Invalid(format!(
                    "payload of {} bytes is not a whole number of {}-byte records",
                    payload.len(),
                    k.record_bytes
                )));
                continue;
            }
            if payload.len() > k.max_io || payload.len() > k.max_batch {
                out[i] = Err(Error::Invalid("payload too large for the GPU path".into()));
                continue;
            }
            if bytes + payload.len() > k.max_batch {
                self.flush(k, &mut batch, &mut out);
                bytes = 0;
            }
            bytes += payload.len();
            batch.push((i, payload));
        }
        self.flush(k, &mut batch, &mut out);
        out
    }

    fn flush(&self, k: &GpuKernel, batch: &mut Vec<(usize, &[u8])>, out: &mut [Result<Vec<u8>>]) {
        if batch.is_empty() {
            return;
        }
        let mut input = Vec::with_capacity(batch.iter().map(|(_, p)| p.len()).sum());
        for (_, payload) in batch.iter() {
            input.extend_from_slice(payload);
        }
        match self.dispatch(k, &input) {
            Ok(result) => {
                let mut at = 0;
                for (i, payload) in batch.iter() {
                    out[*i] = Ok(result[at..at + payload.len()].to_vec());
                    at += payload.len();
                }
            }
            Err(err) => {
                for (i, _) in batch.iter() {
                    out[*i] = Err(err.clone());
                }
            }
        }
        batch.clear();
    }

    fn dispatch(&self, k: &GpuKernel, input: &[u8]) -> Result<Vec<u8>> {
        let size = input.len() as u64;
        let records = (input.len() / k.record_bytes) as u32;
        let groups = records.div_ceil(k.workgroup_size);

        let validation = self.device.push_error_scope(wgpu::ErrorFilter::Validation);
        let oom = self.device.push_error_scope(wgpu::ErrorFilter::OutOfMemory);
        let input = self
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("ge:input"),
                contents: input,
                usage: wgpu::BufferUsages::STORAGE,
            });
        let output = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("ge:output"),
            size,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
            mapped_at_creation: false,
        });
        let readback = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("ge:readback"),
            size,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        let bind = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("ge:io"),
            layout: &k.layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: input.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: output.as_entire_binding(),
                },
            ],
        });
        let mut encoder = self.device.create_command_encoder(&Default::default());
        {
            let mut pass = encoder.begin_compute_pass(&Default::default());
            pass.set_pipeline(&k.pipeline);
            pass.set_bind_group(0, &bind, &[]);
            pass.dispatch_workgroups(groups, 1, 1);
        }
        encoder.copy_buffer_to_buffer(&output, 0, &readback, 0, size);
        let submission = self.queue.submit([encoder.finish()]);
        let (tx, rx) = mpsc::channel();
        readback.slice(..).map_async(wgpu::MapMode::Read, move |r| {
            let _ = tx.send(r);
        });
        let oom = pollster::block_on(oom.pop());
        let validation = pollster::block_on(validation.pop());
        if let Some(err) = oom.or(validation) {
            return Err(Error::Gpu(err.to_string()));
        }

        match self.device.poll(wgpu::PollType::Wait {
            submission_index: Some(submission),
            timeout: Some(k.timeout),
        }) {
            Ok(_) => {}
            Err(wgpu::PollError::Timeout) => return Err(Error::Timeout),
            Err(err) => return Err(Error::Gpu(err.to_string())),
        }
        match rx.try_recv() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(Error::Gpu(format!("map readback: {err}"))),
            Err(_) => return Err(Error::Gpu("readback not mapped after wait".into())),
        }
        let result = readback
            .slice(..)
            .get_mapped_range()
            .map_err(|e| Error::Gpu(format!("read mapped range: {e}")))?
            .to_vec();
        readback.unmap();
        Ok(result)
    }
}

/// A mod's kernel compiled for one [`Gpu`].
pub struct GpuKernel {
    pipeline: wgpu::ComputePipeline,
    layout: wgpu::BindGroupLayout,
    workgroup_size: u32,
    record_bytes: usize,
    /// Most bytes one dispatch may carry on this device.
    max_batch: usize,
    max_io: usize,
    timeout: std::time::Duration,
}
