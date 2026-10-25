//! Mod metadata, read from the module's custom sections *before* any guest
//! code is compiled or run.
//!
//! - `ge:meta`: TOML, see [`ModMeta`].
//! - `ge:wgsl`: optional WGSL compute kernel (UTF-8), required iff `[gpu]` is set.

use serde::Deserialize;
use wasmparser::{Parser, Payload};

use crate::error::{Error, Result};

pub const META_SECTION: &str = "ge:meta";
pub const WGSL_SECTION: &str = "ge:wgsl";

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModMeta {
    /// Task type namespace and name: `[A-Za-z0-9_-]+` each, since they become
    /// part of the `t:<ns>:<name>:` storage keys.
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub version: String,
    /// Suggested limits, as `TaskLimits` (config.toml still overrides them).
    /// Sizes are also capped by the host's `SandboxLimits::max_io`.
    pub chunk_size: Option<u32>,
    pub max_payload: Option<u32>,
    pub max_result: Option<u32>,
    pub gpu: Option<GpuMeta>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GpuMeta {
    /// Compute entry point in `ge:wgsl`.
    #[serde(default = "default_entry")]
    pub entry: String,
    /// u32 words per record. One invocation handles one record, so a payload
    /// must be a whole number of records and the host may pack many payloads
    /// into one dispatch. The result of a payload is the output words at the
    /// same offsets, i.e. it has the payload's length.
    #[serde(default = "default_record_words")]
    pub record_words: u32,
}

fn default_entry() -> String {
    "main".into()
}

fn default_record_words() -> u32 {
    1
}

/// Metadata plus the raw kernel source, as found in the module.
#[derive(Debug, Clone)]
pub struct Sections {
    pub meta: ModMeta,
    pub wgsl: Option<String>,
}

pub fn read_sections(wasm: &[u8]) -> Result<Sections> {
    let mut meta = None;
    let mut wgsl = None;
    for payload in Parser::new(0).parse_all(wasm) {
        let payload = payload.map_err(|e| Error::Load(format!("malformed module: {e}")))?;
        let Payload::CustomSection(section) = payload else {
            continue;
        };
        let slot = match section.name() {
            META_SECTION => &mut meta,
            WGSL_SECTION => &mut wgsl,
            _ => continue,
        };
        if slot.is_some() {
            return Err(Error::Load(format!(
                "duplicate `{}` section",
                section.name()
            )));
        }
        let text = std::str::from_utf8(section.data())
            .map_err(|_| Error::Load(format!("`{}` is not UTF-8", section.name())))?;
        *slot = Some(text.to_owned());
    }
    let meta = meta.ok_or_else(|| Error::Load(format!("missing `{META_SECTION}` section")))?;
    let meta: ModMeta =
        toml::from_str(&meta).map_err(|e| Error::Load(format!("bad `{META_SECTION}`: {e}")))?;
    meta.validate()?;
    match (&meta.gpu, &wgsl) {
        (Some(_), None) => {
            return Err(Error::Load(format!(
                "[gpu] set but no `{WGSL_SECTION}` section"
            )));
        }
        (None, Some(_)) => {
            return Err(Error::Load(format!(
                "`{WGSL_SECTION}` present but no [gpu] table"
            )));
        }
        _ => {}
    }
    Ok(Sections { meta, wgsl })
}

impl ModMeta {
    fn validate(&self) -> Result<()> {
        for (what, value) in [("namespace", &self.namespace), ("name", &self.name)] {
            let ok = !value.is_empty()
                && value.len() <= 64
                && value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
            if !ok {
                return Err(Error::Load(format!(
                    "{what} {value:?} must be 1..=64 of [A-Za-z0-9_-]"
                )));
            }
        }
        if self.chunk_size.is_some_and(|n| n == 0 || n > 65536) {
            return Err(Error::Load("chunk_size must be 1..=65536".into()));
        }
        if let Some(gpu) = &self.gpu
            && !(1..=1024).contains(&gpu.record_words)
        {
            return Err(Error::Load("gpu.record_words must be 1..=1024".into()));
        }
        Ok(())
    }
}
