use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The module, its metadata or its kernel was rejected at load time.
    Load(String),
    /// A call ran past `SandboxLimits::call_timeout` (or the GPU timeout).
    Timeout,
    /// The guest trapped (out-of-bounds access, unreachable, out of memory...).
    Trap(String),
    /// `ge_run` returned this negative code.
    Guest(i64),
    /// Input or output broke the host contract (size, alignment, bounds).
    Invalid(String),
    /// No GPU, or the GPU rejected/lost the work.
    Gpu(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Load(msg) => write!(f, "load: {msg}"),
            Error::Timeout => write!(f, "timed out"),
            Error::Trap(msg) => write!(f, "trap: {msg}"),
            Error::Guest(code) => write!(f, "guest error code {code}"),
            Error::Invalid(msg) => write!(f, "invalid: {msg}"),
            Error::Gpu(msg) => write!(f, "gpu: {msg}"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
