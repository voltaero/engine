//! ZeroMQ transport for the wire protocol in [`crate::wire`].
//!
//! - [`server`]: ROUTER socket in front of `ServerAPI`; one I/O thread owns the
//!   socket and every request is handled on the tokio runtime.
//! - [`client`]: blocking DEALER client for workers, submitters and operators.
//! - [`proxy`]: discovery service that tells clients which server to use.

pub mod client;
pub mod proxy;
pub mod server;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::error::Error;

/// How often background loops wake to check for shutdown.
pub(crate) const POLL_MS: i64 = 100;

/// One process-wide ZeroMQ context; contexts are thread-safe and meant to be
/// shared.
pub(crate) fn context() -> zmq::Context {
    static CTX: OnceLock<zmq::Context> = OnceLock::new();
    CTX.get_or_init(zmq::Context::new).clone()
}

pub(crate) fn zmq_err(what: &str, err: zmq::Error) -> Error {
    Error::io_error(format!("{what}: {err}"))
}

/// Compares two secrets in time independent of where they first differ; only
/// the length leaks.
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    subtle::ConstantTimeEq::ct_eq(a, b).into()
}

/// Background threads of a running server or proxy. Dropping it (or calling
/// [`Background::shutdown`]) stops them and waits for them to exit.
pub struct Background {
    endpoint: String,
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
}

impl Background {
    pub(crate) fn new(
        endpoint: String,
        stop: Arc<AtomicBool>,
        threads: Vec<JoinHandle<()>>,
    ) -> Self {
        Self {
            endpoint,
            stop,
            threads,
        }
    }

    /// The endpoint actually bound (wildcard ports resolved).
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn shutdown(self) {}

    /// Blocks until the background threads exit (e.g. after a fatal socket
    /// error). Never returns on a healthy instance.
    pub fn join(mut self) {
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

impl Drop for Background {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

/// Sleeps for `total`, waking early if `stop` is set. Returns false on stop.
pub(crate) fn sleep_unless_stopped(stop: &AtomicBool, total: Duration) -> bool {
    let step = Duration::from_millis(POLL_MS as u64);
    let mut slept = Duration::ZERO;
    while slept < total {
        if stop.load(Ordering::SeqCst) {
            return false;
        }
        std::thread::sleep(step.min(total - slept));
        slept += step;
    }
    !stop.load(Ordering::SeqCst)
}
