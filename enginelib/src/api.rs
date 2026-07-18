use chrono::Utc;
use dashmap::{DashMap, DashSet};
use tokio::{spawn, time::interval};
use tracing::{Level, debug, info, instrument};

use crate::{
    Identifier, Registry,
    config::Config,
    event::{EngineEventHandlerRegistry, EventBus},
    plugin::LibraryManager,
};
pub use postcard;
pub use postcard::from_bytes;
pub use postcard::to_allocvec;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub struct ServerAPI {
    pub cfg: Config,                 // RW
    pub event_bus: EventBus,         // RW
    pub lib_manager: LibraryManager, // RW
}
