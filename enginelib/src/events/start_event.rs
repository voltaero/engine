use std::{any::Any, process, sync::Arc};

use tracing::info;

use crate::{
    Identifier,
    api::EngineAPI,
    event::Event,
    plugin::{LibraryManager, LibraryMetadata},
};

use super::{Events, ID};

#[derive(Clone, Debug)]
pub struct StartEvent {
    pub modules: Vec<Arc<LibraryMetadata>>,
    pub cancelled: bool,
    pub id: Identifier,
}
