use std::sync::Arc;

use macros::Event;

use crate::{Identifier, plugin::LibraryMetadata};

#[derive(Clone, Debug, Event)]
#[event(namespace = "core", name = "auth_event", cancellable)]
pub struct StartEvent {
    pub modules: Vec<Arc<LibraryMetadata>>,
    pub cancelled: bool,
    pub id: Identifier,
}
