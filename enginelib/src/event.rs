use crate::Identifier;
use crate::Registry;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::instrument;
pub use tracing::{debug, error, event, info, warn};

pub struct EventBus {
    pub event_handler_registry: EngineEventHandlerRegistry,
}
impl Debug for EventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}
pub trait Event: Any + Send + Sync + Debug {
    fn clone_box(&self) -> Box<dyn Event>;
    fn cancel(&mut self);
    fn is_cancelled(&self) -> bool;
    fn get_id(&self) -> Identifier;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

pub trait EventHandler: Any + Send + Sync {
    fn handle(&self, event: &mut dyn Event);
}

#[derive(Clone, Default)]
pub struct EngineEventHandlerRegistry {
    pub event_handlers: HashMap<Identifier, Vec<Arc<dyn EventHandler>>>,
}

impl EngineEventHandlerRegistry {
    pub fn register_handler<H: EventHandler + Send + Sync + 'static>(
        &mut self,
        handler: H,
        identifier: Identifier,
    ) {
        let handler = Arc::new(handler);
        let handlers = self.event_handlers.entry(identifier.clone()).or_default();
        handlers.push(handler);
        debug!(
            "EventBus: Registered handler for event {}.{}",
            identifier.0, identifier.1
        );
    }
}

impl EventBus {
    #[instrument]
    pub fn handle<T: Event>(&self, id: Identifier, event: &mut T) {
        debug!("EventBus: Processing event {}.{}", id.0, id.1);
        let handlers: Option<&Vec<Arc<dyn EventHandler>>> =
            self.event_handler_registry.event_handlers.get(&id);

        if let Some(handlers) = handlers {
            for handler in handlers {
                handler.handle(event)
            }
        } else {
            debug!(
                "EventBus: No event handlers subscribed for event {}.{}",
                id.0, id.1
            );
        }
    }
}
