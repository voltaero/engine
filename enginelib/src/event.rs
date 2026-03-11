use crate::{Identifier, api::EngineAPI};
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::instrument;
pub use tracing::{debug, error, event, info, warn};

pub struct EventRegistrar {
    pub origin: &'static str,
    pub func: fn(&mut EngineAPI), // function signature
}
inventory::collect!(EventRegistrar);

pub fn register_inventory_handlers(api: &mut EngineAPI) {
    register_inventory_handlers_inner(api, None);
}

pub fn register_inventory_handlers_for_origin(api: &mut EngineAPI, origin: &'static str) {
    register_inventory_handlers_inner(api, Some(origin));
}

fn register_inventory_handlers_inner(api: &mut EngineAPI, origin: Option<&'static str>) {
    for item in inventory::iter::<EventRegistrar> {
        if let Some(origin) = origin {
            if item.origin != origin {
                continue;
            }
        }
        (item.func)(api);
    }
}
pub trait EventCTX<C: Event>: EventHandler {
    fn expected_event_id() -> Identifier;

    fn get_event(event: &mut dyn Event) -> &mut C {
        // Runtime validation: ensure event ID matches before unsafe cast
        assert_eq!(
            event.get_id(),
            Self::expected_event_id(),
            "Event ID mismatch: handler expects {:?}, got {:?}",
            Self::expected_event_id(),
            event.get_id()
        );
        // Safety: Event ID verified, type is correct
        unsafe { &mut *(event as *mut dyn Event as *mut C) }
    }

    fn handle(&self, event: &mut dyn Event) {
        let event: &mut C = Self::get_event(event);
        self.handleCTX(event);
    }

    fn handleCTX(&self, event: &mut C);
}

pub struct EventBus {
    pub event_handler_registry: EngineEventHandlerRegistry,
}
impl Debug for EventBus {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    fn receive_cancelled(&self) -> bool {
        false
    }
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
    pub fn register_handler<H: EventHandler + Send + Sync + 'static>(
        &mut self,
        handler: H,
        identifier: Identifier,
    ) {
        self.event_handler_registry
            .register_handler(handler, identifier);
    }

    #[instrument]
    pub fn fire<T: Event>(&self, event: &mut T) {
        let id = event.get_id();
        debug!("EventBus: Firing event {}.{}", id.0, id.1);

        if let Some(handlers) = self.event_handler_registry.event_handlers.get(&id) {
            for handler in handlers {
                if event.is_cancelled() && !handler.receive_cancelled() {
                    continue;
                }
                handler.handle(event);
            }
        } else {
            debug!("EventBus: No handlers for event {}.{}", id.0, id.1);
        }
    }
}
