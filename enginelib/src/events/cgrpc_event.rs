use std::{
    any::Any,
    sync::{Arc, RwLock},
};

use crate::{Identifier, api::EngineAPI, event::Event};

use super::{Events, ID};

#[derive(Clone, Debug)]
pub struct CgrpcEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub handler_id: Identifier,
    pub payload: Vec<u8>,
    pub output: Arc<RwLock<Vec<u8>>>,
}
#[macro_export]
macro_rules! RegisterCgrpcEventHandler {
    ($handler:ident,$handler_mod_id:ident,$handler_id:ident,$handler_fn:expr) => {
        pub struct $handler;
        impl EventHandler for $handler {
            fn handle(&self, event: &mut dyn Event) {
                let event: &mut CgrpcEvent =
                    <Self as EventCTX<CgrpcEvent>>::get_event::<CgrpcEvent>(event);
                self.handleCTX(event);
            }
        }
        impl EventCTX<CgrpcEvent> for $handler {
            fn handleCTX(&self, event: &mut CgrpcEvent) {
                let id: (String, String) = (
                    stringify!($handler_mod_id).to_string(),
                    stringify!($handler_id).to_string(),
                );
                if (id == event.handler_id) {
                    $handler_fn(event)
                }
            }
        }
    };
}
