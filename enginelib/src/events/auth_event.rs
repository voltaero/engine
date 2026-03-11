use std::{
    any::Any,
    sync::{Arc, RwLock},
};

use sled::Db;

use crate::{Identifier, api::EngineAPI, event::Event};

use super::{Events, ID};

#[derive(Clone, Debug)]
pub struct AuthEvent {
    pub cancelled: bool,
    pub id: Identifier,
    pub uid: String,
    pub challenge: String,
    pub db: Db,
    pub output: Arc<RwLock<bool>>,
}
#[macro_export]
macro_rules! RegisterAuthEventHandler {
    ($handler:ident,$handler_fn:expr) => {
        use crate::event::Event;
        use crate::event::EventCTX;
        use crate::event::EventHandler;
        use crate::events::auth_event::AuthEvent;
        pub struct $handler;
        impl EventHandler for $handler {
            fn handle(&self, event: &mut dyn Event) {
                let event: &mut AuthEvent =
                    <Self as EventCTX<AuthEvent>>::get_event::<AuthEvent>(event);
                self.handleCTX(event);
            }
        }
        impl EventCTX<AuthEvent> for $handler {
            fn handleCTX(&self, event: &mut AuthEvent) {
                $handler_fn(event)
            }
        }
    };
}
