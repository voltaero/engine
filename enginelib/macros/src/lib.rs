use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, parse_macro_input};
#[proc_macro_attribute]
pub fn metadata(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item = proc_macro2::TokenStream::from(item);
    quote! {
        #[unsafe(export_name="metadata")]
        #item
    }
    .into()
}
#[proc_macro_attribute]
pub fn module(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item = proc_macro2::TokenStream::from(item);
    quote! {
        #[unsafe(export_name="run")]
        #item
    }
    .into()
}
#[proc_macro_derive(Verifiable)]
pub fn derive_verifiable(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    let expanded = quote! {
        impl Verifiable for #name {
            fn verify(&self, b: Vec<u8>) -> bool {
                let k: Result<#name, _> = enginelib::api::postcard::from_bytes(b.as_slice());
                k.is_ok()
            }
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(Event, attributes(event))]
pub fn derive_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // parse #[event(namespace = "core", name = "auth_event", cancellable)]
    let mut namespace = String::new();
    let mut event_name = String::new();
    let mut cancellable = false;

    for attr in &input.attrs {
        if attr.path().is_ident("event") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("namespace") {
                    let value = meta.value()?;
                    let lit: syn::LitStr = value.parse()?;
                    namespace = lit.value();
                } else if meta.path.is_ident("name") {
                    let value = meta.value()?;
                    let lit: syn::LitStr = value.parse()?;
                    event_name = lit.value();
                } else if meta.path.is_ident("cancellable") {
                    cancellable = true;
                }
                Ok(())
            })
            .unwrap();
        }
    }

    let cancel_impl = if cancellable {
        quote! { fn cancel(&mut self) { self.cancelled = true; } }
    } else {
        quote! { fn cancel(&mut self) { /* no-op */ } }
    };

    let is_cancelled_impl = if cancellable {
        quote! { fn is_cancelled(&self) -> bool { self.cancelled } }
    } else {
        quote! { fn is_cancelled(&self) -> bool { false } }
    };

    quote! {
        impl enginelib::event::Event for #name {
            fn clone_box(&self) -> Box<dyn enginelib::event::Event> { Box::new(self.clone()) }
            #cancel_impl
            #is_cancelled_impl
            fn get_id(&self) -> enginelib::Identifier {
                (#namespace.to_string(), #event_name.to_string())
            }
            fn as_any(&self) -> &dyn std::any::Any { self }
            fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }
        }
    }
    .into()
}

#[proc_macro_attribute]
pub fn event_handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item_fn = parse_macro_input!(item as syn::ItemFn);
    let fn_name = &item_fn.sig.ident;

    // parse attrs: event = AuthEvent, namespace = "core", name = "auth_event", receive_cancelled
    let mut ns = String::new();
    let mut ev_name = String::new();
    let mut receive_cancelled = false;

    let meta_parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("namespace") {
            ns = meta.value()?.parse::<syn::LitStr>()?.value();
        } else if meta.path.is_ident("name") {
            ev_name = meta.value()?.parse::<syn::LitStr>()?.value();
        } else if meta.path.is_ident("receive_cancelled") {
            receive_cancelled = true;
        }
        Ok(())
    });
    parse_macro_input!(attr with meta_parser);

    // extract the event param type (e.g. &mut AuthEvent -> AuthEvent)
    let event_type = match &item_fn.sig.inputs[0] {
        syn::FnArg::Typed(pat_type) => match &*pat_type.ty {
            syn::Type::Reference(type_ref) => match &*type_ref.elem {
                syn::Type::Path(type_path) => type_path.path.segments.last().unwrap().ident.clone(),
                _ => panic!("Expected path type for event"),
            },
            _ => panic!("Expected reference type for event"),
        },
        _ => panic!("Expected typed argument for event"),
    };

    let handler_name_str = fn_name
        .to_string()
        .split('_')
        .map(|s| {
            let mut c = s.chars();
            match c.next() {
                None => String::new(),
                Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
            }
        })
        .collect::<String>();
    let handler_name = syn::Ident::new(&format!("{}Handler", handler_name_str), fn_name.span());
    let register_fn_name = format_ident!("__register_{}", fn_name);

    let is_stateful = item_fn.sig.inputs.len() > 1; // more than just &mut Event

    let receive_cancelled_impl = if receive_cancelled {
        quote! { fn receive_cancelled(&self) -> bool { true } }
    } else {
        quote! { fn receive_cancelled(&self) -> bool { false } }
    };

    if is_stateful {
        // extract the context param type (e.g. &Arc<String> -> Arc<String>)
        let ctx_type = match &item_fn.sig.inputs[1] {
            syn::FnArg::Typed(pat_type) => match &*pat_type.ty {
                syn::Type::Reference(type_ref) => &*type_ref.elem,
                _ => &*pat_type.ty,
            },
            _ => panic!("Expected typed argument for context"),
        };

        quote! {
            #item_fn

            pub struct #handler_name {
                ctx: #ctx_type,
            }

            impl #handler_name {
                pub fn with_ctx(ctx: #ctx_type) -> Self {
                    Self { ctx }
                }
            }

            impl enginelib::event::EventCTX<#event_type> for #handler_name {
                fn expected_event_id() -> enginelib::Identifier {
                    (#ns.to_string(), #ev_name.to_string())
                }
                fn handleCTX(&self, event: &mut #event_type) {
                    #fn_name(event, &self.ctx);
                }
            }

            impl enginelib::event::EventHandler for #handler_name {
                fn handle(&self, event: &mut dyn enginelib::event::Event) {
                    let event = <Self as enginelib::event::EventCTX<#event_type>>::get_event(event);
                    #fn_name(event, &self.ctx);
                }
                #receive_cancelled_impl
            }

            fn #register_fn_name(event_bus: &mut enginelib::event::EventBus, ctx: #ctx_type) {
                event_bus.register_handler(
                    #handler_name::with_ctx(ctx),
                    (#ns.to_string(), #ev_name.to_string()),
                );
            }
        }
        .into()
    } else {
        quote! {
            #item_fn

            pub struct #handler_name;

            impl enginelib::event::EventCTX<#event_type> for #handler_name {
                fn expected_event_id() -> enginelib::Identifier {
                    (#ns.to_string(), #ev_name.to_string())
                }
                fn handleCTX(&self, event: &mut #event_type) {
                    #fn_name(event);
                }
            }

            impl enginelib::event::EventHandler for #handler_name {
                fn handle(&self, event: &mut dyn enginelib::event::Event) {
                    let event = <Self as enginelib::event::EventCTX<#event_type>>::get_event(event);
                    #fn_name(event);
                }
                #receive_cancelled_impl
            }

            fn #register_fn_name(event_bus: &mut enginelib::event::EventBus) {
                event_bus.register_handler(
                    #handler_name,
                    (#ns.to_string(), #ev_name.to_string()),
                );
            }
        }
        .into()
    }
}
