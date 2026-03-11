use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    DeriveInput, Expr, Ident, ItemFn, LitStr, Path, Token,
    parse::{Parse, ParseStream},
    parse_macro_input, parse_quote,
};
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
    let mut item_fn = parse_macro_input!(item as ItemFn);

    let api_ident = match item_fn.sig.inputs.first() {
        Some(syn::FnArg::Typed(pat_type)) => match &*pat_type.pat {
            syn::Pat::Ident(pat_ident) => pat_ident.ident.clone(),
            _ => {
                return syn::Error::new_spanned(
                    &pat_type.pat,
                    "expected first argument to be an identifier (e.g. `api: &mut EngineAPI`)",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                &item_fn.sig,
                "`#[module]` expects a function like `fn run(api: &mut EngineAPI)`",
            )
            .to_compile_error()
            .into();
        }
    };

    item_fn.block.stmts.insert(
        0,
        parse_quote!(::enginelib::event::register_inventory_handlers_for_origin(
            #api_ident,
            env!("CARGO_PKG_NAME"),
        );),
    );
    item_fn.attrs.push(parse_quote!(#[unsafe(export_name="run")]));

    quote!(#item_fn).into()
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

struct EventHandlerArgs {
    namespace: LitStr,
    name: LitStr,
    receive_cancelled: bool,
    ctx: Option<Expr>,
    ctx_fn: Option<Path>,
}

impl Parse for EventHandlerArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut namespace: Option<LitStr> = None;
        let mut name: Option<LitStr> = None;
        let mut receive_cancelled = false;
        let mut ctx: Option<Expr> = None;
        let mut ctx_fn: Option<Path> = None;

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            match key.to_string().as_str() {
                "receive_cancelled" => {
                    receive_cancelled = true;
                }
                "namespace" => {
                    input.parse::<Token![=]>()?;
                    namespace = Some(input.parse::<LitStr>()?);
                }
                "name" => {
                    input.parse::<Token![=]>()?;
                    name = Some(input.parse::<LitStr>()?);
                }
                "ctx" => {
                    input.parse::<Token![=]>()?;
                    ctx = Some(input.parse::<Expr>()?);
                }
                "ctx_fn" => {
                    input.parse::<Token![=]>()?;
                    ctx_fn = Some(input.parse::<Path>()?);
                }
                _ => {
                    return Err(syn::Error::new_spanned(
                        key,
                        "unknown argument (expected `namespace`, `name`, `receive_cancelled`, `ctx`, or `ctx_fn`)",
                    ));
                }
            }

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        let namespace = namespace.ok_or_else(|| {
            syn::Error::new(proc_macro2::Span::call_site(), "missing `namespace = \"...\"`")
        })?;
        let name = name.ok_or_else(|| {
            syn::Error::new(proc_macro2::Span::call_site(), "missing `name = \"...\"`")
        })?;

        if ctx.is_some() && ctx_fn.is_some() {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "use only one of `ctx = ...` or `ctx_fn = ...`",
            ));
        }

        Ok(Self {
            namespace,
            name,
            receive_cancelled,
            ctx,
            ctx_fn,
        })
    }
}

#[proc_macro_attribute]
pub fn event_handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as EventHandlerArgs);
    let EventHandlerArgs {
        namespace,
        name,
        receive_cancelled,
        ctx,
        ctx_fn,
    } = args;
    let item_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &item_fn.sig.ident;

    // extract the event param type (e.g. &mut AuthEvent -> AuthEvent)
    let event_type: syn::Type = match item_fn.sig.inputs.first() {
        Some(syn::FnArg::Typed(pat_type)) => match &*pat_type.ty {
            syn::Type::Reference(type_ref) => (*type_ref.elem).clone(),
            _ => {
                return syn::Error::new_spanned(
                    &pat_type.ty,
                    "expected first parameter to be a reference (e.g. `event: &mut MyEvent`)",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                &item_fn.sig,
                "expected first parameter to be the event (e.g. `fn handler(event: &mut MyEvent)`)",
            )
            .to_compile_error()
            .into();
        }
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
    let inventory_register_fn_name = format_ident!("__inventory_register_{}", fn_name);

    let is_stateful = item_fn.sig.inputs.len() > 1; // more than just &mut Event

    let receive_cancelled_impl = if receive_cancelled {
        quote! { fn receive_cancelled(&self) -> bool { true } }
    } else {
        quote! { fn receive_cancelled(&self) -> bool { false } }
    };

    if is_stateful {
        // extract the context param type (e.g. &Arc<String> -> Arc<String>)
        let ctx_type: syn::Type = match item_fn.sig.inputs.iter().nth(1) {
            Some(syn::FnArg::Typed(pat_type)) => match &*pat_type.ty {
                syn::Type::Reference(type_ref) => {
                    if type_ref.mutability.is_some() {
                        return syn::Error::new_spanned(
                            &pat_type.ty,
                            "context parameter must be a shared reference (e.g. `ctx: &MyCtx`)",
                        )
                        .to_compile_error()
                        .into();
                    }
                    (*type_ref.elem).clone()
                }
                _ => {
                    return syn::Error::new_spanned(
                        &pat_type.ty,
                        "context parameter must be a reference (e.g. `ctx: &MyCtx`)",
                    )
                    .to_compile_error()
                    .into();
                }
            },
            _ => {
                return syn::Error::new_spanned(
                    &item_fn.sig,
                    "expected second parameter to be handler context (e.g. `fn handler(event: &mut E, ctx: &Ctx)`)",
                )
                .to_compile_error()
                .into();
            }
        };

        let maybe_inventory_registrar = match (ctx, ctx_fn) {
            (Some(ctx_expr), None) => quote! {
                fn #inventory_register_fn_name(api: &mut ::enginelib::api::EngineAPI) {
                    let ctx: #ctx_type = (#ctx_expr);
                    #register_fn_name(api, ctx);
                }

                ::enginelib::inventory::submit! {
                    ::enginelib::event::EventRegistrar {
                        origin: env!("CARGO_PKG_NAME"),
                        func: #inventory_register_fn_name,
                    }
                }
            },
            (None, Some(ctx_fn)) => quote! {
                fn #inventory_register_fn_name(api: &mut ::enginelib::api::EngineAPI) {
                    let ctx: #ctx_type = #ctx_fn(api);
                    #register_fn_name(api, ctx);
                }

                ::enginelib::inventory::submit! {
                    ::enginelib::event::EventRegistrar {
                        origin: env!("CARGO_PKG_NAME"),
                        func: #inventory_register_fn_name,
                    }
                }
            },
            (None, None) => quote! {},
            (Some(_), Some(_)) => unreachable!(),
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
                    (#namespace.to_string(), #name.to_string())
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

            pub fn #register_fn_name(api: &mut enginelib::api::EngineAPI, ctx: #ctx_type) {
                api.event_bus.register_handler(
                    #handler_name::with_ctx(ctx),
                    (#namespace.to_string(), #name.to_string()),
                );
            }

            #maybe_inventory_registrar
        }
        .into()
    } else {
        let inventory_registrar = quote! {
            ::enginelib::inventory::submit! {
                ::enginelib::event::EventRegistrar {
                    origin: env!("CARGO_PKG_NAME"),
                    func: #register_fn_name,
                }
            }
        };

        quote! {
            #item_fn

            pub struct #handler_name;

            impl enginelib::event::EventCTX<#event_type> for #handler_name {
                fn expected_event_id() -> enginelib::Identifier {
                    (#namespace.to_string(), #name.to_string())
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

            pub fn #register_fn_name(api: &mut enginelib::api::EngineAPI) {
                api.event_bus.register_handler(
                    #handler_name,
                    (#namespace.to_string(), #name.to_string()),
                );
            }

            #inventory_registrar
        }
        .into()
    }
}
