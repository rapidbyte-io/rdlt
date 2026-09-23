//! The `#[source]` and `#[destination]` attributes for rdlt connectors.
//!
//! Each attribute goes on a connector's trait `impl` block and adds the connector's `ID` and its
//! `VERSION`, which is the version of the crate that defines the connector. Use them through
//! `rdlt_connector::prelude`.

mod expand;

use proc_macro::TokenStream;

/// Declares a source connector's identity: `#[source(id = "io.example.tickets")]`.
#[proc_macro_attribute]
pub fn source(args: TokenStream, item: TokenStream) -> TokenStream {
    expand::connector(args.into(), item.into(), expand::Role::Source)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Declares a destination connector's identity: `#[destination(id = "io.example.warehouse")]`.
#[proc_macro_attribute]
pub fn destination(args: TokenStream, item: TokenStream) -> TokenStream {
    expand::connector(args.into(), item.into(), expand::Role::Destination)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
