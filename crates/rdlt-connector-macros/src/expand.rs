//! Expansion of the connector attributes, written against `proc_macro2` so it is unit-testable.

#[cfg(test)]
mod tests;

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::Parser;
use syn::{ItemImpl, LitStr, parse_quote};

/// Which connector trait an attribute decorates.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Role {
    Source,
    Destination,
}

impl Role {
    fn attribute(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Destination => "destination",
        }
    }

    /// The factory function for this role, and the `RoleFactory` variant it goes in; for a
    /// destination that reads back what it published, the factory that serves that too.
    fn factory(self, read_back: bool) -> TokenStream {
        match (self, read_back) {
            (Self::Source, _) => quote!(::rdlt_connector::RoleFactory::Source(
                ::rdlt_connector::source_factory::<Self>()
            )),
            (Self::Destination, false) => quote!(::rdlt_connector::RoleFactory::Destination(
                ::rdlt_connector::destination_factory::<Self>()
            )),
            (Self::Destination, true) => quote!(::rdlt_connector::RoleFactory::Destination(
                ::rdlt_connector::readable_destination_factory::<Self>()
            )),
        }
    }

    fn trait_name(self) -> &'static str {
        match self {
            Self::Source => "SourceConnector",
            Self::Destination => "DestinationConnector",
        }
    }
}

/// Adds `ID` and `VERSION` to the connector trait `impl` block in `item`, and makes the connector
/// servable by its type (`rdlt_connector::Serve`).
pub(crate) fn connector(
    args: TokenStream,
    item: TokenStream,
    role: Role,
) -> syn::Result<TokenStream> {
    let Args { id, read_back } = parse_args(args, role)?;
    let mut block: ItemImpl = syn::parse2(item)?;
    let implements = block
        .trait_
        .as_ref()
        .and_then(|(_, path, _)| path.segments.last())
        .is_some_and(|segment| segment.ident == role.trait_name());
    if !implements {
        let message = format!(
            "#[{}] goes on an `impl {} for ...` block",
            role.attribute(),
            role.trait_name()
        );
        return Err(syn::Error::new_spanned(&block.self_ty, message));
    }
    block
        .items
        .push(parse_quote!(const ID: &'static str = #id;));
    block.items.push(parse_quote!(
        const VERSION: &'static str = ::core::env!("CARGO_PKG_VERSION");
    ));
    let (generics, _, where_clause) = block.generics.split_for_impl();
    let connector = &block.self_ty;
    let factory = role.factory(read_back);
    let serve = quote! {
        impl #generics ::rdlt_connector::Serve for #connector #where_clause {
            fn factory() -> ::rdlt_connector::RoleFactory { #factory }
        }
    };
    Ok(quote!(#block #serve))
}

/// What a connector attribute says: the connector's id, and whether a destination reads back
/// what it published.
struct Args {
    id: LitStr,
    read_back: bool,
}

fn parse_args(args: TokenStream, role: Role) -> syn::Result<Args> {
    let mut id: Option<LitStr> = None;
    let mut read_back = false;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("id") {
            id = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("read_back") {
            match role {
                Role::Destination => {
                    read_back = true;
                    Ok(())
                }
                Role::Source => Err(meta.error("only a destination reads back what it published")),
            }
        } else {
            Err(meta.error("expected `id = \"...\"`"))
        }
    });
    parser.parse2(args)?;
    let Some(id) = id else {
        let message = format!("#[{}] needs `id = \"...\"`", role.attribute());
        return Err(syn::Error::new(proc_macro2::Span::call_site(), message));
    };
    let value = id.value();
    let valid_chars = value
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-'));
    if value.is_empty() || value.len() > 128 || !valid_chars {
        let message =
            "connector ids are 1-128 bytes of lowercase letters, digits, `.`, `_` and `-`";
        return Err(syn::Error::new_spanned(&id, message));
    }
    Ok(Args { id, read_back })
}
