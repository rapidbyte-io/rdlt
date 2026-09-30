//! Expansion of the connector attributes, written against `proc_macro2` so it is unit-testable.

#[cfg(test)]
mod tests;

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::Parser;
use syn::{ItemImpl, LitStr, parse_quote};

/// Which connector trait an attribute decorates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    ///
    /// A source that tells where it stands says so in its `ACKNOWLEDGES`, which its factory reads.
    fn factory(self, probed: bool) -> TokenStream {
        match (self, probed) {
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

    /// The flag declaring a connector of this role serves certification's probe.
    fn probe(self) -> &'static str {
        match self {
            Self::Source => "acknowledged",
            Self::Destination => "read_back",
        }
    }

    fn trait_name(self) -> &'static str {
        match self {
            Self::Source => "SourceConnector",
            Self::Destination => "DestinationConnector",
        }
    }
}

/// Adds `ID` and `VERSION` to the connector trait `impl` block in `item` (and `ACKNOWLEDGES` for
/// a source that tells where it stands), and makes the connector servable by its type
/// (`rdlt_connector::Serve`).
pub(crate) fn connector(
    args: TokenStream,
    item: TokenStream,
    role: Role,
) -> syn::Result<TokenStream> {
    let Args { id, probed } = parse_args(args, role)?;
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
    if probed && role == Role::Source {
        block.items.push(parse_quote!(
            const ACKNOWLEDGES: bool = true;
        ));
    }
    let (generics, _, where_clause) = block.generics.split_for_impl();
    let connector = &block.self_ty;
    let factory = role.factory(probed);
    let serve = quote! {
        impl #generics ::rdlt_connector::Serve for #connector #where_clause {
            fn factory() -> ::rdlt_connector::RoleFactory { #factory }
        }
    };
    Ok(quote!(#block #serve))
}

/// What a connector attribute says: the connector's id, and whether it serves certification's
/// probe.
struct Args {
    id: LitStr,
    probed: bool,
}

fn parse_args(args: TokenStream, role: Role) -> syn::Result<Args> {
    let mut id: Option<LitStr> = None;
    let mut probed = false;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("id") {
            id = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident(role.probe()) {
            probed = true;
            Ok(())
        } else if meta.path.is_ident("read_back") {
            Err(meta.error("only a destination reads back what it published"))
        } else if meta.path.is_ident("acknowledged") {
            Err(meta.error("only a source tells where it stands"))
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
    Ok(Args { id, probed })
}
