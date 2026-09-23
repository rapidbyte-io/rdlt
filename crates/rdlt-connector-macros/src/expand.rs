//! Expansion of the connector attributes, written against `proc_macro2` so it is unit-testable.

#[cfg(test)]
mod tests;

use proc_macro2::TokenStream;
use quote::ToTokens;
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

    fn trait_name(self) -> &'static str {
        match self {
            Self::Source => "SourceConnector",
            Self::Destination => "DestinationConnector",
        }
    }
}

/// Adds `ID` and `VERSION` to the connector trait `impl` block in `item`.
pub(crate) fn connector(
    args: TokenStream,
    item: TokenStream,
    role: Role,
) -> syn::Result<TokenStream> {
    let id = parse_id(args, role)?;
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
    Ok(block.into_token_stream())
}

fn parse_id(args: TokenStream, role: Role) -> syn::Result<LitStr> {
    let mut id: Option<LitStr> = None;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("id") {
            id = Some(meta.value()?.parse()?);
            Ok(())
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
    Ok(id)
}
