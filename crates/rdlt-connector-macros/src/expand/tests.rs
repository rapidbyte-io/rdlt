use quote::quote;

use super::{Role, connector};

fn expand(args: proc_macro2::TokenStream, item: proc_macro2::TokenStream, role: Role) -> String {
    match connector(args, item, role) {
        Ok(tokens) => tokens.to_string(),
        Err(error) => format!("error: {error}"),
    }
}

#[test]
fn adds_the_id_and_the_crate_version() {
    let expanded = expand(
        quote!(id = "io.example.tickets"),
        quote!(impl SourceConnector for Tickets { type Config = Config; }),
        Role::Source,
    );
    assert!(
        expanded.contains("const ID : & 'static str = \"io.example.tickets\""),
        "{expanded}"
    );
    assert!(
        expanded.contains("env ! (\"CARGO_PKG_VERSION\")"),
        "{expanded}"
    );
    assert!(expanded.contains("type Config = Config"), "{expanded}");
}

#[test]
fn accepts_a_path_qualified_trait() {
    let expanded = expand(
        quote!(id = "io.example.sink"),
        quote!(impl rdlt_connector::DestinationConnector for Sink {}),
        Role::Destination,
    );
    assert!(expanded.contains("const ID"), "{expanded}");
}

#[test]
fn rejects_misuse_with_a_message() {
    let cases = [
        (quote!(), quote!(impl SourceConnector for T {}), "needs `id"),
        (
            quote!(name = "x"),
            quote!(impl SourceConnector for T {}),
            "expected `id",
        ),
        (
            quote!(id = "Io.Example"),
            quote!(impl SourceConnector for T {}),
            "lowercase",
        ),
        (
            quote!(id = ""),
            quote!(impl SourceConnector for T {}),
            "1-128 bytes",
        ),
        (
            quote!(id = "io.x"),
            quote!(impl DestinationConnector for T {}),
            "impl SourceConnector for",
        ),
        (
            quote!(id = "io.x"),
            quote!(impl T {}),
            "impl SourceConnector for",
        ),
    ];
    for (args, item, message) in cases {
        let expanded = expand(args, item, Role::Source);
        assert!(
            expanded.starts_with("error:") && expanded.contains(message),
            "{expanded}"
        );
    }
}
