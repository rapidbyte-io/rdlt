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
fn makes_the_connector_servable_by_its_type_in_its_role() {
    let source = expand(
        quote!(id = "io.example.tickets"),
        quote!(impl SourceConnector for Tickets {}),
        Role::Source,
    );
    assert!(
        source.contains(
            "impl :: rdlt_connector :: Serve for Tickets { fn factory () -> :: rdlt_connector :: \
             RoleFactory { :: rdlt_connector :: RoleFactory :: Source (:: rdlt_connector :: \
             source_factory :: < Self > ()) } }"
        ),
        "{source}"
    );
    let destination = expand(
        quote!(id = "io.example.sink"),
        quote!(
            impl<T: Send> DestinationConnector for Sink<T> where T: Sync {}
        ),
        Role::Destination,
    );
    assert!(
        destination.contains(
            "impl < T : Send > :: rdlt_connector :: Serve for Sink < T > where T : Sync { fn \
             factory () -> :: rdlt_connector :: RoleFactory { :: rdlt_connector :: RoleFactory :: \
             Destination (:: rdlt_connector :: destination_factory :: < Self > ()) } }"
        ),
        "{destination}"
    );
}

#[test]
fn serves_no_probe_whatever_the_attribute_says() {
    let acknowledging = expand(
        quote!(id = "io.example.queue", acknowledged),
        quote!(impl SourceConnector for Queue {}),
        Role::Source,
    );
    assert!(
        acknowledging
            .contains("RoleFactory :: Source (:: rdlt_connector :: source_factory :: < Self > ())"),
        "{acknowledging}"
    );
    // Nothing but the id and a source's `acknowledged` is taken.
    for role in [Role::Source, Role::Destination] {
        let trait_name = match role {
            Role::Source => quote!(SourceConnector),
            Role::Destination => quote!(DestinationConnector),
        };
        let refused = expand(
            quote!(id = "io.example.sink", read_back),
            quote!(impl #trait_name for Sink {}),
            role,
        );
        assert!(
            refused.starts_with("error:") && refused.contains("expected `id"),
            "{refused}"
        );
    }
}

#[test]
fn declares_a_source_that_tells_where_it_stands() {
    let expanded = expand(
        quote!(id = "io.example.queue", acknowledged),
        quote!(impl SourceConnector for Queue {}),
        Role::Source,
    );
    assert!(
        expanded.contains("const ACKNOWLEDGES : bool = true ;"),
        "{expanded}"
    );
    let silent = expand(
        quote!(id = "io.example.queue"),
        quote!(impl SourceConnector for Queue {}),
        Role::Source,
    );
    assert!(!silent.contains("ACKNOWLEDGES"), "{silent}");
    let refused = expand(
        quote!(id = "io.x", acknowledged),
        quote!(impl DestinationConnector for T {}),
        Role::Destination,
    );
    assert!(
        refused.starts_with("error:") && refused.contains("only a source tells"),
        "{refused}"
    );
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
