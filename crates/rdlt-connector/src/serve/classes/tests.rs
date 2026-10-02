use rdlt_wire::limits::Class;

use super::method;

#[test]
fn each_call_s_request_has_the_class_of_what_it_carries() {
    let expected = [
        ("Handshake", Class::Handshake),
        ("Configure", Class::Config),
        ("Check", Class::Control),
        ("Discover", Class::Control),
        ("Plan", Class::State),
        ("Read", Class::Cursor),
        ("Committed", Class::State),
        ("Open", Class::Control),
        ("ApplySchema", Class::Schema),
        ("Write", Class::Data),
        ("Commit", Class::State),
        ("Close", Class::Control),
        ("Heartbeat", Class::Control),
        ("ReadPublished", Class::Control),
        ("ReadAcknowledged", Class::Control),
        ("Unknown", Class::Control),
    ];
    for (name, expected) in expected {
        let path = format!("/rdlt.connector.v1.Connector/{name}");
        assert_eq!(Class::of_request(method(&path)), expected, "{name}");
    }
}
