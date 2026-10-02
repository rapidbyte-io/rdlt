//! The protocol's clients over one connection, one a class of answer, each decoding no more
//! than its class may hold.
//!
//! A decoder holds what an answer's fields become, many times what they took on the wire, before
//! any check of what they say: a client sized for frames would let a catalog of empty entries
//! take gigabytes. Each client takes calls whose answers are of one class.

use rdlt_wire::Limits;
use rdlt_wire::limits::Class;
use rdlt_wire::v1::connector_client::ConnectorClient;
use tonic::transport::Channel;

/// A client of the protocol, over one connection.
pub type Client = ConnectorClient<Channel>;

/// One client a class of answer, over one channel.
#[derive(Clone, Debug)]
pub(crate) struct Clients {
    /// The handshake's.
    pub(crate) handshake: Client,
    /// Calls answered with a control message: a configuration, a check, a report of committed
    /// positions, a schema change, a commit, a close, a write's answers and heartbeats.
    pub(crate) control: Client,
    /// A discovery's.
    pub(crate) catalog: Client,
    /// Calls answered with state or positions: a plan and an open.
    pub(crate) state: Client,
    /// A read's frames.
    pub(crate) data: Client,
}

impl Clients {
    /// The clients over `channel`, each decoding within `limits` for its class.
    pub(crate) fn new(channel: &Channel, limits: &Limits) -> Self {
        let client = |class| sized(channel, limits, class);
        Self {
            handshake: client(Class::Handshake),
            control: client(Class::Control),
            catalog: client(Class::Catalog),
            state: client(Class::State),
            data: client(Class::Data),
        }
    }
}

/// A client over `channel` decoding within `limits` for `class`, and sending what the protocol's
/// largest message may hold.
pub(crate) fn sized(channel: &Channel, limits: &Limits, class: Class) -> Client {
    ConnectorClient::new(channel.clone())
        .max_decoding_message_size(limits.decoding(class))
        .max_encoding_message_size(limits.message_bytes())
}
