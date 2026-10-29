//! The protocol's clients over one connection, one a class of answer, each decoding no more
//! than its class may hold, and what carries the data calls beside them.
//!
//! A decoder holds what an answer's fields become, many times what they took on the wire, before
//! any check of what they say: a client sized for frames would let a catalog of empty entries
//! take gigabytes. Each client takes calls whose answers are of one class.

use rdlt_wire::Limits;
use rdlt_wire::limits::Class;
use rdlt_wire::v1::connector_client::ConnectorClient;

use super::checked::Checked;
use super::opened::Carrier;
use crate::network::Stream;

/// A client of the protocol's generated calls, over one connection.
pub(crate) type Rpc = ConnectorClient<Checked>;

/// A client of the protocol over one connection, for clients that speak the protocol
/// themselves: its generated calls, and the streams of its data calls, raw.
#[derive(Clone, Debug)]
pub struct Client {
    /// The protocol's generated calls, each answer decoded within the data class's bound.
    pub rpc: ConnectorClient<Checked>,
    carrier: Carrier,
}

impl Client {
    /// The client over `channel`, decoding within `limits` and sending what the largest message
    /// of any class may hold, its data calls carried by `carrier`.
    pub(crate) fn new(channel: &Checked, limits: &Limits, carrier: Carrier) -> Self {
        Self {
            rpc: sized(channel, limits, Class::Data),
            carrier,
        }
    }

    /// The client, sending messages of up to `bytes` on every generated call.
    #[must_use]
    pub fn max_encoding_message_size(self, bytes: usize) -> Self {
        Self {
            rpc: self.rpc.max_encoding_message_size(bytes),
            ..self
        }
    }

    /// A new stream for a data call, raw: nothing is sent on it yet.
    ///
    /// # Errors
    ///
    /// Where no stream could be made, or the `Data` call carrying it failed.
    pub async fn stream(&self) -> Result<Box<dyn Stream>, rdlt_connector::ConnectorError> {
        self.carrier.stream().await
    }
}

/// One client a class of answer, over one channel.
#[derive(Clone, Debug)]
pub(crate) struct Clients {
    /// The handshake's and the configuration's, whose answers carry a connector's spec.
    pub(crate) handshake: Rpc,
    /// Calls answered with a control message: a configuration, a check, a report of committed
    /// positions, a schema change, a commit, a close and heartbeats.
    pub(crate) control: Rpc,
    /// A discovery's.
    pub(crate) catalog: Rpc,
    /// Calls answered with state or positions: a plan and an open.
    pub(crate) state: Rpc,
    /// The channel itself, whose `Data` calls carry data calls' streams where no opener does.
    pub(crate) channel: Checked,
}

impl Clients {
    /// The clients over `channel`, each decoding within `limits` for its class.
    pub(crate) fn new(channel: &Checked, limits: &Limits) -> Self {
        let client = |class| sized(channel, limits, class);
        Self {
            handshake: client(Class::Handshake),
            control: client(Class::Control),
            catalog: client(Class::Catalog),
            state: client(Class::State),
            channel: channel.clone(),
        }
    }
}

/// A client over `channel` decoding within `limits` for `class`, and sending what the largest
/// message of any class may hold.
fn sized(channel: &Checked, limits: &Limits, class: Class) -> Rpc {
    ConnectorClient::new(channel.clone())
        .max_decoding_message_size(limits.decoding(class))
        .max_encoding_message_size(limits.largest())
}
