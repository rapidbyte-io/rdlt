//! The protocol's clients over one connection, one a class of answer, each decoding no more
//! than its class may hold, and the data plane's calls beside them.
//!
//! A decoder holds what an answer's fields become, many times what they took on the wire, before
//! any check of what they say: a client sized for frames would let a catalog of empty entries
//! take gigabytes. Each client takes calls whose answers are of one class.

use rdlt_connector::wire::v1;
use rdlt_wire::Limits;
use rdlt_wire::limits::Class;
use rdlt_wire::plane::{self, Chained, Incoming};
use rdlt_wire::prost::Message;
use rdlt_wire::v1::connector_client::ConnectorClient;
use tokio_stream::Stream;

use super::checked::Checked;

/// A client of the protocol's generated calls, over one connection.
pub(crate) type Rpc = ConnectorClient<Checked>;

/// A client of the protocol over one connection, for clients that speak the protocol
/// themselves: its generated calls, and the data plane's.
#[derive(Clone, Debug)]
pub struct Client {
    /// The protocol's generated calls, each answer decoded within the data class's bound.
    pub rpc: ConnectorClient<Checked>,
    channel: Checked,
    /// Bytes: the most a message the data plane sends may take.
    most: usize,
}

impl Client {
    /// The client over `channel`, decoding within `limits` and sending what the largest message
    /// of any class may hold.
    pub(crate) fn new(channel: &Checked, limits: &Limits) -> Self {
        Self {
            rpc: sized(channel, limits, Class::Data),
            channel: channel.clone(),
            most: limits.largest(),
        }
    }

    /// The client, sending messages of up to `bytes` on every call.
    #[must_use]
    pub fn max_encoding_message_size(self, bytes: usize) -> Self {
        Self {
            rpc: self.rpc.max_encoding_message_size(bytes),
            most: bytes,
            ..self
        }
    }

    /// Starts a write of `frames`: its answers, once the connector has answered.
    ///
    /// # Errors
    ///
    /// The status the connector refused the write with, or the transport's failure.
    pub async fn write(
        &mut self,
        frames: impl Stream<Item = v1::WriteFrame> + Send + 'static,
    ) -> Result<Incoming<v1::WriteAck>, tonic::Status> {
        called(&mut self.channel, plane::WRITE, frames, self.most).await
    }

    /// Starts a read the host controls with `controls`: its frames, once the connector has
    /// answered.
    ///
    /// # Errors
    ///
    /// The status the connector refused the read with, or the transport's failure.
    pub async fn read(
        &mut self,
        controls: impl Stream<Item = v1::ReadControl> + Send + 'static,
    ) -> Result<Incoming<v1::ReadFrame>, tonic::Status> {
        called(&mut self.channel, plane::READ, controls, self.most).await
    }

    /// Starts reading back what `request` asks for: its frames, once the connector has
    /// answered.
    ///
    /// # Errors
    ///
    /// The status the connector refused the read-back with, or the transport's failure.
    pub async fn read_published(
        &mut self,
        request: v1::ReadPublishedRequest,
    ) -> Result<Incoming<v1::ReadFrame>, tonic::Status> {
        let request = tokio_stream::once(request);
        called(&mut self.channel, plane::READ_PUBLISHED, request, self.most).await
    }
}

/// Calls the data-plane call at `path` over `channel` with `messages`, each at most `most`
/// bytes: its answers, once the connector has answered.
pub(crate) async fn called<M: Chained + Send + 'static, A: Message + Default>(
    channel: &mut Checked,
    path: &'static str,
    messages: impl Stream<Item = M> + Send + 'static,
    most: usize,
) -> Result<Incoming<A>, tonic::Status> {
    let request = plane::request(path, messages, most);
    Incoming::answer(channel.data(request).await?)
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
    /// The data plane's calls, each answer held to its method's bounds.
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
