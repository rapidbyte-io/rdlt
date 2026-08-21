# rdlt-connector-client

The engine-side half of the rdlt connector wire: dial a served
connector over its socket, run the frozen handshake (identity, protocol
version, state-format negotiation), and answer as the SPI — a
[`Remote`](https://docs.rs/rdlt-connector-client) that implements
`rdlt_connector`'s `Source` and `Destination`, so an embedder drives an
out-of-process connector without learning the wire exists.

Every inbound byte passes the trust boundary's gates before it becomes
host vocabulary; every error frame maps back through one typed
classification mapping. The wire contract itself lives in
[`rdlt-connector-protocol`](https://docs.rs/rdlt-connector-protocol);
the connector-authoring counterpart is
[`rdlt-connector-sdk`](https://docs.rs/rdlt-connector-sdk).
