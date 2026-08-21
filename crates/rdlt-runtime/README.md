# rdlt-connector-runtime

The OS seam between an rdlt engine and its connectors: providers that
turn a `connector: {id, version}` requirement into a managed SPI
adapter — spawn (or dial) the binary, verify the handshake, supervise
the process, and hand back an object implementing `rdlt_connector`'s
`Source`/`Destination`. Resolution of *which* artifact satisfies an id
belongs to the embedding application; this crate never downloads,
registers, or orchestrates.

The default [`Local`](https://docs.rs/rdlt-runtime) provider spawns
release binaries resolved from `PATH`; the process guard unlinks the
connector's socket on every cleanup path — the other half of the
serve-side dead-predecessor sweep.
