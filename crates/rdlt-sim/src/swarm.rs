//! Swarm testing: each seed turns a random subset of the simulation's features on, so rare
//! combinations get seeds of their own rather than being diluted among every feature at once.

#[cfg(test)]
mod tests;

use crate::rng::SplitMix64;

/// What the network's draw mixes into the seed's generator: "network" in ASCII.
const NETWORK: u64 = 0x006e_6574_776f_726b;

/// What the write-ahead log's draw mixes into the seed's generator: "wal" in ASCII.
const WAL: u64 = 0x0077_616c;

/// What streaming's draw mixes into the seed's generator: "stream" in ASCII.
const STREAMING: u64 = 0x7374_7265_616d;

/// What resetting's draw mixes into the seed's generator: "reset" in ASCII.
pub(crate) const RESET: u64 = 0x0072_6573_6574;

/// What the object store's draw mixes into the seed's generator: "objstore" in ASCII.
const OBJECT_STORE: u64 = 0x6f62_6a73_746f_7265;

/// What fat values' draw mixes into the seed's generator: "fat" in ASCII.
const FAT: u64 = 0x0066_6174;

/// What the hostile host's draw mixes into the seed's generator, and its attacks' into each
/// attempt's: "hostile" in ASCII.
pub(crate) const HOSTILE: u64 = 0x0068_6f73_7469_6c65;

/// What the broken connectors' draw mixes into the seed's generator, and their attacks' and
/// their link's into each attempt's: "framing" in ASCII.
pub(crate) const FRAMING: u64 = 0x0066_7261_6d69_6e67;

/// What process placement's draw mixes into the seed's generator: "process" in ASCII.
const PROCESS: u64 = 0x0070_726f_6365_7373;

/// What churn's draw mixes into the seed's generator: "churn" in ASCII.
const CHURN: u64 = 0x0063_6875_726e;

/// Which of the simulation's features one seed exercises.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each feature is on or off, independently"
)]
pub struct Features {
    /// Columns that come and go and change type.
    pub drift: bool,
    /// How many levels drift columns nest: 0 for scalars only.
    pub depth: u32,
    /// Encodings besides each type's plain one: dictionaries, run ends, views, maps and the rest.
    pub encodings: bool,
    /// Streams that push JSON rather than Arrow.
    pub json: bool,
    /// Streams that normalize nested values into child tables.
    pub normalize: bool,
    /// Batches that are slices of larger ones.
    pub sliced: bool,
    /// Injected connector faults and latency.
    pub faults: bool,
    /// Runs that crash, stop, or race a second run.
    pub disruptions: bool,
    /// Destinations that store only some types natively, and the rest as text.
    pub narrow: bool,
    /// Schema settings at every level, type hints, drift columns the source declares, and
    /// destinations that cannot add columns.
    pub settings: bool,
    /// Merge keys that change type, collide across partitions or span two columns.
    pub keys: bool,
    /// Destinations with other identifier rules: any characters, reserved words and table
    /// prefixes.
    pub identifiers: bool,
    /// A destination two pipelines share: the workload's streams split between them, and they
    /// run at once.
    pub shared: bool,
    /// Tasks scheduled in orders the seed varies: sleeps a little longer, and compute jobs on
    /// tasks of their own.
    pub perturb: bool,
    /// Connectors listening on hosts of their own on a simulated network, placed there over
    /// mutual TLS rather than run in the engine's process; with faults, the network partitions
    /// and holds messages, and the connectors crash and stop.
    pub network: bool,
    /// Connectors placed in processes of their own through the host's process placement, whose
    /// processes the simulation plays in the engine's: each says its configuration's canary on
    /// its output, and with faults they are killed, stall and end by themselves; never beside
    /// the network, which places them otherwise.
    pub process: bool,
    /// Pipelines that keep write-ahead logs, whose incremental and change streams, every other
    /// one, cannot read again what they acknowledged; crashes tear the logs' unsynced tails.
    pub wal: bool,
    /// Logs kept in an object store in memory through the engine's `ObjectStoreWal`, whose
    /// requests fail, stall, race and lose their answers with the faults, and whose puts and
    /// deletions land up to twenty seconds after their clients gave up on them, rather than in
    /// the simulation's own store; no listing misses an object, and it changes nothing where no
    /// log is kept.
    pub objects: bool,
    /// Incremental streams whose rows arrive as simulated time passes, which runs follow for a
    /// while before a run reads them to their end; every other one's partitions never end, and
    /// the rest are read again as they grow.
    pub streaming: bool,
    /// Where runs follow the source, a source that retires partitions of the streams they follow,
    /// adds others and names retired ones again while they read, saying in each plan where each
    /// partition ends; one time in two of the streaming seeds.
    pub churn: bool,
    /// Once every phase converged, a stream the source can read again is reset, from its
    /// beginning or with its tables, while runs of every pipeline load, and they converge again.
    pub reset: bool,
    /// Streams pushing JSON whose every eighth record carries a wide object of JSON, declared
    /// as JSON, so most of what a run holds is data; never with a log, whose store in memory
    /// would hold the data too.
    pub fat: bool,
    /// Where the connectors are on the network, a hostile host beside the runs that attacks their
    /// serving, and connectors that now and then start again under a version named after a secret
    /// they were told.
    pub hostile: bool,
    /// Where a hostile host attacks the connectors' serving, connectors on a host of their own
    /// that break the protocol to the engine's host, one met beside each attempt's runs; one time
    /// in two of the hostile seeds.
    pub framing: bool,
}

impl Features {
    /// Every feature on: the simulation as it ran before swarm testing.
    pub const ALL: Self = Self {
        drift: true,
        depth: 3,
        encodings: true,
        json: true,
        normalize: true,
        sliced: true,
        faults: true,
        disruptions: true,
        narrow: true,
        settings: true,
        keys: true,
        identifiers: true,
        shared: true,
        perturb: true,
        network: true,
        process: false,
        wal: true,
        objects: true,
        streaming: true,
        churn: true,
        reset: true,
        fat: false,
        hostile: true,
        framing: true,
    };

    /// The features one seed exercises: every feature one time in eight, else each on or off by
    /// a coin, drift more often than not, the network, the write-ahead log, streaming and
    /// resetting one time in four each, churn one time in two of the streaming seeds, process
    /// placement one time in four of the seeds the network leaves, the log's object store and a
    /// hostile host one time in two each, broken connectors one time in two of the hostile seeds,
    /// and fat values one time in four where no log is kept.
    ///
    /// The network, process placement, the log and its store, streaming, churn, resetting, fat
    /// values, a hostile host and broken connectors are drawn apart from the rest, from the value
    /// the next draw takes but without taking it, so a seed's workload is the same over either
    /// transport, logged or not and in either store, streamed or not, churned or not, reset or
    /// not, and with fat values or without but for those values.
    pub fn draw(rng: &mut SplitMix64) -> Self {
        let next = rng.clone().next_u64();
        let network = SplitMix64::new(next ^ NETWORK).chance(250);
        let wal = SplitMix64::new(next ^ WAL).chance(250);
        let streaming = SplitMix64::new(next ^ STREAMING).chance(250);
        let churn = streaming && SplitMix64::new(next ^ CHURN).chance(500);
        let reset = SplitMix64::new(next ^ RESET).chance(250);
        let objects = SplitMix64::new(next ^ OBJECT_STORE).chance(500);
        let fat = !wal && SplitMix64::new(next ^ FAT).chance(250);
        let hostile = SplitMix64::new(next ^ HOSTILE).chance(500);
        let framing = hostile && SplitMix64::new(next ^ FRAMING).chance(500);
        let process = !network && SplitMix64::new(next ^ PROCESS).chance(250);
        if rng.chance(125) {
            return Self {
                network,
                process,
                wal,
                objects,
                streaming,
                churn,
                reset,
                fat,
                hostile,
                framing,
                ..Self::ALL
            };
        }
        Self {
            drift: rng.chance(800),
            depth: u32::try_from(rng.below(4)).unwrap_or(0),
            encodings: rng.chance(500),
            json: rng.chance(500),
            normalize: rng.chance(500),
            sliced: rng.chance(500),
            faults: rng.chance(500),
            disruptions: rng.chance(500),
            narrow: rng.chance(500),
            settings: rng.chance(500),
            keys: rng.chance(500),
            identifiers: rng.chance(500),
            shared: rng.chance(500),
            perturb: rng.chance(500),
            network,
            process,
            wal,
            objects,
            streaming,
            churn,
            reset,
            fat,
            hostile,
            framing,
        }
    }

    /// Whether the runs' reports count every commit that landed: no run is dropped mid-flight, and
    /// no commit's response is lost, which only a later attempt of the same run would credit.
    pub fn reports_complete(self) -> bool {
        !self.disruptions && !self.faults
    }
}
