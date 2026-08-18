//! The document itself: its typed nodes, the parse through the raw-text
//! security gates, and the config form every arm carries.

use std::{
    io::Read as _,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use super::build::Error;
use super::connector::{self, Connector};
use crate::commit::{BatchPolicy, CommitPolicy};

/// One pipeline, end to end.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Document {
    /// The pipeline's stable name. State and cursors are persisted under it, so
    /// renaming it starts a fresh pipeline rather than continuing this one.
    pub pipeline: String,
    /// Where the write-ahead log lives. Absent defaults to
    /// `.rdlt/<pipeline>` BESIDE THE DOCUMENT — per pipeline, never
    /// shared: the engine replays and clears whatever WAL its workdir
    /// holds, so two pipelines over one directory would hand each other
    /// their crashed spans (a shape the engine refuses typed).
    /// A relative spelling here resolves against the document's own
    /// directory, the same rule path-form configs follow.
    #[serde(default)]
    pub workdir: Option<PathBuf>,
    // singleton_map: YAML's natural `write_mode: {merge: {key: […]}}`
    // singleton-map form for an externally-tagged enum (serde_yaml_ng
    // otherwise wants `!tag` syntax).
    /// How rows land at the destination. Absent defaults to `Append`.
    #[serde(default, with = "serde_yaml_ng::with::singleton_map")]
    pub write_mode: Option<WriteMode>,
    /// How many rows the engine accumulates before each destination
    /// WRITE — and so, for file destinations, how many rows land in
    /// each part.
    ///
    /// Destination-agnostic: `{every_rows: 50000}` means the same to
    /// a file, a table and a warehouse. Absent writes each source
    /// batch straight through, so part size follows the SOURCE's
    /// paging.
    ///
    /// Distinct from `commit_policy`: this is write granularity
    /// (throughput and memory), that is durability (what a crash
    /// costs). A batch never spans a commit.
    ///
    /// The memory it governs is the ENGINE's accumulation. A spawned
    /// connector's in-flight frames are byte-bounded by the connector
    /// sdk's serve budget, and the connector's own batch knobs size
    /// its frames — this knob neither bounds nor needs to bound a
    /// connector process.
    #[serde(default)]
    pub batch_policy: Option<BatchPolicy>,
    /// When accumulated rows are committed — and so, for file
    /// destinations, how many rows land in each part.
    ///
    /// Thresholds, ANY of which ends the commit unit, whichever is
    /// reached first: `{every_bytes: 104857600, every_seconds: 900}`
    /// is "100 MB or every 15 minutes". Absent defaults to committing
    /// at every source checkpoint, which is the safest cadence
    /// because a crash can then cost at most one checkpoint of work.
    #[serde(default)]
    pub commit_policy: Option<CommitPolicy>,
    /// Where rows come from: `connector: {id, version?, path?, config}`,
    /// the one arm form.
    #[serde(deserialize_with = "connector::arm")]
    pub source: Connector,
    /// Where rows go: the same one form as `source`.
    #[serde(deserialize_with = "connector::arm")]
    pub destination: Connector,
}

/// The document form of [`crate::commit::WriteMode`]. Unknown spellings
/// refuse at parse like every other typed node on the document surface:
/// a typo inside a merge block silently ignored is exactly the mistake
/// the uniform `deny_unknown_fields` posture exists to catch.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WriteMode {
    /// Add rows, keeping everything already there.
    Append,
    /// Replace the table's contents with this load's rows, atomically at commit.
    Replace,
    /// Merge on an identity, updating matched rows and inserting the rest.
    Merge {
        /// The columns identifying a row. Must agree with the stream's declared
        /// `primary_key` where it has one — a mismatch is refused at plan time.
        key: Vec<String>,
    },
}

/// A connector config document: an inline mapping, or a string path to
/// a YAML/JSON file read at build time. A string means a path —
/// connector documents are mappings by construction, so the untagged
/// order cannot misread an inline document.
#[derive(Clone, Deserialize)]
#[serde(untagged)]
pub enum Config {
    /// `config: path/to/document.yaml` — the document lives in its
    /// own file, read at resolve time (YAML unless the extension says
    /// `.json`), relative paths joined onto the caller's base.
    Path(String),
    /// The document itself, written where the path would go. Opaque:
    /// only the connector knows its vocabulary, and only the
    /// connector's own config gate validates it.
    Inline(serde_json::Value),
}

/// Manual on purpose, like [`Connector`]'s: an inline document is the
/// connector's own vocabulary and routinely carries credentials, so a
/// derived Debug would print them into any `{:?}` of a [`Document`], a
/// log line, or a test failure message. The path form renders (a path
/// is an address, not a secret); the inline form is elided.
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Config::Path(path) => f.debug_tuple("Path").field(path).finish(),
            Config::Inline(_) => f.debug_tuple("Inline").field(&"<elided>").finish(),
        }
    }
}

/// The most a config or pipeline document may weigh before the read is
/// refused: hand-written YAML/JSON measures in kilobytes, so a
/// multi-megabyte "document" is a wrong path (a data file, a dump) —
/// better refused by size, typed, than slurped whole into memory and
/// fed to a recursive parser. Shared with the CLI's document read AND
/// with the connector sdk's own `Document::from_yaml` gate, so every
/// document read answers to one number.
pub const MAX_DOCUMENT_BYTES: u64 = rdlt_connector_sdk::spi::gate::MAX_DOCUMENT_BYTES;

fn parse_yaml<T: serde::de::DeserializeOwned>(text: &str) -> Result<T, String> {
    // The raw-text graph gate runs BEFORE deserialization: `serde_yaml_ng`
    // expands anchors and aliases while constructing the target value,
    // so an input-byte cap cannot bound the resulting allocation.
    // Pipeline and connector config documents are trees; graph syntax
    // is refused. The gate lives in the sdk (`rdlt_connector_sdk::config`)
    // so this parse and every connector's `Document::from_yaml` answer
    // to the same scanner.
    rdlt_connector_sdk::config::reject_graph_syntax(text)?;
    serde_yaml_ng::from_str(text).map_err(|error| error.to_string())
}

/// Parse one pipeline document through the raw-text security gates.
pub fn parse(text: &str) -> Result<Document, String> {
    parse_yaml(text)
}

/// Read a document file with the [`MAX_DOCUMENT_BYTES`] cap enforced
/// BEFORE the read — the read stops one byte past the cap, so the
/// refusal names that floor ("at least") and the cap.
pub fn read(path: &Path) -> Result<String, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("reading {}: {e}", path.display()))?;
    let mut text = String::new();
    file.take(MAX_DOCUMENT_BYTES + 1)
        .read_to_string(&mut text)
        .map_err(|e| format!("reading {}: {e}", path.display()))?;
    let len = text.len() as u64;
    if len > MAX_DOCUMENT_BYTES {
        return Err(format!(
            "reading {}: the file is at least {len} bytes, over the {MAX_DOCUMENT_BYTES}-byte \
             document cap — a pipeline or connector document is hand-written \
             configuration, so a file this size is almost certainly not the document \
             the path meant to name",
            path.display()
        ));
    }
    Ok(text)
}

impl Config {
    /// Resolve to the connector's opaque config document.
    ///
    /// The path form reads its file — relative paths against `base` —
    /// and parses it as YAML, or as JSON when the extension says so;
    /// absence, a malformed document, and a file over
    /// [`MAX_DOCUMENT_BYTES`] each refuse with a typed
    /// [`Error::Resolve`] naming the path. The inline form clones.
    pub fn resolve(&self, base: &Path) -> Result<serde_json::Value, Error> {
        match self {
            Config::Path(spelled) => {
                let path = base.join(spelled);
                let text = read(&path).map_err(Error::resolve)?;
                if is_json(&path) {
                    serde_json::from_str(&text)
                        .map_err(|e| Error::resolve(format!("parsing {}: {e}", path.display())))
                } else {
                    parse_yaml(&text)
                        .map_err(|e| Error::resolve(format!("parsing {}: {e}", path.display())))
                }
            }
            Config::Inline(value) => Ok(value.clone()),
        }
    }
}

/// Config files: YAML by default, JSON when the file says so — the same
/// document shape either way, exactly as the connectors' own
/// `from_yaml`/`from_json` treat it.
fn is_json(path: &Path) -> bool {
    path.extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("json"))
}

#[cfg(test)]
mod document_cap_tests {
    use super::*;

    /// A config path naming a file over the document cap refuses typed
    /// through a bounded `MAX+1` read: a concurrent grow cannot race a
    /// prior metadata check and feed extra bytes to the parser. The
    /// sparse fixture proves the read itself owns the cap.
    #[test]
    fn an_oversized_config_file_refuses_before_the_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("big.yaml");
        std::fs::File::create(&path)
            .expect("fixture file")
            .set_len(MAX_DOCUMENT_BYTES + 1)
            .expect("sparse size");

        let refused = Config::Path("big.yaml".into())
            .resolve(dir.path())
            .expect_err("an oversized document must refuse");
        assert_eq!(
            refused.to_string(),
            format!(
                "reading {}: the file is at least {} bytes, over the {MAX_DOCUMENT_BYTES}-byte \
                 document cap — a pipeline or connector document is hand-written \
                 configuration, so a file this size is almost certainly not the document \
                 the path meant to name",
                path.display(),
                MAX_DOCUMENT_BYTES + 1
            )
        );

        // At the cap or under it, the ordinary read proceeds.
        let small = dir.path().join("small.yaml");
        std::fs::write(&small, "key: value\n").expect("write");
        assert!(
            Config::Path("small.yaml".into())
                .resolve(dir.path())
                .is_ok()
        );
    }
}

#[cfg(test)]
mod yaml_graph_tests {
    use super::*;

    #[test]
    fn pipeline_anchors_and_aliases_refuse_before_deserialization() {
        let yaml = "pipeline: p\n\
                    source:\n  connector:\n    id: io.example.source\n    config: &shared {token: secret}\n\
                    destination:\n  connector:\n    id: io.example.destination\n    config: *shared\n";
        let error = parse(yaml).expect_err("YAML graph syntax must refuse");
        assert!(error.contains("anchors and aliases"), "{error}");
    }

    #[test]
    fn mid_scalar_apostrophe_does_not_blind_the_graph_guard() {
        // A plain scalar may contain an apostrophe (`john's`) — a quote
        // character is a YAML indicator only at the start of a scalar.
        // A guard that misreads it as quote-open goes blind to every
        // anchor and alias after it, restoring unbounded alias
        // expansion. The graph refusal must still fire.
        let yaml = "pipeline: john's orders\n\
                    big: &big [a, b, c]\n\
                    boom: [*big, *big]\n";
        let error = parse_yaml::<serde_json::Value>(yaml)
            .expect_err("anchors after a mid-scalar apostrophe must still refuse");
        assert!(error.contains("anchors and aliases"), "{error}");
    }

    #[test]
    fn quoted_indicators_and_comments_remain_plain_data() {
        let value: serde_json::Value = parse_yaml(
            "literal_alias: '*not-an-alias'\nliteral_anchor: \"&not-an-anchor\"\n# *comment\n",
        )
        .expect("indicators in scalars and comments are data");
        assert_eq!(value["literal_alias"], "*not-an-alias");
        assert_eq!(value["literal_anchor"], "&not-an-anchor");
    }

    /// Indicator characters as scalar DATA must parse: these are
    /// availability pins on the primary config surface — a graph guard
    /// that refuses `val#ue` is a parser bug wearing a security badge.
    #[test]
    fn indicator_characters_as_data_parse_on_the_config_surface() {
        let value: serde_json::Value = parse_yaml(
            "hash: val#ue\n\
             amp: a&b\n\
             star: a*b\n\
             apostrophe: don't\n\
             block: |\n  this & that\n  * bullet point\n\
             folded: >\n  more & data\n",
        )
        .expect("indicator characters as data must parse");
        assert_eq!(value["hash"], "val#ue");
        assert_eq!(value["amp"], "a&b");
        assert_eq!(value["star"], "a*b");
        assert_eq!(value["apostrophe"], "don't");
        assert_eq!(value["block"], "this & that\n* bullet point\n");
        assert_eq!(value["folded"], "more & data\n");
    }

    /// Hostile positions beyond the value slot: the refusal fires
    /// wherever the parser would see a live anchor or alias.
    #[test]
    fn anchors_and_aliases_refuse_in_sequence_flow_and_merge_positions() {
        for doc in [
            "items:\n  - &x v\n",
            "items: [a, *x]\n",
            "map: {k: &x v}\n",
            "base:\n  <<: *defaults\n",
            "--- &doc\nkey: v\n",
        ] {
            let error = parse_yaml::<serde_json::Value>(doc)
                .expect_err("graph syntax must refuse in every position");
            assert!(error.contains("anchors and aliases"), "{doc:?}: {error}");
        }
    }
}
