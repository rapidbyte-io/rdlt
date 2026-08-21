# SECURITY_SCAN.md — comprehensive security deep scan

Date: 2026-08-21 · Scope: this workspace (`crates/*`) plus its sibling
connectors repository (`~/Repos/rapidbyte/rdlt-connectors`, consumed as a
git dependency and carrying every HTTP/SQL-speaking connector).

## Method

| Pass | Tooling / approach | Result |
|---|---|---|
| Advisory scan | `cargo deny check advisories` (no `cargo-audit` installed; deny covers RustSec) | 2 unmaintained-crate advisories, both dev-only (S-1, S-2) |
| Bans/sources/licenses | `cargo deny check sources bans licenses` | sources ok, bans ok; licenses "failed" only because **the repo ships no `deny.toml`** — default policy rejects everything (T-1) |
| Clippy | `cargo clippy --workspace --all-targets --all-features` (env -u RUSTUP_TOOLCHAIN) | clean |
| Unsafe usage | `cargo geiger` per package (virtual-manifest limitation noted) | no meaningful unsafe surface found in first-party crates |
| Secrets & credentials | manual adversarial review | M-1 fixed; rest sound |
| Filesystem & paths | manual adversarial review (traversal, collision, temp, symlink, deletion) | no exploitable findings; 3 informational |
| SQL / identifiers | manual adversarial review across sqlcore + postgres/duckdb/snowflake/oracle | L-1 low, L-2 informational |
| HTTP client & auth | line-by-line REST source + iceberg boundary review | M-2, M-3 fixed; F-lows recorded |
| Subprocess / IPC / bounds | claim-by-claim verification of SECURITY.md + live YAML-nesting probes | L-5..L-7 low; all bounds claims CONFIRMED |

Five parallel adversarial review passes; every finding below was verified
against source with file:line before recording. No files were modified
except the four remediations listed.

## Remediations applied (were MEDIUM)

### R-1 — Pipeline/config parse errors echoed document tokens to stderr — FIXED
- **Was:** `parse_yaml` (`crates/rdlt/src/document/model.rs:198`) and the
  JSON arm of `Config::resolve` passed serde's raw error through. serde's
  data errors embed the offending token (`invalid type: string "…"`,
  ``unknown variant `…` ``), and pipeline documents carry connector
  credentials inline — a wrong-typed secret rode the refusal into stderr,
  shell history, or `rdlt doctor` output.
- **Now:** both seats render kind/class + line/column only
  (`describe_document_error` / `describe_json_error`), mirroring the wire's
  own `describe_parse_error` discipline ("nothing derived from the
  document's content appears in the output"). Pinned by four new tests
  (`yaml_data_errors_do_not_echo_document_tokens`,
  `json_config_errors_do_not_echo_document_tokens`,
  `yaml_syntax_errors_render_position_only`; rdlt lib tests 17/17 green,
  clippy clean).

### R-2 — Server-supplied next-page URLs fetched blindly (credential forwarding / SSRF) — FIXED
- **Was:** `sequence.rs` `Decision::NextUrl` used any absolute URL from
  response JSON or the `Link:` header verbatim — any host, either scheme.
  Every request rides the source's credentials (bearer header, api-key
  header, or an api key located IN the query string), so one poisoned
  response delivered them to an attacker host; an `http://` next-url under
  an `https://` base silently downgraded the hop. Same class as SSRF into
  link-local metadata addresses.
- **Now** (`rdlt-connectors`, rest connector): absolute next-urls are pinned
  to `base_url`'s origin via `http::origin::same_origin` (scheme + host +
  effective port); divergence refuses typed naming the offending host only
  (never the full URL — a query key rides in it). New config validation
  requires `base_url` to be an absolute http(s) URL with a host, so the pin
  is defined at config time (plain http stays legal for intranet/local).
  Pinned: unit tests on `pin_next_url` +
  `next_url_to_another_host_refuses` integration cell.

### R-3 — Default redirect policy leaked custom credential headers/params across hosts — FIXED
- **Was:** the shared reqwest client used the default redirect policy (up
  to 10 hops anywhere). reqwest strips `Authorization`/`Cookie` on
  cross-host redirects but NOT arbitrary configured auth headers or
  query-located api keys — one 302 to an attacker host delivered them.
- **Now:** `Policy::custom` follows same-origin hops only (immediately
  preceding URL's origin), hop cap kept at reqwest's default 10; divergence
  aborts with a typed error. Token-endpoint requests share the client and
  the protection. Pinned:
  `cross_origin_redirect_refuses` + `same_origin_redirect_follows`.
- Full rest suite after both fixes: **96/96 pass**, clippy clean.

## Findings — LOW (recorded, no action required by the scan's bar)

- **L-1 Oracle verbatim-quote escape hatch**
  (`rdlt-connectors …/oracle/src/source/client.rs:245-259`): a config table
  part that starts and ends with `"` is interpolated verbatim WITHOUT
  quote-doubling, so a quoted spelling can smuggle single-statement
  semantic injection (`"t" UNION SELECT …`). Self-inflicted-only (the
  author holds the DB credentials; postgres source exposes deliberate raw
  SQL anyway), but it is the one identifier path violating the crate's own
  quoting rule. *Recommendation:* reject parts whose inner content
  contains `"`, or document the verbatim arm as a deliberate trusted-config
  hatch.
- **L-2 Snowflake `REMOVE` interpolates server-provided object names**
  (`…/snowflake/src/destination/stage.rs:276-289`): `relative` from `LIST`
  output is formatted into `REMOVE @stage/{relative}` unquoted. Not
  statement-chaining (one statement per execute), result ignored,
  triggerable only by someone who can already write stage storage.
  Informational-to-low.
- **L-3 Dev-dependency advisories** (via `iai-callgrind`): bincode 1.3.3
  (RUSTSEC-2025-0141, unmaintained) and proc-macro-error2 2.0.1
  (RUSTSEC-2026-0173, unmaintained). Both are benchmark/dev-only — absent
  from production binaries. Upstream has no safe upgrade; revisit when
  iai-callgrind moves.
- **L-4 `Secret` infrastructure is dead code today**
  (`crates/rdlt-connector/src/secret.rs`): masking is correct and tested,
  but nothing in production wraps a field yet, and its serde form is
  transparent (plaintext). A drift hazard, not a leak — when a connector
  first wraps a credential field, add a test asserting the config never
  serializes it.
- **L-5 Relative `path:` resolves against process CWD**
  (`crates/rdlt-runtime/src/local.rs:106-113`): launching from an
  attacker-writable directory execs whatever the relative name finds there.
  Within the documented trust model ("installing a connector is an operator
  trust decision"); refusing relative spellings would close it cheaply.
- **L-6 YAML recursion limit is dependency-owned**: deep-nesting safety
  currently rests on `serde_yaml_ng`'s internal limit (empirically refuses
  ~depth 129; scanner itself iterative; 8 MiB pre-cap). An upgrade relaxing
  it would reopen stack-overflow class silently. Cheap hardening: a repo
  pin test asserting a 10⁴-deep document refuses.
- **L-7 `Document::from_json`/`from_value` skip the 8 MiB gate** that
  `from_yaml` applies (`crates/rdlt-connector-sdk/src/config.rs:143-156`).
  Not an amplification today (caller already holds the bytes), but the
  asymmetry is one refactor away from a gap.
- **L-8 REST error Display can embed URLs carrying a query api key**
  (`classify::transport` passes reqwest's Display through; reqwest renders
  `… for url (<full url>)`), so a transient-failure log could leak a
  query-located key. Bearer/basic unaffected.
- **L-9 Unbounded response-body buffering** in the REST source
  (`response.bytes().await`) — read_timeout bounds stalls, not bytes;
  hostile peer can OOM one page. Mitigated by trusted-API posture.

## Accepted risks (verified as deliberate, documented in-code)

- Plain-TCP wire arm ships config documents plaintext by design
  (`crates/rdlt-connector-client/src/wire.rs:141-159` — TLS is the
  deployer's layer); Unix-socket transport is the local default and sound
  within its trust domain.
- Connector discovery/exec sanity checks are explicitly NOT authentication
  (`local.rs:30-37`): PATH first-match wins, stat→exec TOCTOU accepted,
  child-advertised dial path unpinned (child already runs at operator
  identity). Scrubbed env keeps `PATH`/`LD_LIBRARY_PATH` deliberately
  (Oracle Instant Client mechanism).
- Symlink-following reads/writes inside operator-owned directories
  (reference connectors' staging create, doctor probe) sit inside the
  stated "directory ownership is the trust boundary"; the WAL shows the
  hardened `O_NOFOLLOW|O_EXCL` pattern where the boundary matters.
- Postgres source raw `queries[].sql` is product design (config author is
  inside the trust boundary).

## Verified SOUND (coverage summary)

- **Bounds claims in SECURITY.md all confirmed in code:** env scrub +
  process group + kill_on_drop + pgid-recycling-safe group kill; handshake
  line 64 KiB via `.take()`; gRPC caps 64 MiB (18 MiB legal-max replies);
  8 MiB document / 4 MiB cursor gates at every seat; Arrow framing
  pre-passes (negative lengths refused signed, commit-and-memset class
  killed), width/row caps on schema-first judgment; WAL segment layout
  pre-pass incl. density bound vs sparse-giant lies; fstat race windows
  around pre-passes; panic belts with documented limits.
- **No secret reaches disk or logs:** persisted artifacts audited
  (StateDoc, cursors, receipts, WAL, events NDJSON, run report) carry no
  credentials; server-side refusal redaction sweeps config scalars out of
  handshake refusals; CLI surfaces print counters/paths/schema only.
- **Path safety:** part names gated at two seats against separators/`..`/
  controls; BLAKE3 length-prefixed digest makes staged names injective
  (collision-proof); `remove_dir_all` licensed by real-dir check +
  byte-exact ownership marker; socket mint/bind/sweep/unlink lifecycle
  closes squatting, dangling-symlink redirect, and rogue-unlink classes.
- **SQL:** one quoting seam per dialect family, embedded quotes doubled,
  hash-derived derived names, typed literal rendering for watermark
  values, bind parameters for state/receipt writes, golden-SQL pins
  holding the text byte-for-byte; NUL/control bytes fail loud at drivers.
- **OAuth2:** real single-flight (mutex held across fetch), generation-
  tagged 401 eviction, exactly-one re-fetch then fatal, tokens never in
  error text; TLS verification untouched repo-wide (`rustls-tls` forced,
  no `danger_accept_invalid_certs` anywhere).
- **YAML graph gate:** merge keys, tags-on-anchors, multi-document,
  flow/block contexts, block-scalar dedent direction all traced —
  over-refuses only, never hides an alias.
- **Panic audit:** all 169 unwrap/expect/index sites in wire-facing crates
  reviewed — every non-test instance infallible or invariant-asserted.

## Tooling gap

- **T-1 No `deny.toml`**: `cargo deny check licenses/bans` cannot express
  policy (default rejects all licenses). Adding a `deny.toml`
  (allow Apache-2.0/MIT/BSD/ISC/Unicode/MPL-2.0 etc.) makes advisory +
  license checking a runnable gate instead of noise.
- `cargo-audit` not installed (deny's advisory DB covers the gap today).

## Bottom line

After remediation, **zero known issues above low severity remain** in
either repository. The three fixes above are tested and green locally;
they live on uncommitted working trees in BOTH repos (this repo's tree was
already dirty before the scan — commit selection left to the owner).
