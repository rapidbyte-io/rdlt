# Close-out: Snowflake internal-stage PUT as the single ingestion path (023)

Branch `023-snowflake-put`, on top of 022 (merged `1ef4860b`).

What shipped, what deviated from the plan, and what was not done — each with
the evidence that settled it. A disposition without a citation is a defect in
this document, not a detail.

## Contract matrix (SP1–SP8)

| clause | status | evidence |
|---|---|---|
| **SP1** — one path, one boundary | MET | The path-selection branch is deleted, not disabled: the session's staging handle is no longer an `Option`, so no value exists that could mean "some other mechanism". Mechanical residue search for the removed renderers, constants, config types and testkit gates returns nothing (T034). The fork is consumed at the same single boundary — `src/dest/client.rs` — and no library type crosses the crate's public surface. |
| **SP2** — per-part verification | MET | Every returned row's status is inspected and any non-`UPLOADED` value abandons the unit naming the part and carrying the service's message (`src/dest/stage.rs`). Rows loaded are separately checked against rows staged. The partial-failure hazard is real and measured, not defensive: a multi-file upload returns success overall while an individual file failed, and no test in the fork covers it. |
| **SP3** — the unit is pure DML and still atomic | MET | Pinned live: uploading does not commit an open transaction (`live_semantics.rs`), creating the staging object inside a unit is safe while dropping it is not, and setting a session variable does not commit. All schema work precedes the unit; the unit executor still refuses DDL in code. |
| **SP4** — names derived, never echoed | MET | The file list is built from the upload's REPORTED target relative to the load statement's prefix. Two live defects found and fixed here — see D-30 and D-31 — both of which made a plausible-looking name silently wrong. Part names are load-scoped; reclamation is unconditional for this load and age-gated for any other. |
| **SP5** — deletion complete and simultaneous | MET | Both superseded paths and every artefact serving only them removed in the same change: config types, renderers, the measured statement budget and row ceiling, `tests/live_stage.rs`, `tests/batch_knee.rs`, the testkit bucket gate, the bench fixture block, the CLI parse pin, and four dependencies. No shims, aliases or tombstone fields. The SPI's `object-store` feature and its shared recoverability rule STAY — the file connector has seven call sites (T033). |
| **SP6** — local files owned, bounded, typed | MET | One part is built, uploaded and deleted before the next, so peak local usage is one part rather than proportional to the unit. Local failures classify by condition — out of space, read-only, permission — never a bare I/O error, with the transient/fatal reasoning at the site. |
| **SP7** — exactly-once re-proven | MET | See the sweep section below. |
| **SP8** — the record matches reality | MET | Parity rewritten (T040), 022's contract amended where a reader of THAT file finds it (T041), the egress prerequisite documented in advance with the host shape from the account's own allowlist (T042), the dependency arrangement recorded with its consequences and guarded by a check that fails when it changes unnoticed (T006), and 022's uncited "issue filed" assertion resolved (T004). |

## Story matrix

| story | status | independent test |
|---|---|---|
| US1 — rows land through service storage with nothing configured | DELIVERED | A load runs against the account with no ingestion configuration present. |
| US2 — the configuration surface shrinks, visibly | DELIVERED | A document carrying the removed block is refused BY NAME; one without it runs; the generated schema holds no storage vocabulary. |
| US3 — the auth matrix completes | PARTIAL by owner decision — see D-33 | Key-pair and PAT proven live, as in 022. Password and OAuth remain UNPERFORMED; their legs are written and announce their skip. The wrong-credential shape is now covered for all four methods, including both of key-pair's. |
| US4 — the record says what is true | DELIVERED | Every ingestion claim in README, quickstart, parity and the contracts checked against behaviour. |
| US5 — the path that ships is the one measured | DELIVERED | The comparison against 022's recorded figures, with dataset identity stated and gating nothing. |

## Deviations and corrections

### D-29 — The dependency form is load-bearing, and its failure mode is silence

A git dependency compiles, tests, lints and benchmarks exactly like a registry
one. It announces itself only at `cargo publish`, long after a design has
hardened around it. Worse, the two ways to write one differ in a way that is
easy to get backwards: `git` **with** `version` publishes SUCCESSFULLY with the
git source silently stripped — the uploaded crate depends on the registry
version, compiles, and behaves like upstream, with whatever the fork existed to
provide simply absent. `git` **without** `version` refuses before building
anything.

So the careful-looking form is the one that ships wrong code. The fork is
therefore pinned by revision with the version key deliberately omitted, and
`tools/check-git-deps.sh` treats the other form as never allowlistable. The
check reads `cargo metadata` for members — a path dependency inside the
workspace is a member whether or not anyone listed it, and a check reading only
the manifest's `members` list would wave through exactly the silent pass it
exists to prevent (research Q6). It also fails on a moving `branch`/`tag`, on
`[patch]`/`[replace]` redirection, and on an allowlist entry whose recorded
blast radius no longer matches the graph.

Recorded consequence: `rdlt-connector-snowflake`, the `rdlt` facade and
`rdlt-cli` cannot be published — 3 of 13 crates, including both things a user
installs. The publish feature inherits this and is told. Exits: a published
fork consumed under a `package =` rename (zero Rust source changes), or an
upstreamed contribution once the fork is validated in use here.

### D-30 — `MATCH_BY_COLUMN_NAME` nulls what it cannot find, and that broke merge silently

A target column absent from a staged file is set to NULL rather than to its
default. The staging table's arrival column is exactly such a column, so every
staged row tied on it and a merge's last-wins survivor became ARBITRARY —
"newest" quietly stopped meaning newest.

Fixed by projecting columns explicitly, which also states the case
correspondence instead of delegating it to a matching mode. Found by running
against the account; no amount of reading would have surfaced it.

### D-31 — and then the projection was wrong in the other direction

The accessor into a staged file is CASE-SENSITIVE. Quoting the projection with
the target's upper case looked symmetrical and found nothing: every column
arrived NULL, and a non-nullable one failed the load naming a column the file
plainly contained.

The target list is the catalog's case; the projection is the file's. Recorded
at the emission site so the symmetrical-looking version is not "restored"
later.

### D-32 — Non-finite floats now travel, and the loosening is pinned

The deleted encoder REFUSED NaN and the infinities, because no numeric literal
spells them. A parquet file carries them natively and the service's float type
accepts them, so the refusal was a fact about the transport rather than about
the data — it goes with the transport. A test says so, so the change is not
later read as an oversight.

### D-33 — Password and OAuth remain UNPERFORMED, by the same decision as 022

Both are implemented and unit-tested, with live legs written that announce
their skip. Turning them green needs provisioning on the account — a
password-capable user (a service-type user refuses passwords) and an OAuth
security integration. That provisioning was not performed: it changes a real
account's user list and auth surface, so it was put to the owner, who chose to
leave both UNPERFORMED (2026-07-29) — the same disposition 022 reached at D-24,
reached again deliberately rather than by drift.

The entries in 022's unperformed table therefore STAND rather than closing, and
are not restated as met. Nothing about the decision is sticky: the legs read
their credentials from their own entries, so adding those entries turns both
green with no code change.

What US3 DID deliver is the failure half, which needed no provisioning: a key
pair now has both of its wrong-credential shapes covered — material this host
cannot parse, and a well-formed key the account never registered — where
before it had neither.

### D-34 — Remote reclaim was absent and is now at parity, on the service's clock

The deleted path reclaimed stale objects by modification time; the internal
path shipped with no remote reclaim at all, so a load that died after uploading
left an object nothing would ever name. Research Q2 asked whether the listing
exposes age at all. Probed live: it does, as an RFC-2822 `last_modified`.

The comparison runs in SQL on the SERVICE's clock rather than by parsing the
timestamp locally. Two reasons, both load-bearing: this host's clock has no
defined relationship to the one that stamped the object, and hand-parsing a
date format nobody controls to decide what to DELETE is a bug with an expensive
blast radius. The threshold is a day — generous deliberately, because being
wrong in the tight direction deletes parts out from under a live load. Both
halves are proven live: a fresh part survives, a stale one goes.

### D-35 — The sweep gains a mode while losing cells

Research Q3 asked whether the local-write and upload moments are
distinguishable to any durable observer. They are: one leaves a file on this
host, the other an object in the staging area that no statement will name, and
they are reclaimed by different code. So they earn a point each — and each
carries an assertion the sweep actually makes, which is what Q3 required.

Research Q4 asked whether the sweep should gain Merge. It does, at
`sf.unit.publish` and only there: that is where its protocol differs, and
running it at points Append already covers would buy warehouse time instead of
coverage. The rule is stated in code (`modes_for`) so a point added later has
to answer the question.

## SC-010 — the secret sweep, and why it is not only a substring search

| term | tracked files containing it |
|---|---|
| account identifier | 0 |
| login name | 0 |
| key material (base64 body under any PEM marker) | 0 |
| passphrase, in a credential-shaped context | 0 |

**The passphrase needed a different method, and saying so is the point.** It is
four characters, so searching the tree for it as a substring returns 493 files
— every one a coincidence, none a leak. A sweep that reports 493 hits is a
sweep whose output nobody reads, which is worse than no sweep. It is therefore
matched only adjacent to an assignment and a credential word, which is the
shape an actual leak would have.

Key material is checked the same way, by SHAPE rather than by value. Seven
files contain a `BEGIN … PRIVATE KEY` marker; every one is a placeholder, a
redaction string, or the deliberately malformed key the auth tests use. The
check that separates them looks for a base64 BODY line, because that is what
distinguishes a real key from the word "key" — and unlike a value search, it
would catch a key that was never on this machine.

## Semver

`cargo semver-checks check-release --baseline-rev main` on both
semver-sacred crates: **no semver update required** (196 checks pass, 57 skip,
each). Neither `rdlt-core` nor `rdlt-connector` has a source change on this
branch — the SPI's `object-store` feature and its shared recoverability rule
stay exactly as they were, and this connector simply stopped requesting a
feature it no longer uses.

The connector's own configuration change IS breaking: a document setting the
removed storage block no longer parses. That costs nothing to carry, for two
reasons stated rather than assumed — the crate is 0.2.x and nothing in this
workspace has ever been published, so no version in anyone's lockfile can
observe the break. It is surfaced as a typed refusal naming the field rather
than a silent acceptance, which is the part that matters to a user upgrading.

The standing 0.2 → 0.3 bump owed at first publish (recorded in 014, ridden by
015, still unclaimed) is NOT taken here. Nothing in this feature forces it, and
taking it would spend a one-time major on a crate that cannot currently be
published at all — see D-29.

## Unperformed verifications

| verification | why |
|---|---|
| Password auth, live | No password-capable user provisioned on the account (D-33). The leg is written and announces its skip. |
| OAuth auth, live | No OAuth security integration provisioned (D-33). Same. |
| Egress failure observed directly | Blocking the storage host would require a firewall rule, and this process shares the machine's network namespace — the rule would degrade the real host. The account's own allowlist establishes the prerequisite instead: three S3 hosts tagged `STAGE`, none under `snowflakecomputing.com` (T042). |
| CI | E1 stands: CI repair is out of scope, and every CI-only verification is recorded UNPERFORMED, never green. |

## Research questions, all terminal

| # | question | disposition |
|---|---|---|
| 1 | Per-part size bound across sources | Answered — research addendum A2. |
| 2 | Reclamation of staged objects | Answered live; reclaim implemented at parity (D-34). |
| 3 | Crash-point set and observability | Two points, each with an assertion the sweep makes (D-35). |
| 4 | Whether the sweep gains merge | Yes, at the publish only; total still falls (D-35). |
| 5 | The upstream issue | Resolved at T004 — recorded as never filed. |
| 6 | Implicit workspace members | The check reads `cargo metadata`, not the members list (D-29). |
