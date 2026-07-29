# Research: Snowflake Internal-Stage Ingestion (023)

Phase 0. Every decision below is either measured against the real qual account,
verified locally against the toolchain, or read out of the code with a citation.
Where something is **not** established, it is in Open Questions rather than
asserted.

Method note, recorded because it shaped the result: the research ran as parallel
investigation followed by adversarial review, twice. The first review found
seventeen must-fix defects across six topics; the second found seven more after
correction. The surviving pattern is that the *decisions* held while the
*specifics* — line ranges, an inverted read rule, one silent reversal of a
pinned SQL option — did not. So this document records decisions and defers
exact edit sites to implementation, where a compiler and a test suite check
them. Over-specifying a plan is what produced those defects.

---

## D1 — The upload mechanism is reachable, and reachable transparently

**Decision**: consume the forked driver and issue uploads as ordinary statements
through the existing single library boundary.

**Rationale**: the fork handles the upload inside its normal statement path — a
caller submits the statement and receives an ordinary result set. No new API
surface, no second transport, and Principle III (one-boundary wrapping) holds
unchanged: the boundary stays where it is and gains a method, rather than the
connector gaining a sidecar.

**Alternatives rejected**:
- *Hand-rolled client for the upload only* — would put a second transport beside
  the library boundary, which Principle III forbids and which 022 already
  recorded as an escalation of last resort.
- *Vendoring the upload code into this crate* — 6,380 lines of cloud-storage,
  crypto and signing code copied into a connector, with no upstream to track.

---

## D2 — Service facts, measured live before design froze

Each was produced by running it against the qual account with the fork. Each
becomes a pinned live check, in the tradition of 022's three service facts —
because a design that rests on a service behaviour and does not pin it will
discover the change as data loss.

| fact | measured result |
|---|---|
| Upload does **not** commit an open transaction | Rolled-back count **0**, transaction id **identical** across the upload, and `COMMIT`-instead kept the row |
| Already-compressed payloads pass through untouched | `source_compression=PARQUET`, `target_compression=PARQUET`, target does **not** end `.gz`; 1060 → 1072 bytes, the +12 being encryption padding |
| Values survive the encryption round trip | 3/3 rows identical, compared as exact text so negatives and fractions are pinned rather than eyeballed |
| Creating the staging object does **not** commit the unit | 3/3 |
| **Dropping** the staging object **does** commit the unit | 3/3, with table creation as an in-round control going the other way 3/3 |

The transaction result was disambiguated deliberately: a count of zero alone
would also be consistent with the statement having silently *aborted* the unit.
The unchanged transaction id and the `COMMIT`-instead control rule that out.

**Consequence taken**: staging stays inside the unit. Teardown of the staging
object must not.

---

## D3 — A named staging object, not the per-user or per-table alternatives

**Decision**: a named, schema-scoped staging object per pipeline.

**Rationale**: the alternatives fail on isolation, measured rather than assumed.
The per-user area has **no scoping at all** — the same object was listed
identically from a different schema and after switching database, so pipelines
would share one namespace separated only by a naming convention nothing
enforces. The per-table area is fatally narrow: loading a *different* table from
it is refused outright (error 001023), and its lifetime is the table's.

A named object is schema-scoped, role-owned, grantable, independently created
and dropped, addressable fully-qualified from another current schema, and can
load any table. It is the only form that preserves the per-pipeline ownership
discipline the module is already built around.

---

## D4 — Naming: the upload's reported target is the only usable name

**Decision**: the file list in the load statement is built from the *reported
target name*, relative to the prefix named in the load statement.

**Rationale**, and this is the finding most likely to have caused a silent
defect: the service does **not** generate a random name — the only rename is a
compression suffix. But the listing's `name` column is **not** usable in a file
list: feeding it back produced a doubled prefix and a not-found error, and it is
lower-cased regardless of how the object was addressed. A payload that gets
compressed proves the distinction — the pre-suffix name fails, the post-suffix
name loads.

**Rule**: file-list entry = (upload prefix − load-statement prefix) + reported
target. Never the local name, never the listing's name.

**Carried forward unchanged from 022**: column matching stays
**case-insensitive**. The parts carry the arrow schema's lower-case names while
the catalog holds upper-case; this is deliberate, commented at the emission site
and pinned by a test. A corrective research pass proposed reversing it, which
would have broken every load — recorded here so the reversal is not proposed
again.

---

## D5 — Per-row upload verification is mandatory

**Decision**: inspect every returned row's status and fail the unit on any row
that is not a success.

**Rationale**: reproduced live — an upload matching two files where one is
unreadable returns **success overall** with a mixed result set, one row uploaded
and one row in error. An error is returned only when *every* row failed. The
connector's existing habit of running a statement and discarding its rows would
therefore lose data silently. This is the same discipline the load already
applies to its own row count, and the fork has no test covering the partial case
— so ours must.

---

## D6 — Dependency form: omit the version key, and make the check catch its absence being reversed

**Decision**: pin the fork by exact revision with **no `version` key**, and ship
a mechanical check that fails when a git dependency is unrecorded — treating the
version-carrying form as never allowlistable.

**Rationale**, verified locally against the toolchain rather than reasoned:

| form | packaging | consequence |
|---|---|---|
| `git` **and** `version` | **succeeds** | git source **silently stripped**; the packaged manifest carries only the version. The shipped crate resolves upstream, compiles, and fails at run time — nothing warns at any point |
| `git`, no `version` | **fails** | refuses before building |

The version-carrying form is therefore the dangerous one, and the safe form is
the one that looks less careful. This inverts the intuition and is the reason
the check's primary job is catching mode A rather than mode B.

**Blast radius**: three crates — the connector, the facade, and the CLI. The
bench crate is excluded because it does not publish. The propagation rule is not
"dev edges don't count"; it is that an edge propagates if it survives into the
manifest that is uploaded.

**Consequence recorded, not discovered later**: publishing those three is
blocked for the validation period. The exits are an accepted upstream change, or
publishing the fork under its own name and consuming it via a package rename —
which needs zero source changes because every import keeps resolving.

---

## D7 — Configuration shrinks by deletion, and the refusal comes from the existing strictness

**Decision**: delete the storage field outright and rely on the configuration's
existing rejection of unknown fields; explain the removal in the upgrade notes,
not in the type.

**Rationale**: the configuration already refuses unknown fields, so a document
still carrying the removed block is refused **by name**, satisfying the
requirement. Keeping a tombstone field to carry a friendlier message would be a
compatibility shim, which the project forbids, and would leave the removed
vocabulary in the generated schema — failing the criterion that the schema
contain none of it.

**Known limitation, accepted**: the refusal reads as a typo diagnosis rather
than an explanation. The explanation belongs in the upgrade note, which is
required anyway.

---

## D8 — The single path needs local working files, and they need the same ownership discipline as staged objects

**Decision**: parts are built one at a time into a per-load temporary directory,
uploaded, and deleted immediately; only the staged name is retained.

**Rationale**: the upload reads a file from disk, unlike the deleted path which
streamed bytes. Building one part at a time keeps peak local usage at one part
rather than proportional to the unit or the dataset — the same property the
deleted path had for memory.

The ownership rule that the deleted path arrived at the hard way (a defect found
by the crash sweep, where two loads of one pipeline shared a key and a wipe)
carries over unchanged: **local artefacts are load-scoped**, and reclamation
removes this load's leftovers unconditionally and another's only when
demonstrably stale.

**Open**: the per-part size bound. See Open Questions.

---

## D9 — Measurement re-establishes supersession rather than assuming it

**Decision**: re-measure against 022's recorded figures on the identical row
shape, recorded and unbarred.

**Rationale**: the deleted paths carry recorded numbers (582 rows/s for
statements and 2,191 rows/s for the bucket path at 250k; 1,941 rows/s at 1M).
Replacing them on assertion would be a regression in method for a project whose
previous feature overturned its own expectation by measuring. The row shape must
not be "improved" — the point is that the old numbers refer to it.

Benchmark governance forbids a hosted-service cell carrying a bar, so this
records and gates nothing.

---

## D10 — The network prerequisite is documented, not gated

**Decision**: state the storage-egress requirement as a documented prerequisite.

**Rationale**: the upload contacts cloud storage directly, not the account host.
With the statement path deleted there is no route on a network that permits only
the account host. The owner weighed this against the simplification and accepted
it; the comparable tool cannot serve that environment either, so it is parity
rather than regression. Documented in advance beats discovered as a failure.

---

## Open Questions

Carried into implementation deliberately. Each would have been asserted by an
earlier draft; none is established.

1. **Per-part size bound across sources.** The upload buffers whole files with a
   per-file ceiling and several times that in peak memory. What bounds a part is
   source-dependent, and the measurement that motivated "no configuration
   needed" was taken from one source only. Establish the bound for sources that
   deliver arbitrarily large batches, and if a limit must be enforced, decide
   what the refusal names — it must name something the user can change.
2. **Reclamation of staged objects.** The deleted path reclaimed stale objects
   by modification time. Determine what the listing exposes about age for the
   new staging area, and whether the reclaim is as strong. If it is weaker, say
   so and record the cost rather than claiming parity.
3. **Crash-point set and its observability.** Two candidate moments (part
   written locally; part uploaded) may be indistinguishable to every durable
   observer, in which case they do not earn two points. Any point added must
   come with an assertion the sweep can actually make — a review found the
   proposed assertions unsatisfiable against the reclaim semantics that ship.
4. **Whether the sweep should gain merge mode.** It covers append and replace
   only, while merge shipped in 022. Adding it costs cells against a criterion
   requiring the total to fall.
5. **The upstream issue.** A contract clause asserts one was filed; no reference
   exists anywhere in the 022 record. Cite it or record that it was never filed.
6. **The gate's coverage of implicit workspace members.** A review found the
   drafted check never scans them, which is precisely the silent pass it exists
   to prevent.

## Drafts carried, not adopted

`drafts/check-git-deps.sh` and `drafts/allowed-git-deps.toml` were produced
during research and are kept for reference. They are **not** wired in and are
**not** verified: the known gap in Open Question 6 is theirs. Implementation
should treat them as a starting point to verify and test, not as finished work —
and they live under this feature's directory rather than in the repository's
tool directory precisely so nothing mistakes them for shipped.

---

## Addendum A1 (T002) — the dependency-tree delta, measured after adoption

Recorded because the constitution requires dependency-tree compatibility with
the workspace pins to be established rather than assumed, and because a git
dependency cannot offer registry metadata in its place.

**The `put` feature adds**: `aes 0.9.2`, `cbc 0.2.1`, `md-5 0.10.6`,
`base64 0.22.1`, `getrandom 0.4.3`, and their supporting RustCrypto traits.

**It also DUPLICATES four crates**, which is the finding:

| crate | already present, via | added by `put` |
|---|---|---|
| `aes` | 0.8.4 — `rsa → pkcs8 → pkcs5`, the encrypted-PEM path key-pair auth already needs | 0.9.2 |
| `cbc` | 0.1.2 | 0.2.1 |
| `block-buffer` | 0.10.4 | 0.12.1 |
| `block-padding` | 0.3.3 | 0.4.2 |

The fork targets a newer RustCrypto generation than the one `rsa`/`pkcs8`
already pull in. Two AES implementations therefore compile into any build with
the Snowflake connector enabled.

**Disposition**: accepted, not blocking. It costs compile time and binary size,
not correctness — the two copies are used by unrelated code paths (one unwraps
an encrypted private key, the other encrypts a staged part). No version
*conflict* exists; cargo resolves both.

**Carried to the upstream contribution**: aligning the fork's crypto crates
with the generation `rsa` already requires would remove the duplication for
every consumer. That is a better fix than anything this repository can do
locally, and it belongs in the upstream conversation rather than in a patch
here.

**Not taken**: pinning the fork's crypto crates down to 0.8 from this side.
That would mean patching a dependency's dependencies to save binary size, which
buys less than it costs in a tree nobody can then reproduce.

---

## Addendum A2 (T012) — what bounds a part, answered

**Open question 1 is closed, and not the way the research assumed.**

The research argued from one source's byte budget and concluded no
configuration was needed. That reasoning does not hold, for a reason visible in
the engine rather than in any source.

**What was established, with citations**:

- `EngineConfig::byte_budget` (`crates/rdlt-engine/src/lib.rs:39`) defaults to
  **64 MiB** and is documented as *"Bound on in-flight bytes per stage channel
  — this is the RSS cap."*
- It does **not** bound a single message. `channel.rs:223-234`: *"An item larger
  than the WHOLE budget degrades to 'drain the budget' rather than
  deadlocking"* — the permit request is capped with `.min`, so one oversized
  batch passes through rather than blocking forever.
- The transfer's ceiling is **256 MiB**, enforced by the fork
  (`put.rs:30  MAX_UPLOAD_BYTES`).

**Therefore**: a part's size is bounded by whatever batch a source chooses to
emit, and nothing in the engine caps it. A source that materialises one very
large batch produces one very large part, and at 256 MiB the upload refuses.

**Decision**: enforce nothing new, and let the transfer's own refusal stand —
but ensure it is *legible*. Adding a second ceiling in this connector would put
a number in a third place that must agree with two others, and the connector
cannot make a batch smaller once it has been handed one.

**What the refusal must say**, because the earlier draft named a lever that
does not exist for most pipelines: the part's actual size, the ceiling it
exceeded, the table it belongs to, and that the source delivered a batch that
large — so the reader knows the fix is on the source side rather than in this
destination's configuration. Parquet compression gives real headroom (an
encoded part is materially smaller than the arrow bytes it came from), which is
why this is a legibility requirement rather than an expected event.

**Not taken**: splitting an oversized batch into several parts. It would work,
and it would make the connector silently succeed where the engine handed it
something unreasonable — hiding a source misconfiguration that the operator
should see once rather than pay for on every load.
