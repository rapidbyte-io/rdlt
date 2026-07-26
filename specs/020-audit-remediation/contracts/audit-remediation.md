# Contract: Audit Remediation (AR1–AR8)

Clauses this feature must satisfy; the close-out cites these IDs. Per
constitution Principle V they never appear in user-facing strings.

## AR1 — Red before green, or it is not a fix

Every defect fix lands with a regression pin **demonstrated to fail against the
pre-fix build**. A pin observed only to pass after the fix is not evidence and
is not accepted as done. The demonstration is recorded: which build, which
assertion, what it printed.

Two consequences that are not negotiable:

- **A skipping test is green.** A container-backed test that skips when the
  runtime is absent can never be *demonstrated* to fail on the pre-fix build,
  so it is inadmissible as the AR1 pin. Where a live service is the only
  realistic reproduction, the red pin is captured container-free against the
  same code path, and the live cell is confirmation rather than evidence.
- **Where a defect is reachable only from an embedding application** (no
  shipped connector can construct the input), the pin is synthetic and is
  **recorded as synthetic**. It is still required.

## AR2 — The confirmed set is the work; the refuted set is not

The 29 verification-confirmed defects in `NEXT_STEPS.md` are the defect scope.
The 18 refuted claims in its Appendix A are **binding non-goals**: implementing
one is a defect in this feature, not a bonus. Re-raising a refuted claim
requires new evidence that defeats the recorded refutation, recorded alongside
it.

Symmetrically, where research finds the audit itself **wrong**, the correction
is recorded rather than quietly inherited. Two are already known and must
appear in the close-out in corrected form:

- the type-hint defect creates **no child table**; it turns a
  preserved-verbatim JSON column into a NULLed one — worse than the audit
  states, and in the opposite direction;
- the Iceberg field-ID divergence is **guaranteed**, not "plausible", for any
  schema in which a struct or list column is followed by another top-level
  column.

## AR3 — Behaviour changes only where a defect is named

Observable behaviour changes only for the defects named in the source document.
Any further behaviour change discovered to be necessary is recorded as a
deviation with its reasoning before it merges — never absorbed silently.

Two changes in this feature are **schema-affecting** and are called out in
their own increments rather than discovered at review: integer widening on the
shred path, and decimal refusal beyond declared precision. A third is a
**behavioural narrowing** and is bounded by a deliberate pin: a parquet input
regrown by a different writer or different writer properties is refused,
because a whole-file re-encode is not an append.

## AR4 — Persisted formats and identity

`_rdlt_id` and every other emitted identity value stay byte-identical, verified
against a corpus captured from the pre-change build. Golden SQL pins stay
byte-identical. The WAL format does not change.

Exactly two persisted-format decisions are admissible in this feature, and each
was decided on its merits rather than by reflex:

- **`StateDoc` 1 → 2, versioned, with the version actually stamped.** A bump
  that no code writes is inert; the migration note is only real if a document
  written by this build declares 2.
- **`FileProgress` gains an optional field with no bump**, matching the in-tree
  precedent for additive optional fields. A bump there would have changed
  documents for two formats this fix does not touch, and — combined with a
  version-refusal gate — would have made the increment non-revertible.

Any further format change requires the same explicit argument, in the plan,
before it is written.

## AR5 — Typed taxonomy, or no distinction at all

Failures introduced or reclassified use typed constructors and structured
signals. Tests never assert behaviour by substring-matching a rendered message.
User-facing strings carry no internal citation identifiers.

The clause has teeth in one specific place: a new semantic distinction MUST NOT
be expressed as a second free-form string that only substring-matching could
separate. If the distinction cannot be carried in a type within this feature's
scope, it is **deliberately not made**, and that choice is recorded — with the
trigger that would revisit it — rather than approximated in prose.

## AR6 — Greenfield, and the deferral ledger closes

Where an implementation is replaced, the superseded implementation is DELETED
in the same change: no shim, no alias, no dual path, no flag that keeps it
reachable. At close-out, a search of the shipped tree for each replaced
implementation returns zero hits.

Every named deferral whose recorded trigger has fired ends this feature in one
terminal state — taken, rejected with a reason, or re-recorded with a **new**
trigger. A deferral whose trigger has fired and which is simply not mentioned
is indistinguishable from a forgotten defect and is a failure of this clause.

## AR7 — The gate is the gate, and CI is not it

Every increment leaves the full local gate green and is independently
revertible. Green is claimed only from a clean full run, confirmed twice,
because container-backed legs are known to flake.

Continuous integration cannot execute (organisation billing, out of scope). An
item whose verification requires a hosted runner lands as a reviewed change
with its verification **explicitly recorded as unperformed**. It is never
reported as verified, and it never blocks its increment. Stating "CI will catch
it" is not a disposition.

The mutation record is regenerated against the current tree — the committed one
describes code deleted nine features ago — and every survivor reaches a
terminal disposition: killed by a new pin, or recorded as equivalent with a
reason. Where the survivor list exceeds what this feature can triage, the
remainder is a **named deferral with a trigger**, never an untriaged list.

## AR8 — One disposition per item, none silent, and no number by assertion

Every item enumerated in `NEXT_STEPS.md` — roughly 120 across eight sections —
reaches exactly one terminal disposition in the close-out: fixed, rejected with
a recorded reason, or deferred with a named re-trigger. Zero uncited
dispositions. A section this feature declines wholesale is declined explicitly,
not by omission.

For performance items the disposition MUST include a number. An item is not
closed by assertion and is not closed by omission. A candidate that measures no
better is **not shipped**, and its measurement is recorded — with a comment at
the site — so it is not attempted a third time. Two allocation removals in
feature 019 measured worse on airtight counting arguments; a queue that ends in
recorded negatives satisfies this clause, and a change shipped without a
measured win violates it.

All enforcement bars still pass at close-out, and no benchmark cell is worse
than its standing of record.
