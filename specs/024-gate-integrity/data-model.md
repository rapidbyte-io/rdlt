# Data model: Test-gate integrity (024)

**No product entity is created, changed or removed.** This feature touches the
verification apparatus, and FR-016 forbids any change to persisted data formats,
generated SQL, or user-facing vocabulary. `StateDoc`, WAL v2, bench artifact v3,
the `_rdlt_*` table shapes and every golden SQL pin are untouched.

Saying "not applicable" and stopping would be wrong, though: the feature does
introduce entities. They are entities of the GATE — the things a maintainer
reasons about when deciding whether a verdict can be trusted. Naming them is what
lets the contract's clauses be checked against something concrete.

---

## Gate check

One verification step the gate performs.

| field | meaning |
|---|---|
| invocation | The command that runs it, and the gate path that reaches it. |
| selector | Which tests it picks — by binary name, test name, or package. |
| empty-match disposition | `fail` (the default, and required unless a reason is recorded), or `warn` with a written reason at the site. `pass` is not a permitted value. |
| reachability | The gate path that invokes it, or a named exemption with its reason and, where the reason is cost, the measured cost. |

**Invariants**

- A check's empty-match disposition is never `pass` (GI1).
- Every check is reachable from a gate path or exempt by name (GI2).
- Selector KIND matters and is easy to get wrong: a test-name filter does not
  match a binary name. The one live defect this feature found (research R0) is
  exactly this confusion, so a check's selector kind is part of its identity, not
  an incidental spelling.

**State transitions** — a check moves between exactly three states, and the
transition to `silently permissive` is what this feature makes unreachable:

```text
strict  ──(reason recorded at site)──▶  audibly optional
strict  ◀─(reason removed)────────────  audibly optional
   │                                            │
   └──────── FORBIDDEN ─────────────────────────┘
                   ▼
          silently permissive
```

---

## Crash-point registry

A crate's declared list of instrumented failure sites, which must agree with the
sites present in that crate's sources.

| field | meaning |
|---|---|
| declaration | The exported constant naming the sites. |
| instrumented sites | The `crash_point!` invocations actually present in that crate's sources. |
| owning crate | The crate whose sources are scanned — always the crate that declares the registry, never a third party. |

**Invariants**

- Declaration equals instrumented sites, count-exact (GI3).
- The expected set is derived from SOURCES, never from the declaration —
  comparing a constant against itself is the defect, and it is circular in a way
  that always passes.
- Ten registries exist across six crates. The count is part of the model because
  completeness is what is being asserted: a registry nobody checks is
  indistinguishable from one that agrees.

**Failure directions, both of which must be detected**

| change | must |
|---|---|
| site removed from source AND declaration | FAIL — this is the silent-shrink case that currently passes |
| site added to source, absent from declaration | FAIL — an unswept boundary |
| site renamed in one place only | FAIL — falls out of set-equality |

---

## Environment probe

A decision about whether a gated resource — a container runtime, live credentials
— is available.

| field | meaning |
|---|---|
| resource | What is being probed for. |
| mode | `announce-and-skip` (default) or `demand-and-fail` (opt-in). |
| forced answer | An override making the probe report absent regardless. |

**Invariants**

- Default mode is unchanged from today: absent resource → announced skip, never a
  failure. Principle VII requires this (GI5).
- Demanding mode is opt-in and fails naming the missing resource.
- Setting both a forced-absent override and a demand override is itself an
  error. A run that both forces absence and demands presence is a mistake in the
  invocation, and honouring either one silently would hide it.

**State transitions**

```text
                    ┌─ resource present ─▶ AVAILABLE (suite runs)
default mode ───────┤
                    └─ resource absent ──▶ SKIPPED, announced

                    ┌─ resource present ─▶ AVAILABLE (suite runs)
demanding mode ─────┤
                    └─ resource absent ──▶ FAILED, naming the resource

forced-absent ──────── any ──────────────▶ SKIPPED, announced
forced-absent + demanding ─────────────▶ ERROR (contradictory invocation)
```

---

## Suite count record

The per-binary figures the gate is expected to produce, against which drift is
detected.

| field | meaning |
|---|---|
| binary | The test binary the figures belong to. |
| tests run | How many executed. |
| tests skipped | How many self-skipped for want of a resource. |

**Invariants**

- The record is a committed artifact, reviewed as a diff — not a runtime
  assertion. A difference is a REPORT requiring a reason in the commit that
  causes it, not an automatic failure (GI5). A check that fails on every
  legitimate test addition trains maintainers to bump it unread, which is how a
  pin stops pinning.
- The record covers the gate as this feature LEAVES it, not as it was found —
  which is why it is produced in the fourth increment rather than the first.
- It is a NEW artifact. It is not a change to any existing persisted format, so
  Principle IX is not engaged.

---

## Detection demonstration

The evidence GI8 requires per fixed defect. It is an entity because it is what
close-out records, and because "we fixed it" and "we proved it detects" are
different claims.

| field | meaning |
|---|---|
| defect | Which defect this demonstrates detection of. |
| regression introduced | The deliberate change that previously passed silently. |
| observed failure | The gate's actual output when run against it. |
| observed recovery | The gate green once the regression is reverted. |

**Invariants**

- One demonstration per fixed defect; the count of fixed defects without a
  demonstration is zero (SC-007).
- The failure must be OBSERVED and its output recorded, not predicted. A
  demonstration asserting what the gate would do is the same species of
  unverified claim the feature exists to remove.
- The regression must be one that previously passed **silently**. A regression
  the old gate already caught demonstrates nothing about this feature.

---

## Relationships

```text
Gate check ──selects──▶ test binaries
     │
     └──has──▶ empty-match disposition ──(never)──▶ silently permissive

Crash-point registry ──scanned against──▶ its own crate's sources
     │                                          ▲
     └──────── never against itself ────────────┘

Environment probe ──decides──▶ whether a suite runs or skips
                                        │
Suite count record ──observes───────────┘

Detection demonstration ──proves──▶ that a Gate check detects
```

The shape worth noticing: every entity here exists to make an *implicit* thing
explicit. A selector's empty-match behavior was implicit in a flag; a registry's
completeness was implicit in a constant; a group's membership was implicit in a
filter; a suite's having run was implicit in green. The feature's whole content
is converting those into assertions.
