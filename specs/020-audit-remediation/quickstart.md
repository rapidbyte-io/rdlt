# Quickstart: Working an Increment of 020

**Feature**: 020-audit-remediation | **Branch**: `020-audit-remediation`

Read [spec.md](spec.md) for what and why, [research.md](research.md) for the
decisions of record (including three designs that were overturned — do not
re-derive them), and [contracts/audit-remediation.md](contracts/audit-remediation.md)
for the clauses the close-out cites.

## Environment

Everything runs **inside distrobox** — the Fedora Atomic host has no C
toolchain:

```bash
distrobox enter my-distrobox -- bash -lc 'cd /var/home/netf/Repos/rapidbyte/rdlt && <cmd>'
```

Container-backed legs need rootless podman. Tests skip-not-fail when the
runtime is absent, so **a green run on a machine without podman has not
exercised the container legs** — check before claiming coverage.

## The loop for one defect

1. **Find the anchor.** `NEXT_STEPS.md` holds the `file:line` for every item;
   the spec holds the requirement; `research.md` holds the decided shape. If
   the three disagree, the code wins and the documents are amended.

2. **Write the pin and watch it fail.** This is AR1 and it is not optional:

   ```bash
   git stash                      # or check out the merge base
   cargo nextest run -p <crate> -E 'test(<new_test>)'   # MUST fail
   git stash pop
   cargo nextest run -p <crate> -E 'test(<new_test>)'   # MUST pass
   ```

   Record what the red run printed. A pin only ever seen green is not evidence.

   - If the only realistic reproduction is a live container, capture the red
     pin **container-free** — a skipping test is green and proves nothing.
   - If the defect is reachable only from an embedding application, the pin is
     synthetic; write it anyway and record it as synthetic.

3. **Fix it, deleting what it replaces.** Greenfield: no shim, no alias, no
   second path. If a comment at the site is now false, fix it in the same
   change; if it states history rather than a live invariant, replace it.

4. **Run the full local gate before merging.**

   ```bash
   make check      # lint + test + sweep + iai + cold-start
   ```

   `make check` hard-fails without `hyperfine` and `python3` — that is
   deliberate, and documented rather than softened. Claim green only from a
   clean full run, confirmed twice; container legs are known to flake.

5. **Record the disposition.** Every audit item ends in exactly one terminal
   state in `close-out.md`. "Done" needs the evidence; "not done" needs the
   reason and, if deferred, a named re-trigger.

## Commands you will actually use

| purpose | command |
|---|---|
| Everything | `make check` |
| Fast loop | `cargo nextest run -p <crate>` |
| Doc-tests | `cargo test --doc --workspace` |
| Crash sweeps | `make test TARGET=sweep` |
| Property tests | `make test TARGET=prop` |
| Mutation (fresh) | `rm -rf mutants.out mutants.out.old && RDLT_TESTKIT_FORCE_NO_CONTAINERS=1 make test TARGET=mutants` |
| Coverage | `make coverage` |
| Instruction counts | `make bench TARGET=iai` |
| Bars gate | `make bench TARGET=gate` |
| Docs (new verb) | `make docs` |

## Rules that bite

- **No `unsafe`.** A candidate that needs it is rejected regardless of measured
  gain. The CLI `mallopt` FFI is the sole sanctioned exception and is not in
  scope.
- **Never substring-match a rendered error** in a test. Match the typed
  variant or a structured field. If a new distinction cannot be carried in a
  type, do not invent a second free-form string for it — record that the
  distinction is not made.
- **Identity and golden pins are frozen.** After touching the shred path,
  `crates/rdlt-engine/tests/fixtures/shred_identities.txt` must be
  byte-identical. Do not add cases to it in the same increment that changes
  the path — that hides movement.
- **Performance changes need a measured win.** Never ship one on a counting
  argument. Two allocation removals in 019 measured worse (D-13, D-21). A
  recorded negative is a successful outcome; an unmeasured "improvement" is a
  contract violation.
- **CI cannot run** (organisation billing, out of scope). If an item's only
  verification is a hosted runner, land it reviewed and record the
  verification as unperformed. Do not claim it green, and do not let it block.

## Two increments with sequencing you must not reorder

- **Resume integrity before skip-fetch.** The parquet resume check must land
  before the S3 skip-fetch optimisation, because the fetch reorder changes what
  the planner sees.
- **Precision refusal before the decimal grammar table.** The grammar table
  pins what the parser accepts; the precision rows belong to the increment that
  introduces the refusal.

## Where things get recorded

| kind | destination |
|---|---|
| Per-item disposition | `close-out.md` matrix, zero uncited |
| A measurement (win or negative) | `close-out.md` as a D-entry, plus a site comment for negatives |
| A behaviour change beyond a named defect | `close-out.md` deviation, before merge |
| A deferral | `close-out.md` with a **new** trigger |
| A correction to `NEXT_STEPS.md` itself | `close-out.md`, in corrected form (AR2) |
