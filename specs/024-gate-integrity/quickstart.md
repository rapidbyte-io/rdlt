# Quickstart: running and reading the gate (024)

What a maintainer does, and what the verdict means afterwards. There is no
user-facing change in this feature — the audience for this document is whoever
next needs to trust `make check`.

## The routine gate

```sh
env -u RUSTUP_TOOLCHAIN make check
```

`env -u RUSTUP_TOOLCHAIN` is not optional. That variable silently overrides the
1.96.0 pin in `rust-toolchain.toml`, and the only check that notices is the
performance gate — which notices by REFUSING a comparison whose benches showed
zero regressions. If you ever see that refusal, the fix is the environment, never
re-recording baselines.

**If a gate ran recently on this host**, reclaim first:

```sh
make reclaim
until [ "$(ss -tan | grep -c TIME-WAIT)" -lt 20 ]; do sleep 5; done
env -u RUSTUP_TOOLCHAIN make check
```

Back-to-back runs otherwise fail with podman `rootlessport ... bind: address
already in use` — a failed run leaves containers up because fail-fast skips
fixture teardown, and socket residue makes port collisions likely across a run
that starts dozens of containers. That failure is about the host, not your
change.

## What a green verdict now entitles you to assume

After this feature, `make check` exiting zero means:

- **Every selector matched something.** A renamed or deleted test binary fails
  the gate naming the empty selector, rather than passing silently.
- **Every test suite in the repository was either run or is exempt by name**,
  with the exemption's reason — and its measured cost, where cost is the reason —
  written down.
- **Every crash-point registry agrees with its own sources.** A dropped
  instrumented site fails, so the exactly-once matrix cannot quietly shrink.
- **Every test file compiled**, including files built only under a non-default
  configuration.
- **The public surface of the two frozen crates is unchanged** against a recorded
  baseline.

What it does NOT mean, unchanged from before and worth stating:

- Suites needing a container runtime or live credentials may have SKIPPED. They
  announce it, and the counts show it — but green alone does not mean they ran.
  See the next section for how to demand that they do.
- The hour-plus credential-gated sweep did not run. It compiled. Running it is a
  separate deliberate act.

## Demanding that resource-gated suites actually run

Default behavior is skip-not-fail, so a contributor without a container runtime
can still use the gate. When you need the opposite — a machine where the
resources ARE present and you want to know the legs ran — demand them:

```sh
RDLT_TESTKIT_REQUIRE_CONTAINERS=1 \
RDLT_TESTKIT_REQUIRE_SNOWFLAKE=1 \
  env -u RUSTUP_TOOLCHAIN make check
```

A missing resource now fails, naming the resource rather than skipping past it.

The negative overrides remain for the opposite purpose — proving a suite skips
cleanly:

```sh
RDLT_TESTKIT_FORCE_NO_CONTAINERS=1 env -u RUSTUP_TOOLCHAIN make test
```

Setting a FORCE_NO and a REQUIRE for the same resource is an error, not a
precedence puzzle. A run that both forces absence and demands presence is a
mistake in the invocation, and quietly honouring one would hide it.

## Reading the count record

The gate records, per test binary, how many tests ran and how many skipped. A
difference from the committed record is a **report, not automatically a
failure** — adding a test legitimately changes it.

What a difference means depends on its direction:

| observation | reading |
|---|---|
| run count up, skip count same | a test was added; update the record in the same commit |
| run count down, skip count up by the same amount | a suite stopped being armed — a resource went missing, or a probe regressed. Investigate before updating. |
| run count down, skip count unchanged | tests disappeared. This is the case the record exists for. |
| both unchanged | nothing to do |

The record is deliberately not a hard assertion. A check that failed on every
legitimate test addition would train everyone to bump it without reading it,
which is exactly how a pinned number stops being a pin.

## The other verbs

```sh
env -u RUSTUP_TOOLCHAIN make semver
```

Compares the two frozen crates' public surface against a recorded baseline sha.
The baseline is pinned rather than tracking a moving branch, because a baseline
that advances with every merge silently forgives the break it just accepted. It
is re-derivable — `git merge-base main <this feature's branch>` — and advancing
it is a deliberate act visible in a diff.

```sh
env -u RUSTUP_TOOLCHAIN make coverage
```

Now carries the exclusion that the recorded 87.22% figure was actually measured
with, so the number you get is comparable to the number on record. Previously
that exclusion existed only as prose in a close-out document, which meant
reproducing the figure required knowing to look for it.

```sh
env -u RUSTUP_TOOLCHAIN make test TARGET=sweep     # crash sweeps
env -u RUSTUP_TOOLCHAIN make test TARGET=e2e       # now also reached by check
env -u RUSTUP_TOOLCHAIN make test TARGET=prop      # 4096-case property run
```

`TARGET=prop` is worth a note: before this feature it selected a test-name filter
matching nothing and was told to treat that as success, so the extended property
run reported green while executing zero tests. It runs now.

## The one sweep that stays manual

```sh
env -u RUSTUP_TOOLCHAIN cargo nextest run \
  -p rdlt-connector-snowflake --features failpoints \
  --test crash_sweep --no-capture
```

101.5 minutes, live credentials, run by hand — deliberately not in the routine
gate. What this feature changed is that its FILE is now type-checked by every
gate run. Previously nothing compiled it, and during an earlier feature it broke
against deleted APIs with the gate reporting green throughout.
