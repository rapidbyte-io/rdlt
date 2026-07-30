# Detection demonstration — US4 (FR-015, GI5)

A skip and a pass looked identical before this story. Eight crates depend on two
probes to choose between running a suite and skipping it, and a probe that wrongly
reports a resource absent disarms every dependent leg while every suite reports
green.

All eight cases below are pinned by `crates/rdlt-testkit/tests/gating_pin.rs`, so
they are not one-off observations — they fail the gate if the behaviour regresses.

```text
8 tests run: 8 passed, 0 skipped
```

## The container probe

| invocation | outcome | why |
|---|---|---|
| default, runtime present | reports present | unchanged |
| default, runtime absent | reports absent → suite skips, announced | unchanged; the constitution requires this default |
| `FORCE_NO_CONTAINERS`, runtime **present** | reports absent | how the skip path stays verifiable without stopping the runtime |
| `REQUIRE_CONTAINERS`, runtime absent | **FAILS**, naming what is missing | the new posture |
| `REQUIRE_CONTAINERS`, runtime present | satisfied | |
| both set | **ERROR** | |

The demand-and-fail message names every place it looked:

```text
RDLT_TESTKIT_REQUIRE_CONTAINERS is set but no container runtime was found: no
DOCKER_HOST, no podman user socket under XDG_RUNTIME_DIR, no /var/run/docker.sock,
and `podman ps` did not succeed
```

## The credential probe

| invocation | outcome |
|---|---|
| `FORCE_NO_SNOWFLAKE` | resolves to `None` → leg skips, announced |
| `REQUIRE_SNOWFLAKE`, credentials absent | **FAILS**, naming them |
| both set | **ERROR** |

## Why "both set" is an error rather than a precedence rule

A run that both forces absence and demands presence is a mistake in how it was
invoked. Quietly honouring either one produces a result the invoker cannot read —
they asked two contradictory questions and got one answer with no indication which.
Failing names the contradiction.

## A design constraint that improved the tests

The first version of `gating_pin.rs` set process environment variables. It could
not compile: this workspace denies `unsafe_code` and `std::env::set_var` is unsafe
in this edition.

The fix was to separate each probe's DECISION from where its answers come from —
`decide_availability(demanded, forced_absent, probe)` alongside the existing
`credentials_with(lookup)` pattern. That is strictly better than what it replaced:
the demand-and-fail path can now be exercised **on a machine that HAS the
resource**, which is the machine a maintainer would actually be on. An
env-mutating test could only have tested demand-and-fail by removing the resource.
