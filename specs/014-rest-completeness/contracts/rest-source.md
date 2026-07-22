# Contract: REST Source Rules (RS1–RS8)

| # | Rule |
|---|---|
| RS1 | Declarative first: every documented API behavior (pagination family, auth scheme, action, incremental binding, parent linkage) is expressible in the config document and validated at parse with typed errors naming the field. Configs are DATA — no callbacks, ever. |
| RS2 | No infinite loops: every paginator runs under the same-request guard and `max_pages` bound; violation is a typed error naming stream + paginator state. Termination rules per family are documented and cell-pinned. |
| RS3 | Retry posture unchanged (S3): the source classifies (429→RateLimited w/ Retry-After, 5xx/network→Transient, other 4xx→Fatal unless a DECLARED response action says otherwise); the ENGINE owns retries. In-source waits are bounded by `retry_after_cap_secs`; the source never free-loops. |
| RS4 | Secrets never render: token/password/key/client_secret fields are `Secret`-wrapped — Debug/Display/log/error output shows `***`; the grep-proof cell enforces it over every error path. |
| RS5 | Streaming preserved: the no-selector body passthrough (the flagship perf path) is byte-identical in behavior; selector extraction is one parse+reserialize; memory stays bounded by the engine byte budget. The gated REST→PG bar (≥5×) must hold at close-out. |
| RS6 | Composition seam: client (auth+classify+pacing), `Paginator` trait, extraction, and endpoint description are PUBLIC; the in-crate composed example builds from them alone (no raw reqwest in the example). Additive config evolution — existing documents parse unchanged, spellings frozen. |
| RS7 | Crash discipline: `rest.request`/`rest.decode`/`rest.checkpoint` fail points swept with armed-fire pins; crash/rerun converges to exact totals through the engine; parent-child failures name resolved placeholder values. |
| RS8 | Verification: traceability matrix (every config row → behavioral cells, mock-API conformance per pagination×auth×action), ≥80% measured coverage baseline-first, dlt-parity record with individually documented deviations (incl. deliberately-not-ported auto-detection and OAuth JWT), PokeAPI live cell env-gated (`RDLT_NET=1`) with structural asserts. |
