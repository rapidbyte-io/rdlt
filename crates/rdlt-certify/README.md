# rdlt-certify

The conformance boundary of the rdlt connector protocol: point it at
any connector executable — first-party or third-party, any language —
and it certifies the binary over the real wire against the named
clause set: the source resume and cancellation laws, the destination's
exactly-once laws, ten protocol clauses (handshake discipline,
one-batch frames, typed refusals), and a nine-arm SIGKILL matrix at
every message boundary with re-run convergence.

A connector enters the ecosystem when this binary passes it. That is
the whole rule.

```text
rdlt-certify --target path/to/your-connector --role source
```

The clause vocabulary and per-clause verdicts render as text or JSON;
`--explain CLAUSE_ID` prints a clause's contract. The suites it drives
are the same [`rdlt-testkit`](https://docs.rs/rdlt-testkit)
conformance kits an in-process connector answers to — one law, one
strength, both sides of the wire.
