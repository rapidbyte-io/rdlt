# Contract: Snowflake Single-Path Ingestion (SP1–SP8)

Numbered clauses for feature 023. Each is verifiable, and each is either met or
carries a cited disposition at close-out — no clause may be silently satisfied.

This contract **supersedes two clauses of 022's contract**. Those amendments are
stated here rather than implied, because a contract changed by implication is a
contract nobody can audit:

- **SD1** named the library and asserted the internal-stage gap was "deferred on
  a named upstream trigger with the issue filed". The gap is now closed via a
  fork; the library is consumed from that fork at the same single boundary. The
  "issue filed" assertion is separately unevidenced and is resolved by SP8.
- **SD6** asserted the connector "never claims a bulk path it does not have".
  It now has exactly one, and claims exactly one.

| # | Rule |
|---|---|
| **SP1** | **One path, one boundary.** The connector offers exactly ONE ingestion mechanism. No configuration field, size threshold, capability probe, or fallback selects among mechanisms; no branch exists that could. The forked library is consumed at the same single module boundary as before — session lifecycle, statement execution, upload, and error translation live behind it, and no library type crosses the crate's public surface. |
| **SP2** | **Per-part verification is mandatory.** Every part's individual upload outcome is inspected, and any outcome that is not success abandons the unit with a typed error naming the part. An overall success from the transfer is NOT treated as evidence that every part uploaded — this is measured behaviour of the underlying transfer, not a defensive assumption. Rows loaded are separately verified against rows staged, and a mismatch abandons the unit. |
| **SP3** | **The unit is still pure data manipulation, and still atomic.** Publish, receipt and position commit in ONE transaction. All schema work — including creating the staging object — happens strictly before any unit opens. Staging occurs INSIDE the unit, which is safe because the upload does not commit an open transaction; teardown of the staging object occurs OUTSIDE any unit, because dropping it DOES. Both facts are pinned by live checks that fail naming the assumption if the service changes. Replay of a receipted unit publishes nothing and returns the prior receipt. |
| **SP4** | **Names are derived, never echoed.** A file list is built from the transfer's REPORTED target name, relative to the prefix the load statement names. The listing's name column is never round-tripped into a load statement. Column matching remains case-insensitive, because parts carry the encoding's lower-case names against an upper-case catalog. Part names are load-scoped so two loads of one pipeline cannot collide, and reclamation removes this load's residue unconditionally and another load's only when demonstrably stale. |
| **SP5** | **Deletion is complete and simultaneous.** Both superseded ingestion paths and every artefact existing only to serve them — configuration types, credential handling, encoding routines, tuning constants, test suites, testkit gates, fixtures, and dependencies — are removed in the same change that supersedes them. No aliases, shims, deprecated re-exports, or tombstone fields. A document carrying the removed configuration is refused BY NAME, never accepted and ignored. Infrastructure shared with other connectors is NOT removed merely because this connector stopped using it. Verified by mechanical search for residue. |
| **SP6** | **Local working files are owned, bounded and typed.** Parts are built one at a time, so peak local usage is one part rather than proportional to the unit or the dataset. Local artefacts are load-scoped; a concurrent load of the same pipeline cannot delete another's. Local failures — out of space, read-only, permission — classify as typed errors naming the condition, never a bare I/O error. Where a size limit must be enforced, its refusal names something the user can actually change. |
| **SP7** | **Exactly-once survives the change, and is re-proven not assumed.** Every crash point converges to exactly-once totals under repeated failure, including failure during recovery, and every point is proven to have fired. Any crash point that exists carries an assertion the sweep can actually make; a point whose leak the sweep cannot observe is not a point. The sweep's cell count and wall-clock time are recorded and lower than before. Conformance clauses, the differential oracle against another destination, the merge strategy matrix, secret hygiene and option-validation parity all pass on the single path. |
| **SP8** | **The record matches reality, including where it did not.** Claims invalidated by this feature are corrected within it: the parity record's "needs no infrastructure" claim becomes the honest distinction (no bucket, but cloud-storage egress), and the network prerequisite is stated in user-facing documentation in advance of a user meeting it as a failure. The dependency arrangement is recorded with its distribution consequences and its exits, and a mechanical check fails when that arrangement changes unnoticed — covering the form that publishes silently against the registry, and covering every workspace member including implicit ones. 022's uncited "issue filed" assertion is either given its reference or recorded as never filed. Every disposition cites its evidence; every unperformed verification names its reason. |

## Verification map

| clause | how it is verified |
|---|---|
| SP1 | Absence of any selection branch; generated schema carries no storage vocabulary; boundary surface unchanged externally |
| SP2 | A check against a transfer the library reports as successful while a part failed; rows-loaded mismatch check |
| SP3 | Three live checks pinning the measured service facts; replay check; crash sweep |
| SP4 | A load whose part is renamed by compression; a collision check across two loads of one pipeline; the case-insensitivity pin retained from 022 |
| SP5 | Mechanical residue search; refusal check for the removed configuration; use-search proving shared infrastructure retained |
| SP6 | Concurrent-load ownership check; typed-error checks per local failure condition |
| SP7 | Crash sweep with armed-fire pins; recorded cell count and wall clock, compared |
| SP8 | Document review against shipped behaviour; the distribution check exercised against both failure forms and an implicit member |
