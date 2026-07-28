# rdlt-connector-snowflake

The Snowflake destination for [rdlt](https://github.com/rapidbyte/rdlt): loads
land atomically, replays publish nothing, and merges converge on a declared
key.

```yaml
destination:
  snowflake:
    account: MYORG-MYACCT
    user: LOADER
    auth:
      key_pair:
        private_key: /etc/rdlt/snowflake.p8
        passphrase: ${SNOWFLAKE_KEY_PASSPHRASE}
    database: ANALYTICS
    schema: RAW
    warehouse: LOADING_WH
    role: LOADER_ROLE
```

## What this connector assumes about Snowflake

Three service behaviours shape the whole design. Each was verified against a
real account rather than inferred, and each is pinned by a live test that fails
loudly if a service release changes it.

**DDL commits the open transaction.** A `CREATE` or `ALTER` issued inside a
transaction silently commits everything written before it — turning a
half-written load into a durable one with no error anywhere. So all schema work
happens before a commit unit opens, and the executor that runs the unit
*refuses* DDL rather than trusting the code to remember. This is also why
clearing a table for a `replace` load uses `DELETE`, never `TRUNCATE`:
truncation is DDL here and would publish a cleared table before its replacement
rows landed.

**Nothing enforces uniqueness.** Primary keys are informational. A merge that
left duplicates would not be rejected by the database — the only thing keeping
a merge key unique is the merge SQL being right, so the test suite reads the
result back rather than trusting the statement.

**The clock moves during a transaction.** `CURRENT_TIMESTAMP()` is fixed per
*statement*, not per transaction (measured: 2,938 ms of drift across one
transaction). An scd2 boundary needs the retired version's end to equal its
successor's start exactly, so each unit captures one instant into a session
variable and every statement reads that.

## Identifiers

Every identifier is emitted **quoted and upper case**.

Unquoted names fold to upper case, and a quoted lower-case name is a *different
object* — probing the service produced `EVENTS` and `events` coexisting as two
tables. Quoted upper case is also what an unquoted user query resolves to, so
`select * from events` in a worksheet finds what rdlt wrote.

## Type mapping

Closed by construction — every logical type maps to something documented.

| rdlt | Snowflake | note |
|---|---|---|
| `Bool` | `BOOLEAN` | |
| `Int64` | `NUMBER(19,0)` | i64's exact range; the default precision of 38 would accept more than the schema says |
| `Float64` | `FLOAT` | non-finite values are refused rather than rendered |
| `Decimal{p,s}` | `NUMBER(p,s)` | rendered through text, never through a float |
| `Utf8` | `VARCHAR` | |
| `Uuid` | `VARCHAR(36)` | no native UUID type; the canonical text form round-trips |
| `Json` | `VARIANT` | |
| `Binary` | `BINARY` | |
| `TimestampTz` | `TIMESTAMP_TZ` | built from epoch offsets by the service |
| `TimestampNaive` | `TIMESTAMP_NTZ` | a distinct type — collapsing the two would relocate every timestamp |
| `Date` | `DATE` | |
| `Time` | `TIME` | |

Timestamps and dates are built by the service from epoch offsets rather than
formatted here: a rendered datetime depends on session format parameters a user
is free to change underneath the pipeline.

Schema drift adds nullable columns (`ALTER TABLE … ADD COLUMN IF NOT EXISTS`).
Nothing is ever dropped or narrowed.

## Authentication

Exactly one method per `auth:` block; naming none or several is refused at
config load with the alternatives listed.

### Key pair (recommended)

The method Snowflake recommends for unattended access, and the one this
connector is verified against end to end.

```bash
# An encrypted key. Snowflake accepts PKCS#8; the passphrase is supplied
# separately so the key file alone is not a credential.
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rdlt_key.p8
openssl rsa -in rdlt_key.p8 -pubout -out rdlt_key.pub
```

Then register the public key (strip the PEM header, footer and newlines):

```sql
ALTER USER LOADER SET RSA_PUBLIC_KEY = 'MIIBIjANBgkq...';
```

```yaml
auth:
  key_pair:
    private_key: /etc/rdlt/rdlt_key.p8   # a path, or inline PEM text
    passphrase: ${SNOWFLAKE_KEY_PASSPHRASE}
```

### Programmatic access token

Minted in Snowsight under the user's settings. It rides the password channel —
verified against a live account rather than assumed, and it is *not* accepted
on the OAuth channel.

```yaml
auth:
  pat: ${SNOWFLAKE_PAT}
```

### OAuth token

A token the caller has already obtained. Acquiring and refreshing it is the
caller's business — this connector never mints one.

```yaml
auth:
  oauth_token: ${SNOWFLAKE_OAUTH_TOKEN}
```

### Password

Present for parity, not as a recommendation. Snowflake enforces MFA on password
sign-ins and refuses passwords entirely on `TYPE = SERVICE` users, which makes
this the least suitable method for an unattended pipeline.

```yaml
auth:
  password:
    password: ${SNOWFLAKE_PASSWORD}
    passcode: ${SNOWFLAKE_MFA_PASSCODE}   # where the account requires one
```

External-browser SSO is deliberately unsupported: it requires a human at a
browser, which a pipeline does not have.

## Ingestion: two paths

**Batched `INSERT` (default).** Rows travel inside the statements themselves.
Works for every user with no extra infrastructure.

**`COPY INTO` from an external stage (opt-in).** Configure a bucket and rows
leave as parquet, which the service loads itself:

```yaml
    stage:
      s3:
        bucket: my-staging-bucket
        prefix: rdlt/parts
        region: eu-west-2
        access_key: ${AWS_ACCESS_KEY_ID}
        secret_key: ${AWS_SECRET_ACCESS_KEY}
        # Preferred where it exists — keeps the key pair out of the stage
        # definition and therefore out of the account's query history.
        storage_integration: RDLT_STAGE_INT
```

The credentials are needed on both sides: this process writes the parts and the
account reads them back. rdlt scopes its keys under a pipeline-specific prefix,
so a bucket shared with other data is safe, and removes them after the load
commits.

**S3-compatible storage** (MinIO, Ceph, and other non-AWS endpoints) needs the
endpoint allowlisted for your account by Snowflake Support — there is no
self-service parameter, and `CREATE STAGE` is refused until it is done. Set
`endpoint:` once that is in place.

**Internal stages are not supported.** `PUT` is unreachable over the SQL API and
through the driver this connector wraps, so the bucket-free bulk path is not
available. This is the one parity gap against dlt's default; the batched
`INSERT` path covers the same ground without a bucket, more slowly.

## Merge

The full shared strategy vocabulary, identical to the other SQL destinations:
`delete_insert`, `upsert`, `scd2`, with `hard_delete`, `dedup_sort` and
`merge_scope`.

Snowflake has no `ON CONFLICT` and no `DISTINCT ON`, so upserts become
`MERGE INTO` and survivor selection becomes `QUALIFY ROW_NUMBER()`. The
strategies and their semantics are the shared planner's; only the spelling
differs. A differential oracle runs identical inputs through this connector and
the Postgres one and requires the surviving rows to agree.

## Cost controls

```yaml
    table_type: transient        # no fail-safe period; cheaper to keep
    query_tag: nightly-load      # attributable in the account's query history
    session_parameters:
      TIMEZONE: UTC
```

A suspended warehouse auto-resumes; the connector's timeouts absorb the wait
rather than reporting a failure.

## Round trips

Every statement crosses the internet, so the ensure path reads the catalog once
per table per session and then emits only what is genuinely missing. A
steady-state load issues **no schema statements at all**, and the cost of
ensuring a table does not grow with its width — a 200-column table costs what a
3-column table costs.

## Testing against a real account

Live tests are credential-gated: without credentials they skip visibly, and the
suite stays green. Credentials are read from the environment first, then from
`~/.config/rdlt/snowflake/`:

| entry | env | file |
|---|---|---|
| account | `RDLT_SNOWFLAKE_ACCOUNT` | `account` |
| user | `RDLT_SNOWFLAKE_USER` | `user` |
| private key **path** | `RDLT_SNOWFLAKE_PRIVATE_KEY_PATH` | `rdlt_qual_key.p8` |
| passphrase | `RDLT_SNOWFLAKE_KEY_PASSPHRASE` | `passphrase` |
| database | `RDLT_SNOWFLAKE_DATABASE` | `database` |
| warehouse | `RDLT_SNOWFLAKE_WAREHOUSE` | `warehouse` |
| role | `RDLT_SNOWFLAKE_ROLE` | `role` |
| PAT | `RDLT_SNOWFLAKE_PAT` | `pat` |
| bulk-path bucket | `RDLT_SNOWFLAKE_STAGE_*` | `stage.env` |

No account identifier, user name or key material appears anywhere in this
repository, and a mechanical sweep of the tree verifies it.

## License

Apache-2.0 OR MIT, matching the workspace.
