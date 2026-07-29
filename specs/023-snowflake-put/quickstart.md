# Quickstart: Snowflake destination (023)

The whole configuration. There is no ingestion setting, because there is no
ingestion choice.

```yaml
pipeline: orders-nightly
source:
  postgres:
    config: source.yaml
destination:
  snowflake:
    account: MYORG-MYACCT        # the identifier, not a URL
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

Run it:

```sh
rdlt run pipeline.yaml
```

Rows travel as files through storage Snowflake provides. Nothing to create,
nothing to grant, nothing to choose.

## Upgrading from the previous release

**If your document has a `stage:` block, delete it.** It will be refused by
name, and that is deliberate — silently ignoring it would leave you believing
your data took a route it no longer takes.

```yaml
    # DELETE THIS ENTIRE BLOCK — it no longer exists
    stage:
      s3:
        bucket: my-staging-bucket
        access_key: ...
        secret_key: ...
```

You lose nothing by removing it. The bucket existed only because the connector
could not use Snowflake's own staging area; it now can. The bucket it wrote to
can be decommissioned once no pipeline references it, and the credentials it
used can be revoked.

**If you were relying on rows travelling inside statements** — the previous
default, which needed no storage at all — see the prerequisite below. That path
is gone.

## Prerequisite: reaching cloud storage

The connector uploads files directly to the cloud storage backing your Snowflake
account. That traffic does **not** go through
`<account>.snowflakecomputing.com`.

**Check before you deploy** if your network restricts outbound traffic:

```sql
SELECT SYSTEM$ALLOWLIST();
```

The result lists the hosts your client must reach, each with a type. Entries of
the storage type are the ones this connector uploads to. If your firewall
permits only the Snowflake API host, loads will fail — there is no
statement-only fallback.

This is not specific to rdlt: comparable tools stage files the same way and are
equally unable to run in that environment. If you cannot open storage egress,
the options are private connectivity (which routes storage through your own
endpoints) or a host on a network that can.

## What you get without configuring it

| | |
|---|---|
| Exactly-once | a re-run publishes nothing; a crash converges to exact totals |
| Merge | `delete_insert`, `upsert`, `scd2`, with `hard_delete`, `dedup_sort` and `merge_scope` |
| Schema drift | new columns added as nullable; nothing dropped or narrowed |
| Identifiers | quoted upper case, so `select * from orders` in a worksheet finds what was written |
| Cost controls | `table_type: transient`, `query_tag`, `session_parameters` |

## Verifying a load

```sql
SELECT count(*) FROM "ANALYTICS"."RAW"."ORDERS";

-- one row per committed unit; a re-run adds no duplicate
SELECT "LOAD_ID", "COMMIT_SEQ" FROM "ANALYTICS"."RAW"."_RDLT_COMMITS"
ORDER BY "COMMIT_SEQ" DESC LIMIT 5;
```

Staged files are removed once a load commits. Finding some left behind means a
load did not commit — they are inert, named by no receipt, and reclaimed by the
next run of that pipeline.

## Authentication

Four unattended methods; name exactly one. Naming none, or more than one, is
refused at load time.

```yaml
    auth: { key_pair: { private_key: /etc/rdlt/key.p8, passphrase: ${PASS} } }
    auth: { pat: ${SNOWFLAKE_PAT} }
    auth: { oauth_token: ${SNOWFLAKE_OAUTH_TOKEN} }
    auth: { password: { password: ${PW}, passcode: ${MFA} } }
```

Key-pair is recommended for unattended use. Password sign-ins face MFA
enforcement and are refused outright on service-type users. Interactive
browser SSO is unsupported by design — a pipeline has no browser.
