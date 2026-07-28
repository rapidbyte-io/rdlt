# Quickstart: Postgres → Snowflake with one YAML document

A config-only walk from an empty Snowflake schema to a merge-maintained
table. Verbatim-walkable at feature close (SC-006).

## 1. One-time: key pair and Snowflake user

```sh
# Generate an encrypted key pair (you will be prompted for a passphrase):
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 \
  -aes-256-cbc -out rdlt_key.p8
openssl pkey -in rdlt_key.p8 -pubout -out rdlt_key.pub

# Register the public key on the Snowflake user (ACCOUNTADMIN or delegated):
#   ALTER USER MY_LOADER SET RSA_PUBLIC_KEY = '<contents of rdlt_key.pub
#   between the BEGIN/END lines>';
```

Store the key locally (never in the repo):

```sh
mkdir -p ~/.config/rdlt/snowflake && chmod 700 ~/.config/rdlt/snowflake
mv rdlt_key.p8 ~/.config/rdlt/snowflake/
printf %s 'your-passphrase' > ~/.config/rdlt/snowflake/passphrase
chmod 600 ~/.config/rdlt/snowflake/*
```

## 2. The pipeline document

```yaml
pipeline: orders-to-snowflake
write_mode:
  merge:
    key: [id]
source:
  postgres:
    conn: "postgresql://app:secret@db.internal:5432/prod"
    tables:
      - name: orders
destination:
  snowflake:
    account: "MYORG-MYACCT"            # <acct> in <acct>.snowflakecomputing.com
    user: "MY_LOADER"
    auth:
      key_pair:
        # A PATH to the key. A value carrying PEM armour is taken as the key
        # material itself; anything else is a filename.
        private_key: "/home/you/.config/rdlt/snowflake/rdlt_key.p8"
        passphrase: "your-passphrase"
    database: "ANALYTICS"
    schema: "RAW"
    # role/warehouse optional — the user's server-side defaults apply
    merge_strategy: upsert
```

`~` is not expanded — write the path out, or supply the key inline.

## 3. Run

```sh
rdlt run pipeline.yaml
```

First run: the destination creates `"ORDERS"` (quoted-uppercase — plain
`select * from orders` works in a worksheet), sends the rows as batched
`INSERT` statements, merges by `id` with last-wins dedup, and commits
receipt + state atomically. Re-running the same load publishes nothing
(replay). Subsequent runs merge only what the source cursor delivers, and —
with an unchanged schema — issue zero schema statements.

### Optional: bulk loading through your own bucket

With a bucket the rows leave as parquet and Snowflake loads them itself.
Nothing else in the document changes:

```yaml
    stage:
      s3:
        bucket: "my-staging-bucket"
        prefix: "rdlt/parts"
        region: "eu-west-2"
        access_key: "AKIA..."
        secret_key: "..."
```

Internal named stages are **not** supported — `PUT` is unreachable over the
SQL API — so bulk loading needs a bucket you own. Without one the `INSERT`
path above needs no infrastructure at all.

## 4. Verify in Snowflake

```sql
SELECT COUNT(*) FROM ANALYTICS.RAW.ORDERS;
SELECT * FROM ANALYTICS.RAW._RDLT_COMMITS ORDER BY 1 DESC LIMIT 3;
```

## Live-test convention (contributors)

Tests skip unless credentials resolve, in this order: `RDLT_SNOWFLAKE_*`
environment variables, then `~/.config/rdlt/snowflake/` — files named
`account`, `user`, `database`, `warehouse`, `role`, `passphrase`, `pat`, the
key file `rdlt_qual_key.p8`, and `stage.env` for the bulk-path legs. A skip
is ANNOUNCED once per test binary, so a suite that skipped and one that ran
can be told apart.

With credentials present the live legs (conformance, merge matrix,
differential oracle, crash sweep, auth matrix) run in the standard gate
against a per-run scratch schema that is dropped afterward — including on the
failure path, since a panicking test is exactly when a schema gets left
behind.
