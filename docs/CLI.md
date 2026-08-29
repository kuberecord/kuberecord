# The kuberecord CLI

`kubectl kuberecord` answers questions about recorded Kubernetes state changes —
who changed what, when, and what the object looked like before — without needing
the cluster the change happened in to still exist.

It ships as one binary under two names. `kubectl-kuberecord` on your `PATH` makes
`kubectl kuberecord …` work; the same build installed as `kuberecord` works
standalone, which is what an auditor with an object-store archive and no cluster
access wants. Both are built from the same package and behave identically, down to
naming themselves correctly in their own help text.

This page is the reference for **where the CLI reads from** and **how it is
configured**. The commands themselves are documented as they land.

- [Where the data comes from](#where-the-data-comes-from)
- [The cluster identity](#the-cluster-identity)
- [The configuration file](#the-configuration-file)
- [The read-only ClickHouse user](#the-read-only-clickhouse-user)
- [What the CLI asks of Kubernetes](#what-the-cli-asks-of-kubernetes)
- [Evaluation mode](#evaluation-mode)
- [Exit codes](#exit-codes)

## Where the data comes from

Four steps, first match wins, and **the chosen source is always printed on
stderr**:

| Step | How | When it is the right one |
|------|-----|--------------------------|
| 1 | `--source <dir\|s3://bucket/prefix>` | An archive you can reach directly. No cluster, no CRs, no kubeconfig. |
| 2 | `--sink <kind>/<name>` | A cluster with more than one sink, or one you want to name explicitly. |
| 3 | The active profile | You cannot read the sink's Secret — which is most people (see below). |
| 4 | A discovered sink custom resource | The common case: exactly one `ClickHouseSink` or `S3Sink` in the cluster. |

Every one of them announces itself:

```console
$ kubectl kuberecord timeline deploy/checkout -n payments
→ discovered ClickHouseSink/default (clickhouse.kuberecord-system.svc:9000/kuberecord)
→ cluster-id prod-eu-1 (from the operator Deployment kuberecord-system/kuberecord-controller-manager)
```

Those lines go to **stderr**, so `-o json | jq` never receives them. They are not
optional: a tool that silently picked between four sources would eventually read
the wrong one and be believed, and for an audit trail being believed while wrong is
the worst available failure.

**Nothing prints a credential, at any verbosity.** The notice names the host, the
database and the sink; never the password, never the access key.

### `--source`

```console
$ kubectl kuberecord scopes --source ~/archives/kuberecord
$ kubectl kuberecord scopes --source s3://acme-audit/kuberecord
```

A plain path or a `file://` URL is a directory holding `format=jsonl-v1/`. An
`s3://` URL is a bucket and an optional prefix.

For a bucket, credentials come from the **AWS credential chain** — environment,
`~/.aws/config`, SSO, an instance role — because every tool on the machine already
reads it and kuberecord has no business owning a second one. `AWS_REGION` (or
`AWS_DEFAULT_REGION`) selects the region, defaulting to `us-east-1`, which MinIO
ignores; `AWS_ENDPOINT_URL_S3` is honoured by the SDK itself. Set
`AWS_S3_FORCE_PATH_STYLE=true` for a MinIO deployment that needs
`<endpoint>/<bucket>/<key>` addressing — or, better, put the endpoint and the
addressing style in a profile once.

### Discovery, and why it degrades

Discovery reads the `ClickHouseSink` and `S3Sink` custom resources through your
kubeconfig, and resolves the sink's `credentialsSecretRef` from the operator's
namespace. A `ClickHouseSink` needs the Secret; an `S3Sink` authenticating from the
ambient chain — which is the preferred state on a cloud provider — needs nothing at
all.

That Secret is the catch. The operator's aggregated ClusterRole grants Secret reads
in its own namespace and nowhere else, and that narrowness is deliberate
([`docs/RBAC.md`](RBAC.md)); most engineers are not granted the same. When the read
is refused the CLI says exactly that:

```
error: cannot read Secret kuberecord-system/clickhouse-credentials (forbidden);
configure a profile with `kubectl kuberecord config set-profile`
```

Not "connection failed". The database is fine; your permissions are what stopped
the query, and the remedy — a profile naming a read-only user — needs no new grant
from anybody.

The namespace it looks in is `--operator-namespace`, then `operatorNamespace` in
the configuration file, then the namespace of the operator's Deployment if it can
be found by label, then `kuberecord-system`.

## The cluster identity

Every recorded row carries a `cluster_id`: a string chosen when the operator was
installed. It is **not** a kubeconfig cluster entry, which is why the flag is
`--cluster-id` and `--cluster` remains kubectl's own.

It is resolved by five steps, and the answer is printed:

1. `--cluster-id`.
2. The configuration file's mapping for the current kubeconfig context —
   `kuberecord config set-context-cluster-id`.
3. The operator's Deployment in the target cluster, which carries `CLUSTER_ID` in
   its environment (as the Helm chart sets it) or `--cluster-id` in its arguments.
   This is what makes the ordinary case need no configuration at all.
4. The sink itself, if it holds exactly one cluster's history. This is what makes
   an archive on a laptop need no configuration either.
5. An error **listing the values that are there**:

   ```
   error: no cluster identity: this sink holds 3 of them (prod-eu-1, prod-us-1,
   staging). Pass --cluster-id, or record it for this kubeconfig context with
   `kubectl kuberecord config set-context-cluster-id`
   ```

Step 3 is a convenience and never a requirement: an unreachable cluster, a
forbidden Deployment list, or an operator running on the built-in default all
produce a notice on stderr and continue to step 4.

## The configuration file

`${XDG_CONFIG_HOME:-~/.config}/kuberecord/config.yaml`, mode `0600`.

```yaml
apiVersion: cli.kuberecord.io/v1alpha1
kind: Config

# Used when neither --source nor --sink is given. Overridden by --profile.
currentProfile: prod

# Where a sink's credentials Secret and the operator's Deployment are looked for.
# Optional: the CLI searches for the Deployment, then falls back to kuberecord-system.
operatorNamespace: kuberecord-system

profiles:
  prod:
    backend: clickhouse            # clickhouse | s3 | local
    clickhouse:
      addr: clickhouse.example:9000
      database: kuberecord
      username: kuberecord_ro
      passwordEnv: KUBERECORD_CLICKHOUSE_PASSWORD   # or passwordFile: /run/secrets/ch
      tls: true

  archive:
    backend: s3
    s3:
      bucket: acme-audit
      prefix: kuberecord           # no leading or trailing slash
      region: eu-west-1
      endpoint: https://minio.internal:9000   # scheme is mandatory
      forcePathStyle: true

  laptop:
    backend: local
    local:
      path: /home/you/archives/kuberecord
      prefix: ""                   # if the archive was written under one

# kubeconfig context name → kuberecord cluster identity.
contexts:
  prod-eu: prod-eu-1
  kind-kuberecord: local-kind-cluster
```

A few rules the file enforces rather than documents:

- **A password is never stored inline.** `clickhouse.password` is refused with an
  explanation pointing at `passwordEnv` and `passwordFile`. This file gets
  committed to dotfile repositories, synced between machines and pasted into
  issues; a credential in it stops being a credential.
- **S3 credentials are not in this file at all.** They come from the AWS chain.
  `accessKeyId`, `secretAccessKey` and `sessionToken` are refused by name.
- **An unset environment variable is an error, not an empty password.** A profile
  naming `KUBERECORD_CLICKHOUSE_PASSWORD` in a shell that never exported it says so
  here, instead of authenticating as nobody and failing three steps later.
- **A profile carries exactly the one stanza its `backend` names**, and unknown
  fields are refused — a typo that silently did nothing would be worse.

### `kuberecord config`

```console
# Write a profile. The first one in an empty file becomes the active one.
$ kuberecord config set-profile prod --backend clickhouse \
    --addr clickhouse.example:9000 --database kuberecord \
    --username kuberecord_ro --password-env KUBERECORD_CLICKHOUSE_PASSWORD

$ kuberecord config set-profile archive --backend s3 --bucket acme-audit \
    --prefix kuberecord --endpoint https://minio.internal:9000 --force-path-style

$ kuberecord config set-profile laptop --backend local --path ~/archives/kuberecord

# Choose the active one.
$ kuberecord config use-profile archive

# Record which kuberecord cluster a kubeconfig context reads.
$ kuberecord config set-context-cluster-id prod-eu-1          # the current context
$ kuberecord config set-context-cluster-id prod-eu prod-eu-1  # a named one

# Print the file. The document goes to stdout, its path to stderr.
$ kuberecord config view
$ kuberecord config view -o json | jq .profiles
```

## The read-only ClickHouse user

**This is the recommended posture, and it is what a profile should name.** The CLI
never writes: give it a credential that cannot.

```sql
CREATE USER kuberecord_ro IDENTIFIED WITH sha256_password BY 'a-password-you-generated';

-- The two tables of the frozen v1 schema, and nothing else.
GRANT SELECT ON kuberecord.resource_states TO kuberecord_ro;
GRANT SELECT ON kuberecord.watch_scopes   TO kuberecord_ro;

-- Belt and braces: no writes, and a bound on how long one analyst's question runs.
CREATE SETTINGS PROFILE kuberecord_readonly
  SETTINGS readonly = 1, max_execution_time = 60
  TO kuberecord_ro;
```

Then, on each engineer's machine:

```console
$ export KUBERECORD_CLICKHOUSE_PASSWORD='a-password-you-generated'
$ kuberecord config set-profile prod --backend clickhouse \
    --addr clickhouse.example:9000 --database kuberecord \
    --username kuberecord_ro --password-env KUBERECORD_CLICKHOUSE_PASSWORD
```

Why this rather than widening Kubernetes RBAC so everyone can read the operator's
Secret: that Secret holds the credential the operator **writes** with. Handing it to
a person to run queries with gives them the ability to insert rows into an audit
trail, which is the one thing an audit trail must be able to rule out. A separate
read-only user is both easier to grant and strictly safer, and it is revocable
without restarting the operator.

The same reasoning applies to the archive tier: the sink's own S3 credential is
documented as needing `PutObject` and nothing else, so it cannot read the archive
back even if it leaked. Give a reader its own key with `s3:ListBucket` and
`s3:GetObject` on the prefix, and no `PutObject`.

## What the CLI asks of Kubernetes

Only for discovery. `--source` and profiles need nothing.

| Verb | Resource | Needed for |
|------|----------|-----------|
| `get`, `list` | `clickhousesinks`, `s3sinks` (cluster-scoped) | Finding a sink to read |
| `get` | `secrets` in the operator's namespace | A `ClickHouseSink`'s password. **Usually not granted — use a profile.** |
| `list` | `deployments` in the operator's namespace | Reading the cluster identity. Optional; a notice is printed if refused. |

The CLI never writes to the cluster, and it never reads recorded history through
the operator — it reads the sink directly, as a client of the frozen schema
([`docs/SCHEMA.md`](SCHEMA.md)).

## Evaluation mode

Pointing `--source` at a directory or a bucket removes ClickHouse from the picture
entirely: an archive synced to a laptop answers the same questions through the same
commands, with no infrastructure and no credentials beyond the ones you already
have for the bucket.

The trade is query performance, and it is a real one. The object archive has no
index, so a single-object question over a wide window lists and decompresses every
object in that window's partitions. It is honest about that rather than quiet: the
cost is bounded, reported and interruptible. For wide analytics over an archive —
aggregations, joins, anything that reads more than one object's history — use the
DuckDB and Athena recipes in [`docs/QUERIES.md`](QUERIES.md), which are built for
exactly that shape.

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | Success. |
| `1` | Runtime error: a well-formed request that could not be carried out. |
| `2` | Usage error: an unknown flag, a malformed object address, a bad value. |
| `3` | No coverage: nothing was ever watching the requested scope — which is a different fact from "nothing changed", and is reported as one. |

Code `3` is the one worth scripting against. Every other tool in this space
collapses "your query matched nothing" and "nothing was ever recorded here" into a
single successful empty result; kuberecord will not, because those two answers send
an engineer in opposite directions.
