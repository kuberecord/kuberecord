# Tamper-evidence and retention

An `S3Sink` writes an archive that the object store itself can make immutable.
That is the one property this project has which ClickHouse structurally cannot
offer: an append-only table is append-only *by convention*, and anyone who can
reach the database can remove a row from it with a mutation, leaving nothing in
the remaining rows to say it happened. S3 Object Lock is enforced by the store,
against every principal including the one that wrote the object.

This page is about what that buys, exactly, and what it does not. The claim
kuberecord makes is narrow and worth stating in one sentence before the details:
**kuberecord can ask the store to make each object it writes undeletable for a
fixed period; it cannot sign anything, cannot configure the bucket, and cannot
prove that the archive is complete.** Everything below either follows from that
sentence or qualifies it.

## The prerequisite kuberecord cannot set

Object Lock is a **bucket** property, and kuberecord never creates or configures
buckets — on any backend, in any configuration. The sink holds `s3:PutObject`
and nothing else (see [what the credential needs](#what-the-sinks-credential-needs)),
so enabling Object Lock is a human's job, done once, before the sink is pointed
at the bucket.

Two facts about it are permanent and worth reading twice:

- **Object Lock requires S3 Versioning.** Enabling one implies the other.
- **Neither can be undone.** After Object Lock is enabled on a bucket you cannot
  disable it, and you cannot suspend versioning on that bucket again.

The second fact is why this is not a setting to try out on the bucket that holds
your archive. Try it on a throwaway bucket.

### On a new bucket

AWS S3, with the CLI:

```sh
# us-east-1: no location constraint.
aws s3api create-bucket \
  --bucket kuberecord-audit \
  --object-lock-enabled-for-bucket

# Anywhere else, the region has to be stated twice — once for the endpoint the
# CLI talks to, once for where the bucket lives.
aws s3api create-bucket \
  --bucket kuberecord-audit \
  --object-lock-enabled-for-bucket \
  --region eu-central-1 \
  --create-bucket-configuration LocationConstraint=eu-central-1
```

MinIO, with `mc`:

```sh
mc mb --with-lock local/kuberecord-audit
```

Both enable versioning as a side effect, because Object Lock cannot exist
without it.

### On a bucket that already exists

This is possible on AWS S3 and it used not to be, which is why older
documentation — including, until this page was written, kuberecord's own —
says Object Lock can *only* be enabled when a bucket is created. Versioning
first, then the lock configuration:

```sh
aws s3api put-bucket-versioning \
  --bucket kuberecord-audit \
  --versioning-configuration Status=Enabled

aws s3api put-object-lock-configuration \
  --bucket kuberecord-audit \
  --object-lock-configuration '{"ObjectLockEnabled":"Enabled"}'

# Confirm, before pointing a sink at it.
aws s3api get-object-lock-configuration --bucket kuberecord-audit
```

On MinIO the same is true of recent releases and not of older ones, so check
your server's version rather than assuming: the MinIO the integration suite
pins is deliberately an older one, which is why that fixture creates its locked
buckets at creation time.

Enabling Object Lock on an existing bucket does **not** lock anything already in
it. Object Lock applies to object *versions*, and a version's lock is written
when the version is created — so every object already in the bucket stays
exactly as deletable as it was. Only what kuberecord writes from then on carries
retention.

### A bucket-wide default retention, and why it is not the same thing

The same call can carry a default retention, which the store applies to every
object version written into the bucket *that does not bring its own*:

```sh
aws s3api put-object-lock-configuration \
  --bucket kuberecord-audit \
  --object-lock-configuration \
  '{"ObjectLockEnabled":"Enabled","Rule":{"DefaultRetention":{"Mode":"GOVERNANCE","Days":365}}}'
```

An explicit per-object retention overrides the bucket default, so a sink with
`spec.objectLock` set is unaffected by it. A sink *without* `spec.objectLock` is
entirely governed by it — which is a supported way to run this: let the bucket
own the policy and leave the CR silent about retention.

Be deliberate about it, though, because a default retention covers **everything**
kuberecord writes, including the health probe's object. See
[the probe object](#the-probe-object-writes-once-per-minute) for why that is the
one place a bucket default behaves worse than a per-object one.

### What the sink's credential needs

The write path calls exactly one S3 operation, `PutObject`. With
`spec.objectLock` set, that request carries retention headers, and setting a
retention period requires `s3:PutObjectRetention` in addition:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "kuberecordArchiveWrite",
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:PutObjectRetention"],
      "Resource": "arn:aws:s3:::kuberecord-audit/*"
    }
  ]
}
```

Drop `s3:PutObjectRetention` if the sink has no `spec.objectLock` — including
when the bucket's default retention is what applies, since the store adds that
itself and the request does not ask for it.

There is deliberately no `s3:ListBucket`, no `s3:GetObject` and no
`s3:DeleteObject` here. kuberecord cannot list the archive, cannot read it back
(that is [D12](CRDS.md#historyunavailable--a-limit-not-a-fault), the whole reason
an `S3Sink` reports `HistoryUnavailable=True`) and cannot delete from it. Reading
the archive is a **separate consumer with separate rights** — the DuckDB and
Athena recipes in [`docs/QUERIES.md`](QUERIES.md#the-s3-archive), or an auditor,
who needs rather more than the operator does:

| Who | Actions |
|---|---|
| The sink | `s3:PutObject`, and `s3:PutObjectRetention` with `spec.objectLock` |
| A query engine | `s3:ListBucket`, `s3:GetObject` |
| An auditor checking the lock | the above, plus `s3:ListBucketVersions`, `s3:GetObjectVersion`, `s3:GetObjectRetention`, `s3:GetBucketObjectLockConfiguration` |

An auditor needs the version-level actions for two reasons this page comes back
to: on a versioned bucket, [listing objects and listing versions are different
questions](#what-a-retried-upload-does-on-a-versioned-bucket), and a
[delete marker](#object-lock-prevents-destruction-not-concealment) hides a
retained version from the first one.

### What happens if the bucket is not ready

A sink with `spec.objectLock` against a bucket that has no Object Lock
configuration does not degrade quietly. The health probe writes a real object
with real retention headers, the bucket refuses the request, and the refusal is
classified as being about the *shape* of what this sink writes rather than about
reachability:

```
BucketReachable=False   reason: BucketIncompatible
Ready=False
```

That condition never clears on its own — every write this sink attempts fails
identically — so it is a permanent state until a human changes the bucket or the
spec. The full condition reference is
[`docs/CRDS.md`](CRDS.md#s3sink).

## Applying retention: `spec.objectLock`

```yaml
apiVersion: kuberecord.io/v1alpha1
kind: S3Sink
metadata:
  name: archive
spec:
  bucket: kuberecord-audit
  prefix: clusters/prod
  region: eu-central-1
  objectLock:
    mode: GOVERNANCE        # or COMPLIANCE — see below, and mean it
    retainDays: 365
```

That is the whole feature. Both fields are required when the block is present,
`retainDays` is bounded at 36500 (a century, which is a typo guard rather than a
technical limit), and there is no third field: kuberecord sets a retention
period and nothing else. It does not place legal holds, does not set bucket
defaults, and does not read a lock back.

| | |
|---|---|
| **What is set** | The `x-amz-object-lock-mode` and `x-amz-object-lock-retain-until-date` headers on every `PutObject` this sink issues — record objects, scope-log objects, and the first health-probe object. |
| **What it applies to** | One object **version**. A retention period protects the version named in the request; it says nothing about versions written later under the same key. |
| **When it is decided** | When the object is built, from the wall clock: `retainUntil = now + retainDays`. |
| **On a retry** | Unchanged. The request is built once, so every attempt re-sends the same bytes under the same key with the same retain-until date. |

### Why the date comes from the wall clock and not from the records

An object's *key* is dated from the timestamp of the first record in it (see
[the key layout](SCHEMA.md#the-object-key-layout)). Its *retention* is not.
Retention is a property of when the archive received the object, not of when the
events inside it happened — so a record that describes something from an hour
ago still gets a full `retainDays` from the moment it is written, and an object
built from anything older cannot arrive already expired. The two dates answer
different questions, and [object age is not data age](#object-age-is-not-data-age)
is where that distinction starts to matter operationally.

### The probe object writes once per minute

The sink's health probe writes a small fixed-key object,
`<prefix>/.kuberecord-probe`, roughly every 60 seconds, because a probe that
does not write would pass with a read-only credential and then fail every real
write. On a versioned bucket — which is to say on every Object Lock bucket —
each of those writes creates a **new version** of that key.

kuberecord therefore carries retention headers on the **first successful probe
only**. It has to carry them at least once, since a bucket that will not accept
them is exactly the failure the previous section describes; carrying them every
time would mint about 1,440 retained versions a day, each undeletable for
`retainDays`, and under `COMPLIANCE` undeletable by anyone at all. Once is
enough, because Object Lock cannot be disabled on a bucket once enabled, so the
answer cannot change under a running sink — and a change to `spec.objectLock`
arrives as a new configuration fingerprint, therefore as a new sink instance,
which probes afresh.

A **bucket default retention** does not have that escape. It applies to every
version that arrives without its own retention, which includes every probe write
after the first. If you set a bucket default, expect the probe key to accumulate
retained versions at probe cadence, and see
[lifecycle rules](#lifecycle-rules) for the cleanup rule that will not be able
to remove them until they expire.

## `GOVERNANCE` and `COMPLIANCE`

The modes differ in exactly one thing — who can shorten or lift a retention that
has already been applied — and that one thing is the entire compliance question.

| | `GOVERNANCE` | `COMPLIANCE` |
|---|---|---|
| Delete a retained version | Only with `s3:BypassGovernanceRetention`, *and* an explicit `x-amz-bypass-governance-retention: true` on the request | Nobody. Including the account root user. |
| Shorten the retention | Same bypass | Nobody |
| Extend the retention | Anyone with `s3:PutObjectRetention` | Anyone with `s3:PutObjectRetention` |
| Change the mode | Bypass, to `COMPLIANCE` or off | Nobody. `COMPLIANCE` is terminal for that version. |
| Escape hatch if you got it wrong | Grant the bypass permission to one principal, fix it, revoke | Wait out `retainDays`. AWS documents exactly one alternative: closing the AWS account. |

kuberecord passes the mode through as a string and interprets neither. It cannot
weaken one, cannot lift one, and holds no bypass permission of its own.

**Start with `GOVERNANCE`.** It gives the same protection against everyone who
does not hold the bypass permission — which, if you have not granted it, is
everyone — while leaving a way to correct a mistake. Reach for `COMPLIANCE` when
a regulator requires it and you have already run the configuration you intend to
ship, under `GOVERNANCE`, for long enough to know that `retainDays` and the
`spec.policy.redaction` floor are the ones you meant. The specific mistake to
rehearse is not a typo in `retainDays`; it is a redaction path you forgot, which
is the subject of [the limits section](#redaction-remains-forward-only).

## What a retried upload does on a versioned bucket

An object's key is the hex SHA-256 of its uncompressed payload, so a retried
upload of the same batch is a byte-identical `PutObject` under a byte-identical
key. On an **unversioned** bucket that replaces the object in place, and
at-least-once delivery collapses in the store: one key, one object, one copy of
the bytes.

On a **versioned** bucket — every Object Lock bucket, and plenty of others — it
does not. S3 versioning has no concept of an idempotent PUT: the second request
creates a second version. Retention does not prevent that and is not supposed
to. A retention period protects the version it was placed on; in AWS's own
words, retention periods *"don't prevent new versions of the object from being
created"*, and putting an object whose key matches an existing protected object
*"creates a new version of that object"* while the protected one *"remains
locked according to its retention configuration"*.

So the honest statement of the property is:

> **Idempotency is reader-visible, not storage-level.** After a retried upload
> the key has exactly **one current version**, holding exactly the bytes it
> should. The bucket may hold **two versions** of it, byte-identical. Anything
> that reads current versions — `ListObjectsV2`, `GetObject` without a version
> ID, the DuckDB and Athena recipes, `mc ls` — sees one object and cannot tell
> the difference. Anything that reads versions sees the duplicate.

Nothing is refused, nothing is logged, and no reader is misled. The archive's
content is what it should be. What changes is what you are paying for, and how a
cleanup rule behaves.

**The `COMPLIANCE` consequence, explicitly.** The duplicate version is the full
size of the object — up to `spec.rotation.maxObjectBytes`, 64Mi encoded by
default. Under `COMPLIANCE` it cannot be deleted by anyone, by any means, for
the whole of `retainDays`; a lifecycle rule cannot remove it either (see below).
At a year's retention you store and pay for that duplicate for a year. Under
`GOVERNANCE`, a principal holding the bypass permission can delete the
noncurrent version as soon as they notice it.

This is a small cost in practice, and it is worth knowing where it comes from
rather than discovering it in a bill. A duplicate version needs a *lost
acknowledgement*: the object reached the bucket and the response did not reach
the operator, so the writer retried a PUT that had already succeeded. That is
rare, it is the case the content-addressed key exists to make harmless, and it
is asserted against a real locked bucket in CI —
`TestARetriedObjectOnALockedBucketLeavesOneCurrentVersionIntegration` forces
exactly that failure and checks both halves of the claim: one current version,
and at most one extra version that decodes to the identical batch.

## Lifecycle rules

Object Lock and S3 Lifecycle are independent mechanisms that meet on the same
object versions, and the interaction is simple enough to state completely:

- **Lifecycle keeps working.** A configured rule continues to evaluate protected
  objects, including placing delete markers.
- **Expiration cannot delete a protected version.** AWS: *"a locked version of
  an object cannot be deleted by a S3 Lifecycle expiration policy."* This covers
  `NoncurrentVersionExpiration` too, which is the rule you would otherwise reach
  for to clean up the duplicate versions above and the probe key's versions —
  it removes them once their retention has expired, and not before. Lifecycle
  holds no governance bypass.
- **Transitions are unaffected, and the lock survives them.** Object Lock is
  maintained regardless of storage class and across transitions between classes.
  So the cheap shape works exactly as you would hope: retain for seven years,
  transition to a colder class after thirty days, and the retention travels with
  the object.
- **Delete markers are not protected.** They can be placed, and they can be
  removed, on any key regardless of retention. See
  [the limits section](#object-lock-prevents-destruction-not-concealment).

A rule that expires the archive after its retention is therefore the normal
configuration, and the two dates should agree — but not because a mismatch is
harmless. **An `Expiration` rule shorter than `retainDays` is the worst of both
outcomes on a versioned bucket:** it cannot delete the retained version, so you
keep paying for it, and it *can* place a delete marker, so the object disappears
from every reader that does not ask for versions. You end up with an archive that
looks expired and is not. Size the rule against `retainDays`, and if you want the
data gone at the same moment it stops being protected, make those the same number.

### Scope the prefix deliberately

Three kinds of object live under a sink's `spec.prefix`, and only one of them is
records:

```text
<prefix>/format=jsonl-v1/cluster_id=<id>/date=…/hour=…/<hash>.jsonl.zst   records
<prefix>/format=jsonl-v1/scopes/date=…/<hash>.jsonl.zst                   scope log
<prefix>/.kuberecord-probe                                                probe
```

A rule filtered on `<prefix>/format=jsonl-v1/cluster_id=` covers records alone.
A rule filtered on `<prefix>/` covers all three — which is usually what you want
for expiry, and is what reaches the probe key's accumulated versions (a rule
filtered on `<prefix>/.kuberecord-probe` reaches only those, which is the tidier
way to clean up after a bucket default retention).
Do not expire the scope log on a shorter schedule than the records: it is what
tells a future reader whether an absence of records means "nothing changed" or
"nobody was watching" (see [the scope log](SCHEMA.md#the-scope-log)).

### Object age is not data age

A lifecycle rule reasons about how old an *object version* is — the store's own
creation timestamp for that version. kuberecord's key partitions say how old the
*data* is: `date=` and `hour=` come from the timestamp of the **first record** in
the object, never from the wall clock at write time.

The two are close but not identical, and the difference has a direction. An
object stays open, accumulating records, until it rotates — by size, or after
`spec.rotation.maxObjectAge` (5 minutes by default, 1 hour at the ceiling) — and
then a failing PUT may be retried for up to a minute more. So an object is
written *after* the partition it is filed under, by up to roughly
`maxObjectAge` plus the retry window. Enough to cross a midnight: a version
created at `00:03Z` can be filed under yesterday's `date=`.

It does not skew further than that. Records dated from history exist in this
project — a warm-up close-out is dated from what the backend already recorded —
but reading history back is precisely what a `Writer`-only sink cannot do (D12),
so no such record is ever written to an archive. Every record an `S3Sink`
receives is stamped when the operator observes the change.

The practical rules that follow:

- A rule keyed on **object age** (`Expiration: Days`,
  `NoncurrentVersionExpiration`, transitions) behaves exactly as it reads. Use
  it, and size it against `retainDays`, not against the partitions.
- A rule reasoning about the **data's age** — "expire the objects covering
  January" — must read the partition path, with a prefix filter per `date=`
  partition. A day-age rule will not do it, and will be off by up to
  `maxObjectAge` at every boundary.
- A **query** over a time window has the mirror-image of this problem, and the
  answer is in [`docs/QUERIES.md`](QUERIES.md#which-objects-cover-a-time-window):
  widen the lower bound of the partition range by `maxObjectAge`.

## Limits, stated plainly

Object Lock is a strong guarantee about a narrow thing. These are the gaps
between "the objects are locked" and "the audit trail is proven", and none of
them is a defect to be worked around — they are the shape of what this release
ships.

### kuberecord does not sign anything

There is no signature, no notarisation, and no hash chain linking one object to
the next. The SHA-256 in each object key is the writer's own digest of that
object's uncompressed payload, computed by the same process that wrote it: it
detects corruption and makes a retry idempotent, and it proves nothing about
provenance. Anyone who can write to the bucket can write an object whose key is
the correct hash of its own contents.

So Object Lock is protection against **deletion and overwrite**, which is real
and store-enforced, and it is **not a cryptographic chain of custody**. If you
need one, it has to come from outside kuberecord — the store's own audit trail
of who called `PutObject`, and whatever attestation your compliance regime asks
for.

### The operator's own credentials can write new objects

The sink holds `s3:PutObject` on the prefix for as long as it is running, and
retention does not restrict what *new* objects may be created — only what may
happen to versions that already exist. A compromised sink credential cannot
destroy the archive, cannot alter an archived object, and cannot roll a value
back. It can append. An archive that is immutable per object is not therefore an
archive nobody can add to, and a reader who trusts every line because the bucket
is locked has drawn the wrong conclusion from the lock.

### Object Lock prevents destruction, not concealment

A `DELETE` that names no version ID does not delete a protected version; it
inserts a **delete marker**, which becomes the current version. The retained
version survives underneath, untouched — but the key disappears from
`ListObjectsV2`, from an unversioned `GetObject`, and therefore from every
DuckDB or Athena glob over the archive. Delete markers are not WORM-protected
regardless of any retention beneath them.

kuberecord's own credential cannot do this: it has no `DeleteObject`. A
principal that does can hide an arbitrary amount of a locked archive without
breaking a single lock. Two consequences worth designing around:

- **An auditor must list versions, not objects** — `ListObjectVersions`, and
  read past delete markers.
- **Do not grant `s3:DeleteObject` on the archive prefix** to anything, and
  consider denying it in the bucket policy outright. The lock will not do it for
  you.

### Redaction remains forward-only

This is the trade that has to be made knowingly.

`spec.policy.redaction` scrubs values *before* hashing, on the way in, so a
redacted value is never stored — but it applies only to what is written after
the path is configured. Records already written keep whatever they recorded;
redaction is [not retroactive](SCHEMA.md#what-redaction-is-not) on any backend.

On ClickHouse that is recoverable. The rows are in a database, and anyone who can
reach it can remove them with an `ALTER TABLE … DELETE` — which is the same
property that makes the "append-only" claim conventional rather than enforced. It
cuts both ways, and this is the direction in which it cuts for you.

On an S3 archive it is not recoverable at all:

- The object cannot be edited. There is no partial rewrite of an object; the
  format's own versioning policy exists because archived objects may be legally
  immutable ([versioning the object format](SCHEMA.md#versioning-the-object-format)).
- Under `GOVERNANCE`, a principal holding the bypass permission can delete the
  offending object versions. That is the remediation, and it is coarse — the
  object holds a whole batch of unrelated records, and deleting it removes all of
  them.
- Under `COMPLIANCE`, **there is no remediation**. The value stays in the archive
  until the retention expires. Nobody can remove it, including the account root.

Two things follow, and the tee pattern makes both sharper. Get the redaction
floor right *before* enabling `COMPLIANCE`, and keep the floors on the hot and
cold sinks identical — an archive tier must never be a way around a sink's
redaction floor, and on the cold side the mistake is permanent. See
[`docs/TEE.md`](TEE.md#authoring-one).

### An absence in the archive is not evidence of absence

A `Writer`-only sink cannot read its own history, so three behaviours are off
for it: dedup warm-up, zombie garbage collection, and boot reconciliation of
scope epochs. In an archive that means every record is a permanent `Snapshot`,
and an object deleted while the operator was down is never recorded as deleted.
The bucket looks exactly like the bucket of a cluster where nothing was deleted.

Object Lock proves that what is in the archive was not altered. It says nothing
about what never arrived. The scope log narrows the gap from inside the bucket —
it records when watching started and stopped — and the supported way to close it
is the tee pattern: a `ClickHouseSink` alongside, which can read its own history
and therefore records deletions properly.
See [`HistoryUnavailable` — a limit, not a fault](CRDS.md#historyunavailable--a-limit-not-a-fault).

### There is no legal hold, and no bucket configuration

kuberecord sets retention periods only. It does not place or remove legal holds
(`s3:PutObjectLegalHold`), does not read lock state back, does not set bucket
defaults, and does not verify that Object Lock is enabled beyond the health
probe's write succeeding or failing. Those are all deliberately operations of
the account, not of the operator — and each is one more permission that would
otherwise have to be granted to a process running in a cluster.

## See also

- [`docs/CRDS.md`](CRDS.md#s3sink) — the `S3Sink` conditions, including
  `BucketIncompatible` and `HistoryUnavailable`.
- [`docs/SCHEMA.md`](SCHEMA.md#physical-mapping-to-s3-objects) — the object
  format and key layout as a versioned public contract (D15).
- [`docs/QUERIES.md`](QUERIES.md#the-s3-archive) — reading the archive, which is
  a separate consumer with separate rights.
- [`docs/TEE.md`](TEE.md) — running a queryable timeline and an immutable
  archive from one watch, and the four things to get right when you do.
- [`config/samples/kuberecord.io_v1alpha1_s3sink.yaml`](../config/samples/kuberecord.io_v1alpha1_s3sink.yaml)
  — a production-shaped `S3Sink`, `objectLock` included.
