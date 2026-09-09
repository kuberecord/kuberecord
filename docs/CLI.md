# The kuberecord CLI

`kubectl kuberecord` answers questions about recorded Kubernetes state changes —
who changed what, when, and what the object looked like before — without needing
the cluster the change happened in to still exist.

It ships as one binary under two names. `kubectl-kuberecord` on your `PATH` makes
`kubectl kuberecord …` work; the same build installed as `kuberecord` works
standalone, which is what an auditor with an object-store archive and no cluster
access wants. Both are built from the same package and behave identically, down to
naming themselves correctly in their own help text.

This page is the reference for **the commands**, **where the CLI reads from** and
**how it is configured**.

- [Installing](#installing)
- [Global flags](#global-flags)
- [`timeline`](#timeline)
- [`diff`](#diff)
- [`get --at`](#get---at)
- [`blame`](#blame)
- [`scopes`](#scopes)
- [`version`](#version)
- [Output formats](#output-formats)
- [Structured output](#structured-output)
- [Where the data comes from](#where-the-data-comes-from)
- [Running the CLI outside the cluster](#running-the-cli-outside-the-cluster)
- [Backend capability differences](#backend-capability-differences)
- [The cluster identity](#the-cluster-identity)
- [The configuration file](#the-configuration-file)
- [The read-only ClickHouse user](#the-read-only-clickhouse-user)
- [What the CLI asks of Kubernetes](#what-the-cli-asks-of-kubernetes)
- [Evaluation mode](#evaluation-mode)
- [Exit codes](#exit-codes)

## Installing

Four channels, one build. Whichever you use, the bytes come from the archives a
tagged release publishes — krew, Homebrew and `go install` are three ways of
getting a copy of the same artifact, not three builds of the same source.

```sh
# 1. krew, which is how a kubectl user finds a plugin.
kubectl krew install kuberecord
kubectl kuberecord version

# 2. Homebrew, on macOS and on Linux. The one channel that installs both names.
brew install kuberecord/tap/kuberecord

# 3. The release archive, directly. Verifiable, and the only way to install on
#    Windows.
curl -fsSLO https://github.com/kuberecord/kuberecord/releases/download/v0.3.2/kuberecord_v0.3.2_linux_amd64.tar.gz
tar -xzf kuberecord_v0.3.2_linux_amd64.tar.gz
install -m 0755 kubectl-kuberecord kuberecord ~/.local/bin/

# 4. From source, with a Go toolchain.
go install github.com/kuberecord/kuberecord/cmd/kubectl-kuberecord@v0.3.2
```

They do not all give you the same thing, and the difference is the two names:

| | `kubectl kuberecord …` | `kuberecord …` | Version stamp | Signature you can check |
|---|---|---|---|---|
| `kubectl krew install` | yes | no | yes | krew checks the `sha256` |
| `brew install` | yes | yes | yes | brew checks the `sha256` |
| Release archive | yes | yes | yes | yes — cosign, [`VERIFYING.md`](VERIFYING.md#the-cli-archives) |
| `go install` | yes | no | module version only | no — you are building it |

**krew installs the plugin only**, because that is what krew is: a plugin
manager. `kubectl kuberecord …` works; the standalone `kuberecord`, which is what
an auditor reading an archive with no cluster wants, comes from Homebrew or from
the release archive. They are the same bytes either way — one compilation, copied
into the second name, so the two can never be built from different
trees.

**`go install` builds rather than downloads.** It gets you `kubectl-kuberecord`
and nothing else, it reports the module version rather than the release stamp —
`commit` and `buildDate` come out of what the Go toolchain recorded — and there is
no signature over a binary you compiled yourself. Pin a tag rather than `@latest`
if you want to know what you got.

**Windows** is release archives only: `kuberecord_v0.3.2_windows_amd64.zip`, which
carries `kubectl-kuberecord.exe` and `kuberecord.exe`. krew supports Windows and
the plugin manifest declares it; Homebrew does not run there.

The manifest krew consumes (`kuberecord.yaml`) and the Homebrew formula
(`kuberecord.rb`) are themselves release assets, generated from the archives and
listed in `checksums.txt` — so the digests they publish are covered by the same
signature as everything else a release attaches.

### Shell completion

The same binary generates its own completion script, so completion arrives from
whichever of the four channels you used — there is nothing extra to download.
`brew install` writes the bash, zsh and fish scripts during installation and you
are done; the other three channels want one of the lines below.

```sh
# bash — needs the bash-completion package, which your OS package manager has.
kuberecord completion bash > /etc/bash_completion.d/kuberecord            # Linux
kuberecord completion bash > $(brew --prefix)/etc/bash_completion.d/kuberecord   # macOS

# zsh — after `echo "autoload -U compinit; compinit" >> ~/.zshrc`, if you have
# not enabled completion before.
kuberecord completion zsh > "${fpath[1]}/_kuberecord"                    # Linux
kuberecord completion zsh > $(brew --prefix)/share/zsh/site-functions/_kuberecord  # macOS

# fish
kuberecord completion fish > ~/.config/fish/completions/kuberecord.fish

# PowerShell — append the output to your profile to keep it.
kuberecord completion powershell | Out-String | Invoke-Expression
```

Each of `completion bash`, `completion zsh`, `completion fish` and
`completion powershell` writes the script to **stdout**, so it can be redirected
or sourced. `--no-descriptions` omits the gloss shown beside each candidate;
bash appends that gloss to the candidate itself rather than showing it in a
second column, which is the one shell where a long menu reads better without it.

**`kubectl kuberecord` is completed by kubectl, not by a script from here.** A
shell function binds to a single word, and that one is two. kubectl completes a
plugin from an executable named `kubectl_complete-<plugin>` on your `PATH`, which
it calls with the words typed so far — and the protocol it speaks is the one this
binary already answers, so the whole of it is two lines:

```sh
printf '#!/usr/bin/env sh\nexec kubectl-kuberecord __complete "$@"\n' \
  > ~/.local/bin/kubectl_complete-kuberecord && chmod +x $_
```

Running `kubectl kuberecord completion bash` prints that same instruction to
stderr, because the script it writes on stdout is the standalone command's and a
krew install has only the plugin.

**What completes, and what does not.** Subcommands, flag names, the values of
every flag whose values are a closed set (`--output`, `--color`, `--backend`,
and the kind half of `--sink`/`--from-sink`), the profiles in
[your configuration file](#the-configuration-file), and `kubectl`'s built-in
resource short names — `deploy`, `sts`, `cm`, `ing` and the rest, each shown
beside the kind it names.

Object *names* do not complete, and neither do the short names a CRD declares.
Both would mean contacting the API server on a keystroke, which is a surprising
thing for a TAB press to do and the same instinct behind
[the CLI not forwarding your port](#the-cli-will-not-forward-the-port-for-you).
Everything the menu offers comes from the binary or from your own configuration
file. Nothing here dials a cluster or a backend.

## Global flags

Every command carries the same two sets of persistent flags, and the split
between them is worth knowing: the first set is kuberecord's, the second is
`kubectl`'s own, inherited unchanged so that a plugin behaves like the thing it
plugs into.

### kuberecord's own

| Flag | Default | What it does |
|------|---------|--------------|
| `--source <dir\|s3://bucket/prefix>` | — | Read directly from a location, bypassing sink discovery. A plain path or a `file://` URL is a directory holding `format=jsonl-v1/`. Step 1 of [where the data comes from](#where-the-data-comes-from). It replaces the chain; `--sink-addr` corrects one field of it — see [`--source` versus `--sink-addr`](#--source-versus---sink-addr). |
| `--sink <Kind>/<name>` | — | Read through a configured sink custom resource, named explicitly — `ClickHouseSink/default`, `S3Sink/cold`. Step 2. |
| `--sink-addr <host:port>` | — | Dial this endpoint instead of the one the resolved ClickHouse backend recorded, which is what a forwarded port needs. It replaces the address and **nothing else**, and the notice on stderr says so — see [`--sink-addr`](#--sink-addr), [`--source` versus `--sink-addr`](#--source-versus---sink-addr) and [Running the CLI outside the cluster](#running-the-cli-outside-the-cluster). |
| `--profile <name>` | the file's `currentProfile` | Use this profile from [the configuration file](#the-configuration-file). Step 3. |
| `--cluster-id <id>` | resolved, and the answer printed | Selects **which cluster's records to read from the sink** — the `cluster_id` column stamped on every row. Resolved in five steps if you omit it; see [The cluster identity](#the-cluster-identity). |
| `--cluster <name>` *(kubectl's)* | the current context's cluster | Selects **a cluster entry from your kubeconfig** — which API server `kubectl` connects to. It is kubectl's own flag, described here rather than in the list below only because of the pair note that follows. |
| `--operator-namespace <ns>` | searched, then `kuberecord-system` | Where a sink's credentials Secret and the operator's Deployment are looked for. |
| `-o`, `--output <format>` | `table` | One of `table`, `wide`, `json`, `jsonl`, `yaml`, `diff`. Not every command accepts every one — see [Output formats](#output-formats). |
| `--color <mode>` | `auto` | `auto`, `always` or `never`. Under `auto`, colour is on only when stdout is a terminal and `NO_COLOR` is unset; `--color=always` overrides `NO_COLOR`, which is what the flag is for. |
| `--max-objects <n>` | `0` (no limit) | Abort a scan that fetches more than this many stored objects, naming this flag. It bounds the *work*, which `--limit` cannot do without an index — see [Cold scans](#cold-scans). |
| `--yes` | assumed off a terminal | Answer the confirmation a wide or unmeasurable scan of an unindexed backend asks for. Assumed when the output is not a terminal, so a script never waits on a prompt. |
| `-v`, `--v <n>` | `0` | Verbosity of the diagnostics written to **stderr**. It never changes what goes to stdout, so raising it cannot disturb a pipe. |
| `-h`, `--help` | — | Help for the command it is given to. |

`--source`, `--sink` and `--profile` are the three ways of naming a backend, and
they are tried in that order. Whichever wins is announced on stderr, always.

**`--cluster` and `--cluster-id` are different things, and both are frequently
correct at once.** One says *where to connect*, the other says *whose records to
read*, and nothing requires them to agree. Reading a production cluster's history
through a kubeconfig context that points at a bastion means setting both, and
setting them differently: `--cluster` picks the entry that reaches an API server,
`--cluster-id` picks the identity stamped on the rows you want back. Neither
implies the other, and neither is ever derived from the other — which is why the
second flag carries the suffix, and why `--help` says so on the flag itself.

### Inherited from `kubectl`

These come from `genericclioptions.ConfigFlags` — the same code `kubectl` itself
uses — and mean exactly what they mean there. They are listed rather than
described, because a divergent description of somebody else's flag is a lie
waiting to happen — `--cluster` is described above only because leaving it
undescribed is what lets it be read as `--cluster-id`:

`--as`, `--as-group`, `--as-uid`, `--as-user-extra`, `--cache-dir`,
`--certificate-authority`, `--client-certificate`, `--client-key`, `--cluster`,
`--context`, `--disable-compression`, `--insecure-skip-tls-verify`,
`--kubeconfig`, `-n`/`--namespace`, `--request-timeout`, `-s`/`--server`,
`--tls-server-name`, `--token`, `--user`.

Three notes on how they interact with the rest:

- **`--cluster` is kubectl's, `--cluster-id` is kuberecord's**, they are not the
  same selection, and you may well need both — described with the pair
  [in the table above](#kuberecords-own).
- **`-n`/`--namespace` narrows an object address**, and for [`scopes`](#scopes)
  alone it does *not* default to the kubeconfig's current namespace: a compliance
  question about what was being recorded means the whole cluster unless you say
  otherwise.
- **Every one of them is inert under `--source`.** Reading an archive contacts no
  API server, so a kubeconfig flag has nothing to configure — with one exception,
  [resolving a short name like `deploy`](#reading-an-archive-without-a-cluster),
  which is server-side discovery data.

## `timeline`

```console
$ kubectl kuberecord timeline deploy/checkout -n payments
→ discovered ClickHouseSink/default (clickhouse.kuberecord-system.svc:9000/kuberecord)
→ cluster-id prod-eu-1 (from the operator Deployment kuberecord-system/kuberecord-controller-manager)
Kind:     apps/Deployment
Object:   payments/checkout
Cluster:  prod-eu-1
UID:      7c9e6679-7425-40de-944b-e07fc1f90ae7
Coverage: 2026-07-02T09:14:00Z → open (ClusterStreamRule/all-workloads)

TIME (UTC)               EVENT     ACTOR                      CHANGE
2026-08-28 14:02:58.001  Added     kubectl-client-side-apply  full state recorded
2026-08-28 14:03:11.482  Modified  kubectl-client-side-apply  ~ spec.…containers[0].resources.limits.memory: 2Gi → 512Mi
2026-08-28 14:05:02.117  Modified  kube-controller-manager    3 ops
2026-08-28 14:09:40.900  Modified  unknown                    ~ metadata.…deployment.kubernetes.io/revision: 1 → 2
! 3 rows are shortened to fit the CHANGE column; pass --full to print every operation
```

The header and the table go to **stdout**; every banner, notice and explanation
goes to **stderr**. One sentence: stdout is the data, stderr explains it. That is
what makes `timeline … | wc -l` count changes.

Lines opening `!` are notices. They are the qualifications on the answer — a
backend that records no deletions, a window with no state before it, a column
that could not show a row whole — and they are the one thing on the screen a
reader cannot skip and still read the table correctly, so they are marked and
never dimmed. Lines opening `→` are provenance: which sink, which cluster
identity, which step of the resolution chain answered. **On a terminal those
recede, unless the chain chose something you would not assume** — see
[Where the data comes from](#where-the-data-comes-from).

A line opening `error:` is the third register, and it is **red**: it says no
result arrived, which is a different thing from a notice qualifying one that did.
Everything printed after it — a usage block, or the routes past the failure — keeps
the weight it was written in, so the line that stopped the command stays the one
that stands out. See [Exit codes](#exit-codes).

**Rows read oldest first, so the newest change is the last line printed** — the
one immediately above your prompt, because this CLI does not page. Which changes
are selected is a separate matter and has not moved: `--limit` still takes the
**newest** N, and only their layout runs forward. `--reverse` puts the newest at
the top.

### Flags

| Flag | What it does |
|------|--------------|
| `--since`, `--until` | Bound the window. Either a duration — `90m`, `6h`, `3d`, `2w`, `1d6h` — or an instant: `2026-08-20`, `2026-08-20 14:00:00`, `2026-08-20T14:00:00Z`. Both read as *ago*. |
| `--from`, `--to` | Aliases for `--since` and `--until`, spelled the way the structured output and the query contract spell these bounds. Giving one bound under both names with two different values is a usage error. |
| `--limit` | At most this many changes. It selects the **newest** N, and they are displayed oldest first. Default `100`; `0` means no limit. |
| `--reverse` | Show the same changes newest first. It reorders rows; it does not select different ones. |
| `--actor`, `--exclude-actor` | Field-manager predicates. Repeatable. `--exclude-actor` is applied second and wins on conflict. |
| `--field` | Field-path prefixes, repeatable. Either spelling works: `spec.containers[0].image` or `spec.containers.0.image`. |
| `--uid` | Pin the timeline to one incarnation. |
| `--all-incarnations` | Show every incarnation in the window, with a `UID` column. |
| `--full` | Print every operation of every patch, unshortened. The footer names it when a row was shortened without it. |
| `--with-events` | Interleave the Kubernetes Events recorded about the object. |

`-o wide` adds the full UID and the resource version, and prints timestamps at the
nanosecond precision the schema records.

Against an object archive the window is not a filter, it is the work — see
[Cold scans](#cold-scans) for what a wide `--since` costs there, and for the
`--yes` and `--max-objects` flags that go with it.

### How a change is summarized

A one-operation patch is one line, with the operation's glyph — `+` added, `-`
removed, `~` replaced — the field path, and the values. A larger patch is
summarized as `N ops`, and `--full` expands it:

```console
$ kubectl kuberecord timeline deploy/checkout -n payments --full
2026-08-28 14:05:02.117  Modified  kube-controller-manager    3 ops
    ~ spec.replicas: 3 → 5
    + spec.paused: true
    - spec.minReadySeconds: 10

2026-08-28 14:09:40.900  Modified  deployment-controller      ~ spec.replicas: 5 → 7
```

An expanded block is closed by a blank line, and only a row that actually
expanded gets one — a timeline the `CHANGE` column held entirely reads exactly as
it does without the flag. On a terminal the expanded operations also **recede**:
they are the detail you asked to see, so they are dimmed and the row above them
becomes the spine of the page without being touched. The `+`, `-` and `~` stay at
full intensity inside the dimmed line, because they are how you scan an
eleven-operation patch for the *kind* of change in it.

Nothing is highlighted, here as in [`get`](#everything-but-the-object-recedes),
and the blank line is why: under `--color=never`, under `NO_COLOR` and any time
stdout is not a terminal, the separation is still there in the characters. A
`--full` redirected to a file is the same document with the escapes gone.

The count carries no glyph. `+`, `-` and `~` mean an operation happened, here and
in the hunk view, and a summary is not an operation — `~3 ops` said, in the only
vocabulary this column has, that something called "3 ops" had been replaced.

**`--full` is named once, in the footer, and only when a row was actually
shortened.** A row summarized as a count and a row whose path the column had to
elide are both rows the flag would show more of; a timeline where every row fits
prints no footer at all, so a footer that is there means there is something
behind it.

Paths are RFC 6901 JSON Pointers converted to a dotted form with bracketed array
indices, elided in the middle when they exceed the column. The head is kept
because a change under `spec` and one under `status` are different news; the tail
is kept because that is what names the field.

**The value on the left of the arrow is reconstructed, not recorded.** An RFC 6902
operation carries the value it wrote and not the one it replaced, so the CLI
anchors a single `StateAt` just before the oldest change it is about to show and
replays the patches forward in memory — one round trip, not one per row. Three
things follow, and each is announced on stderr rather than left to be inferred:

- With `--actor`, `--exclude-actor` or `--field` in force the rows shown are **not
  consecutive**, so the arrow is dropped. Replaying only the surviving patches
  over a real base state would produce a document the object was never in, and the
  numbers read out of it would be confident and wrong.
- If the base has aged out of the retention window, or the backend cannot
  reconstruct state, the new value is still exact and the old one is absent.
- A patch that will not apply to the reconstructed state stops the replay at that
  row, and the notice names the row.

### Incarnations

Kubernetes reuses names. A `(namespace, name)` pair may have belonged to several
objects with different UIDs, and a timeline that spliced their histories together
would be a coherent-looking account of something that never happened. So the
newest incarnation is shown, the header names its UID, and the others are named
too:

```
! payments/checkout has had 2 incarnations in this window; showing the newest
  (7c9e6679-…). Pass --all-incarnations to see them all, or --uid to pin one
```

`--all-incarnations` adds a `UID` column, so no two of them can blur together in
one table.

[`diff`](#diff) and [`blame`](#blame) print the same banner over the same
selection, and neither has `--all-incarnations` — a diff or a field table spanning
two UIDs is the splice this section exists to prevent. On those two the banner
offers `--uid`, and names `timeline --all-incarnations` as where the others can be
read:

```
! payments/checkout has had 2 incarnations in this window; showing the newest
  (7c9e6679-…). Pass --uid to pin one, or `timeline --all-incarnations` to see
  them all
```

### An empty result is never presented on its own

Every empty timeline is explained against the watch scopes that were open at the
time, and there are exactly three answers:

| What was found | What you get |
|----------------|--------------|
| The scope was watched across the whole window | `no changes recorded … The scope was confirmed watched over <interval>` — the silence is real. |
| The scope opened *after* the window started | A warning naming that instant and the rule that opened it: a change before then would not have been recorded. |
| No scope ever covered it | Exit **3**, and a message saying so. This is a finding, not an empty result — [`scopes`](#scopes) is where you go next. |

A backend with no scope log to read says that instead, and exits `0`: it cannot
tell the three apart, and pretending otherwise would be the failure this section
exists to prevent.

### A filter that matched nothing is not an empty window

`--actor`, `--exclude-actor` and `--field` are pushed into the *query*, so the
changes they remove never arrive. That makes a filtered timeline that matched
nothing look, from the renderer's side, exactly like a window in which nothing
happened — and the three answers above would then explain a filter's doing against
coverage, which is a different and false claim.

So when a predicate is in force and no change survived it, the same window is read
once more with the predicates taken out, and what comes back decides:

| What the re-read found | What you get |
|------------------------|--------------|
| Changes are there | `changes are recorded … and --actor nobody matched none of them; the window itself is not empty` — the filter's answer, not the object's. |
| Nothing is there | The filter is not what emptied it, so the three answers above apply unchanged — including exit **3** when no scope ever covered it. |
| The re-read failed | It says so, and names the failure. Neither reading is asserted. |

The notice prints the values as well as the flags, because the usual cause is a
field manager spelled the way a person remembers it rather than the way the API
server records it, and seeing the string back is what makes that visible. Note too
that **a deletion records no actors**, so any `--actor` excludes every deletion.

`diff` and `blame` need no re-read: their `--field` narrows what is *rendered*
rather than what is read, so both counts are already in hand and the notice
carries them.

### `--with-events` that finds no Events

The same rule applies to the question `--with-events` asks inside the first one.
When the flag is given and nothing is interleaved, the watch scopes are consulted
about `Event` — **both** API spellings, `v1` and `events.k8s.io/v1`, since a rule
may name either and gets the same stream — and the three answers are the same
three:

| What was found | What you get |
|----------------|--------------|
| Events were being recorded | `Events were confirmed recorded over <interval>` — nothing was said about this object while that scope was open. |
| No rule streams Events | The gap, and the YAML that closes it. |
| No scope log to read | It says it cannot tell those two apart. Exit stays `0`. |

The second is the common one, because the `events` watch preset ships disabled
and a rule has to name `Event` before anything is captured. It is no longer what
a fresh [quickstart](../examples/quickstart/) produces: that rule streams
`v1/Event` from its demo namespace, so the flag has Events to interleave in the
environment that exists to demonstrate it.

```
! --with-events found no Events: no rule streams Events to this sink.
  Add them to a rule and they will appear here:
      - group: ""
        version: v1
        kind: Event
```

A bare `timeline` says none of this. The notice is owed to somebody who asked for
Events; a command that volunteered it to everyone would be answering a question
nobody put to it, and the coverage read that builds it is not paid for either.

Closing that gap is a sizing decision as much as a configuration one: Events are
captured for the whole watched scope and correlated to a subject here, at read
time, and an occurrence-count bump writes a full row rather than a diff. Read
[Event volume](SCHEMA.md#event-volume) before widening the rule beyond a
namespace.

### Events and no changes at all

The mirror of the section above, and it is the one that looks like an answer.

Correlation takes an Event's `involvedObject` from the **Event row itself** and
matches it against the object you named; your object's own rows are never
consulted. So a rule that captures `v1/Event` but not the subject's kind gives
you working Events — nothing is dropped — beside no `Added` or `Modified` rows at
all, because the object's own changes were never recorded. Read as a page, that
says the object never changed. It may have changed all day.

So a timeline holding **Event rows and none of the object's own** is explained
against the coverage of *the object's kind*, and the three answers are the three
answers:

| What was found | What you get |
|----------------|--------------|
| The kind was watched across the window | `every row here is a Kubernetes Event: no change to … is recorded in …. The scope was confirmed watched over <interval>` — the object really was quiet. |
| Nothing ever watched the kind | The Events are named as Events, and the reason they are here is spelled out. Exit stays **0**. |
| No scope log to read | It says it cannot tell those two apart. Exit stays `0`. |

```
! every row here is a Kubernetes Event: nothing was ever watching apps/Deployment
  payments/checkout in cluster "prod-eu-1", so its own changes were never
  recorded. The Events are here because a rule captures Events, not because this
  object is watched; the `scopes` command lists what is being recorded
```

The second answer stays at exit `0` rather than joining the exit **3** finding
above it. That message says *this silence is not evidence that it did not change*,
and there is no silence: the command produced rows, correlated and worth reading.
A non-zero exit beside a populated `-o json` document would be telling a script
the opposite of what the document holds.

A timeline with changes in it says none of this, and neither does an empty one —
the [three answers](#an-empty-result-is-never-presented-on-its-own) already cover
that, and both readings go through the same function against the same scope log,
so they cannot come to disagree about it.

### What a backend cannot record

An object archive holds no deletions at all (see [`docs/TEE.md`](TEE.md) and the
S3 tier's design), so a timeline over one always simply stops. Without saying so,
that silence reads as "the object is still there":

```
! the objectsource backend does not record deletions, so this timeline ending is
  not evidence that the object still exists; it may have been deleted while
  unobserved. Check what was being watched with `scopes`
```

The same backend cannot answer an unbounded question either, so a window is
supplied — the last 24 hours — and announced. `--since` widens it, and a window
wider than seven days is [confirmed first](#cold-scans).

### Colour, width and paging

Colour follows `--color=auto|always|never`. Under `auto` it is on only when the
stream being written to is a terminal and `NO_COLOR` is unset; `--color=always`
overrides `NO_COLOR`, which is what the flag is for. The two streams are decided
separately, because they are two destinations: the table is coloured when
**stdout** is a terminal, and notices, provenance and the `error:` line when
**stderr** is. `kuberecord timeline … -o json | jq` on a terminal therefore still
shows a failure in red, and `2> failure.log` still captures one with no escape
sequences in it. The table is laid out to the terminal's width, or
to 120 columns when output is not a terminal. **There is no pager**: output goes
to stdout and stays there, so `| less -R` is yours to choose.

### Reading an archive without a cluster

`--source` needs no cluster, but resolving `deploy` into `apps/Deployment` does:
short names and plural resource names come from the server's own discovery data.
When the cluster cannot be reached, give the kind as it is recorded and no
discovery is needed:

```console
$ kuberecord timeline Deployment.apps/checkout -n payments --source ~/archives/kuberecord
```

Without `-n`, an address resolved this way is read as cluster-scoped.

## `diff`

The detail view, once `timeline` has named a suspect. It asks the backend the
same question `timeline` asks — same window, same incarnation, same coverage
consultation — and spends the whole page on the answer instead of one column of
it.

```console
$ kuberecord diff deploy/checkout -n payments --since 2h
Kind:     apps/Deployment
Object:   payments/checkout
Cluster:  prod-eu-1
UID:      7c9e6679-7425-40de-944b-e07fc1f90ae7
Coverage: 2026-07-02T09:14:00Z → open (ClusterStreamRule/all-workloads)

2026-08-28 14:03:11.482 UTC  Modified  kubectl-client-side-apply
  ~ spec.template.spec.containers[0].resources.limits.memory
      - 2Gi
      + 512Mi

2026-08-28 14:05:02.117 UTC  Modified  kube-controller-manager
  ~ spec.replicas
      - 3
      + 5
  + spec.paused
      + true
  - spec.minReadySeconds
      - 10
```

`+` is green, `-` is red, `~` is yellow. Blocks read oldest first, as
[`timeline`](#timeline)'s rows do and for the same reason; `--reverse` puts the
newest at the top.

### Flags

| Flag | Meaning |
|------|---------|
| `--since` | Only changes at or after this point: a duration (`6h`, `90m`, `3d`, `2w`) or an instant (`2026-08-20`, `2026-08-20T14:00:00Z`). |
| `--until` | Only changes at or before this point, in the same forms. |
| `--from`, `--to` | Aliases for `--since` and `--until`. |
| `--limit` | Examine at most this many changes. It selects the **newest** N, and they are displayed oldest first. Default 100; zero means no limit. |
| `--reverse` | Newest first. It reorders the blocks; it does not select different ones. |
| `--uid` | Pin the diff to one incarnation. |
| `--field` | Only changes touching one of these paths, matched by prefix, with every hunk of those changes. |
| `--full` | Print every operation and every value in full. |
| `--exit-code` | `0` when there are no changes, `1` when there are, as `git diff` does. |

`diff` reads exactly what `timeline` reads, so a wide `--since` costs exactly the
same against an object archive: see [Cold scans](#cold-scans).

### The old value is reconstructed, not stored

A recorded patch is RFC 6902, which carries the *new* value and nothing else. The
value on the left of each hunk comes from replaying the object's state up to that
change — one reconstruction per incarnation, not one per row. Where that replay
could not run, the hunk says `- (prior value not established)` rather than
leaving the field looking as though it had no value before, and a notice on
stderr says why.

This is why `--field` narrows what is *shown* rather than what is fetched. A path
predicate pushed into the query would make the returned rows a non-consecutive
slice of history, and replaying only those patches would report values the object
never held. So the query goes out unfiltered, the replay runs over everything it
needs, and a notice reports how many changes were examined — `--limit` bounds the
changes examined, not the ones shown.

### Redacted values are marked

A value that reads `[REDACTED]` is what is **stored**: redaction happens on the
way in, before hashing, so nothing downstream can tell it from a ConfigMap whose
value genuinely is that string. `diff` renders it dim and marks it:

```
  ~ data.password
      - [REDACTED]  (redacted by policy)
      + [REDACTED]  (redacted by policy)
```

See [`docs/SCHEMA.md`](SCHEMA.md#redaction) for what follows from that — in
particular that two states differing *only* in a redacted value produce one row,
not two.

### Nothing fills the terminal

A value over 200 characters is cut with `…(N more bytes, --full)`, and a change
touching more than 20 fields shows the first 20 and counts the rest. A fat
PodTemplate or a CRD carrying a large OpenAPI schema is exactly the case this
exists for. `--full` prints everything.

### `--exit-code`

`git diff`'s contract: `0` for no changes, `1` for changes found. Nothing prints
`error:` for it — the exit code is a finding, not a failure, and the notice
beside the document says so.

Exit `3` still outranks it. A script told "no changes" when nothing was ever
watching has been given the one answer [the empty-result rule](#an-empty-result-is-never-presented-on-its-own)
exists to prevent, so a scope nobody watched is reported as such whatever
`--exit-code` asked for.

## `get --at`

What did this look like before?

```console
$ kuberecord get deploy/checkout -n payments --at 2h
# Reconstructed state — NOT A DEPLOYABLE MANIFEST.
#
# object:          apps/Deployment payments/checkout
# cluster:         prod-eu-1
# uid:             7c9e6679-7425-40de-944b-e07fc1f90ae7
# at:              2026-08-28T13:00:00Z
# base row:        2026-08-28T14:05:02Z (Checkpoint)
# patches applied: 0
# coverage:        2026-07-02T09:14:00Z → open (ClusterStreamRule/all-workloads)
#
# This is what kuberecord recorded, not what the API server held. Do not
# `kubectl apply -f` it: metadata.managedFields, metadata.resourceVersion and
# metadata.generation were stripped at capture, and every field a redaction
# policy covers carries the sentinel [REDACTED] in place of its value.
apiVersion: cli.kuberecord.io/v1alpha1
kind: Object
metadata:
  cluster_id: prod-eu-1
  backend: clickhouse
  coverage: ...
  reconstruction:
    reconstructed: true
    not_deployable: true
    at: "2026-08-28T13:00:00Z"
    base_ts: "2026-08-28T14:05:02.117Z"
    base_event: Checkpoint
    patches_applied: 0
items:
- at: "2026-08-28T13:00:00Z"
  uid: 7c9e6679-7425-40de-944b-e07fc1f90ae7
  base_ts: "2026-08-28T14:05:02.117Z"
  base_event: Checkpoint
  patches_applied: 0
  sha256: 283f5a59…
  object:
    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: checkout
      namespace: payments
    ...
```

The reconstructed state is the **last** key of the item, under the six facts a
reader judges it by — how old the base row is, how many patches were replayed over
it — so a document of any length ends with the object it was run for.

The state is at `.items[0].object`, inside the same [envelope](#structured-output)
every other command answers in — so `kubectl apply -f` on this file fails loudly
instead of applying a stripped object, which is the strongest form of the warning
above it.

Pulling the state back out is one `jq`, and it should be one `jq` that reads the
marker rather than one that steps over it — the document it hands you is evidence,
not a manifest:

```console
$ kubectl kuberecord get deploy/checkout -n payments --at 2h -o json \
  | jq -e 'if .metadata.reconstruction.not_deployable
           then .items[0].object
           else error("not a reconstruction: refusing to treat this as evidence") end'
```

`yq '.items[0].object'` is the same thing for the YAML form, where the header
above the document says it in words as well. See
[`metadata.reconstruction`](#metadatareconstruction-on-get) for why the marker is
there.

| Flag | Meaning |
|------|---------|
| `--at` | The instant to reconstruct for: a duration or an instant, as `--since` takes. Defaults to now, which is the newest recorded state. |
| `--uid` | Pin the reconstruction to one incarnation. Empty means the newest incarnation alive at `--at`, never a blend of two. |
| `--verify` | Re-hash the reconstruction and compare it against the digest recorded for it. |

`-o` is `yaml` (the default), `json` or `jsonl`. There is nothing for the tabular
formats to lay out — a reconstructed object is a document, not a row.

### The header is not a courtesy

What comes out looks exactly like a manifest, and the obvious next thing to do
with it is `kubectl apply -f`. That would be wrong in three ways at once, none of
them visible in the document: volatile metadata was stripped before the state was
recorded, redacted fields carry a sentinel instead of their values, and the
document describes a past somebody deliberately moved the object out of. So the
header is mandatory and says so in those words. JSON has no comment syntax, so
for `-o json` and `-o jsonl` the identical block goes to **stderr** and stdout
stays something `jq` can read.

The provenance in it is not diagnostics. A state assembled from a base an hour
old and two patches deserves more confidence than one assembled from a base three
months old and four hundred, and `base row` and `patches applied` are what let a
reader judge which they have.

On a terminal the block is **dimmed** — all of it except `NOT A DEPLOYABLE
MANIFEST`, which is not. Provenance is a fact you need available and do not need
to re-read; that one phrase is the line that has to survive you skimming past the
rest of them.

The header solves this for a person and not for a script: stderr is the stream
`2>/dev/null` discards and a pipe never reads, so `get … -o json | jq` would
otherwise receive a reconstruction with nothing in its input saying so. The same
facts are therefore carried as fields on stdout, in every format, as
[`metadata.reconstruction`](#metadatareconstruction-on-get).

### Everything but the object recedes

`-o yaml` on a terminal dims the whole kuberecord wrapper: the envelope's
`apiVersion`, `kind` and `metadata`, and the six bookkeeping fields above the
state (`at`, `uid`, `base_ts`, `base_event`, `patches_applied`, `sha256`). **The
recorded object is the only thing left at full intensity**, so you find it by
everything around it receding rather than by counting keys.

Nothing is highlighted, and that matters when you redirect: under
`--color=never`, under `NO_COLOR`, and any time stdout is not a terminal, the
document is byte for byte what it has always been. `yq '.items[0].object'`, a
`> object.yaml`, and a diff against a file you saved last week are all unaffected.
`--color=always` forces the escapes on, which is worth knowing before piping that
into a parser.

### `--verify`

Every row carries the SHA-256 of the state it recorded. `--verify` canonicalizes
the reconstruction — re-serializing it with sorted object keys, the procedure
[`docs/SCHEMA.md`](SCHEMA.md) specifies — hashes it, and compares:

```console
$ kuberecord get deploy/checkout -n payments --at 2h --verify
! verified: the reconstructed state hashes to 9f2b…, which is the digest recorded for it
```

A mismatch exits `1` and names both digests. It means the archive and the replay
disagree about what this object looked like, which is a chain-of-custody finding
and not a rounding error. A row carrying no digest is reported as unverifiable
rather than passed: reporting success there would be inventing an assurance
nobody gave.

`--verify` is an assertion rather than an annotation — the document is written
only if the check holds. `kuberecord get … --verify > object.yaml` is how this
flag gets used, and a disputed reconstruction is the last thing that should land
in that file.

## `blame`

Per-field attribution: which recorded change last wrote each field, and who made
it. `timeline` and `diff` are organized by change — here is an instant, here is
what moved. This is organized the other way round, which is the shape of the
question somebody usually arrives with: not "what happened at 14:05" but "who set
this, and when".

```console
$ kuberecord blame deploy/checkout -n payments --since 2026-08-28T14:04:00Z
Kind:     apps/Deployment
Object:   payments/checkout
Cluster:  prod-eu-1
UID:      7c9e6679-7425-40de-944b-e07fc1f90ae7
Window:   2026-08-28T14:04:00Z to now
Base:     2026-08-28T14:02:58Z (Added) plus 1 patch
Coverage: 2026-07-02T09:14:00Z → open (ClusterStreamRule/all-workloads)

FIELD                                                     LAST CHANGED             ACTOR
metadata.annotations.deployment.kubernetes.io/revision    2026-08-28 14:09:40.900  unknown
spec.template.spec.containers[0].resources.limits.cpu     2026-08-28 14:07:20.044  argocd-application-controller
spec.template.spec.containers[0].resources.limits.memory  2026-08-28 14:07:20.044  argocd-application-controller
spec.minReadySeconds  (removed)                           2026-08-28 14:05:02.117  kube-controller-manager
spec.paused                                               2026-08-28 14:05:02.117  kube-controller-manager
spec.replicas                                             2026-08-28 14:05:02.117  kube-controller-manager
apiVersion                                                (before window)          -
kind                                                      (before window)          -
metadata.name                                             (before window)          -
metadata.namespace                                        (before window)          -
spec.template.spec.containers[0].image                    (before window)          -
spec.template.spec.containers[0].name                     (before window)          -
```

Rows are most recently written first, so the top of the page is what moved last.

### Flags

| Flag | Meaning |
|------|---------|
| `--since` | Attribute changes at or after this point: a duration (`6h`, `90m`, `3d`, `2w`) or an instant (`2026-08-20`, `2026-08-20T14:00:00Z`). |
| `--until` | Attribute nothing after this point, in the same forms. The fields listed are the ones the object held then. |
| `--from`, `--to` | Aliases for `--since` and `--until`. |
| `--uid` | Pin the attribution to one incarnation. |
| `--field` | Only fields at or beneath these paths. Repeatable. |
| `--depth` | Collapse every path to at most this many levels. `0`, the default, shows every field. |

There is no `--limit`, and that is deliberate: a limit takes the newest changes in
the window, which would move the replay's anchor to the oldest change *fetched* and
make fields written inside the window render as `(before window)` — a false
statement produced by a flag rather than by the data. The window is the bound here;
`--max-objects` is still the circuit breaker for a cold scan. There is no
`--all-incarnations` either, because one field table spanning two UIDs attributes
fields to changes made to two different objects that happened to share a name. The
banner over a reused name says so — it offers `--uid` and points at
[`timeline --all-incarnations`](#incarnations) rather than naming a flag this
command would refuse.

### A field is attributed to the change that wrote it, not to the one that named it

A write to an interior node writes everything beneath it. When argocd replaces the
whole `resources` block, it is the change that last set the memory limit inside it
— even though the memory limit appears nowhere in its patch, and even though
kubectl named that limit directly four minutes earlier. A blame that matched only
exact pointers would credit kubectl, confidently, in a column somebody is about to
take to a change-review meeting.

The rows that are not patches are handled with the same care. A checkpoint carries
both a diff and the state that diff produced, so its diff is the attribution and
its data is the state. A row carrying full state and *no* diff — a first sighting, a
snapshot, a modification whose diff could not be produced — moved fields without
saying which, so its leaves are compared against the state before it and only the
ones that differ are attributed. Attributing all of them would name somebody against
fields they left alone.

### `(before window)` is a row, not an omission

The replay starts from the newest full-state row at or **before the start of the
window**, which is what the `Base:` line names. Most of a fat object's fields were
last written before any bounded window, and they are listed with `(before window)`
in place of a timestamp rather than dropped — a dropped row would read as a field
the object does not have. Widen `--since` and they acquire an attribution.

The two cells go together: `(before window)` in LAST CHANGED and `-` in ACTOR. That
dash is not `unknown`, which is what a change that recorded no field managers
renders as. One says no change was read for this field; the other says a change was
read and had no name on it.

### A removed field keeps its row

A field deleted inside the window is not in the object any more, so nothing in its
end state would list it. It is kept, marked `(removed)`, and attributed to the
change that deleted it — who removed the memory limit is one of the two questions
this command answers, and a table that silently omitted the answer would be a
silence the output offers no way to notice.

### `--depth` for a fat object

`--depth N` collapses every path to at most `N` levels and adds a `FIELDS` column
saying how many of the object's fields each row now stands for:

```console
$ kuberecord blame deploy/checkout -n payments --depth 2
FIELD                            LAST CHANGED             FIELDS  ACTOR
metadata.annotations             2026-08-28 14:09:40.900  1       unknown
spec.template                    2026-08-28 14:07:20.044  4       argocd-application-controller
spec.minReadySeconds  (removed)  2026-08-28 14:05:02.117  1       kube-controller-manager
spec.paused                      2026-08-28 14:05:02.117  1       kube-controller-manager
spec.replicas                    2026-08-28 14:05:02.117  1       kube-controller-manager
apiVersion                       2026-08-28 14:02:58.001  1       kubectl-client-side-apply
kind                             2026-08-28 14:02:58.001  1       kubectl-client-side-apply
metadata.name                    2026-08-28 14:02:58.001  1       kubectl-client-side-apply
metadata.namespace               2026-08-28 14:02:58.001  1       kubectl-client-side-apply
```

(The window is unbounded here, so nothing is older than it.)

A collapsed row carries the newest write beneath it. Levels are **JSON Pointer
tokens**, so an array index is one of them: `--depth 4` collapses a container array
into a single row and `--depth 5` gives a row per container. That is the object's
real structure rather than the display grammar's, and it is the only counting that
does not mis-measure a key containing dots — an annotation named
`deployment.kubernetes.io/revision` is one level, not three.

### `--field` selects fields here, not changes

For `timeline` and `diff`, a path predicate selects whole *changes*: a change that
touched the path is shown entire, other fields included, because those are the
context for the one asked about. Here the rows are fields, so it selects fields.
Both commands use the same prefix rule, so they agree about which paths
`spec.template` covers even though they apply the answer to different things.

Like `diff --field`, it narrows what is *shown* and not what is read: the replay
needs the whole consecutive run, and a filtered slice of history would attribute
fields to changes that did not write them.

## `scopes`

What was being recorded, and when. This is the compliance view, and it is the
command every other command's empty result points at.

```console
$ kubectl kuberecord scopes -n payments
Cluster: prod-eu-1
Scope:   every kind in namespace payments
Window:  all recorded history

KIND             NAMESPACE  FROM                     TO                       RULE
apps/Deployment  payments   2026-06-01 08:00:00.000  2026-07-02 09:14:00.000  StreamRule/payments/workloads
apps/Deployment  (all)      2026-07-02 09:14:00.000  (open)                   ClusterStreamRule/all-workloads
ConfigMap        payments   2026-07-02 09:14:00.000  2026-08-11 17:31:22.000  (not recorded)
```

| Flag | Meaning |
|------|---------|
| `--kind` | Only scopes for this kind. Takes what the object commands take — `deploy`, `deployments.apps`, `Deployment.apps` — and needs a cluster for the first two. |
| `-n`, `--namespace` | Only scopes covering this namespace, cluster-wide rules included. Without it, **every** namespace: unlike the object commands, the kubeconfig's current namespace does not narrow a compliance question. |
| `--since`, `--until` | Only periods overlapping this window. A period that merely overlaps is shown **whole**. |
| `--from`, `--to` | Aliases for `--since` and `--until`. |

`-o` is `table` (the default), `wide`, `json`, `jsonl` or `yaml`. `wide` widens
the timestamps to the nanosecond precision the schema records.

### Reading a row

Three cells say something a blank would not:

- **`(open)`** in `TO` means the scope is being watched *now*. There is no
  recorded end because there has not been one.
- **`(all)`** in `NAMESPACE` is the all-namespaces scope, not a missing value. A
  cluster-wide rule was watching objects in every namespace, including the one
  you asked about — which is why it appears in a namespaced listing, with a
  notice on stderr saying so. Your `--namespace` was not ignored.
- **`(not recorded)`** in `RULE` is an interval closed by a recovery pass whose
  rule no longer exists. That is a real state, and a blank there would read as a
  rule named by the empty string.

### An interval's end is not a deletion

The end of a period says **the recorder stopped watching**. It says nothing
whatever about the objects in that scope: they may still exist, they may have
been deleted three weeks later, and this table cannot tell you which. That is the
whole reason it exists — everything after an interval's end is unobserved, and
unobserved is not the same as unchanged.

### An empty listing is a finding

No periods means nothing was ever watching what you asked about, so no silence
anywhere in this cluster's history means what it appears to mean for that scope.
That is exit **3**, the same code `timeline` reaches when it works the fact out
from the other end, so one script keys on one code whichever command it asked:

```console
$ kuberecord scopes --kind Secret -n payments
Cluster: prod-eu-1
Scope:   Secret in namespace payments
Window:  all recorded history
error: no watch coverage recorded for the requested scope: no watch scope covering
Secret in namespace payments was open in cluster "prod-eu-1" during all recorded
history, so a silence there is not evidence that nothing changed — nothing was
being recorded to change
```

(For `Secret` specifically the answer is permanent: it is hard-denied as a
watchable kind.)

A backend with no scope log at all cannot answer this command — there is no other
half of the question to fall back to — so it exits `1` naming the backend, rather
than printing an empty table that would read as "nothing was watching".

## `version`

Which build is running, and what it can read.

```console
$ kuberecord version
kuberecord v0.3.0
  commit  77514b632925
  built   2026-08-31T21:04:11Z
  go      go1.25.7 linux/amd64

query backends compiled in:
  clickhouse  engine clickhouse   — schema v1 in ClickHouse
  s3          engine objectsource — jsonl-v1 archive in an S3-compatible bucket
  local       engine objectsource — jsonl-v1 archive in a directory
```

The version, the commit and the build date are stamped into the binary at release
time, so they identify the artifact rather than a source tree that resembles it. A
build made any other way reports what the Go toolchain recorded — a module version
for `go install`, the revision and its `-dirty` mark for a build from a checkout —
and prints `unknown` where nothing could say. A commit with no `-dirty` on it was
built from exactly that tree.

**The backend list is what this build can read**, not what the project supports,
and it is the first thing to check when a `--source` or a profile is refused. The
`engine` column is the value that appears as `metadata.backend` in
[structured output](#structured-output), so an answer you are holding can be
matched to the row that produced it — `s3` and `local` are two ways of reaching one
engine, which is why the column is not redundant.

By itself it contacts nothing: no cluster, no sink, no network. That is
deliberate — the reason to run it is usually that something else already failed.
[`--check`](#version---check) is the exception, and it is opt-in for the same
reason.

`-o json` and `-o yaml` render the same facts as a document. It carries the same
`apiVersion` as every other structured answer and the same additive-only promise,
with a `kind` of its own:

```console
$ kuberecord version -o json
{
  "apiVersion": "cli.kuberecord.io/v1alpha1",
  "kind": "Version",
  "version": "v0.3.0",
  "commit": "77514b632925",
  "buildDate": "2026-08-31T21:04:11Z",
  "goVersion": "go1.25.7",
  "platform": "linux/amd64",
  "backends": [
    {
      "name": "clickhouse",
      "engine": "clickhouse",
      "description": "schema v1 in ClickHouse"
    },
    {
      "name": "s3",
      "engine": "objectsource",
      "description": "jsonl-v1 archive in an S3-compatible bucket"
    },
    {
      "name": "local",
      "engine": "objectsource",
      "description": "jsonl-v1 archive in a directory"
    }
  ]
}
```

`-o jsonl` and `-o diff` are refused by name rather than quietly rendered as
something else: this is one document, not a stream, and there are no change
operations in it to diff.

There is no `--version` flag. kubectl has none either, and cobra's built-in one is
handled before any command runs — so it could not honour `-o`, and
`kuberecord --version -o json` would print a table while appearing to have been
asked for JSON.

### Flags

| Flag | Meaning |
|------|---------|
| `--check` | Also resolve the backend, ask it whether it answers, and report the sink, the cluster identity and what came back. Without it nothing is contacted. See [`version --check`](#version---check). |

`-o` is `table` (the default), `wide`, `json` or `yaml`.

### `version --check`

**This is the first thing to run when something is not working.** It answers the
question the bare command cannot: not "which build is this" but "does this setup
work".

It resolves the backend exactly as a query command would, asks it whether it
answers, and reports the four facts that decide whether anything else will
succeed — the sink, the engine behind it, the cluster identity, and what came
back:

```console
$ kuberecord version --check
kuberecord v0.4.0
  commit  77514b632925
  built   2026-09-05T11:02:44Z
  go      go1.25.7 linux/amd64

query backends compiled in:
  clickhouse  engine clickhouse   — schema v1 in ClickHouse
  s3          engine objectsource — jsonl-v1 archive in an S3-compatible bucket
  local       engine objectsource — jsonl-v1 archive in a directory

setup:
  backend     ClickHouseSink/default (clickhouse.kuberecord-system.svc:9000/kuberecord)
  engine      clickhouse
  cluster-id  prod-eu-1 (from the operator Deployment kuberecord-system/kuberecord-controller-manager)
  reachable   the backend answered
```

The last row is the probe, and it reports one of four outcomes in the same words
[`config resolve`](#config-resolve) reports them in:

| Outcome | What it means |
|---|---|
| `reachable` | the backend answered |
| `unreachable` | it did not, and the failure is printed below with its explanation |
| `cannot be checked` | the engine cannot say without running a real query — a statement about the engine, not a fault, and it does not fail the command |
| `not checked` | there was nothing to reach, because the backend chain resolved nothing |

The question put to the backend is which clusters it holds. That is the cheapest
question the read plane has, and it exercises the whole path rather than a socket:
DNS, the connection, the credential, and — for ClickHouse — the database being the
one the sink named. A bare TCP dial would pass against a server holding somebody
else's history.

**A failure exits `1` and prints the same explanation every other command prints**
— see [Running the CLI outside the cluster](#running-the-cli-outside-the-cluster).
The document is still written first, because a bug report needs the build *and*
the setup:

```console
$ kuberecord version --check
…
setup:
  backend      ClickHouseSink/default (clickhouse.kuberecord-system.svc:9000/kuberecord)
  engine       clickhouse
  cluster-id   prod-eu-1 (from the operator Deployment kuberecord-system/kuberecord-controller-manager)
  unreachable  the backend could not be reached
               cannot reach ClickHouseSink/default at clickhouse.kuberecord-system.svc:9000: …

for which step decided what, run `kuberecord config resolve --check`

error: cannot reach ClickHouseSink/default at clickhouse.kuberecord-system.svc:9000: …

! ClickHouseSink/default records the address clickhouse.kuberecord-system.svc:9000.
…
```

**This is a summary, and [`config resolve --check`](#config-resolve) is the
detail.** The two put one question through one piece of machinery and differ only
in how much of the walk they show: nine steps decide the four facts above, and
when one of them is not what you expected — a profile written months ago shadowing
discovery, a `--context` pointing at the wrong cluster — that command names the
step that decided it. Start here; go there when the summary is surprising.

`-o json` and `-o yaml` add a `setup` block to the document, with the probe under
the field names `config resolve` gives it, so one `jq` recipe reads either:

```console
$ kuberecord version --check -o json
{
  "apiVersion": "cli.kuberecord.io/v1alpha1",
  "kind": "Version",
  …
  "setup": {
    "backend": "ClickHouseSink/default (clickhouse.kuberecord-system.svc:9000/kuberecord)",
    "engine": "clickhouse",
    "clusterID": "prod-eu-1",
    "clusterIDSource": "from the operator Deployment kuberecord-system/kuberecord-controller-manager",
    "check": {
      "requested": true,
      "outcome": "reachable",
      "detail": "the backend answered"
    }
  }
}
```

**Without `--check` the block is absent entirely**, rather than present and empty.
A bare `version` looked at no configuration, and a document carrying a hollow
`setup` would be claiming otherwise.

**No credential appears in any format, at any verbosity.** The block names the
host, the database, the sink and the cluster; never the password, never the
access key.

How to check that the binary this reports on is the one you verified:
[`VERIFYING.md`](VERIFYING.md#the-cli-archives).

## Output formats

Six values for `-o`, three of them renderings and three of them serializations:

| Format | What it is |
|--------|-----------|
| `table` | The default. A header block on the object, then one row per item, laid out to the terminal's width or to 120 columns when stdout is not a terminal. |
| `wide` | The same table with nothing elided: full UIDs, resource versions, and timestamps at the nanosecond precision the schema records. |
| `diff` | The hunk rendering — path, old value, new value — which is [`diff`](#diff)'s own shape. |
| `json` | One [envelope](#structured-output) document, complete before it is written. |
| `yaml` | The same document in YAML. |
| `jsonl` | The envelope head on the first line, then one item per line as it arrives. Memory does not scale with the result. |

**Not every command accepts every one**, and a command that cannot produce a
format **refuses it by name** rather than quietly rendering something else. A user
who asked for one shape and received another has been answered in a form their eye
or their script cannot read, and finding that out at the `jq` is worse than finding
it out here.

| | `table` | `wide` | `diff` | `json` | `yaml` | `jsonl` |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| [`timeline`](#timeline) | ✅ default | ✅ | ❌ | ✅ | ✅ | ✅ |
| [`diff`](#diff) | ✅ default | ✅ | ✅ | ✅ | ✅ | ✅ |
| [`get`](#get---at) | ❌ | ❌ | ❌ | ✅ | ✅ default | ✅ |
| [`blame`](#blame) | ✅ default | ✅ | ❌ | ✅ | ✅ | ✅ |
| [`scopes`](#scopes) | ✅ default | ✅ | ❌ | ✅ | ✅ | ✅ |
| [`version`](#version) | ✅ default | ✅ | ❌ | ✅ | ✅ | ❌ |
| [`config view`](#config-view-and-config-get-profiles) | ✅ default | ✅ | ❌ | ✅ | ✅ | ❌ |
| [`config get-profiles`](#config-view-and-config-get-profiles) | ✅ default | ✅ | ❌ | ✅ | ✅ | ❌ |
| [`config current-profile`](#config-current-profile) | ✅ default | ✅ | ❌ | ✅ | ✅ | ❌ |
| [`config resolve`](#config-resolve) | ✅ default | ✅ | ❌ | ✅ | ✅ | ❌ |
| `config set-profile` | ✅ default | ✅ | ❌ | ✅ | ✅ | ❌ |
| `config use-profile` | ✅ default | ✅ | ❌ | ✅ | ✅ | ❌ |
| `config delete-profile` | ✅ default | ✅ | ❌ | ✅ | ✅ | ❌ |
| `config set-context-cluster-id` | ✅ default | ✅ | ❌ | ✅ | ✅ | ❌ |

Each ❌ has a reason, and the error says it:

- **`timeline`, `blame` and `scopes` refuse `diff`.** Their rows are one line each
  by design — a change, a field, a period — and `diff` is a whole command that
  spends the whole page on the same changes with the old value beside the new one.
  A second entrance to that rendering would be a second place for the two to drift.
- **`get` refuses `table` and `wide`.** A reconstructed object is a document rather
  than a row, and there is nothing for a tabular format to lay out. Its default is
  `yaml`, which is the shape people want and the one that carries the **NOT A
  DEPLOYABLE MANIFEST** header inside the document rather than on another stream.
- **`version`, `config view`, `config get-profiles`, `config current-profile`,
  `config resolve` and the four `config` subcommands that write refuse `jsonl`.** It
  is a streaming format for a result larger than memory, and each of these is exactly
  one document — a profile listing included, since it is as long as the configuration
  file and no longer, and the active profile's name most of all.
- **The four `config` subcommands that write render nothing at all for `table` and
  `wide`.** Their whole report is the confirmation on stderr, because the data of
  a write is the file it wrote — and there is no result set to lay out in columns.
  `-o json` and `-o yaml` add a document on stdout for a script to read; see
  [writes are scriptable](#writes-are-scriptable). They refuse `diff` for the
  reason `config resolve` does: there is no patch anywhere in a configuration
  write.
- **`config view` renders YAML for `table` and `wide`** rather than refusing them,
  because a configuration file *is* YAML and `table` is the global default a user
  who typed no `-o` at all arrives with. `diff` is refused: there is no patch here.
- **`config resolve` refuses `diff` too**, and for the same reason: it reports two
  chains of decisions, and there is no patch anywhere in it. So do
  `config get-profiles`, which reports the state of a file, and
  `config current-profile`, whose whole answer is one word.

And one ✅ is worth a sentence, because it looks like a flag being ignored and is
not. **`wide` on `version`, `config view`, `config get-profiles`,
`config current-profile` and `config resolve` renders exactly what `table` does**,
and on the four writing `config` subcommands it renders no document at all, as
`table` does. `wide` means *the same table with nothing elided*, and each of these
is a document that elides nothing at any width — a build identity, a configuration
file, a profile listing whose cells are addresses and paths people paste, one
profile name, two chains of decisions, a write that has already been confirmed on
stderr — so the flag's guarantee is met rather than dropped. Where a command genuinely cannot produce a format it refuses
it by name, which is the case the list above enumerates.

For `table`, `wide` and `diff` the header, the notices and every explanation go to
**stderr** and the rows go to **stdout**. For `json`, `jsonl` and `yaml` the
envelope carries the same facts as fields, so a parser gets on one stream what a
reader gets on two. **There is no pager** in either case: output goes to stdout and
stays there, so `| less -R` is yours to choose.

## Structured output

`-o json`, `-o jsonl` and `-o yaml` produce a **versioned envelope**, and it is a
public contract. People script against this; a field renamed a release later
breaks a runbook silently, because `jq` reports nothing for a path that no longer
exists and the pipeline keeps running while producing empty findings.

```json
{
  "apiVersion": "cli.kuberecord.io/v1alpha1",
  "kind": "Timeline",
  "metadata": {
    "cluster_id": "prod-eu-1",
    "backend": "clickhouse",
    "coverage": {
      "available": true,
      "summary": "2026-07-02T09:14:00Z → open (ClusterStreamRule/all-workloads)",
      "intervals": [
        {
          "api_group": "apps",
          "kind": "Deployment",
          "namespace": "",
          "rule_ref": "ClusterStreamRule/all-workloads",
          "from": "2026-07-02T09:14:00Z",
          "to": null
        }
      ]
    }
  },
  "items": [
    {
      "ts": "2026-08-28T14:03:11.482Z",
      "event_type": "Modified",
      "actors": ["kubectl-client-side-apply"],
      "uid": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
      "resource_version": "1002",
      "api_version": "apps/v1",
      "sha256": "",
      "labels": {},
      "data": {},
      "diff": [
        {
          "op": "replace",
          "path": "/spec/template/spec/containers/0/resources/limits/memory",
          "value": "512Mi"
        }
      ]
    }
  ]
}
```

### The five kinds

| `kind` | Produced by | What one item is |
|--------|-------------|------------------|
| `Timeline` | `timeline` | One recorded change, as the schema stores it. |
| `Diff` | `diff` | The same, plus `hunks` and `patch_error`. |
| `Object` | `get` | One reconstruction: `object` plus the provenance for it. |
| `Coverage` | `scopes` | One watch-scope interval. |
| `Blame` | `blame` | One field's attribution: which change last wrote it. |

`items` is always a list, including when it is empty. `metadata` carries
`cluster_id`, `backend` and `coverage` on every kind, and `reconstruction` on
`Object` alone.

**Key order is declaration order, in every format.** A YAML envelope opens
`apiVersion`, `kind`, `metadata`, `items` — the order every Kubernetes document
opens in, and the order `-o json` has always emitted. A document that began
`apiVersion, items` pushed the two fields that say what it is below a
several-hundred-line array, and read as malformed even when it was not. Items
follow the same rule, which puts the bulky fields at the end of each one: `data`
and `diff` after the eight columns that identify a change, and the reconstructed
`object` after the six facts a reader judges a reconstruction by. The
[`version`](#version) and [`config resolve`](#config-resolve) documents open the
same way.

It is a property of the rendering rather than of the contract. Nothing `jq` or
`yq` returns depends on it, and a consumer reading `-o yaml` *positionally* is the
only one that notices.

**Seven documents carry the same `apiVersion` without being envelopes**, and the
difference is deliberate. [`version`](#version) renders a `Version` document,
`config view` renders a `Config` one,
[`config get-profiles`](#config-view-and-config-get-profiles) renders a
`Profiles`, [`config current-profile`](#config-current-profile) renders a
`CurrentProfile`, [`config resolve`](#config-resolve) renders a `Resolution`, and
the `config` subcommands that write render a
[`ProfileChange` or a `ContextMapping`](#writes-are-scriptable); none is the answer
to a query, so none has a `metadata` block or an `items` list. A `Version` carrying
`cluster_id: ""` and an empty coverage report would be inviting a consumer to read
three fields that could never mean anything. ([`version --check`](#version---check)
adds a `setup` block naming a cluster, and that is not a `metadata` block: it is
what the resolution chains chose, reported because it was asked for, and it is
absent when it was not.) What they do share is the contract those five kinds are
governed by — the same `apiVersion`, and therefore the same
[additive-only policy](#the-additive-only-policy).

### Item field names are the schema's column names

`ts`, `event_type`, `actors`, `uid`, `resource_version`, `api_version`, `data`,
`diff`, `sha256`, `labels` — spelled exactly as
[`docs/SCHEMA.md`](SCHEMA.md) spells the columns, because the two are the same
data reached two ways. A `jq` recipe written against a SQL result transfers here
unchanged, which is the point of the mirroring rather than a detail of it. An
empty `actors` or `labels` is `[]` and `{}`, never `null`.

**The names are the schema's; two of the types are not.** `data` and `diff` are
`String` columns in ClickHouse because ClickHouse stores strings, and emitting
them as strings would mean handing a consumer a JSON document with JSON inside a
string — a second parse before any path can be reached, and in YAML an escaped
payload that wraps mid-token and cannot be read at all. So structured output
carries them as what they are:

| Field | Type in `-o json`, `-o jsonl` and `-o yaml` | Empty |
|-------|---------------------------------------------|-------|
| `data` | **Object** — the full recorded state, as recorded. | `{}` on a row that carries no state: a modification, a deletion. |
| `diff` | **Array** — the RFC 6902 operations, in order, exactly as recorded. Each entry keeps its `path` as a JSON Pointer, which is what a patch library takes. | `[]` on a row that carries no patch: a first sighting, a snapshot, a deletion. |

Empty is the empty structure and never `null`, and the key is never omitted: an
absent patch and an empty patch are different facts, and dropping the key would
make you test for presence to learn something the value already says.

The bytes are carried through untouched, so a large integer, a float's written
form and the key order of the recorded object all survive the trip. On a
`Checkpoint`, `diff` describes the transition `data` already reflects and **must
not be applied over it** — parsing the column does nothing to change that.

**A column that will not parse fails the command.** A stored `diff` that is not a
JSON array is corrupt evidence, and printing the raw string in its place would
hide exactly what an audit tool exists to surface. The CLI names the row by its
`ts` and `uid`, exits `1`, and emits nothing for it. The tables and the hunk view
are unaffected — they mark the row `unreadable patch` and render the rest of the
history — so a damaged row is still visible in the renderings that have somewhere
to say so.

A `Diff` item adds `hunks`, one per patch operation:

```json
{ "op": "replace",
  "path": "spec.template.spec.containers[0].resources.limits.memory",
  "pointer": "/spec/template/spec/containers/0/resources/limits/memory",
  "old": "2Gi", "old_known": true, "new": "512Mi" }
```

A `Blame` item is one field rather than one change, so it carries the change's own
columns for the change that last wrote it:

```json
{ "path": "spec.template.spec.containers[0].resources.limits.memory",
  "pointer": "/spec/template/spec/containers/0/resources/limits/memory",
  "attributed": true, "ts": "2026-08-28T14:07:20.044Z",
  "actors": ["argocd-application-controller"], "uid": "7c9e6679-…",
  "resource_version": "1004", "event_type": "Modified",
  "removed": false, "fields": 1 }
```

**Read `attributed`, not `ts`**: a null `ts` is the table's `(before window)` — the
field's last write is older than the window — and the other columns are then their
zero values rather than an answer. `fields` is how many of the object's fields the
item stands for, which is `1` unless `--depth` collapsed a subtree into it.

`path` is the dotted grammar `--field` accepts and the table prints; `pointer` is
RFC 6901 as recorded, for a JSON Patch library. **Read `old_known`, not `old`**:
`old` is `null` both when the value really was JSON null and when the state replay
could not establish it, and those are different facts. A redacted value arrives as
the literal sentinel `[REDACTED]`.

### `metadata.coverage` on every command that queries changes

This is the empty-result rule in a form a script can branch on. Two answers with zero items
mean opposite things, and one field tells them apart without a second query:

| `available` | `intervals` | What it means |
|-------------|-------------|---------------|
| `true` | non-empty | The scope was watched. An empty `items` means nothing changed. |
| `true` | `[]` | **Nothing was ever watching.** An empty `items` means nothing was recorded. Exit `3`. |
| `false` | `[]` | The backend has no scope log and cannot say which of the two you have. |

### `metadata.reconstruction` on `get`

`get` is the one command whose items are **assembled** rather than read: the state
in an `Object` envelope was rebuilt by replaying patches over a recorded base, and
it looks exactly like a manifest. A consumer reading structured output must treat
an `Object` document as **evidence rather than as a manifest** — it is not what
the API server held, and it must never be applied. This field is how a script
knows that without reading prose on another stream:

```json
"reconstruction": {
  "reconstructed": true,
  "not_deployable": true,
  "at": "2026-08-28T13:00:00Z",
  "base_ts": "2026-08-28T14:05:02.117Z",
  "base_event": "Checkpoint",
  "patches_applied": 0
}
```

| Field | Meaning |
|-------|---------|
| `reconstructed` | Always `true` where the field appears. Absent on every other kind, so `.metadata.reconstruction.reconstructed` is falsey for a `Timeline` without a consumer needing to know which kinds are assembled. |
| `not_deployable` | Always `true`. The machine-readable form of **NOT A DEPLOYABLE MANIFEST**: volatile metadata was stripped before the state was recorded, redacted fields carry `[REDACTED]` in place of their values, and the document describes a past somebody deliberately moved the object out of. |
| `at` | The instant the state was reconstructed **for** — the `--at` that was asked about, not the wall clock the command ran on. |
| `base_ts` | The timestamp of the full-state row the replay started from. |
| `base_event` | That row's `event_type`, as the schema records it. |
| `patches_applied` | How many patches were replayed over the base. |

The last four are the same facts the header renders and are spelled as an `Object`
item spells them, so a `jq` recipe transfers between the marker, the item and a
SQL result. They are what a reader judges the answer by: a base an hour old and
two patches invites more confidence than a base three months old and four hundred.

The header on stderr is unchanged, and for `-o yaml` the comment block above the
document is unchanged too. A reader gets both; a parser gets one.

### The additive-only policy

Within one `apiVersion`, fields may be **added** and are never renamed, removed or
repurposed — the same policy the frozen schema carries. Consumers must ignore
fields they do not recognize. Anything else — including a field's type — is a
break, and is recorded as one in
[`CHANGELOG.md`](https://github.com/kuberecord/kuberecord/blob/main/CHANGELOG.md).

`v1alpha1` is where such a break is still affordable, and v0.4.0 spent it once:
`data` and `diff` stopped being strings. An `alpha` version is the part of the
contract that says so out loud, and the intent is that it is spent rarely and
never quietly.

### `jsonl` streams

`-o jsonl` writes the envelope head on the first line and then one item per line,
as each one arrives from the backend. Memory does not scale with the result, so a
six-figure timeline can be piped into something that reads it a line at a time:

```
{"apiVersion":"cli.kuberecord.io/v1alpha1","kind":"Timeline","metadata":{…}}
{"ts":"2026-08-28T14:05:02.117Z","event_type":"Modified",…,"data":{},"diff":[{"op":"replace","path":"/spec/replicas","value":5}]}
{"ts":"2026-08-28T14:09:40.9Z","event_type":"Modified",…,"data":{},"diff":[{"op":"replace","path":"/metadata/annotations/deployment.kubernetes.io~1revision","value":"2"}]}
```

The head line carries no `items` key — it cannot, since nothing has been read yet
— but it carries the full `metadata`, coverage included, so a consumer processing
the stream as it arrives already knows whether an empty stream means "nothing
changed" or "nothing was watching".

One case holds items back, and it is bounded by a number you typed rather than by
the result: the display order is oldest first while `--limit N` selects the
newest N, so those N have to be read before the oldest of them can be written, and
at most N are held. `--limit 0` makes the two orderings select the same changes,
so the query is simply asked oldest-first and nothing is held at all; `--reverse`
asks for the order the backend already emits and holds nothing either.

`json` and `yaml` are single documents and are complete before they are written,
which is what a single document means.

### Two recipes

The flagship question — who changed what — as one line per change:

```console
$ kubectl kuberecord timeline deploy/checkout -n payments -o jsonl \
  | jq -r 'select(.ts) | "\(.ts)  \(.actors | join(","))  \([.diff[].path] | join(" "))"'
2026-08-28T14:03:11.482Z  kubectl-client-side-apply  /spec/template/spec/containers/0/resources/limits/memory
2026-08-28T14:05:02.117Z  kube-controller-manager    /spec/replicas /spec/paused /spec/minReadySeconds
```

`select(.ts)` is what skips the head line: it is the only line with no `ts`.
`.diff` is an array, so `[.diff[].path]` reaches the paths directly — there is
nothing to parse first, and a row with no patch yields an empty line rather than
an error.

Or, with the field paths already decoded, from `diff`:

```console
$ kubectl kuberecord diff deploy/checkout -n payments --since 24h -o json \
  | jq -r '.items[] | . as $c | .hunks[] | "\($c.ts)  \($c.actors[0])  \(.path): \(.old) → \(.new)"'
2026-08-28T14:03:11.482Z   kubectl-client-side-apply  spec.template.spec.containers[0].resources.limits.memory: 2Gi → 512Mi
2026-08-28T14:05:02.117Z   kube-controller-manager    spec.replicas: 3 → 5
```

An operation whose prior value the replay could not establish renders `null`
there too, which is why a script that cares reads `old_known` rather than testing
`old`.

Telling an empty result from an unobserved one, in a script that must not confuse
them:

```console
$ result=$(kubectl kuberecord timeline deploy/checkout -n payments --since 24h -o json)
$ jq -r 'if (.items | length) > 0 then "changed"
         elif .metadata.coverage.available | not then "backend cannot say"
         elif (.metadata.coverage.intervals | length) == 0 then "NOT WATCHED"
         else "watched, unchanged" end' <<<"$result"
```

Exit code `3` carries the same finding for a script that would rather branch on
that; the envelope is still written to stdout, so both work.

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

**They are dimmed on a terminal when they say the expected thing, and left at full
weight when they do not.** Both lines print on every invocation, so a register that
never varied would be one you learned to skip inside a week — which is exactly the
week one of them starts saying something you needed to read. Two states, and
nothing else about the line changes:

| The line says | Weight | Why |
|---------------|--------|-----|
| Step 4 answered — the cluster's own sink | dimmed | There was exactly one, so the tool went to the single place the cluster points at. Nothing in it to check. |
| Step 1, 2 or 3 answered — `--source`, `--sink`, a profile | full weight | Something **shadowed** what discovery would have found: a flag from a shell alias, a stanza written months ago. |
| The identity came from `--cluster-id` or the context mapping | dimmed | Your own words handed back. |
| The identity came from the operator's Deployment or from the sink | full weight | The tool worked it out. It is usually right, and when it is not it does not fail — it returns another cluster's history looking exactly like an answer. |

The `→` marker itself never changes weight, so the two markers stay tellable apart
at a glance whatever the line after them is doing. Neither state is ever promoted
to a `!` notice: nothing has gone wrong, and that tier means the data on its own
misleads.

**Under `--color=never`, `NO_COLOR` or a redirect this changes nothing at all** —
same lines, same words, same order, byte for byte. There is no `--quiet`, and
deliberately: suppressing provenance would remove the record of where an answer
came from, which is not a thing an audit reader should be able to switch off by
accident. `2>/dev/null` is still there, and having to type it is the point.

Steps 2 and 4 read an address a cluster wrote for itself, which is why the first
thing many people meet is a `no such host` from a laptop. That is the subject of
[Running the CLI outside the cluster](#running-the-cli-outside-the-cluster), and
step 1 does not have it at all.

Step 3 has a rule of its own worth knowing before you configure a profile: once a
profile answers, the chain is finished with it. A profile whose credential
reference cannot be read **fails the command** rather than falling through to
step 4 — see [A profile that cannot resolve is
fatal](#a-profile-that-cannot-resolve-is-fatal-and-says-what-to-do-instead), which
is also where the three ways past that failure are written out.

To see which step would win — and why the earlier ones had nothing to say — without
running a query, ask: [`kuberecord config resolve`](#config-resolve). It reports
both chains and contacts nothing unless `--check` says to. For the short answer —
which sink, which cluster, and does it answer —
[`kuberecord version --check`](#version---check) reports the same four facts
without the steps.

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

### `--sink-addr`

```console
$ kubectl port-forward -n kuberecord-system svc/clickhouse 9000:9000
$ kubectl kuberecord timeline deploy/checkout -n payments --sink-addr 127.0.0.1:9000
→ discovered ClickHouseSink/default (127.0.0.1:9000/kuberecord, address from --sink-addr)
```

A `ClickHouseSink` answers five questions — address, database, username,
credentials and dial timeout — and exactly one of them is wrong when the CLI runs
outside the cluster the sink was written for. `--sink-addr` replaces **that one**.
The database, the user, the credentials read from the sink's Secret, the TLS
setting and the dial timeout all still come from wherever the address came from.

The step that answered does not change, and neither does the word the notice uses
for it: the line still says `discovered`, because the custom resource really was
consulted and four of its five answers are the ones in use. What changes is the
description, which gains `address from --sink-addr` — so the override is in the
notice, and the line cannot be read as a claim about what the cluster recorded.

It applies wherever the chain lands on ClickHouse: a discovered sink, an explicit
`--sink ClickHouseSink/<name>`, or a ClickHouse profile. The routes with no
endpoint of that shape **refuse** it — a usage error, exit 2 — rather than
accepting the flag and doing nothing with it:

| Given with | Why it is refused |
|---|---|
| `--source` | It reads that location directly, so nothing recorded an endpoint to replace. |
| A profile whose backend is `s3` or `local` | An archive is read as objects; it is never dialled. |
| `--sink S3Sink/<name>`, or a discovered `S3Sink` | An object store. Its bucket, region and endpoint URL come from the custom resource. |

The value is `host:port` — `127.0.0.1:9000`, `[::1]:9000` — with no scheme, and a
bare host is refused by name. Nothing here resolves it: that is the dial's job, and
a validation that succeeded where the dial then failed would be two error paths
reporting one problem.

**The CLI will not forward the port for you.** Forwarding needs `create` on
`pods/portforward`, a write verb, and a tool whose value is that it cannot alter
anything does not acquire one for a convenience. When a dial fails against an
address that only resolves inside a cluster, it prints the `kubectl port-forward`
line to run and the `--sink-addr` to follow it with, pre-filled from the sink it
just read. The whole of that failure, both routes out of it and why an archive
read with `--source` never meets it:
[Running the CLI outside the cluster](#running-the-cli-outside-the-cluster).

For a setup you come back to, write it down once with
[`config set-profile`](#kuberecord-config) instead. This flag is for the one-off:
a colleague's cluster, a CI job that forwards and then queries, a debugging
session that should leave nothing on disk.

### `--source` versus `--sink-addr`

They are routinely read as two spellings of "read from somewhere else", and they
are not alternatives at all. `--source` **replaces** the resolution chain:
step 1 answers, and no custom resource, Secret or kubeconfig is consulted after
it. `--sink-addr` **corrects one field** of what the chain already found: the
endpoint, with the database, the username, the credentials, the TLS setting and
the dial timeout still coming from the custom resource discovery read. Given
together they are a usage error — under `--source` nothing recorded an endpoint,
so there is nothing for the override to replace.

| | `--source` | `--sink-addr` |
|---|---|---|
| **The question it answers** | "Read this archive, here." | "Everything the cluster recorded is right except that I am not in it." |
| **Position in [the chain](#where-the-data-comes-from)** | **Step 1**, and it wins outright: steps 2, 3 and 4 never run. | **No step.** A modifier on whichever of steps 2, 3 and 4 answered — the notice still names that step, because the custom resource really was read. |
| **Contacts the cluster** | No. No kubeconfig, no custom resource, no Secret. One exception, and it is not about the data: expanding a short kind like `deploy` reads the server's [discovery data](#reading-an-archive-without-a-cluster). Give `Deployment.apps` and even that goes. | Whatever the step it modifies did. On a discovered or `--sink`-named custom resource, yes: the sink is read and its `credentialsSecretRef` resolved exactly as without the flag. On a ClickHouse profile, no — the file already held everything but the address. |
| **Backends it applies to** | `s3` and `local` — a `format=jsonl-v1/` archive in a bucket or a directory. Never ClickHouse: a server is dialled, not enumerated. | `clickhouse` only, wherever the chain lands on it — a discovered sink, `--sink ClickHouseSink/<name>`, or a ClickHouse profile. Every other route [refuses it by name](#--sink-addr). |
| **What it supplies** | The whole location, and nothing else: bucket and prefix, or a directory. Credentials come from the AWS credential chain, region from `AWS_REGION`. | One field, `host:port`. Five things a `ClickHouseSink` answers, four of them untouched. |
| **What the notice says** | `using --source …` — at full weight, because something shadowed what discovery would have found. | The step that answered, with `address from --sink-addr` inside the parentheses. |

Four cases, and the fourth is the common one:

- **`--source`**, when the recorded history is an archive you can already reach:
  an `S3Sink` bucket, a directory synced to a laptop, [evaluation
  mode](#evaluation-mode). It is the answer for an auditor with no cluster access
  at all, and the only one of the four that works on a plane.
- **`--sink-addr`**, when discovery is right and you are somewhere it did not
  expect: a `kubectl port-forward` you have just opened, a colleague's cluster, a
  CI job that forwards and then queries. One invocation, nothing left on disk.
- **A profile**, when either of those is something you will type more than once.
  [`config set-profile --from-sink`](#--from-sink) writes one from the sink
  itself — the forwarded address substituted, the database and the user carried
  over, the password still read from your own environment. It survives the
  kubeconfig context changing under it, which a shell alias holding a flag does
  not.
- **Neither**, when the CLI runs where the operator does, or the recorded address
  resolves from where you are — an in-cluster job, a `kubectl exec`, a ClickHouse
  on a public endpoint or across a VPN. Discovery answers, the notice is dimmed
  because there is nothing in it to check, and both of these flags would be a way
  of overriding something that was already correct.

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

The other way discovery can succeed and still leave you with no answer is the
address it discovers: correct for the operator, unresolvable from a laptop. See
[Running the CLI outside the cluster](#running-the-cli-outside-the-cluster).

### A profile that cannot resolve is fatal, and says what to do instead

A profile answers step 3, and when what it references cannot be read — an
environment variable this shell never exported, a password file that is not there,
a stanza naming a backend this build does not have — the chain **stops**. It does
not carry on to discovery.

That is deliberate and it is not going to change. You configured a profile;
answering from the cluster's own sink instead would read from somewhere you did not
choose and report success, which is the same property that stops this tool
[substituting an address](#the-cli-will-not-forward-the-port-for-you) when one does
not resolve. An audit answer that carries an unstated "…from somewhere" is worse
than no answer.

So the failure names every route past itself, filled in with your own values:

```console
$ kuberecord timeline deploy/checkout -n payments
error: profile "prod": the environment variable KUBERECORD_CLICKHOUSE_PASSWORD is not set, and this profile names it as where its password comes from

! profile "prod" is where this invocation reads from, and the chain stops here.

It is the currentProfile in
/home/engineer/.config/kuberecord/config.yaml

Falling through to the cluster's own sink would read from somewhere you did not choose
and report success, so a profile that cannot be resolved is fatal rather than skipped.
Three routes get past it, and all three work today.

Export the variable this profile names as where its password comes from:

    export KUBERECORD_CLICKHOUSE_PASSWORD=…

Or skip the profile for this one invocation. --sink is step 2 and a profile is step 3,
so a named sink is reached first and its credential comes from the Secret it references.
`kubectl get clickhousesinks` names the ones this cluster holds:

    kuberecord timeline … --sink ClickHouseSink/<name>

Or stop this one answering. The file also defines archive, staging. Which of those has a
credential that resolves right now is the first command below; the second switches to it:

    kuberecord config get-profiles
    kuberecord config use-profile archive

To watch the chain make this decision, with this step's own reason beside it:

    kuberecord config resolve
```

The three are genuinely different decisions rather than three spellings of one:

| Route | What it does | When it is the one |
|---|---|---|
| Export the variable, or create the file | Makes the reference the profile holds resolve. | The profile is right and your shell is missing a line. This is nearly always the answer. |
| `--sink <kind>/<name>` | Skips the profile entirely — step 2 is reached before step 3 — and takes the address, the database, the user **and the credential** from the sink custom resource and the Secret it names. | You can read that Secret, and you want one answer now. |
| `config use-profile <other>`, or `--profile <other>` | Leaves this profile in the file and stops it being the one that answers. | The stanza is stale. [`config delete-profile`](#creating-replacing-and-deleting-a-profile) removes it for good. |

Switching raises a question of its own — *will the other one work?* — and the
message names the command that answers it, because for the commonest cause it is a
coin toss: one exported variable per shell is normal, and you have just been told
yours is not the one this profile wanted.
[`config get-profiles`](#config-view-and-config-get-profiles) reports the same
credential state for every profile in the file, in a column.

**`--sink-addr` is not one of them, and the message says so when you pass it.** It
[corrects one field](#--source-versus---sink-addr) of whatever the chain found —
the endpoint — and never a credential, so against a profile whose password
reference is unresolvable it changes nothing: the password is read before the
override is applied. Passing it is a good sign you want the second route, and
`--sink ClickHouseSink/<name> --sink-addr 127.0.0.1:9000` is that route with your
forwarded port still in it.

If the profile is the only one in the file there is nothing to switch to, and the
message says that instead of naming a profile you do not have — offering
[`config set-profile`](#creating-replacing-and-deleting-a-profile) to write another,
and `config delete-profile <name> --force` to remove this one and let the chain fall
through to discovery again.

A misspelled `--profile` is a different failure with a different message: it names
the profiles the file does define, and it is never a fall-through either.

## Running the CLI outside the cluster

The address a `ClickHouseSink` records is written for the operator, which runs
inside the cluster:

```yaml
spec:
  connection:
    addr: clickhouse.kuberecord-quickstart.svc:9000
```

That is the correct value, and there is no other value it could hold. A Service
DNS name is how one pod reaches another; it resolves through the cluster's own
resolver and nowhere else. Nothing about it is a misconfiguration — but a laptop
is not in the cluster, so a CLI that discovers that sink and dials what it says
gets `no such host`.

So the CLI says that, rather than leaving you with the resolver's opinion:

```console
$ kubectl kuberecord timeline deploy/checkout-api -n quickstart-demo
→ discovered ClickHouseSink/default (clickhouse.kuberecord-quickstart.svc:9000/kuberecord)
→ cluster-id kuberecord-quickstart (from the operator Deployment kuberecord-system/kuberecord-controller-manager)
error: cannot reach ClickHouseSink/default at clickhouse.kuberecord-quickstart.svc:9000: dial tcp: lookup clickhouse.kuberecord-quickstart.svc: no such host

! ClickHouseSink/default records the address clickhouse.kuberecord-quickstart.svc:9000.

That name resolves inside the cluster and nowhere else, so discovery was right and so is
the sink: this machine is simply outside it. kuberecord reads a cluster and never acts on
one, so it will not forward a port for you.

Forward it yourself, then re-run against the forwarded address:

    kubectl port-forward -n kuberecord-quickstart svc/clickhouse 9000:9000
    kubectl kuberecord timeline … --sink-addr 127.0.0.1:9000

Or write it down once, and every later invocation reads it:

    kubectl kuberecord config set-profile local --from-sink ClickHouseSink/default
    kubectl kuberecord config use-profile local

That reads this same sink, records 127.0.0.1:9000 in place of the address above,
and takes the database and the user from it. The forward is still yours to run:
a profile records an address, it does not open a tunnel.

Export KUBERECORD_CLICKHOUSE_PASSWORD first: it is the variable that profile
records, and the password it wants is the sink's own user's — a credential that can
write to the audit trail. Add --username <read-only user> to the set-profile line
above to read as one instead, and it records a variable of its own.

Both routes, and why this tool will not forward the port for you:
docs/CLI.md#running-the-cli-outside-the-cluster
```

The Service and its namespace come out of the address itself, so the
`port-forward` line is the one to run rather than a template to fill in. The
second route names the sink for the same reason: [`--from-sink`](#--from-sink)
reads the stanza back out of the custom resource the first line of the message
already named, so neither block leaves you a value to supply. Below is what each
route is for.

### The one-off: a forwarded port and `--sink-addr`

```console
$ kubectl port-forward -n kuberecord-quickstart svc/clickhouse 9000:9000
$ kubectl kuberecord timeline deploy/checkout-api -n quickstart-demo --sink-addr 127.0.0.1:9000
→ discovered ClickHouseSink/default (127.0.0.1:9000/kuberecord, address from --sink-addr)
→ cluster-id kuberecord-quickstart (from the operator Deployment kuberecord-system/kuberecord-controller-manager)
```

Discovery still does everything it did: the custom resource is read, its Secret is
resolved, the database and the user are the sink's own. [`--sink-addr`](#--sink-addr)
replaces the endpoint and nothing else, and the notice says so — which is what
keeps the line an honest account of where the answer came from.

This is the right route for a cluster you are visiting: a colleague's, a CI job
that forwards and then queries, an investigation that should leave nothing behind
on disk.

### The repeated: a profile

For a cluster you come back to, write the answer down once and stop passing
flags. `--from-sink` reads the sink you already have and fills in the stanza,
substituting the forwarded address for the one only the cluster can resolve:

```console
$ kubectl kuberecord config set-profile local --from-sink ClickHouseSink/default
→ wrote profile "local" in ~/.config/kuberecord/config.yaml
→ made "local" the active profile (it is the only one)

ClickHouseSink/default records clickhouse.kuberecord-quickstart.svc:9000.

That name resolves inside the cluster and nowhere else, so the profile records
127.0.0.1:9000 instead and expects a forwarded port beside it:

    kubectl port-forward -n kuberecord-quickstart svc/clickhouse 9000:9000
…

$ export KUBERECORD_CLICKHOUSE_PASSWORD=…
$ kubectl port-forward -n kuberecord-quickstart svc/clickhouse 9000:9000
$ kubectl kuberecord timeline deploy/checkout-api -n quickstart-demo
→ using profile local (ClickHouse at 127.0.0.1:9000/kuberecord)
```

The port-forward is still yours to run — a profile records an address, it does not
open a tunnel — but nothing else has to be repeated, and the profile survives the
kubeconfig context changing under it. The password is **not** copied out of the
sink's Secret: the profile names an environment variable, and the password that
variable has to hold is the password of the user the profile records. That user is
the sink's own by default, and the sink's own can write to the audit trail — so add
`--username` to read as [a read-only one](#the-read-only-clickhouse-user) instead,
which also gives the profile a variable of its own to read.
The whole subcommand, including which flags survive `--from-sink` and what an
`S3Sink` does instead, is [`--from-sink`](#--from-sink). If you would rather not
learn a flag to get here, `kubectl kuberecord config set-profile` with nothing after
it [asks](#asking-instead-of-knowing-the-flags), reaches the same place, and prints
the command above at the end.

### The CLI will not forward the port for you

It could. It will not, and the reason is worth stating rather than leaving as an
omission somebody files a bug about.

Forwarding a port is `create` on `pods/portforward` — a **write** verb, on the
Kubernetes API, in the cluster being audited. Everything else this tool does is
`get` and `list`. That difference is the whole claim: an audit reader that cannot
alter what it is auditing is a tool you can hand to somebody you would not give
write access to, and can run against a production cluster during an incident
without being one more thing that might have caused it. Acquiring a write verb to
save a reader one command is a bad trade, and it is not one that can be
un-acquired later — a permission a tool has ever needed is a permission its users
have been granted.

The same reasoning is why every kubectl flag is [inert under
`--source`](#inherited-from-kubectl): a reader that reaches for the cluster when
something goes wrong is a reader whose promises need footnotes.

So the failure above is a **diagnostic**, never a fallback. Nothing retries,
nothing substitutes an address, and nothing tries `127.0.0.1` to see whether a
forward happens to be open already. Recognising a cluster-internal address means
saying so; a CLI that quietly connected somewhere other than where it was told
would make every answer it gave carry an unstated "…from somewhere".

The recognition is deliberately narrow, and the narrowness is the reason to trust
it. Both halves must hold: the address has to be one that only resolves inside a
cluster — a `.svc`, `.svc.cluster.local` or `.cluster.local` name, or a bare
single-label host — **and** the failure has to be a name that did not resolve or a
connection that was refused. A timeout, a TLS failure, an authentication rejection
and a ClickHouse protocol error are all a backend that was reached and answered,
and none of them prints this message. Telling the on-call engineer of a production
ClickHouse that has fallen over to run `kubectl port-forward` would send them
somewhere the fault is not, at the moment they can least afford it.

To provoke the same diagnosis deliberately — without running a query — use
[`kuberecord version --check`](#version---check), which is also how you confirm the
forwarded port worked. To see which step chose the address in the first place, use
[`kuberecord config resolve --check`](#--check).

### None of this applies to `--source`

An archive is **named, not discovered**. [`--source`](#--source) takes a bucket or
a directory and reads it directly: no custom resource is consulted, no Secret is
resolved, no kubeconfig is used, and nothing anywhere holds an address written for
a reader inside the cluster. There is no cluster-versus-laptop mismatch to hit,
because there is no cluster in the picture.

```console
$ kuberecord timeline Deployment.apps/checkout-api -n quickstart-demo \
    --source s3://acme-audit/kuberecord --since 24h
```

That is not a workaround for the friction on this page — it is what
[evaluation mode](#evaluation-mode) and an `S3Sink` archive are, and it is why
an auditor with a synced directory and no cluster access can answer the same
questions from a plane. It is also why `--source` is not a third route out of
the failure above and `--sink-addr` is not a way of reading an archive: they sit
at different layers, and which one a given situation calls for is
[`--source` versus `--sink-addr`](#--source-versus---sink-addr). The whole path is
[`examples/zero-infra/`](../examples/zero-infra/). What it costs is query
performance on wide questions, stated in [Backend capability
differences](#backend-capability-differences) and [Cold scans](#cold-scans).

One honest caveat, because "no friction" would be too strong: if the object store
*itself* runs in the cluster — the MinIO in that example does — you will forward a
port to reach it, exactly as you would for any other in-cluster service. The
difference is that you point `--source` and the AWS credential chain at whatever
you can reach, and nothing has discovered an address on your behalf that could
turn out to be wrong.

## Backend capability differences

Three names, two engines. `kuberecord version` prints the mapping, and it is the
first thing to check when a `--source` or a profile is refused:

```console
$ kuberecord version
query backends compiled in:
  clickhouse  engine clickhouse   — schema v1 in ClickHouse
  s3          engine objectsource — jsonl-v1 archive in an S3-compatible bucket
  local       engine objectsource — jsonl-v1 archive in a directory
```

`s3` and `local` are the **same engine** reaching the same `format=jsonl-v1`
layout through two different ways of getting bytes, so they answer identically and
degrade identically. Only the engine matters below, and it is what
`metadata.backend` reports in [structured output](#structured-output).

Every engine **declares** what its storage can express, and the CLI keys its
behaviour on the declaration rather than on the backend's name — so a future
indexed backend inherits the right treatment by declaring it, and a backend cannot
quietly gain a capability it never claimed. The conformance suite checks the
declaration against detected behaviour in both directions, so neither half can
drift.

| Capability | `clickhouse` | `objectsource` | What the difference costs you |
|---|:---:|:---:|---|
| `deletions` | ✅ | ❌ | An object archive holds no `Deleted` rows at all. A timeline over one that simply stops carries an **explicit notice** saying the object may have been deleted without the deletion ever being recorded. No `Deleted` row is ever synthesized to close the gap — history with no deletions in it is otherwise indistinguishable from history of a cluster where nothing was deleted. |
| `server_side_filter` | ✅ | ❌ | **No consequence for the content.** `--actor` and `--field` produce an identical result either way, which is the agreement property the conformance suite pins. The consequence is cost: without pushdown, `--limit` does not bound the work, so a wide window is estimated, confirmed and reported on. |
| `point_query` | ✅ | ❌ | ClickHouse seeks to one object's rows. The archive has no index, so a single-object question costs every object in the partitions its window lands in — see [Cold scans](#cold-scans) for the estimate, the confirmation and `--max-objects`. |
| `time_bound_required` | ❌ | ✅ | An unbounded question against the archive is refused up front, naming the flag that fixes it, rather than started and never finished. With neither end given the CLI supplies **24 hours** and announces it; `--since` widens it. |

Two things are the same on both, and are worth stating because they are the ones
people assume are the difference:

- **The answer's content.** Same envelope, same field names, same items, same
  ordering. That is what the query conformance suite exists to hold, and what makes
  a `jq` recipe transfer between a ClickHouse profile and an archive on a laptop.
- **The scope log.** Both record `watch_scopes` — `scopes/` in the archive — so
  both can tell "nothing changed" from "nothing was watching", and both exit `3`
  for the second. `scopes` needs no window against either, because the scope log is
  one small object per day rather than one per hour.

Nothing here is hidden or worked around. A question the backend cannot answer is
reported as a capability gap, never as an empty result, and a command that can
answer half of it answers half and says which half. What the
archive's reader does about each of these, in more detail and beside the DuckDB
recipes that cover what the CLI deliberately does not, is
[`docs/QUERIES.md`](QUERIES.md#what-the-cli-reads).

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

Step 4 is the only one that questions the backend, which is why
[`config resolve`](#config-resolve) withholds it unless `--check` is given, and
reports the identity as `undetermined` rather than dialling to find out.

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

### The schema, field by field

| Field | Type | Default | Meaning |
|---|---|---|---|
| `apiVersion` | string | stamped on every write | `cli.kuberecord.io/v1alpha1`. Empty in a hand-written file is read as the current version; a value that is *present and wrong* is refused, because that one is a real disagreement about what the fields mean. |
| `kind` | string | stamped on every write | `Config`. Same rule. |
| `currentProfile` | string | none | The profile used when `--profile` is not given. Empty is an ordinary state: a cluster with a sink custom resource needs no profile at all. |
| `operatorNamespace` | string | searched, then `kuberecord-system` | Where discovery looks for the operator's Deployment and for a sink's credentials Secret. |
| `profiles` | map[string]Profile | none | The configured places to read history from, by name. |
| `contexts` | map[string]string | none | kubeconfig context name → kuberecord cluster identity. Step 2 of [the cluster-id chain](#the-cluster-identity), and what makes a long-lived multi-cluster setup zero-flag. |

A **profile** is one place to read from. `backend` is named explicitly rather than
inferred from which stanza is filled in, so a profile with the wrong stanza is a
validation error naming both halves rather than a silent switch to whichever one
was found:

| Field | Type | Required | Meaning |
|---|---|---|---|
| `backend` | `clickhouse` \| `s3` \| `local` | yes | Which stanza below describes this profile. Exactly the matching one must be present. |
| `clickhouse` | stanza | with `backend: clickhouse` | See below. |
| `s3` | stanza | with `backend: s3` | See below. |
| `local` | stanza | with `backend: local` | See below. |

**`clickhouse`** — the frozen v1 tables in a ClickHouse:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `addr` | string | — | Native-protocol endpoint, `host:port`. Required. |
| `database` | string | the server's own | Holds the frozen v1 tables. Rarely right to omit — the operator's own default is `kuberecord`. |
| `username` | string | the server's own | A [read-only user](#the-read-only-clickhouse-user) is the recommended posture. |
| `passwordEnv` | string | none | Name of an environment variable holding the password. |
| `passwordFile` | string | none | Path to a file holding it, trailing newline trimmed. |
| `tls` | bool | `false` | Connect over TLS with the platform's trust store and a TLS 1.2 floor. A private CA belongs in that store, where every other client on the machine will also find it. |
| `password` | — | — | **Refused by name**, with an explanation pointing at the two fields above. |

At most one of `passwordEnv` and `passwordFile` may be set. Neither means no
password, which is what a local evaluation server usually wants.

**`s3`** — an archive in an S3-compatible bucket:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `bucket` | string | — | Holds the archive. Required. |
| `prefix` | string | none | The archive's key prefix — the sink's `spec.prefix` — with no leading or trailing slash. Empty is ordinary: a bucket dedicated to one archive. |
| `region` | string | `us-east-1` | The SDK requires one even against MinIO, which ignores it. A wrong region cannot resolve to somebody else's bucket, because S3 bucket names are global — it fails loudly instead. |
| `endpoint` | string | AWS | The S3 API endpoint, **scheme mandatory**. This is how MinIO and other S3-compatible stores are addressed. |
| `forcePathStyle` | bool | `false` | Address the bucket as `<endpoint>/<bucket>/<key>`, which most in-cluster MinIO deployments need. |
| `accessKeyId`, `secretAccessKey`, `sessionToken` | — | — | **Refused by name.** Credentials come from the AWS chain. |

**`local`** — an archive in a directory:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `path` | string | — | The directory containing `format=jsonl-v1/`. Required. |
| `prefix` | string | none | If the archive was written under one. |

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
# Answer questions instead of knowing the flags. It prints the flag form at the end.
$ kuberecord config set-profile

# Write a profile. The first one in an empty file becomes the active one.
$ kuberecord config set-profile prod --backend clickhouse \
    --addr clickhouse.example:9000 --database kuberecord \
    --username kuberecord_ro --password-env KUBERECORD_CLICKHOUSE_PASSWORD

# Or read the whole stanza out of the sink the operator already writes to.
$ kuberecord config set-profile local --from-sink ClickHouseSink/default

# The same, and read through it from here on: --use activates what it writes.
$ kuberecord config set-profile local --from-sink ClickHouseSink/default --use

$ kuberecord config set-profile archive --backend s3 --bucket acme-audit \
    --prefix kuberecord --endpoint https://minio.internal:9000 --force-path-style

$ kuberecord config set-profile laptop --backend local --path ~/archives/kuberecord

# Choose the active one, and ask which it is.
$ kuberecord config use-profile archive
$ kuberecord config current-profile
$ PROFILE=$(kuberecord config current-profile) || exit 1

# Remove one. Deleting the active profile needs --force, which also clears the
# active pointer.
$ kuberecord config delete-profile stale
$ kuberecord config delete-profile local --force

# Record which kuberecord cluster a kubeconfig context reads.
$ kuberecord config set-context-cluster-id prod-eu-1          # the current context
$ kuberecord config set-context-cluster-id prod-eu prod-eu-1  # a named one

# Print the file. The document goes to stdout, its path to stderr.
$ kuberecord config view
$ kuberecord config view -o json | jq .profiles

# Print its state instead: which profile is active, what each points at, and
# whether its credential resolves on this machine.
$ kuberecord config get-profiles

# Or just the active profile's name, which is the shape a script wants.
$ kuberecord config current-profile

# Ask what the resolution chains would choose, without running a query.
$ kuberecord config resolve
$ kuberecord config resolve --check
```

Eight subcommands, and three of them have flags of their own:

| Subcommand | Arguments | Flags |
|---|---|---|
| `config set-profile` | `[NAME]` | the table below — or none of them, which [asks](#asking-instead-of-knowing-the-flags). `--use` names no field, so it asks too |
| `config use-profile` | `NAME` | none |
| [`config current-profile`](#config-current-profile) | none | none |
| `config delete-profile` | `NAME` | `--force` — see [the profile lifecycle](#creating-replacing-and-deleting-a-profile) |
| `config set-context-cluster-id` | `[CONTEXT] CLUSTER_ID` | none — with one argument it writes the current context, which `--context` selects |
| `config view` | none | none — `-o yaml` (the default) or `-o json` |
| `config get-profiles` | none | none — see [`config view` and `config get-profiles`](#config-view-and-config-get-profiles) |
| `config resolve` | none | `--check` — see [`config resolve`](#config-resolve) |

Four of them write nothing. `config view` prints the file,
[`config get-profiles`](#config-view-and-config-get-profiles) prints its state and
[`config current-profile`](#config-current-profile) prints the active profile's
name alone; `config resolve` is here because a profile is one step of
[where the data comes from](#where-the-data-comes-from), and the question it
answers is the one a reader of this file has when the file turns out not to be the
step that won.

The four that write also render a document for `-o json` and `-o yaml`, so
creating a profile in a script is not a step whose outcome has to be reconstructed
by diffing the file. See [writes are scriptable](#writes-are-scriptable).

`config set-profile` carries one flag per field of the stanza its `--backend`
selects. A flag belonging to a different backend is a validation error naming
both halves, for the same reason the file refuses a mismatched stanza:

| Flag | Backend | Writes |
|------|---------|--------|
| `--from-sink <kind>/<name>` | — | every field below, read from a sink custom resource. Mutually exclusive with `--backend` — see [`--from-sink`](#--from-sink). |
| `--backend <kind>` | — | `backend`. One of `clickhouse`, `s3`, `local`. Required unless `--from-sink` is given. |
| `--addr <host:port>` | `clickhouse` | `clickhouse.addr` |
| `--database <name>` | `clickhouse` | `clickhouse.database` |
| `--username <user>` | `clickhouse` | `clickhouse.username`. Survives `--from-sink`; a user other than the sink's own also defaults `passwordEnv` to a variable of its own — see [the read-only user](#the-read-only-clickhouse-user). |
| `--password-env <VAR>` | `clickhouse` | `clickhouse.passwordEnv` |
| `--password-file <path>` | `clickhouse` | `clickhouse.passwordFile` |
| `--tls` | `clickhouse` | `clickhouse.tls` |
| `--bucket <name>` | `s3` | `s3.bucket` |
| `--region <region>` | `s3` | `s3.region`. Defaults to `us-east-1`, which MinIO ignores. |
| `--endpoint <url>` | `s3` | `s3.endpoint`. Scheme mandatory. |
| `--force-path-style` | `s3` | `s3.forcePathStyle` |
| `--path <dir>` | `local` | `local.path` — the directory containing `format=jsonl-v1/`. |
| `--prefix <prefix>` | `s3`, `local` | `prefix`. No leading or trailing slash. |
| `--use` | — | `currentProfile` — makes this profile the active one as well as writing it. See [activation is opt-in](#activation-is-opt-in). |

There is no `--password`. That is not an omission: see the first rule above.

#### `config view` and `config get-profiles`

Two subcommands read the file and they answer different questions, which is the
same split `kubectl` makes between `config view` and `config get-contexts`.

**`config view` prints the file.** That is the right answer to *"what did I write
down"*, and it is the one to reach for when a stanza needs to be checked field by
field or pasted into an issue. It renders the document unchanged — nothing is
redacted, which is safe by construction, because [the file cannot hold a
credential](#the-configuration-file).

**`config get-profiles` prints its state.** One row per profile: which is active,
what each one points at, and where its credential comes from — with **whether that
reference resolves on this machine**, checked as the table is drawn.

```console
$ kuberecord config get-profiles
# /home/you/.config/kuberecord/config.yaml
CURRENT  NAME     BACKEND     TARGET                       CREDENTIAL
         archive  s3          s3://audit-archive/prod      ambient
*        local    clickhouse  127.0.0.1:9000/kuberecord    env KUBERECORD_CLICKHOUSE_PASSWORD (not set)
         prod     clickhouse  ch.observability:9000/audit  env KUBERECORD_CLICKHOUSE_PASSWORD (set)
```

The `*` and the `CURRENT` column are `kubectl config get-contexts`'s own, so the
output is legible without instruction. Rows are sorted by name. The file's path
goes to **stderr**, so `-o json | jq` receives the document alone.

**`config current-profile` prints the active profile's name and nothing else.** It
is the third question in this family and the only one shaped for a program rather
than a reader: the table is the answer to *"what is in the file"*, and one token is
the answer to *"what do I put in `$PROFILE`"*. See
[`config current-profile`](#config-current-profile).

**The credential column is the reason to run this rather than `view`.** A profile
stores the *name* of an environment variable or the *path* of a file; whether that
name is exported in the shell you are in, or that file is on this disk, is not
something the file can say. It is also the commonest reason a query stops — see [A
profile that cannot resolve is
fatal](#a-profile-that-cannot-resolve-is-fatal-and-says-what-to-do-instead), whose
message points back here.

| `CREDENTIAL` | Means |
|---|---|
| `env NAME (set)` | The profile names an environment variable and this shell exports it. A variable exported *empty* is `set`: that was a decision somebody made. |
| `env NAME (not set)` | It names one and this shell does not export it. Every command that resolves through this profile will fail, naming the variable. |
| `file PATH (present)` | It names a password file and that file could be read. |
| `file PATH (missing)` | The file is not there. Create it, or rewrite the stanza to name a variable instead. |
| `file PATH (unreadable)` | The file *is* there and could not be read — mode `0000`, or a directory that denies traversal. A different fix from `missing`, which is why it is a different word. |
| `ambient` | An `s3` profile. Credentials come from the AWS credential chain — environment, shared config, SSO, an instance role — which this tool does not re-implement and therefore does not check. |
| `none` | Nothing to resolve: a `local` archive, or a ClickHouse profile naming neither reference, which is ordinary for an evaluation server with no password. |

Whether it resolves is decided by the same code path a query resolves a password
through, so a column saying `set` cannot disagree with what the next query finds.

Three properties are worth knowing:

- **No credential value is printed**, in any format, at any verbosity. What is
  printed is the reference — a variable name, a file path — which is what the file
  itself holds.
- **Nothing is contacted.** This reads the file and the environment. Whether the
  backend *answers* is [`config resolve --check`](#--check), and a second command
  that dialled would be two answers to one question that could differ.
- **`CURRENT` is the file's active pointer**, not this invocation's. `--profile`
  does not move the `*`: what *this* command line would resolve to has nine steps
  behind it, and [`config resolve`](#config-resolve) is where that is inspected.

**An empty configuration is not an error.** It is the state of every first
invocation, and of every user whose cluster has a sink custom resource to
discover — who needs no profile at all. The header is printed with no rows under
it, so that a file holding nothing is distinguishable from a file that could not
be read, and the exit code is `0`:

```console
$ kuberecord config get-profiles
# /home/you/.config/kuberecord/config.yaml
CURRENT  NAME  BACKEND  TARGET  CREDENTIAL
! this file defines no profiles, which is the ordinary state of a first invocation: with none, resolution falls through to discovering a sink from the cluster.
  To write one — with no flags it asks for what it needs, and its first question is whether to read the settings out of a sink this cluster already holds:
      kuberecord config set-profile
```

`-o json` and `-o yaml` render a `Profiles` document. It is not an
[envelope](#structured-output) — no question about recorded history was asked — and
it carries no stanza, because `config view -o json` is the command whose subject is
the file:

```console
$ kuberecord config get-profiles -o json | jq '.profiles[] | select(.credential.state == "not set")'
{
  "name": "local",
  "current": true,
  "backend": "clickhouse",
  "target": "127.0.0.1:9000/kuberecord",
  "credential": {
    "source": "env",
    "reference": "KUBERECORD_CLICKHOUSE_PASSWORD",
    "state": "not set"
  }
}
```

| Field | Meaning |
|---|---|
| `path` | The file the listing was read from, so documents collected from several machines are distinguishable. |
| `currentProfile` | The active pointer, and `""` when none is active. Always present, so an empty pointer is a value to read rather than a missing key to infer from. |
| `profiles` | One entry per profile, sorted by name. Always a list, including when it is empty. |
| `profiles[].name` | The key this profile has in the file. |
| `profiles[].current` | Whether this is the active profile — the row the `*` marks. A field rather than something to derive by comparing with `currentProfile`. |
| `profiles[].backend` | `clickhouse`, `s3` or `local`. |
| `profiles[].target` | The locator, with defaults applied: the address and database a query would open rather than the fields as typed. |
| `profiles[].credential.source` | `env`, `file`, `ambient` or `none`. |
| `profiles[].credential.reference` | The variable name or the file path. Absent for a source that names neither. |
| `profiles[].credential.state` | `set`, `not set`, `present`, `missing`, `unreadable` or `not checked` — the table above, in one field. A word rather than a boolean: `resolves: false` on an ambient credential nobody checked would be a claim this command did not make. |

#### `config current-profile`

**`config get-profiles` is for a reader; this is for a program.** They report the
same fact — which profile is active — and the difference is entirely one of shape.
The table marks the active row with `*`, which is right when the question is what
the file holds and wrong when the question is what to put in a variable:

```console
$ kuberecord config current-profile
local

$ PROFILE=$(kuberecord config current-profile) || exit 1
```

One token on stdout, no header, no decoration, and **nothing on stderr** — which
is where this departs from `config view` and `config get-profiles`, both of which
print the file's path there. `kubectl config current-context` prints nothing beside
its answer either, and the path is available from this command's own `-o json` for
a program that needs it. Extracting the starred row from the table is three lines
of `awk` and breaks the day an unrelated profile's address gets longer, because the
columns are laid out to the width of their content.

**No active profile is an error, and exits `1`.** That is the point of it: `$( )`
cannot tell an empty answer from no answer, so a script that captured `""` and
carried on would query wherever the rest of
[the resolution chain](#where-the-data-comes-from) reached — a sink discovered from
the cluster, most likely — while believing it had been told which profile to use.
The message names the way out, and which way out depends on what the file holds:

```console
$ kuberecord config current-profile
error: no profile is active in ~/.config/kuberecord/config.yaml, so there is no name to print: choose one with `kuberecord config use-profile archive`, or see which of them has a credential that resolves right now with `kuberecord config get-profiles` (also defined: prod)
```

A file that defines no profiles at all gets the other message. It does not offer
`use-profile`, because there is nothing to switch to and a remedy naming no command
reads as though you should have known which name to substitute — and it says
plainly that an empty file is not a broken one, since every command that queries
data resolves perfectly well without a profile:

```console
$ kuberecord config current-profile
error: no profile is active, and ~/.config/kuberecord/config.yaml defines none: write one with `kuberecord config set-profile`, which with no flags asks for what it needs, after which `kuberecord config get-profiles` reports the file's state. An empty file is not a broken one — with no profile, resolution falls through to discovering a sink from the cluster — but there is no name to print, and printing nothing would let a script carry on as though it had been told which profile to use
```

**Nothing is contacted**, and unlike `config get-profiles` not even the environment
is read: there is no credential reference in the answer to resolve. **`CURRENT` is
the file's active pointer, not this invocation's** — `--profile` does not change
what this prints, for the reason it does not move the `*` in the table. What *this*
command line would resolve to has nine steps behind it and is
[`config resolve`](#config-resolve)'s question.

`-o json` and `-o yaml` render a `CurrentProfile` document. Two fields, and the
restraint is deliberate: what the profile points at and whether its credential
resolves is `get-profiles`' subject, and
`config get-profiles -o json | jq '.profiles[] | select(.current)'` is the way to
ask for it.

```console
$ kuberecord config current-profile -o json
{
  "apiVersion": "cli.kuberecord.io/v1alpha1",
  "kind": "CurrentProfile",
  "path": "/home/you/.config/kuberecord/config.yaml",
  "currentProfile": "local"
}
```

| Field | Meaning |
|---|---|
| `path` | The file the pointer was read from, so documents collected from several machines are distinguishable. It is the one fact the bare form omits. |
| `currentProfile` | The active profile's name. Never empty in a rendered document: no active profile is a failure with no document at all, which is what distinguishes it from the same field on a [`Profiles`](#config-view-and-config-get-profiles) listing, where `""` is an ordinary state. Spelled the way `Profiles` and `ProfileChange` spell it, so one `jq` path reads the active profile out of all three. |

#### Creating, replacing and deleting a profile

**`set-profile` is an upsert.** A name already in the file is replaced, and the
line on stderr names the profile that is gone:

```console
$ kuberecord config set-profile local --backend clickhouse --addr 127.0.0.1:9000
→ updated profile "local" in ~/.config/kuberecord/config.yaml (was: ClickHouse at 10.0.1.5:9000/kuberecord)
```

Creating one prints `→ wrote profile "local" in …` as before. One line is the whole
of the difference, and it is deliberately not a prompt or a `--force`: re-running
`set-profile` after a forwarded port moved is the wizard's single most common real
case, and a command that refused it would break the route new users are put on.
What the line has to do is make the destructive half visible, because nothing else
holds that stanza once the file is written.

**The replacement is the whole stanza, not a field merge.** A profile that named
`passwordFile` and is rewritten with `--password-env` keeps no reference to the
file; a field the second command did not mention is absent, not inherited.

A merge is the tempting alternative and it is refused on purpose. The profile it
produced would depend on what was in the file beforehand, which makes it
unreconstructible from the command that wrote it — and every message this
subcommand prints is a claim that running that command again produces this
profile, which a merge would falsify on any machine whose file started out
different. It would also make the destructive case worse rather than better: a
stanza half from a hand-tuned profile and half from a flag is a configuration
nobody wrote.

There is no `update-profile`. With `set-profile` documented as an upsert it would
be a synonym, and a second name for one operation is how a CLI surface begins to
sprawl.

**`delete-profile NAME` removes one**, and says what it removed for the same
reason:

```console
$ kuberecord config delete-profile stale
→ deleted profile "stale" from ~/.config/kuberecord/config.yaml (was: local archive at /archives/kuberecord)
```

Deleting the **active** profile is refused without `--force`, because the
resolution chain would then name a profile that does not exist. The refusal names
both routes past it, since they are different decisions:

```console
$ kuberecord config delete-profile local
error: "local" is the active profile, and deleting it would leave the resolution chain naming a profile that does not exist: either switch first with `kuberecord config use-profile archive` and delete it after, or delete it and clear the active pointer with `kuberecord config delete-profile local --force`

$ kuberecord config delete-profile local --force
→ deleted profile "local" from ~/.config/kuberecord/config.yaml (was: ClickHouse at 10.0.1.5:9000/kuberecord)
→ no profile is active now: the resolution chain falls through to the steps after it
→ to choose another: `kuberecord config use-profile archive`
```

`--force` **clears the active pointer** as well as removing the stanza. That is
what makes it a deletion rather than a way to corrupt the file: a `currentProfile`
naming nothing is refused when the file is read, so the next command would not
resolve to a missing profile — it would refuse to read the configuration at all.

A name the file does not hold is an error listing the names it does, which is the
shape every "missing key" message in this tool uses:

```console
$ kuberecord config delete-profile locl
error: no profile named "locl" in ~/.config/kuberecord/config.yaml (defined: archive, local)
```

The reason deletion is a command at all, rather than *"edit the YAML"*: the person
who needed prompts to write a profile is not the person who should be hand-editing
one, and a stale profile is not inert. It sits at step 3 of
[the resolution chain](#where-the-data-comes-from) and shadows discovery — which is
a confusion [`config resolve`](#config-resolve) was partly built to diagnose, and
removal is the fix you reach for the moment you have diagnosed it.

That closes the lifecycle: **create** with `set-profile`, **inspect** with
[`config get-profiles`](#config-view-and-config-get-profiles) or
[`config current-profile`](#config-current-profile), **switch** with
`use-profile`, **delete** with `delete-profile`. It is `kubectl config`'s own five
verbs, one for one, and every step is a command rather than a text editor.

It is not inert in a second way either: a profile that answers stops the chain even
when it cannot be resolved, so a stanza pointing at a variable you no longer export
fails every command rather than quietly letting discovery take over. That failure
names `use-profile`, `delete-profile` and both flag routes past it — see [A profile
that cannot resolve is
fatal](#a-profile-that-cannot-resolve-is-fatal-and-says-what-to-do-instead).

#### Activation is opt-in

**Writing a profile does not make it the one that answers.** The active profile is
consulted by every command that names neither [`--source`](#--source) nor
`--sink`, so activating on every write would point `timeline`, `diff` and `get` at
a store you may have written in order to *inspect* it — a side effect on
everything you type next, decided by a command you ran to record an address.
`kubectl config set-context` does not switch either.

**`--use` writes and activates in one command**, and that is the whole of the
concession:

```console
$ kuberecord config set-profile archive --backend s3 --bucket acme-audit --use
→ wrote profile "archive" in ~/.config/kuberecord/config.yaml
→ made "archive" the active profile, as asked
```

Without it, the write says nothing about which profile answers — and where there
is somewhere to be sent next, the `config use-profile` line is printed instead:
[`--from-sink`](#--from-sink) and [the
questions](#asking-instead-of-knowing-the-flags) both end with it.

**A profile written into an otherwise empty file is activated anyway**, and the
line says why:

```console
$ kuberecord config set-profile local --backend clickhouse --addr 127.0.0.1:9000
→ wrote profile "local" in ~/.config/kuberecord/config.yaml
→ made "local" the active profile (it is the only one)
```

There is nothing to displace and no second reading of it: the only profile in the
file is the one that answers, and requiring a second command to make it usable
would be ceremony with no decision in it. It is the single exception, and both
halves of the rule are load-bearing — **a cleared pointer is not an empty file.**
`config delete-profile --force` deliberately leaves a file with profiles in it and
no active one, so that resolution falls through to discovering a sink; the next
profile written there is *not* activated, because there is a decision to make and
`--use` is where it is made.

**`--use` on the profile that already answers changes nothing, and says so.** It is
not an error — the write succeeded and the profile is active, which is the state
the invocation asked for — but a flag with no visible effect has to account for
itself:

```console
$ kuberecord config set-profile local --backend clickhouse --addr 127.0.0.1:9000 --use
→ updated profile "local" in ~/.config/kuberecord/config.yaml (was: ClickHouse at 10.0.1.5:9000/kuberecord)
→ --use changed nothing: "local" is already the active profile
```

Rewriting the profile that answers says so too, whether or not `--use` was given,
because that write is the one an upsert notice cannot describe on its own: the
stanza it just replaced is the one the next command reads.

**Activation is reported whenever it happens**, in the same
[provenance](#colour-width-and-paging) register as the rest of this subcommand's
lines. Where the answer comes from must be legible in scrollback, and a change to
it that nobody typed most of all.

#### Writes are scriptable

The four subcommands that write — `set-profile`, `use-profile`, `delete-profile`
and `set-context-cluster-id` — render a document for `-o json` and `-o yaml`. The
confirmation stays on stderr either way, so `| jq` receives the document alone.

The three that act on a profile render a `ProfileChange`:

```console
$ kuberecord config set-profile local --backend clickhouse --addr 127.0.0.1:9000 -o json
→ updated profile "local" in ~/.config/kuberecord/config.yaml (was: ClickHouse at 10.0.1.5:9000/kuberecord)
{
  "apiVersion": "cli.kuberecord.io/v1alpha1",
  "kind": "ProfileChange",
  "action": "updated",
  "name": "local",
  "path": "/home/you/.config/kuberecord/config.yaml",
  "profile": {
    "backend": "clickhouse",
    "clickhouse": { "addr": "127.0.0.1:9000" }
  },
  "previous": {
    "backend": "clickhouse",
    "clickhouse": { "addr": "10.0.1.5:9000", "database": "kuberecord" }
  },
  "currentProfile": "local"
}
```

| Field | Meaning |
|---|---|
| `action` | `created`, `updated`, `deleted` or `activated`. The field to branch on: which of the two stanzas is present depends on it. |
| `name` | The profile acted on. |
| `path` | The file written, so documents collected from several machines are distinguishable. |
| `profile` | The stanza this name now carries. Absent for a deletion. |
| `previous` | The stanza this write displaced — replaced by an update, or removed by a deletion. Absent when nothing was displaced, including for `activated`, which moves a pointer and destroys nothing. |
| `currentProfile` | The active pointer *after* the write, and `""` when none is active. Always present, so an empty pointer is a value to read rather than a missing key to infer from — which is exactly what `delete-profile --force` produces. |

`set-context-cluster-id` renders a `ContextMapping`, whose subject is different
and which therefore is not the same kind (`context`, `clusterID`,
`previousClusterID` when the context was already mapped, and `path`). That
subcommand is an upsert too, and it reports a remap the same way:

```console
$ kuberecord config set-context-cluster-id prod-eu prod-eu-2
→ context "prod-eu" reads cluster "prod-eu-2" (was: "prod-eu-1")
```

Neither kind is an [envelope](#structured-output): no `metadata`, no `items`,
because no question about recorded history was asked. Both carry the same
`apiVersion` and are governed by the same
[additive-only policy](#the-additive-only-policy).

#### `--from-sink`

```console
$ kuberecord config set-profile local --from-sink ClickHouseSink/default
→ wrote profile "local" in ~/.config/kuberecord/config.yaml
→ made "local" the active profile (it is the only one)

ClickHouseSink/default records clickhouse.kuberecord-quickstart.svc:9000.

That name resolves inside the cluster and nowhere else, so the profile records
127.0.0.1:9000 instead and expects a forwarded port beside it:

    kubectl port-forward -n kuberecord-quickstart svc/clickhouse 9000:9000

Database kuberecord and user kuberecord are the sink's own.
Its own credential is Secret kuberecord-system/clickhouse-credentials, key "password".
The profile does not copy it: kuberecord's password comes from $KUBERECORD_CLICKHOUSE_PASSWORD.

That user is the sink's own writer, so this profile can write to the audit trail.
Give --username a read-only user instead; the grants it needs are at
docs/CLI.md#the-read-only-clickhouse-user
```

It reads the named sink through the same discovery path a query uses, and writes
the stanza its kind calls for. It is the second of the two routes in [Running the
CLI outside the cluster](#running-the-cli-outside-the-cluster) — the one for a
cluster you come back to. The point is the address, which is precisely the
field that must differ from the custom resource — otherwise there would be no
reason to write a profile at all:

| `--addr` | The custom resource's address | The profile records |
|---|---|---|
| given | anything | what `--addr` says |
| omitted | cluster-internal (`*.svc`, `*.cluster.local`, a bare host) | `127.0.0.1:<the recorded port>`, with a notice and the `kubectl port-forward` line |
| omitted | anything else | the recorded address, unchanged |

The classifier is the one the unreachable-backend message uses, so the command
that rewrites an address and the message that explains why it needed rewriting
cannot disagree. Nothing is dialled either way: this writes a file, and an
address that cannot be reached is something the CLI names rather than something
it tries to work around.

**The password is not copied.** The sink's Secret is read to confirm it holds the
key the sink names — a Secret created with `--from-literal=PASSWORD=…` is reported
here rather than three steps later — and the value is never extracted. The profile
names `KUBERECORD_CLICKHOUSE_PASSWORD` unless `--password-env` or `--password-file`
says otherwise, and the file rule above applies to it exactly as it does to a
hand-written stanza. A Secret you may not read is a notice, not a failure: nothing
in the written profile depends on it, and being unable to read it is the ordinary
state this whole subcommand exists for. Both routes rest on that — the questions
[behave the same way](#asking-instead-of-knowing-the-flags), and differ only in
asking the two credential questions rather than taking their defaults, because
there is somebody there to ask.

**`--username` and the variable are one decision.** A profile reading as a
[read-only user](#the-read-only-clickhouse-user) defaults to a variable derived
from that user's name rather than to `KUBERECORD_CLICKHOUSE_PASSWORD`, because a
username and a password are halves of one credential and two profiles naming two
principals cannot share one variable. The message above says which principal was
written and where its password comes from, in one sentence, and it recommends a
read-only user only when the profile does not already read as one:

| `--username` | The profile reads as | Its password comes from | The message |
|---|---|---|---|
| omitted, or the sink's own user | the sink's own user | `KUBERECORD_CLICKHOUSE_PASSWORD` | says that user can write to the audit trail, and names `--username` |
| a different user | that user | `KUBERECORD_CLICKHOUSE_PASSWORD_<THAT_USER>` | states the pair, and recommends nothing — the advice has been taken |

Four flags survive `--from-sink`, and they are the ones a `ClickHouseSink` cannot
state or must not state for a *reader*: `--addr`, `--username`, `--password-env` /
`--password-file`, and `--tls` (`spec.connection` carries no TLS field at all).
Everything else is refused by name, because the custom resource states it and a
profile that disagreed would read somewhere other than where the sink writes.

An `S3Sink` transfers directly — bucket, prefix, region, endpoint and path style —
and takes no overrides: an object store has no address that resolves only inside a
cluster, and its credentials are not in this file at all. A cluster-internal
`endpoint` is recorded as it stands and said so in the notice; an endpoint carries
a scheme and a certificate name as well as a host, so substituting a forwarded port
for it is not a guess this command makes.

**The profile is written, not activated**, unless `--use` is given. An existing
choice is never overridden; the `config use-profile` line to run next is printed
instead. The one exception is the rule the whole subcommand already follows: a
profile written into an otherwise empty file becomes the active one, and says so.
See [activation is opt-in](#activation-is-opt-in).

#### Asking instead of knowing the flags

`config set-profile` with **no flags of its own**, on a terminal, asks. There is no
`--interactive`: a flag to request the behaviour you get by typing nothing is a flag
nobody finds, and `gh auth login` sets the precedent.

The first question is whether to read the settings out of a sink this cluster
already holds — which is [`--from-sink`](#--from-sink) reached without having had
to know it exists. That ordering is the whole point. A wizard whose first question
is "what is the address?" has not helped anybody, because not knowing the address
is why they are here.

```console
$ kuberecord config set-profile local

Writing a profile: where this command reads recorded history from.
A profile never holds a password — it names an environment variable or a file.
Ctrl-D at any question stops, and writes nothing.

Read the settings from a sink custom resource in this cluster? [Y/n] y

Which sink should this profile read from?
  1) ClickHouseSink/default — the frozen v1 schema in a ClickHouse instance
  2) S3Sink/archive — a jsonl-v1 archive in an S3-compatible bucket
> [ClickHouseSink/default] 1

ClickHouseSink/default records clickhouse.kuberecord-quickstart.svc:9000.
That name resolves inside the cluster and nowhere else.

ClickHouse native-protocol endpoint, as host:port.
> [127.0.0.1:9000]

ClickHouseSink/default authenticates as kuberecord, which can write to the
audit trail.

Which ClickHouse user this profile reads as. A read-only user is the recommended posture; see docs/CLI.md#the-read-only-clickhouse-user.
> [kuberecord] kuberecord_ro

Where does kuberecord_ro's password come from?
  1) environment — an environment variable, named next
  2) file — a file, named next
> [environment]

Name of an environment variable holding the ClickHouse password.
> [KUBERECORD_CLICKHOUSE_PASSWORD_KUBERECORD_RO]

Make this the active profile? [y/N]
> → wrote profile "local" in ~/.config/kuberecord/config.yaml
…
→ to make it the active profile: `kuberecord config use-profile local`

The same thing without the questions:
  kuberecord config set-profile local --from-sink ClickHouseSink/default --addr 127.0.0.1:9000 --username kuberecord_ro --password-env KUBERECORD_CLICKHOUSE_PASSWORD_KUBERECORD_RO
```

Eight things about it are worth stating, because each is a decision rather than an
accident:

- **The last line is the point.** The questions are for somebody who does not know
  the flags; the equivalent command is what they are holding afterwards. It is the
  line to paste into a bug report, the line to lift into a CI job, and the reason a
  second profile does not need a second conversation. It reproduces the profile
  exactly — a test writes one both ways and compares the file.
- **Nothing here validates anything.** Every answer is put through the same
  validator the file is read with and the flags are checked by, so a value the flags
  refuse is refused here in the same sentence, and a value they take is taken. A
  shared test table drives both routes and asserts exactly that. A second validator
  — even one that agreed on the day it was written — is one that drifts into
  accepting a value the file will later refuse.
- **There is no password prompt**, and cannot be: a profile never stores a password
  inline, so what is asked for is the *name* of an environment variable or the path
  of a file. Nothing secret is typed, echoed, held in memory or left in scrollback.
- **The user and the password source are one decision, so they are one pair of
  questions.** A ClickHouse username and password are halves of one credential, and
  the moment the CLI is about to recommend a
  [read-only user](#the-read-only-clickhouse-user) is the moment it has to ask which
  user rather than recommend one and record another. So the second question names
  the answer to the first, and the variable it offers is derived from it. Pressing
  return through both writes exactly what `--from-sink` writes with no flags: this
  adds a question, not a requirement.
- **The last question is the only one that is not about the profile**, and it
  defaults to no: making this the active profile changes where every later command
  reads from, so it is asked rather than assumed — and `--use` answers it before it
  is asked. It is skipped where there is nothing to decide: a first profile in an
  empty file is activated regardless, and a profile that already answers cannot be
  made to answer more. Answer yes and the printed command gains `--use`, so the
  line reproduces the activation as well as the stanza. See [activation is
  opt-in](#activation-is-opt-in).
- **Off a terminal it is an error, never a wait.** Standard input that is not a
  terminal exits `2` naming both flag forms. A wizard that blocked in CI would hang
  the pipeline until something killed it, and the message saying what was wanted
  would never arrive.
- **Ctrl-D at any question writes nothing.** The file is written after the last
  answer, so stopping earlier leaves nothing to undo — and it says so rather than
  returning silently to a shell prompt.
- **A Secret it cannot read costs one more question, not the conversation.** The
  operator's ClusterRole reads Secrets in its own namespace and most engineers have
  less than that, so this is the ordinary shape rather than the edge. Everything the
  profile needs came out of the custom resource; the Secret was being read only to
  confirm a key, and the value was never going to be stored. So the questions carry
  on:

  ```console
  ClickHouse native-protocol endpoint, as host:port.
  > [127.0.0.1:9000]

  Read the connection settings from ClickHouseSink/default.
  Cannot read its Secret (forbidden) — that is fine: a profile stores where
  your password lives, not the operator's.

  ClickHouseSink/default authenticates as kuberecord, which can write to the
  audit trail.
  ```

  The two credential questions follow, exactly as they do when the Secret *was*
  read: the sentence above them is the only difference, and it is there because a
  reader who has just been refused a Secret should be told that nothing is broken.
  Every *other* way reading the sink can fail — a custom resource that is gone, one
  you may not read, one whose spec does not decode — still ends the command, because
  each is a failure of the thing you named in the menu.

A global flag is not one of this command's flags. `--context`, `--kubeconfig` and
`--operator-namespace` say which cluster the first question would list sinks from,
so an invocation carrying one is precisely an invocation that wants to be asked;
`--color`, `-o` and `-v` have no opinion about a profile either. Anything from the
table above, or `--from-sink`, means you have said what you want, and the flag path
runs unchanged.

`--sink-addr` is refused here whichever route you are on. It replaces the endpoint
of one invocation's *resolved* backend ([`--sink-addr`](#--sink-addr)), and this
command resolves nothing and dials nothing — so a value given here would parse,
change no field, and leave you believing you had set the address the profile
records. `--addr` is the flag that sets it.

#### `config resolve`

Nine steps decide where an answer comes from — four for
[the backend](#where-the-data-comes-from), five for
[the cluster identity](#the-cluster-identity) — and a working command reports them
in two lines of notice. That is the right amount of ceremony for an answer somebody
wanted. It is the wrong amount when the chain chose something you did not expect: a
profile written months ago shadowing discovery, a `--context` pointing at the wrong
cluster, an identity read from an operator that is not the one you meant. The result
is then wrong in a way that looks right.

`config resolve` runs both chains, prints what every step decided, and stops. It
answers no question about recorded history and returns no rows.

It is the detailed half of a pair. [`version --check`](#version---check) puts the
same question through the same machinery and reports the four facts the chains
produced; this reports the nine steps that produced them. Run that one first, and
this one when its answer is surprising.

```console
$ kuberecord config resolve
backend
  --source             silent       not given
  --sink               silent       not given
  profile              silent       ~/.config/kuberecord/config.yaml defines no profiles
  discovery            answered     the cluster's only sink

  resolved             ClickHouseSink/default (clickhouse.kuberecord-system.svc:9000/kuberecord)
  engine               clickhouse
  capabilities         deletions=yes, server_side_filter=yes, point_query=yes, time_bound_required=no

cluster identity
  --cluster-id         silent       not given
  context mapping      silent       ~/.config/kuberecord/config.yaml maps no kubeconfig contexts
  operator Deployment  answered     prod-eu-1
  the sink             not reached

  resolved             prod-eu-1 (from the operator Deployment kuberecord-system/kuberecord-controller-manager)

reachability
  not checked          nothing was dialled; --check asks the backend whether it answers
```

Every step reports one of five outcomes:

| Outcome | What it means |
|---|---|
| `answered` | it produced the chain's result |
| `silent` | it was consulted and had nothing — no flag, no active profile, no mapping for this context |
| `failed` | it had something to say and could not say it, and the chain stopped there |
| `not reached` | an earlier step answered, or the chain stopped before this one |
| `withheld` | it would have contacted the backend, and `--check` was not given |

The `capabilities` line is the chosen engine's own declaration, in the names
[the capability table](#backend-capability-differences) and `-o json` both use.
It is on one line because the question it answers is comparative: two setups that
answer the same query differently differ here, and two of these reports can be put
side by side.

##### `--check`

**Nothing is dialled without it.** The configuration most worth inspecting is the
one whose backend cannot be reached, and a command that dialled in order to
describe itself would stall for a dial timeout on exactly that case. The identity
chain's last step — the only part of resolution that questions the backend — is
therefore `withheld` by default, and an identity that only that step could have
produced is reported as `undetermined` rather than as a failure. Nothing is wrong:
one step has not been taken.

With `--check`, the backend is asked which clusters it holds. That is the cheapest
question the read plane has and the one the identity chain's last step asks anyway,
and it exercises the whole path rather than a socket: DNS, the connection, the
credential, and — for ClickHouse — the database being the one the sink named.

```console
$ kuberecord config resolve --check
…
reachability
  unreachable          the backend could not be reached
                       cannot reach ClickHouseSink/default at clickhouse.kuberecord-system.svc:9000: …
error: cannot reach ClickHouseSink/default at clickhouse.kuberecord-system.svc:9000: …

! ClickHouseSink/default records the address clickhouse.kuberecord-system.svc:9000.
…
```

A failure here prints the same explanation the query commands print — see
[Running the CLI outside the cluster](#running-the-cli-outside-the-cluster) — so
the diagnostic is identical wherever you meet it. A backend that cannot answer the
question without running a real query reports `cannot be checked` and does not fail
the command: that is a statement about the engine, not a fault.

##### Output and exit codes

`-o json` and `-o yaml` emit a `cli.kuberecord.io/v1alpha1` document with
`kind: Resolution`, carrying both chains, their steps and the declared
capabilities. "Paste the output of `kuberecord config resolve -o json`" is a better
first question in a support thread than "what does your config look like".
`-o jsonl` and `-o diff` are refused by name: the document is one item, and there
are no change operations in it.

| Code | When |
|------|------|
| `0` | both chains resolved — or the identity is `undetermined` because `--check` was not given, which is not a failure |
| `1` | a chain failed, or `--check` could not reach the backend |
| `2` | the invocation was malformed — including a malformed `--sink`, which is the same usage error a query command gives |

**No credential appears in any format, at any verbosity.** The report names the
host, the database, the sink and the file paths it read; never the password, never
the access key.

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

Or, against a cluster whose sink the CLI can see, without looking any of it up:

```console
$ kuberecord config set-profile prod --from-sink ClickHouseSink/default \
    --username kuberecord_ro
```

**A username and a password are one credential pair.** `--username` is one of the
four flags [`--from-sink`](#--from-sink) accepts for exactly this reason: a profile
records which user it reads as, so the password it reads has to be that user's. A
profile naming `kuberecord_ro` and a variable holding the operator's password
authenticates as neither.

That is also why a profile reading as somebody other than the sink's own user gets
a **variable of its own** — `KUBERECORD_CLICKHOUSE_PASSWORD_KUBERECORD_RO` for the
user above, uppercased with everything a shell will not accept replaced by `_`.
One variable holds one password, so four profiles naming four principals and all
reading `KUBERECORD_CLICKHOUSE_PASSWORD` are four profiles of which at most one
authenticates. The variable is printed when the profile is written, and
`--password-env` overrides it.

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

The whole path — `helm install`, an `S3Sink`, and these commands against the
archive it writes, with no database anywhere — is runnable in one command and
documented step by step at
[`examples/zero-infra/`](../examples/zero-infra/):

```sh
make quickstart-zero-infra
```

The trade is query performance, and it is a real one. The object archive has no
index, so a single-object question over a wide window lists and decompresses every
object in that window's partitions — every object, not only the ones belonging to
the object you asked about. Ninety days of a busy cluster is thousands of objects
and gigabytes off the wire for a table with four rows in it.

That is the deliberate price of having no database to run, and the CLI states
it rather than hiding it: the cost is bounded, reported and interruptible. **For
wide analytics over an archive** — aggregations, joins, anything that reads more
than one object's history — **use the DuckDB and Athena recipes in
[`docs/QUERIES.md`](QUERIES.md)**, which are built for exactly that shape. This CLI
answers narrow questions honestly; it is not a query engine, and pointing it at a
question DuckDB answers in one pass will be slow in a way no flag fixes.

### Cold scans

Five things surround every scan of an unindexed backend. None of them applies to
ClickHouse, which seeks to the object's rows: they are keyed on the backend's
declared capabilities, not on its name, so a future indexed backend inherits the
right behaviour by declaring it.

**The window defaults to 24 hours.** With neither end given — under either
spelling, `--since`/`--until` or `--from`/`--to` — a backend that needs a time
bound gets one day rather than everything. It is
announced on stderr, and an empty result names it. `--since` widens it.

**The cost is printed before the first object is fetched.**

```console
$ kuberecord timeline deploy/checkout -n payments --source ~/archives/kuberecord --since 3d
! ~1,240 objects, ~3.1 GiB to scan for 3d: the objectsource backend has no index, so this window is the work
```

The figures come from the listing alone — nothing is opened to produce them — so
the warning costs a fraction of a listing rather than a fraction of the scan. Both
are *stored* bytes: what comes off the wire, which is what predicts the wait and
the egress bill.

**A window wider than 7 days asks first.**

```console
$ kuberecord timeline deploy/checkout -n payments --source ~/archives/kuberecord --since 90d
~14,890 objects, ~37.2 GiB — continue? [y/N]
```

Anything that is not `y` reads nothing at all. The question is asked only when
somebody can answer it: with stdout or stdin redirected, or with `--yes`, it is
assumed, and stderr says which of the two reasons applied. **A script therefore
never hangs on a prompt** — and never silently skips one either, because the line
that says the confirmation was assumed is printed regardless.

**A scan whose size could not be determined asks too, however narrow the window.**
A failed listing does not stop the question being answerable — the estimate is a
courtesy, and refusing to answer because the warning could not be assembled would
be the degradation making itself into the failure. But it does mean the window has
stopped being a proxy for cost, so the width no longer decides:

```console
$ kuberecord timeline deploy/checkout -n payments --source s3://acme-audit --since 6h
! the size of this scan could not be estimated (listing s3://acme-audit: AccessDenied), so it is unknown
an unmeasured number of objects, because its size could not be determined — continue? [y/N]
```

A failed estimate is not evidence of a small scan; it is the absence of evidence,
and a six-hour window against an archive that cannot be listed is exactly the
invocation this tool knows least about. Refusing it stops with the same message and
the same exit code as refusing a wide one — there is only one way to say no. `--yes`
and a non-terminal still pass it without asking, unchanged.

Confirming it imposes no ceiling of its own. `--max-objects` remains the only bound
on the work and remains opt-in: a silent limit here would truncate a scan you had
just agreed to, and — since a pipeline never confirms — would bound the same command
for a person while leaving it unbounded in a script.

**Progress goes to stderr while it runs**, repainted in place and only when stderr
is a terminal, so `2>/dev/null` and a redirected log get none of it:

```console
scanning 412/1,240 objects, 1.1 GiB read
```

**`--max-objects N` is the circuit breaker.** It bounds the *work*, which `--limit`
cannot do without an index — `--limit 100` still costs every object in the window
before the newest hundred changes can be known. A scan that passes `N` stops and
says so, naming the flag:

```console
error: reading the timeline of payments/checkout: the scan reached 5,001 objects,
past the --max-objects=5000 circuit breaker, and was stopped before it had read the
whole window; narrow it with --since, or raise --max-objects
```

**Ctrl-C stops it cleanly.** The interruption travels through the context: fetches
stop being scheduled, the iterator is closed, and the command exits `1` with the
window it did not finish reading named. It never exits `0` with a short result,
because a timeline that is short by an unknown amount is worse than no timeline. A
second Ctrl-C is fatal in the ordinary way, which is the escape hatch if a backend
is not honouring its context.

| Flag | Meaning |
|------|---------|
| `--yes` | Answer the confirmation. Assumed when stdout or stdin is not a terminal. |
| `--max-objects` | Stop a scan that fetches more than this many stored objects. `0`, the default, means no limit. |

Both are global flags: they apply to any command whose backend has to scan, and are
inert against one that does not.

The estimate, the confirmation and the progress line cover `timeline`, `diff` and
`blame`, whose scans are driven by the window you type. `get --at` is not gated: it
walks backwards from one instant and stops at the first full state it finds, so its
cost is a property of the archive's checkpoint cadence rather than of a flag.
Neither is `scopes`, which reads the scope log — one small object per day, not one
per hour. Ctrl-C stops all five.

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | Success. For `diff --exit-code`, additionally "no changes". |
| `1` | Runtime error: a well-formed request that could not be carried out — including an interrupted scan and one stopped by `--max-objects`, neither of which may present its partial reading as an answer. For `diff --exit-code`, additionally "changes found", which is a finding rather than a failure and prints no `error:` line. |
| `2` | Usage error: an unknown flag, a malformed object address, a bad value. |
| `3` | No coverage: nothing was ever watching the requested scope — which is a different fact from "nothing changed", and is reported as one. |

`diff --exit-code` is the one place `0` and `1` carry a second meaning, which is
why it is opt-in: it overloads codes that otherwise only mean success and failure.
Code `3` outranks it either way.

Every code but `0` prints one `error:` line to stderr, **red** on a terminal, in a
single write so that nothing else sharing stderr can land inside it. A usage error
appends the command's own usage block beneath it, uncoloured — a page of flag
descriptions in red is a page nobody reads. `diff --exit-code`'s exit `1` is the
exception that prints no line at all: the changes it found are in the document
above, and calling that a failure would misread it.

Code `3` is the one worth scripting against. Every other tool in this space
collapses "your query matched nothing" and "nothing was ever recorded here" into a
single successful empty result; kuberecord will not, because those two answers send
an engineer in opposite directions.
