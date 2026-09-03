# Releasing kuberecord

Strangers install tags, not `main`. This page is the policy for what a tag
promises, plus the mechanics of cutting one.

- [Versioning policy](#versioning-policy)
- [What a release publishes](#what-a-release-publishes)
- [Cutting a release](#cutting-a-release)
- [Rehearsing a release](#rehearsing-a-release)
- [What the gate refuses](#what-the-gate-refuses)

Verifying a release you downloaded is the other side of this page, and it is its
own: [`VERIFYING.md`](VERIFYING.md).

## Versioning policy

kuberecord carries **four version numbers, and they are deliberately
independent**. Conflating them is the mistake this section exists to prevent: an
operator upgrade is not a schema migration, and a schema that is frozen does not
mean an operator that cannot change. The fourth is new in v0.2.0 — the S3 object
format is its own contract on its own timeline, because a storage format
that outlives the operator by years cannot be versioned with it.

| What | Version | What it promises |
|---|---|---|
| **The operator** (image, chart, `install.yaml`) | `v0.x.y` — semver, **pre-1.0** | A **minor bump may break**: flags, defaults, RBAC and behaviour are all fair game while the leading digit is `0`. Every break is spelled out in [`CHANGELOG.md`](../CHANGELOG.md). A **patch** bump is fixes only — no new flags, no new permissions, no behaviour change beyond the bug named in the notes. |
| **The CRDs** | `kuberecord.io/v1alpha1` | Alpha in the Kubernetes sense, and the honest reading of it: a field may be removed, renamed or re-defaulted in an operator minor. There are **no conversion webhooks**, so there is exactly one served and stored version and an incompatible change is a manual edit of your custom resources, guided by the changelog. |
| **The ClickHouse schema** | `v1` — **frozen** | Within `v1` no column is renamed, retyped, repurposed or removed, and neither the engines nor the sort keys change. Changes are additive only. This is the strongest of the four promises, and it does not weaken when the operator's does: see [`SCHEMA.md`](SCHEMA.md#stability--versioning). |
| **The S3 object format** | `jsonl-v1` — **frozen**, stamped into every key | Within `jsonl-v1` the line format, the field names, the key layout and the definition of the content hash do not change; fields may only be added, and a reader must tolerate ones it does not know. A change that cannot be expressed additively ships as a sibling `format=jsonl-v2/` partition rather than a rewrite — archived objects may be under Object Lock and legally immutable. See [`SCHEMA.md`](SCHEMA.md#versioning-the-object-format). |

Two consequences worth stating outright:

- **An operator minor may break while the schema does not.** Rows written by
  `v0.1.0` are readable by every later `v0.x`, and a dashboard or query built
  against schema `v1` keeps working across operator upgrades. That asymmetry is on
  purpose — the operator is young, the data is not disposable.
- **The Helm chart's `version` and `appVersion` are the operator's version.** The
  chart is not independently versioned: `Chart.yaml`'s `version` is `X.Y.Z` and
  its `appVersion` is `vX.Y.Z`, both equal to `VERSION` in the Makefile. A chart
  that could move independently would be a fourth number to reason about, for the
  benefit of a chart that only ever installs one operator.

### Pre-1.0, concretely

`v0.x` is not a licence to break things casually — it is a statement that the
project has not yet promised otherwise. In practice:

- Removals and renames land in a **minor** bump, never a patch, and always with a
  migration note.
- The `Removed` section of a release's changelog entry is the complete list. If it
  is not there, it did not happen.
- `v1.0.0` is the point at which the operator's own surface gets the same
  treatment the schema already has. Nothing here dates that.

## What a release publishes

Pushing a tag matching `vX.Y.Z` (or `vX.Y.Z-<prerelease>`) runs
[`.github/workflows/release.yml`](../.github/workflows/release.yml), which
publishes:

| Artifact | Notes |
|---|---|
| `ghcr.io/kuberecord/kuberecord:vX.Y.Z` | Multi-arch (`linux/amd64`, `linux/arm64`, `linux/s390x`, `linux/ppc64le`), built by `make release-image`, which is the repository's existing buildx target. |
| `oci://ghcr.io/kuberecord/charts/kuberecord:X.Y.Z` | The same packaged chart as the `.tgz` below, in the registry. Note the tag has no `v` — a Helm chart version is semver, and the chart's is `X.Y.Z`. From v0.3.0 onward. |
| `oci://ghcr.io/kuberecord/charts/kuberecord:artifacthub.io` | Not a chart version — the same repository under a fixed tag, holding [`artifacthub-repo.yml`](../artifacthub-repo.yml) as a one-layer OCI artifact. It is the ownership claim [Artifact Hub](https://artifacthub.io) checks for the listing's verified-publisher flag, and an OCI chart repository has no `index.yaml` to serve it beside, so it ships as an artifact. Republished unchanged on every tag, by `make release-chart-metadata-push`. |
| `install.yaml` | `kubectl apply -f` it: CRDs, RBAC and the manager, with the image above pinned exactly. For a non-prerelease tag it is byte-identical to the committed [`dist/install.yaml`](../dist/install.yaml) — the artifact you download is the file that was reviewed. |
| `kuberecord-X.Y.Z.tgz` | The packaged Helm chart, `--version X.Y.Z --app-version vX.Y.Z`. |
| `kuberecord_vX.Y.Z_<os>_<arch>.tar.gz` (`.zip` for Windows) | The CLI, five archives — `linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`, `windows/amd64` — each carrying both `kubectl-kuberecord` and `kuberecord` plus the licence. Cross-compiled cgo-free by `make build-cli`, which asserts `CGO_ENABLED=0` against each produced binary. From v0.3.0 onward. |
| `kuberecord.yaml` | The krew plugin manifest: one `uri` and `sha256` per platform, generated from the archives above by `hack/krew-manifest.sh`. This is the file submitted to [kubernetes-sigs/krew-index](https://github.com/kubernetes-sigs/krew-index), and `kubectl krew install --manifest-url` takes it directly. From v0.3.0 onward. |
| `kuberecord.rb` | The Homebrew formula, generated by `hack/homebrew-formula.sh` from the four archives brew has a platform for. The release pushes this exact file to [`kuberecord/homebrew-tap`](https://github.com/kuberecord/homebrew-tap), so what a `brew install` runs is a published, checksummed asset rather than a file that appeared in a repository. From v0.3.0 onward. |
| `kuberecord-X.Y.Z-sbom.spdx.json` | An SPDX 2.3 SBOM of the published image, produced by syft from the pushed `linux/amd64` manifest by digest. One document, because every platform is the same static binary in the same base image. |
| `kuberecord-cli-X.Y.Z-sbom.spdx.json` | The same, for the CLI: syft over the `linux/amd64` binary. It is a separate document because the CLI's dependency set genuinely differs from the operator's. |
| `checksums.txt` | `sha256` over every asset above — install artifacts, CLI archives, the krew manifest, the Homebrew formula and both SBOMs. Verify with `sha256sum -c checksums.txt` in the directory you downloaded them to. |
| `checksums.txt.sigstore.json` | The keyless cosign signature over that file. From v0.3.0 onward. |
| The Release body | That version's section of [`CHANGELOG.md`](../CHANGELOG.md), extracted verbatim by `hack/changelog-section.sh`. The changelog *is* the release notes. |

And, from v0.2.0 onward, the evidence for all of it:

| Evidence | Notes |
|---|---|
| A **cosign signature** on the image | Keyless: no key exists, so none can leak. The signature is bound to a Fulcio certificate naming this workflow at this tag, and `--recursive` means the manifest list *and* each per-platform manifest carry one. |
| A **cosign signature** on the chart | The same identity, over the chart's OCI manifest. Not `--recursive`: a chart is one manifest, so there is nothing under it. From v0.3.0. |
| **SLSA build provenance** for the image | Recorded against this repository *and* pushed into the registry beside the image, so verification need not depend on this repository's API. Carrying either across a mirror takes a referrers-aware copy — see [`VERIFYING.md`](VERIFYING.md#the-build-provenance). |
| **SLSA build provenance** for every attached asset | Generated from `checksums.txt` itself, so what is checksummed and what is attested are the same set by construction. |
| A **cosign signature** on `checksums.txt` | `sign-blob`, because a file on a Release page has no digest a registry could name. One signature therefore covers every attached asset — the CLI archives, the install artifacts, the krew manifest, the Homebrew formula and both SBOMs — since each is a line in that file. From v0.3.0. |

The verification commands, and — just as important — what they do and do not
prove, are [`VERIFYING.md`](VERIFYING.md). The release job runs them against what
it just published, before the Release page that advertises them exists: a
signature that does not verify fails the release rather than shipping.

### Why the chart is published twice

The registry copy is the primary one, and the release asset is kept because
removing it would break every install command already written down.

The asset URL is the reason there are two. It is
`github.com/kuberecord/kuberecord/releases/download/…`, and for anyone who wrote
it down before the move to the organization it is the old path, served by a
GitHub redirect. That redirect survives a rename — but it is destroyed
permanently, and irrecoverably, the moment anyone creates a repository named
`kuberecord` under the old account. It is the one consequence of the migration
that cannot be undone afterwards. A registry reference has no such dependency.

Both copies are **the same bytes**, and that is enforced by construction rather
than by care: the release pushes the archive `make release-artifacts` produced,
never a second packaging of it. `helm package` stamps the current time into every
tar header, so packaging twice yields two archives with two digests for one
version — and `checksums.txt` would then describe only one of them. Pushing the
packaged file makes the registry artifact's layer digest exactly the `sha256`
that `checksums.txt` lists, so the checksums and the SLSA attestation cover the
chart however it was fetched.

That is also why the push happens in the job that builds the artifacts, and why
that job carries `packages: write` — a permission the release workflow otherwise
keeps confined to the image job.

The chart is pushed **before** the GitHub Release is created, for the same reason
the image is: the Release page tells a reader to `helm install oci://…`, and a
page that says so before the artifact exists hands its first readers a 404.

**There is no floating `latest` tag**, for images or for artifacts. What a cluster
runs is decided by the tag somebody chose, not by whatever moved last. To pin
harder than a tag, resolve the digest once and use it — the chart takes
`image.digest`:

```sh
# A digest is the hash of the manifest bytes, so this is exact by construction.
# macOS: shasum -a 256. With crane installed it is one word: `crane digest <ref>`.
docker buildx imagetools inspect --raw ghcr.io/kuberecord/kuberecord:vX.Y.Z | sha256sum
```

(Not `--format '{{.Manifest.Digest}}'`: on at least one shipped buildx — Docker
Desktop's v0.22 — a template referencing `.Manifest` is ignored and the default
listing is printed instead, with exit code 0. Anything parsing that output gets
whatever the text happens to look like.)

A tag carrying a prerelease suffix is published as a GitHub prerelease, so it
stays out of "latest release" on the repository's front page.

## Cutting a release

Everything below happens on a branch, in one commit, reviewed like any other.

1. **Write the changelog section.** Rename `## [Unreleased]` work into
   `## [X.Y.Z] - YYYY-MM-DD`, keep the Keep-a-Changelog groups (`Added`,
   `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`), add a fresh empty
   `## [Unreleased]` above it, and add the two link definitions at the bottom.
   Anything breaking goes under `Removed` or `Changed` in so many words — this is
   the only place a `v0.x` minor's breakage is documented.
2. **Bump the version in the two places that carry it.** `VERSION` in the
   [`Makefile`](../Makefile) and `version` / `appVersion` in
   [`Chart.yaml`](../deploy/charts/kuberecord/Chart.yaml). They must agree; a test
   and the release gate both check.
3. **Regenerate the committed manifest**: `make build-installer`. It pins the new
   image tag, and CI fails if it is stale.
4. **Rehearse** (below), then merge.
5. **Tag the merge commit and push it:**

   ```sh
   git tag -a v0.2.0 -m 'kuberecord v0.2.0'
   git push origin v0.2.0
   ```

   Tag the commit whose CI is green. The release workflow gates on the version and
   the notes, not on the test suites — those already ran on the commit, and
   re-running them at tag time would only tell you what you already knew, an hour
   later.
6. **Verify the published release before announcing it.** The workflow already
   verified its own signature and attestation, which is the check that must fail
   *before* the Release page exists; this is the same check from outside, as a
   stranger runs it, against what is now public:

   ```sh
   make release-verify RELEASE_VERSION=v0.2.0         # the image
   make release-chart-verify RELEASE_VERSION=v0.2.0   # the chart in the registry
   ```

   [`VERIFYING.md`](VERIFYING.md) is what you are pointing anyone who asks at, so
   it is worth having run its commands yourself once per release.
7. **Submit the krew manifest**, once the Release page exists:

   ```sh
   make krew-index-pr RELEASE_VERSION=v0.3.0
   ```

   This is the one publishing step the workflow does not do for you, and the
   reason is that it opens a pull request against a repository this project does
   not own. See below.

## The two distribution channels

`kubectl krew install kuberecord` and `brew install kuberecord/tap/kuberecord` are
the channels most people will use, and neither is hand-maintained. Both documents
are generated from the archives — a `sha256` somebody typed is wrong exactly once,
and it stays wrong until an install fails on a platform nobody here runs.

**Homebrew is automatic.** The `tap` job of the release workflow downloads the
published `kuberecord.rb` back off the Release page, checks it against the signed
`checksums.txt`, and pushes it to
[`kuberecord/homebrew-tap`](https://github.com/kuberecord/homebrew-tap). It
downloads rather than rebuilds on purpose: the formula people install from is then
a file the release signed, verifiable by anyone afterwards.

That job needs a token, because a workflow's own `GITHUB_TOKEN` is scoped to the
repository it runs in and cannot write to another. The repository secret
`HOMEBREW_TAP_TOKEN` holds a fine-grained personal access token with **Contents:
write on `kuberecord/homebrew-tap` and nothing else**. Without it the job fails
loudly, after the Release is already published — which is the right way round: the
release is not held hostage to the tap, and a tap that silently stopped updating
would be discovered by a user on an old version.

A **prerelease is never pushed to the tap.** `brew install` has no way to ask for a
stable version, so a formula pointing at `v0.3.0-rc.1` hands the candidate to
everybody. `make release-brew-push` refuses one by name and says so.

**krew is manual, once per release.** A tag push must not open a pull request
against somebody else's repository, so `make krew-index-pr` is a maintainer's
command rather than a workflow step. It downloads the published manifest, re-checks
every URI against the bytes GitHub is serving, forks
[kubernetes-sigs/krew-index](https://github.com/kubernetes-sigs/krew-index),
and opens the PR.

Two things follow from that, and both are worth knowing before you tag:

- **It cannot be run before the release is published.** krew-index's own CI fetches
  every `uri` in the manifest and checks its digest, so a PR opened against a tag
  whose assets do not exist yet fails on arrival.
- **Merging is external, and slow.** krew-index review latency is measured in
  weeks. It does not block the release, and it should not: the archives, the
  Homebrew tap and `go install` all work the moment the tag is published.

The generation and checking targets, all of which a rehearsal runs:

```sh
make release-krew-manifest RELEASE_VERSION=v0.3.0   # → dist/release/kuberecord.yaml
make release-brew-formula RELEASE_VERSION=v0.3.0    # → dist/release/kuberecord.rb
make release-krew-verify RELEASE_VERSION=v0.3.0     # digests, against the local archives
```

And the two that need something published:

```sh
make release-krew-verify-published RELEASE_VERSION=v0.3.0  # every URI, against what GitHub serves
make release-brew-fetch RELEASE_VERSION=v0.3.0             # the published formula, signature checked
```

## Rehearsing a release

`make release-dry-run` is the whole sequence with nothing published: the notes,
both install artifacts, the five CLI archives, the krew manifest and the Homebrew
formula, both SBOMs, their checksums, `verify-packaging` over each install path,
the full multi-arch image build with no registry to push to, and the chart push
against a throwaway registry.

Two of the supply-chain steps cannot be rehearsed, because performing them *is*
publishing: a signature writes to a registry and to a public transparency log,
and an attestation writes a record against the repository. The rehearsal goes as
far as it can — it builds a single-platform image locally so the SBOM scan runs
against a real image, it computes every attestation subject, and it prints the
signing and verification commands it is deliberately not running. What that
leaves unproven is stated in
[`VERIFYING.md`](VERIFYING.md#a-rehearsal-proves-less-than-a-release).

The CLI is not one of those two. A cross-compile publishes nothing, so a rehearsal
builds all five archives for real, asserts `CGO_ENABLED=0` against every binary in
them, and produces the CLI's SBOM from the binary it just built rather than from a
stand-in — that document needs nothing pushed, unlike the image's. Only the
signature over `checksums.txt` is printed instead of made.

The chart push is the exception, and it is genuinely exercised rather than
printed: `make release-chart-rehearse` stands up a throwaway OCI registry on this
machine, performs a real `helm push` of the real packaged chart into it, pushes
the Artifact Hub metadata artifact beside it with `oras`, checks that the digest
the chart push reports still parses, and destroys the registry again. A push to a
registry nobody can reach is not a publication, so a rehearsal is free to do it —
and it catches the failures that would otherwise wait for a tag: a malformed
reference, a flag that moved, an archive that was never packaged.

```sh
# Rehearse the committed version.
make release-dry-run

# Rehearse a release candidate. A prerelease with no section of its own uses the
# section of the version it is a candidate for.
make release-dry-run RELEASE_VERSION=v0.2.0-rc.1
```

Output lands in `dist/release/` (git-ignored). The same rehearsal runs in CI from
the workflow's `workflow_dispatch` trigger, which additionally attaches the
artifacts to the run so a candidate's `install.yaml` can be diffed against the
last release's.

The individual pieces are available on their own, which is what the workflow
calls:

```sh
make release-verify-version RELEASE_VERSION=v0.2.0   # the tag agrees with the tree
make release-notes RELEASE_VERSION=v0.2.0            # → dist/release/RELEASE_NOTES.md
make release-artifacts RELEASE_VERSION=v0.2.0        # install.yaml + chart + CLI archives + checksums
make build-cli                                       # cross-compile the CLI, both names, five platforms
make release-cli RELEASE_VERSION=v0.2.0              # → dist/release/kuberecord_v0.2.0_<os>_<arch>.*
make release-cli-sbom RELEASE_VERSION=v0.2.0         # the CLI binary, described by syft
make release-krew-manifest RELEASE_VERSION=v0.2.0    # → dist/release/kuberecord.yaml
make release-brew-formula RELEASE_VERSION=v0.2.0     # → dist/release/kuberecord.rb
make release-krew-verify RELEASE_VERSION=v0.2.0      # both documents, against the local archives
make release-image RELEASE_VERSION=v0.2.0 BUILDX_OUTPUT=   # build every platform, push nothing
make release-sbom-local RELEASE_VERSION=v0.2.0       # a local image, described by syft
make release-checksums RELEASE_VERSION=v0.2.0        # re-hash whatever is in dist/release/
make release-chart-rehearse RELEASE_VERSION=v0.2.0   # a real chart push, to a throwaway registry
make release-rehearse-publishing                     # print the steps a rehearsal must not run
```

Those that only make sense against something published, and that the workflow
runs for real, need cosign (and syft, and `gh` for the provenance half):

```sh
make release-image-digest RELEASE_VERSION=v0.2.0     # what the push actually produced
make release-sign RELEASE_VERSION=v0.2.0             # keyless, so it needs an OIDC identity
make release-verify RELEASE_VERSION=v0.2.0           # the commands VERIFYING.md publishes
make release-chart-login                             # helm *and* cosign; CHART_REGISTRY_USER/_TOKEN from the environment
make release-chart-push RELEASE_VERSION=v0.2.0       # the packaged chart, into the registry
make release-chart-sign RELEASE_VERSION=v0.2.0       # over the digest the push reported
make release-chart-verify RELEASE_VERSION=v0.2.0     # the chart command VERIFYING.md publishes
make release-chart-metadata-push                     # artifacthub-repo.yml, under the artifacthub.io tag
make release-artifacts-sign RELEASE_VERSION=v0.2.0   # sign-blob over checksums.txt, covering every asset
make release-artifacts-verify RELEASE_VERSION=v0.2.0 # the blob command VERIFYING.md publishes
make release-krew-verify-published RELEASE_VERSION=v0.2.0  # every krew/brew URI, as GitHub serves it
make release-brew-fetch RELEASE_VERSION=v0.2.0       # the published formula, signature checked
make release-brew-push RELEASE_VERSION=v0.2.0        # HOMEBREW_TAP_TOKEN from the environment
make krew-index-pr RELEASE_VERSION=v0.2.0            # the PR against kubernetes-sigs/krew-index
```

## What the gate refuses

The workflow's first job runs before any image is pushed, because publishing is
not undoable. It fails a release when:

- **The tag disagrees with the tree it points at** — `VERSION` in the Makefile, or
  the chart's `version`/`appVersion`. The tag decides what is published and
  `VERSION` decides what the artifacts pin; if they disagree, the release hands
  out manifests for a version nobody built. A prerelease tag is allowed to be
  `vX.Y.Z-rc.1` against `VERSION = X.Y.Z`, which is what a candidate *is*.
- **The tag has no changelog section.** `hack/changelog-section.sh` looks for
  `## [X.Y.Z]`; `## [Unreleased]` can never satisfy it. A release whose notes
  nobody wrote publishes an empty page to the one audience that has no other
  source of information about the project, and the tag is the last point at which
  that is cheap to fix.

Both checks are runnable before tagging — that is the point of them being make
targets rather than YAML — and `test/release` covers the extractor and this
wiring under `make test`.
