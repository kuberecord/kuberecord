# Releasing kuberecord

Strangers install tags, not `main`. This page is the policy for what a tag
promises, plus the mechanics of cutting one.

- [Versioning policy](#versioning-policy)
- [What a release publishes](#what-a-release-publishes)
- [Cutting a release](#cutting-a-release)
- [Rehearsing a release](#rehearsing-a-release)
- [What the gate refuses](#what-the-gate-refuses)

## Versioning policy

kuberecord carries **four version numbers, and they are deliberately
independent**. Conflating them is the mistake this section exists to prevent: an
operator upgrade is not a schema migration, and a schema that is frozen does not
mean an operator that cannot change. The fourth is new in v0.2.0 — the S3 object
format is its own contract on its own timeline (D15), because a storage format
that outlives the operator by years cannot be versioned with it.

| What | Version | What it promises |
|---|---|---|
| **The operator** (image, chart, `install.yaml`) | `v0.x.y` — semver, **pre-1.0** | A **minor bump may break**: flags, defaults, RBAC and behaviour are all fair game while the leading digit is `0`. Every break is spelled out in [`CHANGELOG.md`](../CHANGELOG.md). A **patch** bump is fixes only — no new flags, no new permissions, no behaviour change beyond the bug named in the notes. |
| **The CRDs** | `kuberecord.io/v1alpha1` | Alpha in the Kubernetes sense, and the honest reading of it: a field may be removed, renamed or re-defaulted in an operator minor. There are **no conversion webhooks** (D4), so there is exactly one served and stored version and an incompatible change is a manual edit of your custom resources, guided by the changelog. |
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
| `ghcr.io/yelzhy/kuberecord:vX.Y.Z` | Multi-arch (`linux/amd64`, `linux/arm64`, `linux/s390x`, `linux/ppc64le`), built by `make release-image`, which is the repository's existing buildx target. |
| `install.yaml` | `kubectl apply -f` it: CRDs, RBAC and the manager, with the image above pinned exactly. For a non-prerelease tag it is byte-identical to the committed [`dist/install.yaml`](../dist/install.yaml) — the artifact you download is the file that was reviewed. |
| `kuberecord-X.Y.Z.tgz` | The packaged Helm chart, `--version X.Y.Z --app-version vX.Y.Z`. |
| `checksums.txt` | `sha256` over both artifacts. Verify with `sha256sum -c checksums.txt` in the directory you downloaded them to. |
| The Release body | That version's section of [`CHANGELOG.md`](../CHANGELOG.md), extracted verbatim by `hack/changelog-section.sh`. The changelog *is* the release notes. |

**There is no floating `latest` tag**, for images or for artifacts. What a cluster
runs is decided by the tag somebody chose, not by whatever moved last. To pin
harder than a tag, resolve the digest once and use it — the chart takes
`image.digest`:

```sh
docker buildx imagetools inspect ghcr.io/yelzhy/kuberecord:v0.1.0 --format '{{.Manifest.Digest}}'
```

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

## Rehearsing a release

`make release-dry-run` is the whole sequence with nothing published: the notes,
both install artifacts, their checksums, `verify-packaging` over each install
path, and the full multi-arch image build with no registry to push to.

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
make release-artifacts RELEASE_VERSION=v0.2.0        # install.yaml + chart + checksums
make release-image RELEASE_VERSION=v0.2.0 BUILDX_OUTPUT=   # build every platform, push nothing
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
