# Verifying a release

A project whose subject is tamper-evident audit has to be checkable itself.
Everything a tag publishes therefore carries evidence of where it came from: the
image is signed, the image and every attached asset carry build provenance, and
the image ships with an SBOM. This page is how to check each of those, and — in
the last section, which matters as much as the commands — what checking them does
and does not tell you.

One sentence before the details: **these signatures say that this exact release
workflow, in this repository, at this tag, produced these exact bytes.** They say
nothing about the records kuberecord writes once it is running; kuberecord signs
none of those, and [`RETENTION.md`](RETENTION.md#kuberecord-does-not-sign-anything)
is where that limit is spelled out.

- [What a release publishes, and what backs it](#what-a-release-publishes-and-what-backs-it)
- [Prerequisites](#prerequisites)
- [Which identity verifies which release](#which-identity-verifies-which-release)
- [The image signature](#the-image-signature)
- [The build provenance](#the-build-provenance)
- [The checksums](#the-checksums)
- [The SBOM](#the-sbom)
- [Pinning what you verified](#pinning-what-you-verified)
- [What this proves, and what it does not](#what-this-proves-and-what-it-does-not)
- [When verification fails](#when-verification-fails)

## What a release publishes, and what backs it

| Asset | Evidence |
|---|---|
| `ghcr.io/kuberecord/kuberecord:vX.Y.Z` | A keyless **cosign signature** over the manifest list *and* over each per-platform manifest, plus a **SLSA build provenance** attestation stored both against this repository and beside the image in the registry. |
| `install.yaml` | Its `sha256` in `checksums.txt`, and a **SLSA build provenance** attestation naming that digest. |
| `kuberecord-X.Y.Z.tgz` | The same. |
| `kuberecord-X.Y.Z-sbom.spdx.json` | The same, and it is itself the inventory of the image. |
| `checksums.txt` | The provenance attestation's subject list *is* this file's contents, so the two cannot disagree. |

Releases from **v0.2.0** onward carry all of it. `v0.1.0` predates this work and
has `checksums.txt` only — there is no signature to find, and its absence is not
a failure to investigate.

## Prerequisites

```sh
brew install cosign gh          # or see each project's install instructions
gh auth login                   # provenance is fetched through the GitHub API
```

- **cosign v3.0 or newer** (`cosign version`). The release is signed by cosign
  v3.1.3, which stores each signature under the conventional `sha256-<digest>`
  tag beside the image — so nothing here depends on your registry supporting the
  OCI referrers API. If verification fails on an older cosign, upgrade before
  drawing any other conclusion.
- **gh v2.49 or newer** for `gh attestation verify`, which enforces the
  `https://slsa.dev/provenance/v1` predicate type by default — the one these
  attestations carry.
- `sha256sum` (GNU) or `shasum` (macOS) for the checksums.

The commands below are also wired up as make targets, which is how CI runs them
against every release before the Release page exists — see
[`DEVELOPMENT.md`](DEVELOPMENT.md#releasing--make-release-dry-run).

## Which identity verifies which release

This repository moved from the personal account `yelzhy` to the `kuberecord`
organization after v0.2.0 was published. **Two identities are therefore correct,
and which one to pin depends on the tag you are verifying:**

| Tag | `--certificate-identity` / `--repo` | Image |
|---|---|---|
| `v0.1.0` | *none* — predates signing, `checksums.txt` only | `ghcr.io/yelzhy/kuberecord:v0.1.0` |
| `v0.2.0` | `https://github.com/yelzhy/kuberecord/.github/workflows/release.yml@refs/tags/v0.2.0` | `ghcr.io/yelzhy/kuberecord:v0.2.0` |
| `v0.2.1` and later | `https://github.com/kuberecord/kuberecord/.github/workflows/release.yml@refs/tags/vX.Y.Z` | `ghcr.io/kuberecord/kuberecord:vX.Y.Z` |

**The old identity is not a mistake, and it is not evidence of anything wrong
with those releases.** A keyless signature binds to the Fulcio certificate that
was issued to the release workflow at the moment it ran, and that certificate
names the repository as it was then. It is already in Rekor, the public
append-only transparency log — which is precisely the property that makes it
worth anything. A certificate in the log cannot be reissued, rewritten or
retargeted, so a release signed before the transfer verifies against the old
identity permanently, and would verify against the new one only if someone had
forged it. Pinning the new identity against `v0.2.0` is the command being wrong,
not the release.

The same split applies to the SLSA provenance: attestations for `v0.2.0` were
recorded against the `yelzhy/kuberecord` repository, so `--repo yelzhy/kuberecord`
and `--signer-workflow yelzhy/kuberecord/.github/workflows/release.yml` are what
verify them.

The commands in the rest of this page are written for the current identity. To
verify `v0.2.0`, substitute both halves — the identity *and* the image path, since
GHCR packages did not move with the repository:

```sh
cosign verify \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity https://github.com/yelzhy/kuberecord/.github/workflows/release.yml@refs/tags/v0.2.0 \
  ghcr.io/yelzhy/kuberecord:v0.2.0

gh attestation verify oci://ghcr.io/yelzhy/kuberecord:v0.2.0 \
  --repo yelzhy/kuberecord \
  --signer-workflow yelzhy/kuberecord/.github/workflows/release.yml
```

## The image signature

```sh
cosign verify \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity https://github.com/kuberecord/kuberecord/.github/workflows/release.yml@refs/tags/v0.2.1 \
  ghcr.io/kuberecord/kuberecord:v0.2.1
```

Both flags are load-bearing, and cosign refuses a keyless verification without
them for a good reason:

- **`--certificate-identity`** is *who signed*: a workflow file, in a repository,
  on a ref. Not "somebody at this project" — that exact workflow at that exact
  tag. Change the tag in the command when you change the tag in the image.
- **`--certificate-oidc-issuer`** is *who vouched for that identity*. GitHub's
  Actions OIDC provider is the only issuer that can mint it.

Drop either one and you are asking "is this signed by anyone at all?", which any
attacker who can obtain a Sigstore certificate can satisfy.

To accept any tag of this repository rather than one specific tag — for a policy
engine, say — pin the workflow and leave the ref open, anchored so a lookalike
repository name cannot match in the middle of the string:

```sh
cosign verify \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github\.com/kuberecord/kuberecord/\.github/workflows/release\.yml@refs/tags/' \
  ghcr.io/kuberecord/kuberecord:v0.2.1
```

cosign prints the checks it performed — that the claims match, that the
certificate chains to Fulcio, and that the signature is in the transparency log —
followed by the signed payload as JSON. The exact wording moves between cosign
versions; the exit status is what to script against.

The release is a manifest list, and it is signed `--recursive`: the index and
every platform manifest under it each carry a signature. So the digest your
cluster actually resolved verifies too, which is the one that matters if you pin
per architecture:

```sh
# The multi-arch index's own digest: the hash of its manifest bytes.
digest="sha256:$(docker buildx imagetools inspect --raw ghcr.io/kuberecord/kuberecord:v0.2.1 \
  | sha256sum | cut -d' ' -f1)"

cosign verify \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity https://github.com/kuberecord/kuberecord/.github/workflows/release.yml@refs/tags/v0.2.1 \
  "ghcr.io/kuberecord/kuberecord@$digest"
```

## The build provenance

Provenance answers a different question from the signature: not "who signed
this?" but "what built this, from which source, on which runner?". It is a
[SLSA](https://slsa.dev/) statement generated by GitHub, and `gh` verifies it:

```sh
gh attestation verify oci://ghcr.io/kuberecord/kuberecord:v0.2.1 \
  --repo kuberecord/kuberecord \
  --signer-workflow kuberecord/kuberecord/.github/workflows/release.yml
```

`--signer-workflow` is the provenance equivalent of `--certificate-identity`:
without it, an attestation produced by *any* workflow in the repository is
accepted, which is a weaker claim than the one you can have for free.

The same command verifies a file you downloaded from the Release page. What it
proves is that the bytes on your disk are the bytes that workflow produced:

```sh
gh attestation verify install.yaml \
  --repo kuberecord/kuberecord \
  --signer-workflow kuberecord/kuberecord/.github/workflows/release.yml

gh attestation verify kuberecord-0.2.1.tgz --repo kuberecord/kuberecord
gh attestation verify kuberecord-0.2.1-sbom.spdx.json --repo kuberecord/kuberecord
```

To verify somewhere without network access, fetch the bundles first and carry
them with the artifacts:

```sh
gh attestation download oci://ghcr.io/kuberecord/kuberecord:v0.2.1 --repo kuberecord/kuberecord
gh attestation verify oci://ghcr.io/kuberecord/kuberecord:v0.2.1 \
  --repo kuberecord/kuberecord --bundle <the downloaded .jsonl>
```

The image's attestation is also pushed into the registry next to the image, so
verification does not have to depend on this repository's API still answering.
That does not make it automatic across a mirror, and the distinction is worth
knowing before you rely on it: signatures and attestations live under their own
references, so a plain `docker pull && docker push` carries **neither**. Copy with
a tool that knows about them — `cosign copy`, `crane copy`, `oras cp` — or plan to
verify against the original registry.

## The checksums

```sh
# In the directory you downloaded the assets to.
sha256sum -c checksums.txt      # macOS: shasum -a 256 -c checksums.txt
```

`checksums.txt` covers `install.yaml`, the packaged chart and the SBOM. It is the
weakest of the three checks on its own — it proves the files agree with a list
that was published beside them, not who published either — and it is the fastest.
Its real value is that the provenance attestation for the artifacts is generated
*from this exact file*, so the set of things checksummed and the set of things
attested cannot drift apart.

## The SBOM

`kuberecord-X.Y.Z-sbom.spdx.json` is an **SPDX 2.3** document, produced by
[syft](https://github.com/anchore/syft) from the **published `linux/amd64`
image, by digest** — not from a source tree, and not from a build that only
resembles the release.

One document covers the release because there is only one inventory to report:
every platform in the manifest list is the same statically linked binary built
from the same `go.mod` into the same `gcr.io/distroless/static` base, differing
only in the architecture it was compiled for. Four near-identical documents would
imply a difference that is not there.

Read it, or hand it to a scanner (`grype` is a separate install):

```sh
syft convert kuberecord-0.2.1-sbom.spdx.json -o table
grype sbom:./kuberecord-0.2.1-sbom.spdx.json
```

## Pinning what you verified

Verifying a tag and then deploying that tag leaves a gap: a tag is a mutable
pointer. Resolve it once, verify the digest, and deploy the digest.

```sh
digest="sha256:$(docker buildx imagetools inspect --raw ghcr.io/kuberecord/kuberecord:v0.2.1 \
  | sha256sum | cut -d' ' -f1)"
```

Hashing the raw manifest is deliberate rather than baroque: a digest *is* the hash
of those bytes, and `--format '{{.Manifest.Digest}}'` is unreliable — on Docker
Desktop's buildx v0.22 a template naming `.Manifest` is ignored and the
human-readable listing is printed instead, successfully. On macOS use
`shasum -a 256`; with [crane](https://github.com/google/go-containerregistry)
installed, `crane digest <ref>` is the same answer in one word.

The Helm chart takes `image.digest` for exactly this; `install.yaml` pins the tag,
so edit it if you need digest pinning through that path.
[`RELEASING.md`](RELEASING.md#what-a-release-publishes) explains why there is no
floating `latest` to begin with.

## What this proves, and what it does not

### It proves nothing about the records kuberecord writes

This is the most important line on the page, because the two claims sound alike
and are unrelated. A verified image means *the operator you are about to run is
the one this project built*. It says nothing about the rows in ClickHouse or the
objects in S3 that the operator goes on to write: **kuberecord does not sign
anything it writes**, and the SHA-256 in an S3 object key is the writer's own
digest of its own payload — corruption detection, not provenance. That limit, and
what Object Lock does and does not add on top of it, is
[`RETENTION.md`](RETENTION.md#kuberecord-does-not-sign-anything).

### Provenance is about the build, not about the code

An attestation says: this image was built by this workflow, from this commit, on
a GitHub-hosted runner. It does not say the code is correct, that the review was
thorough, or that no dependency in it has a CVE. Likewise an SBOM is an
inventory, not a clean bill of health — `grype` reading it is the step that
produces a security opinion, and that opinion is about the day you ran it.

### Keyless means trusting two services instead of one key

There is no long-lived signing key here, so there is no key to steal, rotate or
lose — and no key for you to pin instead. What you trust instead is GitHub's OIDC
provider (that the identity in the certificate is real) and Sigstore's public
Fulcio and Rekor instances (that the certificate was issued and the signature
logged). The transparency log is the point: it is public and append-only, so a
signature that was made cannot later be made to have not existed.

The flip side is that every signature is a **public** statement. The Rekor entry
naming this repository, this workflow and this digest is world-readable and
permanent.

### A rehearsal proves less than a release

The `workflow_dispatch` rehearsal path runs the whole release with nothing
published, which necessarily means it signs nothing and attests nothing — those
are publications. A green rehearsal proves the steps are wired up, the SBOM scan
finds a real image, and the subject list is complete. It cannot prove a signature
verifies. Only the tag does that, which is why the release job verifies its own
signature before the Release page that advertises it exists.

## When verification fails

A failure is a reason to stop, not a reason to reach for
`--insecure-ignore-tlog`. What each one usually means:

| Symptom | Usual cause |
|---|---|
| `no matching CertificateIdentity found … expected SAN value X, got Y` | The identity in the command is not the one that signed. Usefully, cosign prints the value it *did* find as `got` — that string is what to pin, and most often the difference is the tag at the end of it. |
| `no matching CertificateIdentity found` on `v0.2.0`, where `got` names `yelzhy/kuberecord` | Expected. That release was signed before the repository moved to the `kuberecord` organization, and its certificate is immutable — see [Which identity verifies which release](#which-identity-verifies-which-release). Pin the old identity for that tag. |
| `no signatures found` | The image predates v0.2.0, or it was mirrored by a tool that copies manifests without their signatures. |
| `gh attestation verify` finds no attestation | The same two causes, plus: `--repo` names the repository the attestation was recorded against, which is not necessarily the registry namespace. |
| A checksum mismatch | Re-download before concluding anything; a truncated download is far more likely than a tampered asset. Then verify the provenance, which does not depend on `checksums.txt` being honest. |
| Verification of a *tag* passes but a digest fails | You are verifying a different image than you resolved. Resolve the digest once and use it in both places. |

If a signature that should exist does not verify, that is worth reporting as a
security issue rather than working around.
