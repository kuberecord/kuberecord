# Security Policy

kuberecord records what a cluster looked like, for people who later need that
record to be trustworthy. Reports that affect the integrity, confidentiality or
availability of that record are taken seriously, and so are reports about the
operator's own footprint in a cluster.

## Supported versions

kuberecord is pre-1.0, so **only the latest minor release line is serviced**.
Fixes land on `main` and ship in the next patch or minor release; there are no
backports to older minors.

| Version | Supported |
|---|---|
| `main` (unreleased) | ✅ Fixes land here first |
| `0.2.x` (latest release line) | ✅ Supported |
| `0.1.x` | ❌ Upgrade — see [`docs/UPGRADING.md`](docs/UPGRADING.md) |

When a new minor is released, the line it replaces stops being supported at that
moment. [`docs/RELEASING.md`](docs/RELEASING.md#versioning-policy) is the full
versioning policy.

## Reporting a vulnerability

**Please do not open a public issue, discussion or pull request for a security
vulnerability.** Report it privately, by either route:

1. **Preferred — a private GitHub security advisory.** Go to the repository's
   [Security tab](https://github.com/kuberecord/kuberecord/security/advisories/new)
   and choose **Report a vulnerability**. This keeps the report, the discussion and
   the eventual fix in one private place, and it is the fastest route to a
   coordinated release.
2. **Email** — <zhyrova.yelyzaveta@gmail.com>, the same address the
   [Code of Conduct](CODE_OF_CONDUCT.md) names. Use this if you cannot open a draft
   advisory, or if you would rather not use GitHub.

Please include, as far as you can establish it:

- the affected version, image digest or commit;
- the component — operator, CRD validation, RBAC, a sink backend, the Helm chart
  or `dist/install.yaml`;
- what an attacker gains, and what access they need to start;
- reproduction steps, or a minimal manifest and the resulting behaviour.

## What to expect

- **Acknowledgement within 3 business days.** If you have not heard back, please
  resend — a lost mail is far more likely than a deliberate silence.
- **An assessment within 7 days**, saying whether the report is accepted, and if
  so what the fix and timeline look like.
- **Coordinated disclosure.** The advisory is published together with the release
  that fixes it, and it names the reporter unless you ask otherwise. If a fix will
  take longer than 90 days, we will say so and agree a date with you rather than
  let the report go quiet.

## Scope

**In scope** — the operator and everything a release publishes: the Go code under
`api/`, `cmd/`, `internal/` and `test/`, the CRDs and their CEL validation, the
RBAC the project ships, the Helm chart, `dist/install.yaml`, and the released
container images. Anything that lets an operator install escalate beyond the
permissions it was granted, that leaks credentials or redacted values into logs or
a sink, or that lets a record be written that misrepresents what the cluster
actually contained, is squarely in scope.

**Not in scope**, and better raised as a normal issue:

- **The evaluation shortcuts in [`examples/quickstart/`](examples/quickstart/)** —
  the committed password and the `emptyDir` storage are documented as shortcuts for
  a throwaway kind cluster, not as a production posture.
- **Configuration a user controls** — an object store or a ClickHouse instance left
  open to the world, over-broad grants added to the aggregated ClusterRole, or
  credentials committed to your own repository.
- **Dependency advisories with no reachable path** in kuberecord. If you *can*
  reach it, that is a finding — please report it with the path.
- Missing hardening headers, or scanner output with no demonstrated impact.

## Verifying what you run

Every release from v0.2.0 onward is signed and carries build provenance and an
SBOM, so you can check that the bytes you are about to run came from this
repository's release workflow. The commands, the identity to pin, and the honest
limits of what a signature proves are in
[`docs/VERIFYING.md`](docs/VERIFYING.md#what-this-proves-and-what-it-does-not).

One limit is worth repeating here, because it is the one most easily assumed away:
**kuberecord signs its own releases, not the records it writes.** A verified
operator is not a verified audit trail —
[`docs/RETENTION.md`](docs/RETENTION.md#kuberecord-does-not-sign-anything) spells
out what tamper-evidence the archive does and does not give you.
